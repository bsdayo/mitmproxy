import asyncio
import json
import logging

import aiomqtt
import httpx
from mitmproxy import addonmanager, ctx, exceptions, http

from .history import HistoryWriter, websocket_url
from .stats import HOST, Snapshot, discovery, fetch

logger = logging.getLogger(__name__)


class Sacp:
    def __init__(self):
        self.access_token: str | None = None
        self.building_id: int | None = None
        self._credentials_changed = asyncio.Event()
        self._state_changed = asyncio.Event()
        self._snapshot: Snapshot | None = None
        self.task: asyncio.Task | None = None
        self._running = False

    def load(self, loader: addonmanager.Loader):
        for name, kind, default, help_text in (
            (
                "mqtt_host",
                str,
                "",
                "MQTT broker hostname or IP; omit to disable MQTT display.",
            ),
            ("mqtt_port", int, 1883, "MQTT broker TCP port."),
            ("mqtt_username", str, "", "MQTT username; omit for anonymous access."),
            ("mqtt_password", str, "", "MQTT password; requires a username."),
            ("mqtt_client_id", str, "sacp", "MQTT client ID."),
            ("mqtt_prefix", str, "sacp", "Room state topic prefix."),
            (
                "discovery_prefix",
                str,
                "homeassistant",
                "Home Assistant discovery prefix.",
            ),
            ("interval", int, 3600, "Query and publish interval in seconds."),
            ("url", str, "", "Home Assistant base URL; omit to disable history sync."),
            ("token", str, "", "Home Assistant administrator long-lived access token."),
        ):
            loader.add_option(f"sacp_ha_{name}", kind, default, help_text)

    def configure(self, updated: set[str]):
        if bool(ctx.options.sacp_ha_url) != bool(ctx.options.sacp_ha_token):
            raise exceptions.OptionsError(
                "sacp_ha_url and sacp_ha_token must be set together"
            )
        if ctx.options.sacp_ha_url:
            try:
                websocket_url(ctx.options.sacp_ha_url)
            except ValueError as error:
                raise exceptions.OptionsError(str(error)) from error
        if ctx.options.sacp_ha_interval < 1:
            raise exceptions.OptionsError("sacp_ha_interval must be at least 1 second")
        if ctx.options.sacp_ha_mqtt_host:
            if not 1 <= ctx.options.sacp_ha_mqtt_port <= 65535:
                raise exceptions.OptionsError(
                    "sacp_ha_mqtt_port must be between 1 and 65535"
                )
            if (
                ctx.options.sacp_ha_mqtt_password
                and not ctx.options.sacp_ha_mqtt_username
            ):
                raise exceptions.OptionsError(
                    "sacp_ha_mqtt_password requires sacp_ha_mqtt_username"
                )
        if self._running and any(name.startswith("sacp_ha_") for name in updated):
            self.done()
            self.running()

    def running(self):
        self._running = True
        if ctx.options.sacp_ha_mqtt_host or ctx.options.sacp_ha_url:
            self.task = asyncio.create_task(self.run())

    def done(self):
        self._running = False
        if self.task:
            self.task.cancel()
            self.task = None

    def requestheaders(self, flow: http.HTTPFlow):
        request = flow.request
        port = 443 if request.scheme == "https" else 80
        if (request.host_header or "").lower() not in (
            HOST,
            f"{HOST}:{port}",
        ):
            return
        previous_credentials = self.access_token, self.building_id
        if "X-Access-Token" in request.headers:
            self.access_token = request.headers["X-Access-Token"]
            logger.info("Captured X-Access-Token")
        if "buildingId" in request.query:
            try:
                building_id = int(request.query["buildingId"])
                if building_id < 0:
                    raise ValueError
            except ValueError:
                logger.error("Invalid buildingId in captured request")
            else:
                self.building_id = building_id
                logger.info("Captured buildingId=%s", building_id)
        if (
            self.access_token is not None
            and self.building_id is not None
            and (self.access_token, self.building_id) != previous_credentials
        ):
            self._credentials_changed.set()

    async def run(self):
        history = (
            HistoryWriter(ctx.options.sacp_ha_url, ctx.options.sacp_ha_token)
            if ctx.options.sacp_ha_url
            else None
        )
        async with httpx.AsyncClient(trust_env=False) as client:
            client.headers.clear()
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self.query_loop(client, history))
                if ctx.options.sacp_ha_mqtt_host:
                    tasks.create_task(self.mqtt_loop())
                if history:
                    tasks.create_task(history.run())

    async def mqtt_loop(self):
        while True:
            try:
                async with aiomqtt.Client(
                    ctx.options.sacp_ha_mqtt_host,
                    port=ctx.options.sacp_ha_mqtt_port,
                    username=ctx.options.sacp_ha_mqtt_username or None,
                    password=ctx.options.sacp_ha_mqtt_password or None,
                    identifier=ctx.options.sacp_ha_mqtt_client_id,
                    keepalive=30,
                ) as mqtt:
                    logger.info("MQTT connected")
                    async with asyncio.TaskGroup() as tasks:
                        tasks.create_task(self.publish_loop(mqtt))
                        tasks.create_task(self.watch_connection(mqtt))
            except* Exception:
                logger.error("MQTT worker failed; retrying in 5 seconds")
            await asyncio.sleep(5)

    async def watch_connection(self, mqtt: aiomqtt.Client):
        # Iterating also detects disconnection while the publisher is sleeping.
        async for _ in mqtt.messages:
            pass
        raise ConnectionError("MQTT message stream closed")

    async def query_loop(
        self, client: httpx.AsyncClient, history: HistoryWriter | None
    ):
        while True:
            self._credentials_changed.clear()
            token, building_id = self.access_token, self.building_id
            if token is None or building_id is None:
                logger.error("Waiting to capture X-Access-Token and buildingId")
            else:
                try:
                    async with asyncio.timeout(30):
                        snapshot = await fetch(client, token, building_id)
                    if (token, building_id) != (self.access_token, self.building_id):
                        # A room/account switch during the request must not publish
                        # the old account's response as the current snapshot.
                        continue
                    self._snapshot = snapshot
                    self._state_changed.set()
                    if history:
                        history.submit(snapshot.readings)
                except Exception as error:
                    logger.error("State update failed (%s)", type(error).__name__)
            try:
                await asyncio.wait_for(
                    self._credentials_changed.wait(),
                    timeout=ctx.options.sacp_ha_interval,
                )
            except TimeoutError:
                pass

    async def publish_loop(self, mqtt: aiomqtt.Client):
        while True:
            self._state_changed.clear()
            snapshot = self._snapshot
            if snapshot:
                building_id = snapshot.building_id
                config = discovery(building_id, ctx.options.sacp_ha_mqtt_prefix)
                async with asyncio.timeout(30):
                    await mqtt.publish(
                        f"{ctx.options.sacp_ha_discovery_prefix}/device/sacp_{building_id}/config",
                        payload=json.dumps(config),
                        qos=1,
                        retain=True,
                    )
                    await mqtt.publish(
                        config["state_topic"],
                        payload=json.dumps(snapshot.values),
                        qos=1,
                    )
                logger.info("Published room statistics: buildingId=%s", building_id)
            await self._state_changed.wait()
