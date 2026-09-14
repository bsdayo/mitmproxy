import asyncio
import json
import logging

import aiomqtt
import httpx
from mitmproxy import ctx, exceptions, http

from .stats import HOST, discovery, fetch

logger = logging.getLogger(__name__)


class Sacp:
    def __init__(self):
        self.access_token: str | None = None
        self.building_id: int | None = None
        self.task: asyncio.Task | None = None
        self._running = False

    def load(self, loader):
        for name, kind, default, help_text in (
            (
                "mqtt_host",
                str,
                "",
                "MQTT broker hostname or IP; omit to disable Home Assistant.",
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
        ):
            loader.add_option(f"sacp_ha_{name}", kind, default, help_text)

    def configure(self, updated):
        if ctx.options.sacp_ha_mqtt_host:
            if not 1 <= ctx.options.sacp_ha_mqtt_port <= 65535:
                raise exceptions.OptionsError(
                    "sacp_ha_mqtt_port must be between 1 and 65535"
                )
            if ctx.options.sacp_ha_interval < 1:
                raise exceptions.OptionsError(
                    "sacp_ha_interval must be at least 1 second"
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
        if ctx.options.sacp_ha_mqtt_host:
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

    async def run(self):
        async with httpx.AsyncClient(trust_env=False) as client:
            client.headers.clear()
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
                            tasks.create_task(self.publish_loop(client, mqtt))
                            tasks.create_task(self.watch_connection(mqtt))
                except* Exception:
                    logger.error("MQTT worker failed; retrying in 5 seconds")
                await asyncio.sleep(5)

    async def watch_connection(self, mqtt):
        # Iterating also detects disconnection while the publisher is sleeping.
        async for _ in mqtt.messages:
            pass

    async def publish_loop(self, client, mqtt):
        while True:
            token, building_id = self.access_token, self.building_id
            if token is None or building_id is None:
                logger.error("Waiting to capture X-Access-Token and buildingId")
            else:
                try:
                    async with asyncio.timeout(30):
                        values = await fetch(client, token, building_id)
                        config = discovery(building_id, ctx.options.sacp_ha_mqtt_prefix)
                        await mqtt.publish(
                            f"{ctx.options.sacp_ha_discovery_prefix}/device/sacp_{building_id}/config",
                            payload=json.dumps(config),
                            qos=1,
                            retain=True,
                        )
                        await mqtt.publish(
                            config["state_topic"],
                            payload=json.dumps(values),
                            qos=1,
                        )
                    logger.info("Published room statistics: buildingId=%s", building_id)
                except aiomqtt.MqttError:
                    raise
                except Exception as error:
                    logger.error("State update failed (%s)", type(error).__name__)
            await asyncio.sleep(ctx.options.sacp_ha_interval)
