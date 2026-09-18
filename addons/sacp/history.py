"""Daily meter statistics stored exclusively in Home Assistant."""

import asyncio
import json
import logging
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import groupby
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from websockets.asyncio.client import ClientConnection, connect

from .stats import TIMEZONE, Reading

logger = logging.getLogger(__name__)
RETRY_SECONDS = 30
MAX_PENDING = 1024
DAY_MS = 86_400_000


def websocket_url(url: str) -> str:
    parts = urlsplit(url)
    if (
        parts.scheme not in {"http", "https", "ws", "wss"}
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise ValueError("HA URL must be an HTTP(S) or WS(S) URL without credentials")
    # Accessing port also rejects malformed port numbers during configuration.
    _ = parts.port
    scheme = {"http": "ws", "https": "wss"}.get(parts.scheme, parts.scheme)
    path = parts.path.rstrip("/")
    if not path.endswith("/api/websocket"):
        path += "/api/websocket"
    return urlunsplit((scheme, parts.netloc, path, "", ""))


def slot(reading: Reading) -> datetime:
    # HA stores hourly buckets. Daily consumption belongs to the previous
    # local day's final bucket, not to the midnight at the start of today.
    return (reading.read_at - timedelta(hours=1)).astimezone(UTC)


def plan_statistics(
    readings: list[Reading], existing: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Append daily deltas or correct the latest reading; never guess gap usage."""
    rows = {row["start"]: row for row in existing}
    result = []
    for reading in sorted(readings, key=lambda item: item.read_at):
        start = slot(reading)
        timestamp = int(start.timestamp() * 1000)
        if rows and timestamp < max(rows):
            logger.warning("Ignoring stale reading for %s", reading.statistic_id)
            continue
        current = rows.get(timestamp)
        if current is not None and current["state"] == reading.value:
            continue
        previous_times = [time for time in rows if time < timestamp]
        total = 0
        if previous_times:
            previous_time = max(previous_times)
            previous = rows[previous_time]
            for key in ("state", "sum"):
                value = previous.get(key)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                ):
                    raise ValueError("HA returned invalid cumulative statistics")
            total = previous["sum"]
            if timestamp - previous_time != DAY_MS:
                logger.warning(
                    "Missing daily readings for %s; preserving sum and rebasing. "
                    "Consumption across this gap is excluded from statistics.",
                    reading.statistic_id,
                )
            elif reading.value < previous["state"]:
                logger.warning(
                    "Meter decreased for %s; preserving sum and rebasing",
                    reading.statistic_id,
                )
            else:
                total = float(
                    Decimal(str(total))
                    + Decimal(str(reading.value))
                    - Decimal(str(previous["state"]))
                )
        row = {"start": start.isoformat(), "state": reading.value, "sum": total}
        result.append(row)
        rows[timestamp] = row | {"start": timestamp}
    return result


class HAConnection:
    def __init__(self, socket: ClientConnection):
        self.socket = socket
        self.sequence = 0

    async def authenticate(self, token: str):
        async with asyncio.timeout(30):
            if json.loads(await self.socket.recv()).get("type") != "auth_required":
                raise ValueError("Unexpected HA authentication handshake")
            await self.socket.send(json.dumps({"type": "auth", "access_token": token}))
            if json.loads(await self.socket.recv()).get("type") != "auth_ok":
                raise ValueError("HA authentication failed")

    async def command(self, command: str, **payload: Any) -> Any:
        self.sequence += 1
        async with asyncio.timeout(30):
            await self.socket.send(
                json.dumps({"id": self.sequence, "type": command, **payload})
            )
            response = json.loads(await self.socket.recv())
        if (
            response.get("id") != self.sequence
            or response.get("type") != "result"
            or response.get("success") is not True
        ):
            # Never log arbitrary server responses or the authentication payload.
            raise ValueError(f"HA command failed: {command}")
        return response.get("result")

    async def statistics(
        self, ids: list[str], **period: Any
    ) -> dict[str, list[dict[str, Any]]]:
        return await self.command(
            "recorder/statistics_during_period",
            statistic_ids=ids,
            period="hour",
            types=["state", "sum"],
            units={"energy": "kWh", "volume": "m³"},
            **period,
        )

    async def import_statistics(self, reading: Reading, rows: list[dict[str, Any]]):
        electricity = reading.kind == "electricity"
        await self.command(
            "recorder/import_statistics",
            metadata={
                "statistic_id": reading.statistic_id,
                "source": "sacp",
                "name": (
                    f"SACP {reading.building_id} "
                    f"{reading.kind.replace('_', ' ').title()} (Daily) "
                    f"[{reading.meter_id}]"
                ),
                "unit_of_measurement": "kWh" if electricity else "m³",
                "unit_class": "energy" if electricity else "volume",
                "mean_type": 0,
                "has_sum": True,
            },
            stats=rows,
        )
        # Import success means queued, not committed. Read back before dropping
        # pending readings, so reconnecting cannot double-count or lose progress.
        expected = {
            int(datetime.fromisoformat(row["start"]).timestamp() * 1000): row
            for row in rows
        }
        async with asyncio.timeout(30):
            while True:
                stored = await self.statistics(
                    [reading.statistic_id],
                    start_time=rows[0]["start"],
                    end_time=(
                        datetime.fromisoformat(rows[-1]["start"]) + timedelta(hours=1)
                    ).isoformat(),
                )
                actual = {
                    row["start"]: row for row in stored.get(reading.statistic_id, [])
                }
                if all(
                    time in actual
                    and all(
                        actual[time].get(key) is not None
                        and math.isclose(actual[time][key], row[key], abs_tol=1e-8)
                        for key in ("state", "sum")
                    )
                    for time, row in expected.items()
                ):
                    return
                await asyncio.sleep(0.25)


class HistoryWriter:
    def __init__(self, url: str, token: str):
        self.url = websocket_url(url)
        self.token = token
        self.pending: dict[tuple[str, datetime], Reading] = {}
        self.changed = asyncio.Event()

    def submit(self, readings: tuple[Reading, ...]):
        for reading in readings:
            local_time = reading.read_at.astimezone(TIMEZONE)
            if (
                local_time.time() != datetime.min.time()
                or local_time > datetime.now(TIMEZONE)
                or reading.value < 0
            ):
                logger.warning(
                    "Skipping invalid daily reading for %s", reading.statistic_id
                )
                continue
            key = reading.statistic_id, reading.read_at
            self.pending[key] = reading
            while len(self.pending) > MAX_PENDING:
                del self.pending[next(iter(self.pending))]
                logger.warning("HA retry buffer full; discarded oldest pending reading")
        if self.pending:
            self.changed.set()

    async def sync(self):
        batch = sorted(
            self.pending.values(), key=lambda item: (item.statistic_id, item.read_at)
        )
        if not batch:
            return
        async with connect(
            self.url,
            proxy=None,
            open_timeout=15,
            close_timeout=5,
            max_size=16 * 1024**2,
        ) as socket:
            ha = HAConnection(socket)
            await ha.authenticate(self.token)
            ids = sorted({reading.statistic_id for reading in batch})
            # Daily rows are sparse. Reading the full series also recovers an old
            # baseline after a prolonged outage without any local checkpoint.
            existing = await ha.statistics(ids, start_time="1970-01-01T00:00:00+00:00")
            for statistic_id, group in groupby(
                batch, key=lambda item: item.statistic_id
            ):
                readings = list(group)
                rows = plan_statistics(readings, existing.get(statistic_id, []))
                if rows:
                    await ha.import_statistics(readings[-1], rows)
                    logger.info(
                        "Backfilled %s: %s daily readings", statistic_id, len(rows)
                    )
                for reading in readings:
                    key = reading.statistic_id, reading.read_at
                    if self.pending.get(key) == reading:
                        del self.pending[key]

    async def run(self):
        while True:
            await self.changed.wait()
            self.changed.clear()
            try:
                await self.sync()
            except Exception as error:
                logger.error(
                    "HA history sync failed (%s); retrying in %s seconds",
                    type(error).__name__,
                    RETRY_SECONDS,
                )
                await asyncio.sleep(RETRY_SECONDS)
                self.changed.set()
