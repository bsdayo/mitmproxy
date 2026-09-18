"""Daily meter statistics stored exclusively in Home Assistant."""

import asyncio
import json
import logging
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from itertools import groupby
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from websockets.asyncio.client import ClientConnection, connect

from .stats import TIMEZONE, Reading

logger = logging.getLogger(__name__)
RETRY_SECONDS = 30
MAX_PENDING = 1024
DAY_MS = 86_400_000


def parse_price(value: str) -> Decimal | None:
    if not value.strip():
        return None
    try:
        price = Decimal(value)
    except InvalidOperation as error:
        raise ValueError("price must be a finite non-negative number") from error
    if not price.is_finite() or price < 0:
        raise ValueError("price must be a finite non-negative number")
    return price


def plan_cost_statistics(
    usage: list[dict[str, Any]], price: Decimal
) -> list[dict[str, Any]]:
    """Convert the current usage batch to costs at a fixed price."""
    result = []
    for row in usage:
        amount = float(Decimal(str(row["sum"])) * price)
        if not math.isfinite(amount):
            raise ValueError("calculated cost must be finite")
        result.append(
            {
                "start": row["start"],
                # Both fields are monetary amounts; never store meter readings
                # in a statistic whose unit is CNY.
                "state": amount,
                "sum": amount,
            }
        )
    return result


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
    """Require time-ordered readings; correct the latest row, exclude gap usage."""
    rows = {row["start"]: row for row in existing}
    result = []
    for reading in readings:
        start = slot(reading)
        timestamp = int(start.timestamp() * 1000)
        if rows and timestamp < max(rows):
            logger.warning("Ignoring stale reading for %s", reading.statistic_id)
            continue
        current = rows.get(timestamp)
        if current is not None and current["state"] == reading.value:
            continue
        previous_time = max((time for time in rows if time < timestamp), default=None)
        total = 0
        if previous_time is not None:
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
    """Sequential HA commands with confirmation that imported rows are stored."""

    def __init__(self, socket: ClientConnection) -> None:
        self.socket = socket
        self.sequence = 0

    async def authenticate(self, token: str) -> None:
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
        self, statistic_ids: list[str], **period: Any
    ) -> dict[str, list[dict[str, Any]]]:
        return await self.command(
            "recorder/statistics_during_period",
            statistic_ids=statistic_ids,
            period="hour",
            types=["state", "sum"],
            units={"energy": "kWh", "volume": "m³"},
            **period,
        )

    async def import_statistics(
        self, reading: Reading, rows: list[dict[str, Any]], *, cost: bool = False
    ) -> None:
        statistic_id = reading.statistic_id
        if cost:
            statistic_id = (
                f"sacp:{reading.building_id}_{reading.kind}_cost_{reading.meter_id}"
            )
            unit, unit_class = "CNY", None
        elif reading.kind == "electricity":
            unit, unit_class = "kWh", "energy"
        else:
            unit, unit_class = "m³", "volume"
        await self.command(
            "recorder/import_statistics",
            metadata={
                "statistic_id": statistic_id,
                "source": "sacp",
                "name": (
                    f"SACP {reading.building_id} "
                    f"{reading.kind.replace('_', ' ').title()}"
                    f"{' Cost' if cost else ''} (Daily) "
                    f"[{reading.meter_id}]"
                ),
                "unit_of_measurement": unit,
                "unit_class": unit_class,
                "mean_type": 0,
                "has_sum": True,
            },
            stats=rows,
        )
        await self._wait_for_import(statistic_id, rows)

    async def _wait_for_import(
        self, statistic_id: str, rows: list[dict[str, Any]]
    ) -> None:
        # Import success means queued, not committed. Read back before dropping
        # pending readings, so reconnecting cannot double-count or lose progress.
        expected = {
            int(datetime.fromisoformat(row["start"]).timestamp() * 1000): row
            for row in rows
        }
        end_time = (
            datetime.fromisoformat(rows[-1]["start"]) + timedelta(hours=1)
        ).isoformat()
        async with asyncio.timeout(30):
            while True:
                stored = await self.statistics(
                    [statistic_id],
                    start_time=rows[0]["start"],
                    end_time=end_time,
                )
                actual = {row["start"]: row for row in stored.get(statistic_id, [])}
                if all(
                    timestamp in actual
                    and actual[timestamp].get(key) is not None
                    and math.isclose(
                        actual[timestamp][key], row[key], rel_tol=0, abs_tol=1e-8
                    )
                    for timestamp, row in expected.items()
                    for key in ("state", "sum")
                ):
                    return
                await asyncio.sleep(0.25)


class HistoryWriter:
    """Buffer readings in memory and resume from HA's stored statistics."""

    def __init__(
        self, url: str, token: str, prices: dict[str, Decimal] | None = None
    ) -> None:
        self.url = websocket_url(url)
        self.token = token
        self.prices = prices or {}
        self.pending: dict[tuple[str, datetime], Reading] = {}
        self.changed = asyncio.Event()

    def submit(self, readings: tuple[Reading, ...]) -> None:
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
            if len(self.pending) > MAX_PENDING:
                del self.pending[next(iter(self.pending))]
                logger.warning("HA retry buffer full; discarded oldest pending reading")
        if self.pending:
            self.changed.set()

    async def sync(self) -> None:
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
            statistic_ids = sorted({reading.statistic_id for reading in batch})
            # Daily rows are sparse. Reading the full series also recovers an old
            # baseline after a prolonged outage without any local checkpoint.
            existing = await ha.statistics(
                statistic_ids, start_time="1970-01-01T00:00:00+00:00"
            )
            for statistic_id, group in groupby(
                batch, key=lambda item: item.statistic_id
            ):
                readings = list(group)
                rows = plan_statistics(readings, existing.get(statistic_id, []))
                if rows:
                    reading = readings[-1]
                    price = self.prices.get(reading.kind)
                    if price is not None:
                        costs = plan_cost_statistics(rows, price)
                        # Commit costs first: if either import fails, the usage
                        # records still select this batch for an idempotent retry.
                        await ha.import_statistics(reading, costs, cost=True)
                    await ha.import_statistics(reading, rows)
                    logger.info(
                        "Backfilled %s: %s daily readings", statistic_id, len(rows)
                    )
                for reading in readings:
                    key = reading.statistic_id, reading.read_at
                    # Keep any replacement received while imports were awaiting HA.
                    if self.pending.get(key) == reading:
                        del self.pending[key]

    async def run(self) -> None:
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
