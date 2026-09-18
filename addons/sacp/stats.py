"""Fetch room readings and describe their MQTT entities for Home Assistant."""

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

HOST = "sacp.szhzzd.top"
VERSION = "0.2.0"
TIMEZONE = ZoneInfo("Asia/Shanghai")
METER_KINDS = {1: "electricity", 2: "cold_water", 3: "hot_water"}


@dataclass(frozen=True)
class Reading:
    building_id: int
    kind: str
    meter_id: int
    value: int | float
    read_at: datetime

    @property
    def statistic_id(self) -> str:
        # A replacement meter must never be compared with the previous meter.
        return f"sacp:{self.building_id}_{self.kind}_{self.meter_id}"


@dataclass(frozen=True)
class Snapshot:
    building_id: int
    values: dict[str, int | float | str]
    readings: tuple[Reading, ...]


HEADERS = {
    "invitationUserId": "-1",
    "X-Access-Identity": "USER_APPLETS",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "landlordRequest": "1",
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 26_6_1 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 "
        "MicroMessenger/8.0.76(0x18004c39) NetType/WIFI Language/en"
    ),
    "Referer": "https://servicewechat.com/wxf13738187fc6cebb/53/page-frame.html",
}


async def fetch(
    client: httpx.AsyncClient, access_token: str, building_id: int
) -> Snapshot:
    response = await client.get(
        f"https://{HOST}/sacp/api/tenant/hs/listingsRoom/getRoomInfo",
        params={"buildingId": building_id},
        headers=HEADERS | {"X-Access-Token": access_token},
    )
    if not response.is_success:
        raise ValueError(f"upstream returned HTTP {response.status_code}")
    data = response.json()
    if data["success"] is not True or data["code"] != 200:
        raise ValueError("upstream reported a business failure")

    result = data["result"]
    meters = result["monitordataList"]
    values: dict[str, int | float | str] = {
        "balance": result["totalBalance"],
        "monthly_total_cost": result["monthTotalBill"],
    }
    readings: list[Reading] = []
    for energy, kind in METER_KINDS.items():
        meter = _meter(meters, energy)
        values[f"{kind}_meter"] = meter["dataItemValue"]
        values[f"monthly_{kind}_meter"] = meter["monthTotalValue"]
        values[f"monthly_{kind}_cost"] = meter["monthTotalMoney"]

        meter_id = meter["meterId"]
        if type(meter_id) is not int or meter_id < 0:
            raise ValueError("invalid meterId")
        read_at = datetime.strptime(
            meter["dataItemValueTime"], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=TIMEZONE)
        readings.append(
            Reading(building_id, kind, meter_id, meter["dataItemValue"], read_at)
        )

    for field, value in values.items():
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError(f"upstream field {field} must be a finite number")
    values.update(
        {f"{reading.kind}_read_at": reading.read_at.isoformat() for reading in readings}
    )
    return Snapshot(building_id, values, tuple(readings))


def _meter(meters: list[dict[str, Any]], energy: int) -> dict[str, Any]:
    for meter in meters:
        if type(meter["energy"]) is int and meter["energy"] == energy:
            return meter
    raise ValueError(f"upstream response is missing energy {energy} meter")


def discovery(building_id: int, mqtt_prefix: str) -> dict[str, Any]:
    device_id = f"sacp_{building_id}"
    components: dict[str, dict[str, Any]] = {}
    for field, name, unit, device_class in (
        ("balance", "Balance", "CNY", "monetary"),
        ("electricity_meter", "Electricity Meter", "kWh", "energy"),
        ("cold_water_meter", "Cold Water Meter", "m³", "water"),
        ("hot_water_meter", "Hot Water Meter", "m³", "water"),
        ("monthly_electricity_meter", "Monthly Electricity", "kWh", "energy"),
        ("monthly_electricity_cost", "Monthly Electricity Cost", "CNY", "monetary"),
        ("monthly_cold_water_meter", "Monthly Cold Water", "m³", "water"),
        ("monthly_cold_water_cost", "Monthly Cold Water Cost", "CNY", "monetary"),
        ("monthly_hot_water_meter", "Monthly Hot Water", "m³", "water"),
        ("monthly_hot_water_cost", "Monthly Hot Water Cost", "CNY", "monetary"),
        ("monthly_total_cost", "Monthly Total Cost", "CNY", "monetary"),
    ):
        sensor: dict[str, Any] = {
            "platform": "sensor",
            "name": name,
            "unique_id": f"{device_id}_{field}",
            "value_template": f"{{{{ value_json.{field} }}}}",
            "unit_of_measurement": unit,
            "device_class": device_class,
        }
        if device_class != "monetary":
            sensor["state_class"] = "total_increasing"
        components[field] = sensor
    for kind in METER_KINDS.values():
        field = f"{kind}_read_at"
        components[field] = {
            "platform": "sensor",
            "name": f"{kind.replace('_', ' ').title()} Reading Time",
            "unique_id": f"{device_id}_{field}",
            "device_class": "timestamp",
            "entity_category": "diagnostic",
            "value_template": f"{{{{ value_json.{field} }}}}",
        }
    return {
        "device": {
            "identifiers": [device_id],
            "name": f"SACP {building_id}",
        },
        "origin": {
            "name": "sacp",
            "sw_version": VERSION,
        },
        "state_topic": f"{mqtt_prefix}/{building_id}/state",
        "qos": 1,
        "components": components,
    }
