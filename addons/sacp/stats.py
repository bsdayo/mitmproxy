import math

import httpx

HOST = "sacp.szhzzd.top"
VERSION = "0.1.0"

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
) -> dict[str, int | float]:
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
    electricity = _meter(meters, 1)
    cold_water = _meter(meters, 2)
    hot_water = _meter(meters, 3)
    values = {
        "balance": result["totalBalance"],
        "electricity_meter": electricity["dataItemValue"],
        "cold_water_meter": cold_water["dataItemValue"],
        "hot_water_meter": hot_water["dataItemValue"],
        "monthly_electricity_meter": electricity["monthTotalValue"],
        "monthly_electricity_cost": electricity["monthTotalMoney"],
        "monthly_cold_water_meter": cold_water["monthTotalValue"],
        "monthly_cold_water_cost": cold_water["monthTotalMoney"],
        "monthly_hot_water_meter": hot_water["monthTotalValue"],
        "monthly_hot_water_cost": hot_water["monthTotalMoney"],
        "monthly_total_cost": result["monthTotalBill"],
    }
    for field, value in values.items():
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError(f"upstream field {field} must be a finite number")
    return values


def _meter(meters: list[dict], energy: int) -> dict:
    for meter in meters:
        if type(meter["energy"]) is int and meter["energy"] == energy:
            return meter
    raise ValueError(f"upstream response is missing energy {energy} meter")


def discovery(building_id: int, mqtt_prefix: str) -> dict:
    device_id = f"sacp_{building_id}"
    components = {}
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
        sensor = {
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
