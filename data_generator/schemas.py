"""
Data schemas for the vehicle telemetry pipeline.

Field names and enum values are modelled on the community-documented Lucid
mobile-gateway protobufs in nshp/python-lucidmotors (MIT, unofficial, no
affiliation with Lucid Motors) — see vehicle_state_service.proto. Those
describe the app-facing API, not Lucid's internal vehicle-to-cloud stream;
the event envelope below (event_id, sequence_number, event_type) is our own.
All data is synthetic.

Two Kafka topics, both keyed by VIN:
  vehicle_telemetry      one event per message; event_type selects which
                         group of fields is populated (others are null)
  vehicle_state_updates  latest-state snapshot per vehicle, MERGEd into a
                         one-row-per-VIN Delta table
"""

import random
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

# ── Lucid enum values (from vehicle_state_service.proto) ──────

MODELS = ["MODEL_AIR", "MODEL_GRAVITY"]

MODEL_VARIANTS = {
    "MODEL_AIR": [
        "MODEL_VARIANT_PURE",
        "MODEL_VARIANT_TOURING",
        "MODEL_VARIANT_GRAND_TOURING",
        "MODEL_VARIANT_SAPPHIRE",
        "MODEL_VARIANT_DREAM_EDITION",
    ],
    "MODEL_GRAVITY": [
        "MODEL_VARIANT_TOURING",
        "MODEL_VARIANT_GRAND_TOURING",
    ],
}

BATTERY_PACK_TYPES = [
    "BATTERY_PACK_TYPE_AIR_16",
    "BATTERY_PACK_TYPE_AIR_18",
    "BATTERY_PACK_TYPE_AIR_22",
]

# Usable capacity (kWh) per pack type — illustrative.
PACK_CAPACITY_KWH = {
    "BATTERY_PACK_TYPE_AIR_16": 88.0,
    "BATTERY_PACK_TYPE_AIR_18": 92.0,
    "BATTERY_PACK_TYPE_AIR_22": 112.0,
}

# Rated range per 1% state of charge (km) — illustrative.
KM_PER_PERCENT = {"MODEL_AIR": 8.3, "MODEL_GRAVITY": 7.2}

POWER_STATES = [
    "POWER_STATE_SLEEP",
    "POWER_STATE_DRIVE",
    "POWER_STATE_ACCESSORY",
    "POWER_STATE_LIVE_CHARGE",
    "POWER_STATE_SLEEP_CHARGE",
    "POWER_STATE_LIVE_UPDATE",
    "POWER_STATE_MONITOR",
]

WARNING_STATES = ["WARNING_OFF", "WARNING_ON"]

BATTERY_PRECON_STATUSES = [
    "BATTERY_PRECON_OFF",
    "BATTERY_PRECON_ON",
    "BATTERY_PRECON_UNAVAILABLE",
]

CHARGE_STATES_WEIGHTED = [
    ("CHARGE_STATE_CHARGING", 50),
    ("CHARGE_STATE_CHARGING_END_OK", 15),
    ("CHARGE_STATE_CABLE_CONNECTED", 8),
    ("CHARGE_STATE_CHARGING_PAUSED", 6),
    ("CHARGE_STATE_CHARGING_SCHEDULED", 6),
    ("CHARGE_STATE_CHARGING_STOPPED", 5),
    ("CHARGE_STATE_CHARGING_END_ERROR", 3),
    ("CHARGE_STATE_EVSE_MALFUNCTION", 2),
    ("CHARGE_STATE_TEMP_PROTECTION", 2),
    ("CHARGE_STATE_AUTHENTICATION_PNC_FAILURE", 2),
    ("CHARGE_STATE_CABLE_CONNECTION_FAULT", 1),
]

UPDATE_STATES_WEIGHTED = [
    ("UPDATE_STATE_IN_PROGRESS", 40),
    ("UPDATE_STATE_SUCCESS", 35),
    ("UPDATE_SUCCESS_WITH_WARNINGS", 6),
    ("UPDATE_STATE_WAITING_ON_BCM", 8),
    ("UPDATE_STATE_FAILED", 5),
    ("UPDATE_FAILED_DRIVE_ALLOWED", 4),
    ("UPDATE_FAILED_NO_ACTION", 2),
]

# ── Our own envelope values (not from the protos) ─────────────

EVENT_TYPES_WEIGHTED = [
    ("BATTERY", 50),
    ("CHARGING", 25),
    ("DIAGNOSTIC", 15),
    ("OTA", 10),
]

ECUS = ["BMS", "VCU", "TCU", "BCM", "ADAS", "HVAC", "INVERTER"]

# Real OBD-II / EV DTC codes; descriptions kept short.
DTC_CODES = [
    "P0A80",  # replace hybrid/EV battery pack
    "P0AA6",  # HV system isolation fault
    "P0A7F",  # battery pack deterioration
    "P1A00",  # HV interlock circuit
    "U0100",  # lost communication with ECM
    "U0293",  # lost communication with HV control module
    "B1000",  # ECU internal malfunction
    "C0035",  # wheel speed sensor circuit
]

SOFTWARE_VERSIONS = ["2.4.6", "2.5.2", "2.6.1", "3.0.0"]

# World Manufacturer Identifier used for synthetic VINs. Illustrative.
VIN_WMI = "50E"

# Home regions across the US and Canada (lat, lon).
HOME_REGIONS = [
    (37.5485, -121.9886),   # Newark, CA
    (32.8795, -111.7574),   # Casa Grande, AZ
    (34.0522, -118.2437),   # Los Angeles, CA
    (37.7749, -122.4194),   # San Francisco, CA
    (47.6062, -122.3321),   # Seattle, WA
    (49.2827, -123.1207),   # Vancouver, BC
    (43.6532, -79.3832),    # Toronto, ON
    (40.7128, -74.0060),    # New York, NY
    (25.7617, -80.1918),    # Miami, FL
    (30.2672, -97.7431),    # Austin, TX
    (39.7392, -104.9903),   # Denver, CO
    (41.8781, -87.6298),    # Chicago, IL
]

# ── Kafka message schemas (JSON keys) ─────────────────────────

TELEMETRY_FIELDS = [
    # envelope
    "event_id",                 # UUID
    "vin",                      # 17-char VIN (PII — never written raw downstream)
    "event_type",               # BATTERY | CHARGING | DIAGNOSTIC | OTA
    "event_time",               # ISO-8601 UTC, time the vehicle recorded it
    "sequence_number",          # per-VIN monotonic counter
    # vehicle context (every event)
    "model",                    # Model enum
    "model_variant",            # ModelVariant enum
    "software_version",         # e.g. "2.6.1"
    "power_state",              # PowerState enum
    "latitude",                 # GPS (PII — coarsened downstream)
    "longitude",                # GPS (PII — coarsened downstream)
    "odometer_km",              # ChassisState.odometer_km
    # BATTERY (BatteryState); charge_percent also set on CHARGING
    "charge_percent",
    "remaining_range",          # km
    "kwhr",
    "capacity_kwhr",
    "battery_health_level",     # 0–100
    "max_cell_temp",            # °C
    "min_cell_temp",            # °C
    "preconditioning_status",   # BatteryPreconStatus enum
    "battery_pack_type",        # BatteryPackType enum
    # CHARGING
    "charge_state",             # ChargeState enum
    "charge_session_id",        # UUID
    "charging_power_kw",
    "energy_added_kwh",
    # DIAGNOSTIC
    "ecu",
    "dtc_code",
    "warning_state",            # WarningState enum
    # OTA
    "update_state",             # UpdateState enum
    "target_software_version",
    "update_progress_percent",
]

VEHICLE_STATE_FIELDS = [
    "vin",                      # primary key for the MERGE
    "model",
    "model_variant",
    "battery_pack_type",
    "software_version",
    "power_state",
    "odometer_km",
    "charge_percent",
    "battery_health_level",
    "last_event_type",
    "sequence_number",          # MERGE only applies newer sequence numbers
    "event_count",              # always 1 per snapshot; summed by the MERGE
    "updated_at",               # event_time of the snapshot
    "first_seen_at",
]


# ── Fleet ─────────────────────────────────────────────────────

@dataclass
class Vehicle:
    """One simulated vehicle. Mutable: its state evolves as events are made."""
    vin: str
    model: str
    model_variant: str
    battery_pack_type: str
    capacity_kwhr: float
    battery_health_level: float
    software_version: str
    latitude: float
    longitude: float
    odometer_km: float
    charge_percent: float
    power_state: str = "POWER_STATE_SLEEP"
    sequence_number: int = 0
    charge_session_id: Optional[str] = None
    first_seen_at: Optional[str] = None
    clock: Optional[datetime] = None   # vehicle's own event clock


_VIN_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  # VINs exclude I, O, Q


def _make_vin(rng: random.Random, model: str) -> str:
    line = "A" if model == "MODEL_AIR" else "G"
    serial = "".join(rng.choice("0123456789") for _ in range(6))
    middle = "".join(rng.choice(_VIN_CHARS) for _ in range(7))
    return f"{VIN_WMI}{line}{middle}{serial}"


def build_fleet(num_vehicles: int, seed: int = 42) -> List[Vehicle]:
    """Create a deterministic fleet so VINs are stable across runs."""
    rng = random.Random(seed)
    fleet = []
    for _ in range(num_vehicles):
        model = rng.choices(MODELS, weights=[75, 25])[0]
        pack = (
            "BATTERY_PACK_TYPE_AIR_22" if model == "MODEL_GRAVITY"
            else rng.choice(BATTERY_PACK_TYPES)
        )
        lat, lon = rng.choice(HOME_REGIONS)
        fleet.append(Vehicle(
            vin=_make_vin(rng, model),
            model=model,
            model_variant=rng.choice(MODEL_VARIANTS[model]),
            battery_pack_type=pack,
            capacity_kwhr=PACK_CAPACITY_KWH[pack],
            battery_health_level=round(rng.uniform(78.0, 100.0), 2),
            software_version=rng.choice(SOFTWARE_VERSIONS[:3]),
            latitude=lat + rng.uniform(-0.3, 0.3),
            longitude=lon + rng.uniform(-0.3, 0.3),
            odometer_km=round(rng.uniform(500, 80_000), 1),
            charge_percent=round(rng.uniform(15, 95), 1),
        ))
    return fleet
