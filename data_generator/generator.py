"""
High-throughput vehicle telemetry generator.

Simulates a fleet of vehicles publishing vehicle-to-cloud telemetry to Kafka.
For every telemetry event a latest-state snapshot is also published so the
vehicle_state Delta table can be upserted (MERGE) in near-real-time.

Ordering: vehicles are sharded across producer threads (vehicle i belongs to
thread i % num_threads), so every VIN is written by exactly one producer and
Kafka's per-key ordering holds. Each event also carries a per-VIN
sequence_number so consumers can enforce ordering and idempotency themselves.
"""

import json
import logging
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import orjson
from kafka import KafkaProducer
from kafka.errors import KafkaError

from .schemas import (
    BATTERY_PRECON_STATUSES,
    CHARGE_STATES_WEIGHTED,
    DTC_CODES,
    ECUS,
    EVENT_TYPES_WEIGHTED,
    KM_PER_PERCENT,
    POWER_STATES,
    SOFTWARE_VERSIONS,
    TELEMETRY_FIELDS,
    UPDATE_STATES_WEIGHTED,
    Vehicle,
    build_fleet,
)

logger = logging.getLogger(__name__)

# JSON sidecar for metrics (avoids DuckDB cross-process lock conflicts)
_METRICS_DIR = Path(__file__).resolve().parent.parent / "logs"
_GEN_JSON_PATH = _METRICS_DIR / "generator_metrics.json"

THERMAL_SPIKE_PROBABILITY = 0.02


def _weighted_choice(rng: random.Random, weighted: list) -> str:
    values, weights = zip(*weighted)
    return rng.choices(values, weights=weights)[0]


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _next_software_version(current: str) -> str:
    idx = SOFTWARE_VERSIONS.index(current) if current in SOFTWARE_VERSIONS else 0
    return SOFTWARE_VERSIONS[min(idx + 1, len(SOFTWARE_VERSIONS) - 1)]


# ── Event builders: each fills one group of fields ────────────

def _battery_fields(v: Vehicle, rng: random.Random) -> dict:
    v.charge_percent = round(min(100.0, max(1.0, v.charge_percent - rng.uniform(0, 1.5))), 1)
    min_temp = round(rng.uniform(15.0, 35.0), 1)
    spread = rng.uniform(25.0, 30.0) if rng.random() < THERMAL_SPIKE_PROBABILITY else rng.uniform(0.5, 6.0)
    return {
        "charge_percent": v.charge_percent,
        "remaining_range": round(v.charge_percent * KM_PER_PERCENT[v.model], 1),
        "kwhr": round(v.capacity_kwhr * v.charge_percent / 100.0, 2),
        "capacity_kwhr": v.capacity_kwhr,
        "battery_health_level": v.battery_health_level,
        "max_cell_temp": round(min_temp + spread, 1),
        "min_cell_temp": min_temp,
        "preconditioning_status": rng.choice(BATTERY_PRECON_STATUSES),
        "battery_pack_type": v.battery_pack_type,
    }


def _charging_fields(v: Vehicle, rng: random.Random) -> dict:
    state = _weighted_choice(rng, CHARGE_STATES_WEIGHTED)
    if v.charge_session_id is None:
        v.charge_session_id = str(uuid.uuid4())
    charging = state == "CHARGE_STATE_CHARGING"
    power_kw = round(rng.uniform(7.0, 300.0), 1) if charging else 0.0
    added = round(power_kw * rng.uniform(0.01, 0.1), 2)
    v.charge_percent = round(min(100.0, v.charge_percent + added / v.capacity_kwhr * 100), 1)
    fields = {
        "charge_percent": v.charge_percent,
        "charge_state": state,
        "charge_session_id": v.charge_session_id,
        "charging_power_kw": power_kw,
        "energy_added_kwh": added,
    }
    if state not in ("CHARGE_STATE_CHARGING", "CHARGE_STATE_CABLE_CONNECTED",
                     "CHARGE_STATE_CHARGING_PAUSED", "CHARGE_STATE_CHARGING_SCHEDULED"):
        v.charge_session_id = None  # session ended
    return fields


def _diagnostic_fields(v: Vehicle, rng: random.Random) -> dict:
    return {
        "ecu": rng.choice(ECUS),
        "dtc_code": rng.choice(DTC_CODES),
        "warning_state": "WARNING_ON" if rng.random() < 0.3 else "WARNING_OFF",
    }


def _ota_fields(v: Vehicle, rng: random.Random) -> dict:
    target = _next_software_version(v.software_version)
    state = _weighted_choice(rng, UPDATE_STATES_WEIGHTED)
    progress = 100 if state in ("UPDATE_STATE_SUCCESS", "UPDATE_SUCCESS_WITH_WARNINGS") \
        else rng.randint(0, 99)
    if progress == 100:
        v.software_version = target
    return {
        "update_state": state,
        "target_software_version": target,
        "update_progress_percent": progress,
    }


EVENT_BUILDERS = {
    "BATTERY": _battery_fields,
    "CHARGING": _charging_fields,
    "DIAGNOSTIC": _diagnostic_fields,
    "OTA": _ota_fields,
}

POWER_STATE_FOR_EVENT = {
    "CHARGING": "POWER_STATE_LIVE_CHARGE",
    "OTA": "POWER_STATE_LIVE_UPDATE",
}


def _advance_vehicle(v: Vehicle, rng: random.Random, now: datetime) -> None:
    """Move the vehicle's clock, odometer and position forward."""
    if v.clock is None:
        v.clock = now - timedelta(hours=24) + timedelta(seconds=rng.randint(0, 3600))
        v.first_seen_at = _iso(v.clock)
        # Real vehicles keep counting across reconnects; derive the starting
        # sequence from wall-clock time so a re-run continues past earlier runs.
        v.sequence_number = int(now.timestamp()) * 1_000_000
    v.clock += timedelta(seconds=rng.randint(1, 120))
    v.sequence_number += 1
    if v.power_state == "POWER_STATE_DRIVE":
        v.odometer_km = round(v.odometer_km + rng.uniform(0.1, 3.0), 1)
        v.latitude += rng.uniform(-0.01, 0.01)
        v.longitude += rng.uniform(-0.01, 0.01)


def generate_telemetry_event(v: Vehicle, rng: random.Random, now: datetime) -> dict:
    """Generate the next telemetry event for one vehicle."""
    _advance_vehicle(v, rng, now)
    event_type = _weighted_choice(rng, EVENT_TYPES_WEIGHTED)
    v.power_state = POWER_STATE_FOR_EVENT.get(event_type, rng.choice(POWER_STATES))

    event = dict.fromkeys(TELEMETRY_FIELDS)
    event.update({
        "event_id": str(uuid.uuid4()),
        "vin": v.vin,
        "event_type": event_type,
        "event_time": _iso(v.clock),
        "sequence_number": v.sequence_number,
        "model": v.model,
        "model_variant": v.model_variant,
        "software_version": v.software_version,
        "power_state": v.power_state,
        "latitude": round(v.latitude, 6),
        "longitude": round(v.longitude, 6),
        "odometer_km": v.odometer_km,
    })
    event.update(EVENT_BUILDERS[event_type](v, rng))
    return event


def generate_state_snapshot(v: Vehicle, event: dict) -> dict:
    """Latest-state snapshot emitted alongside each telemetry event."""
    return {
        "vin": v.vin,
        "model": v.model,
        "model_variant": v.model_variant,
        "battery_pack_type": v.battery_pack_type,
        "software_version": v.software_version,
        "power_state": v.power_state,
        "odometer_km": v.odometer_km,
        "charge_percent": v.charge_percent,
        "battery_health_level": v.battery_health_level,
        "last_event_type": event["event_type"],
        "sequence_number": v.sequence_number,
        "event_count": 1,
        "updated_at": event["event_time"],
        "first_seen_at": v.first_seen_at,
    }


class TelemetryGenerator:
    """Multi-threaded Kafka producer for vehicle telemetry.

    Publishes to two Kafka topics, both keyed by VIN:
      1. topic        — telemetry events
      2. state_topic  — latest-state snapshots (for Delta MERGE)
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        state_topic: str,
        target_bytes: int,
        batch_size: int = 10_000,
        num_threads: int = 4,
        num_vehicles: int = 2000,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.state_topic = state_topic
        self.target_bytes = target_bytes
        self.batch_size = batch_size
        self.num_threads = num_threads
        self.fleet = build_fleet(num_vehicles)

        self._bytes_sent = 0
        self._records_sent = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._batches_completed = 0
        self._error_count = 0
        self._batch_times_ms: list[float] = []
        self._publish_latencies_ms: list[float] = []
        self._run_start_time: Optional[float] = None

    def _create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: orjson.dumps(v),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            linger_ms=50,
            batch_size=65536,
            buffer_memory=134217728,  # 128 MB
            compression_type="lz4",
            max_request_size=10485760,  # 10 MB
            retries=3,
            # One in-flight request per connection keeps per-key order intact on retry
            max_in_flight_requests_per_connection=1,
        )

    def _produce_batch(self, producer: KafkaProducer, vehicles: List[Vehicle],
                       rng: random.Random) -> tuple:
        """Produce one batch. Returns (bytes_sent, records_sent)."""
        batch_start = time.perf_counter()
        batch_bytes = 0
        records = 0
        now = datetime.now(timezone.utc)

        for _ in range(self.batch_size):
            if self._stop_event.is_set():
                break
            vehicle = rng.choice(vehicles)
            event = generate_telemetry_event(vehicle, rng, now)
            batch_bytes += len(orjson.dumps(event))
            producer.send(self.topic, key=vehicle.vin, value=event)
            producer.send(self.state_topic, key=vehicle.vin,
                          value=generate_state_snapshot(vehicle, event))
            records += 1

        flush_start = time.perf_counter()
        producer.flush()
        publish_latency_ms = (time.perf_counter() - flush_start) * 1000
        batch_time_ms = (time.perf_counter() - batch_start) * 1000

        with self._lock:
            self._batch_times_ms.append(batch_time_ms)
            self._publish_latencies_ms.append(publish_latency_ms)

        return batch_bytes, records

    def _worker(self, thread_id: int) -> tuple:
        """Worker thread: produces events for its own shard of the fleet."""
        vehicles = self.fleet[thread_id::self.num_threads]
        rng = random.Random(thread_id)
        producer = self._create_producer()
        thread_bytes = 0
        thread_records = 0

        try:
            while not self._stop_event.is_set():
                batch_bytes, batch_records = self._produce_batch(producer, vehicles, rng)
                thread_bytes += batch_bytes
                thread_records += batch_records

                with self._lock:
                    self._bytes_sent += batch_bytes
                    self._records_sent += batch_records
                    total = self._bytes_sent
                    self._batches_completed += 1

                elapsed = time.time() - self._run_start_time
                throughput = (total / 1e6) / elapsed if elapsed > 0 else 0
                self._write_metrics_json(
                    status="running",
                    elapsed_seconds=round(elapsed, 2),
                    throughput_mb_per_sec=round(throughput, 2),
                )

                if total >= self.target_bytes:
                    self._stop_event.set()

                progress_pct = min(100.0, (total / self.target_bytes) * 100)
                logger.info(
                    f"[Thread-{thread_id}] Batch done | "
                    f"Thread: {thread_bytes / 1e9:.2f} GB | "
                    f"Global: {total / 1e9:.2f} / {self.target_bytes / 1e9:.2f} GB "
                    f"({progress_pct:.1f}%)"
                )
        except KafkaError as e:
            logger.error(f"[Thread-{thread_id}] Kafka error: {e}")
            with self._lock:
                self._error_count += 1
            raise
        finally:
            producer.close()

        return thread_bytes, thread_records

    def run(self) -> dict:
        """Execute the data generation run. Returns summary stats."""
        logger.info(
            f"Starting generation: target={self.target_bytes / 1e9:.2f} GB, "
            f"vehicles={len(self.fleet)}, threads={self.num_threads}, "
            f"batch_size={self.batch_size}"
        )
        start_time = time.time()
        self._run_start_time = start_time
        self._write_metrics_json(status="running")

        with ThreadPoolExecutor(max_workers=self.num_threads) as pool:
            futures = {pool.submit(self._worker, tid): tid for tid in range(self.num_threads)}
            for future in as_completed(futures):
                tid = futures[future]
                try:
                    tb, tr = future.result()
                    logger.info(f"[Thread-{tid}] Finished: {tb / 1e9:.2f} GB, {tr:,} records")
                except Exception as exc:
                    logger.error(f"[Thread-{tid}] Failed: {exc}")

        elapsed = time.time() - start_time
        throughput_mbps = (self._bytes_sent / 1e6) / elapsed if elapsed > 0 else 0

        summary = {
            "total_bytes": self._bytes_sent,
            "total_records": self._records_sent,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_mb_per_sec": round(throughput_mbps, 2),
            "target_bytes": self.target_bytes,
        }
        logger.info(
            f"Generation complete: {self._bytes_sent / 1e9:.2f} GB, "
            f"{self._records_sent:,} records in {elapsed:.1f}s "
            f"({throughput_mbps:.1f} MB/s)"
        )
        self._write_metrics_json(
            status="completed",
            elapsed_seconds=round(elapsed, 2),
            throughput_mb_per_sec=round(throughput_mbps, 2),
        )
        return summary

    def _write_metrics_json(self, *, status: str = "running",
                            elapsed_seconds: float = 0.0,
                            throughput_mb_per_sec: float = 0.0):
        """Atomically write generator metrics to a JSON sidecar file."""
        bt = self._batch_times_ms[-200:]
        pl = self._publish_latencies_ms[-200:]
        data = {
            "status": status,
            "start_time": self._run_start_time,
            "target_bytes": self.target_bytes,
            "bytes_sent": self._bytes_sent,
            "records_sent": self._records_sent,
            "batches_completed": self._batches_completed,
            "elapsed_seconds": elapsed_seconds,
            "throughput_mb_per_sec": throughput_mb_per_sec,
            "avg_batch_time_ms": round(sum(bt) / len(bt), 2) if bt else 0.0,
            "avg_publish_latency_ms": round(sum(pl) / len(pl), 2) if pl else 0.0,
            "batch_timings_ms": bt,
            "publish_latencies_ms": pl,
            "errors": self._error_count,
            "last_update": time.time(),
        }
        try:
            _METRICS_DIR.mkdir(parents=True, exist_ok=True)
            tmp = _GEN_JSON_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(data), encoding="utf-8")
            tmp.replace(_GEN_JSON_PATH)
        except OSError as exc:
            logger.warning(f"Could not write generator metrics sidecar: {exc}")
