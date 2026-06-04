#!/usr/bin/env python3
"""Generate 10 pre-baked ride_requests batches with engineered anomalies.

Each batch covers 6 hours of data (enough for 72 five-minute windows per zone).
With minTrainingSize=50, anomalies start appearing from batch 1 since the full
original dataset (288 windows) is always published first as the baseline.

Anomaly zones rotate across batches so the demo shows different zones spiking.

Usage:
    python scripts/generate_batches.py
    # Generates assets/data/batch_01.jsonl through batch_10.jsonl
"""

import io
import json
import base64
import random
import struct
from datetime import datetime, timezone, timedelta
from pathlib import Path

import avro.io
import avro.schema

ZONES = [
    "French Quarter",
    "Central Business District (CBD)",
    "Warehouse District",
    "Bywater",
    "Marigny",
    "Garden District",
    "Uptown",
]

EMAILS = [
    "rider.{n}@gmail.com",
    "passenger.{n}@yahoo.com",
    "user.{n}@outlook.com",
    "customer.{n}@hotmail.com",
    "traveler.{n}@icloud.com",
]

SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "ride_requests_value",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {"name": "request_id", "type": "string"},
        {"name": "customer_email", "type": "string"},
        {"name": "pickup_zone", "type": "string"},
        {"name": "drop_off_zone", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "number_of_passengers", "type": "int"},
        {"name": "request_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})

# Anomaly schedule: which zone spikes in each batch
ANOMALY_SCHEDULE = [
    ("French Quarter", 3.5),
    ("Warehouse District", 3.0),
    ("Central Business District (CBD)", 3.5),
    ("Bywater", 3.0),
    ("Marigny", 3.5),
    ("Uptown", 3.0),
    ("Garden District", 3.5),
    ("French Quarter", 4.0),
    ("Central Business District (CBD)", 4.0),
    ("Warehouse District", 3.5),
]

NUM_BATCHES = 10
BATCH_DURATION_HOURS = 6
RECORDS_PER_ZONE_PER_HOUR = 220  # ~3.6 per minute, normal rate
SCHEMA_ID = 100008  # Must match the registered schema ID


def _encode_avro(record: dict, schema) -> bytes:
    """Encode a record to Avro binary with schema registry wire format."""
    writer = avro.io.DatumWriter(schema)
    buf = io.BytesIO()
    encoder = avro.io.BinaryEncoder(buf)
    # The avro library with timestamp-millis logical type expects datetime objects
    writer.write(record, encoder)
    payload = buf.getvalue()
    # Wire format: magic byte (0) + 4-byte schema ID (big-endian) + payload
    return b"\x00" + struct.pack(">I", SCHEMA_ID) + payload


def _generate_batch(batch_num: int, base_time: datetime, schema) -> list:
    """Generate one batch of ride requests."""
    rng = random.Random(42 + batch_num)
    records = []

    anomaly_zone, anomaly_multiplier = ANOMALY_SCHEDULE[batch_num - 1]
    # Anomaly window: spike for ~2 hours in the middle of the batch
    spike_start_h = 2
    spike_end_h = 4

    batch_start = base_time + timedelta(hours=(batch_num - 1) * BATCH_DURATION_HOURS)
    req_counter = batch_num * 100000

    for hour_offset in range(BATCH_DURATION_HOURS):
        current_hour_start = batch_start + timedelta(hours=hour_offset)
        is_spike_hour = spike_start_h <= hour_offset < spike_end_h

        for zone in ZONES:
            # Determine records for this zone/hour
            base_count = RECORDS_PER_ZONE_PER_HOUR
            if zone == anomaly_zone and is_spike_hour:
                count = int(base_count * anomaly_multiplier)
            else:
                # Normal variance ±15%
                count = int(base_count * rng.uniform(0.85, 1.15))

            for i in range(count):
                req_counter += 1
                # Spread evenly within the hour
                ts_offset = timedelta(seconds=rng.uniform(0, 3600))
                ts = current_hour_start + ts_offset

                drop_zone = rng.choice([z for z in ZONES if z != zone])
                email_template = rng.choice(EMAILS)
                passengers = rng.choices([1, 2, 3, 4, 5, 6], weights=[40, 25, 15, 10, 7, 3])[0]
                price = round(rng.uniform(8.0, 250.0), 2)

                record = {
                    "request_id": f"REQ-{req_counter}",
                    "customer_email": email_template.format(n=req_counter),
                    "pickup_zone": zone,
                    "drop_off_zone": drop_zone,
                    "price": price,
                    "number_of_passengers": passengers,
                    "request_ts": ts,
                }

                # Encode to Avro
                avro_bytes = _encode_avro(record, schema)
                jsonl_record = {
                    "key": None,
                    "value": base64.b64encode(avro_bytes).decode(),
                    "partition": rng.randint(0, 5),
                    "offset": req_counter,
                }
                records.append(jsonl_record)

    # Shuffle to simulate realistic arrival order
    rng.shuffle(records)
    return records


def main():
    schema = avro.schema.parse(SCHEMA_STR)
    output_dir = Path(__file__).resolve().parent.parent / "assets" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use a fixed base time far enough in the future to avoid watermark conflicts
    # Each run starts from "now" effectively (the publish script doesn't care about absolute time,
    # but Flink does for windowing). We use Feb 11, 2026 as base (day after original data ends).
    base_time = datetime(2026, 2, 11, 0, 0, 0, tzinfo=timezone.utc)

    print(f"Generating {NUM_BATCHES} batches of ride request data...")
    print(f"  Base time: {base_time}")
    print(f"  Each batch: {BATCH_DURATION_HOURS} hours, ~{RECORDS_PER_ZONE_PER_HOUR * len(ZONES) * BATCH_DURATION_HOURS} records")
    print()

    for batch_num in range(1, NUM_BATCHES + 1):
        records = _generate_batch(batch_num, base_time, schema)
        anomaly_zone = ANOMALY_SCHEDULE[batch_num - 1][0]

        outfile = output_dir / f"batch_{batch_num:02d}.jsonl"
        with open(outfile, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        batch_start = base_time + timedelta(hours=(batch_num - 1) * BATCH_DURATION_HOURS)
        batch_end = batch_start + timedelta(hours=BATCH_DURATION_HOURS)
        print(f"  Batch {batch_num:2d}: {len(records):,} records | "
              f"{batch_start.strftime('%m/%d %H:%M')}-{batch_end.strftime('%H:%M')} UTC | "
              f"Spike: {anomaly_zone}")

    print(f"\nDone. Files written to {output_dir}/batch_*.jsonl")


if __name__ == "__main__":
    main()
