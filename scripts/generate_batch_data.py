#!/usr/bin/env python3
"""
Generate 10 batch data files with escalating surge multipliers and rotating zones.

Each batch covers 24 hours of ride request data. The surge zone rotates across
batches to match the knowledge base events, so the RAG pipeline can explain
surges with the corresponding event context.

Design:
- Each batch has ~288 five-minute windows (24 hours)
- First ~100 windows: steady-state training data (baseline)
- Remaining windows: mix of steady-state + 8 surge windows
- Surge intensity escalates across batches (3x → 12x baseline)
- Surge zone rotates to match knowledge base events:
  - Batch 1: French Quarter (Mardi Gras Parade)
  - Batch 2: Central Business District (Saints Game)
  - Batch 3: Bywater (Bywater Biennale)
  - Batch 4: Marigny (Frenchmen Street Live Music Festival)
  - Batch 5: Uptown (Bayou Classic)
  - Batch 6: Garden District (Home & Garden Tour)
  - Batch 7: French Quarter (French Quarter Festival)
  - Batch 8: Central Business District (Essence Music Festival)
  - Batch 9: Warehouse District (Art Walk)
  - Batch 10: French Quarter (Jazz at Preservation Hall — massive surge)

Usage:
    python scripts/generate_batch_data.py
    uv run python scripts/generate_batch_data.py
"""

import base64
import io
import json
import random
import struct
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import fastavro
except ImportError:
    print("Error: fastavro is required. Install with: pip install fastavro")
    sys.exit(1)

SCHEMA = {
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
}

PARSED_SCHEMA = fastavro.parse_schema(SCHEMA)

# Schema ID used in the wire format (arbitrary, gets rewritten by publish_data.py)
SCHEMA_ID = 100008

ZONES = [
    "French Quarter",
    "Marigny",
    "Bywater",
    "Warehouse District",
    "Uptown",
    "Garden District",
    "Central Business District (CBD)",
]

# Surge zone rotation per batch — matches knowledge base events
SURGE_ZONES = [
    "French Quarter",                    # Batch 1: Mardi Gras Parade
    "Central Business District (CBD)",   # Batch 2: Saints Game
    "Bywater",                           # Batch 3: Bywater Biennale
    "Marigny",                           # Batch 4: Frenchmen Street Live Music
    "Uptown",                            # Batch 5: Bayou Classic
    "Garden District",                   # Batch 6: Home & Garden Tour
    "French Quarter",                    # Batch 7: French Quarter Festival
    "Central Business District (CBD)",   # Batch 8: Essence Music Festival
    "Warehouse District",                # Batch 9: Art Walk
    "French Quarter",                    # Batch 10: massive surge
]

# Baseline: average requests per 5-min window per zone
BASELINE_REQUESTS_PER_WINDOW = 12

# Each batch covers 24 hours = 288 five-minute windows
WINDOWS_PER_BATCH = 288

# Surge multipliers for each batch
SURGE_MULTIPLIERS = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

# Number of surge windows per batch (concentrated in the second half)
SURGE_WINDOWS_PER_BATCH = 8

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
FIRST_NAMES = [
    "james", "mary", "john", "patricia", "robert", "jennifer", "michael",
    "linda", "david", "elizabeth", "william", "barbara", "richard", "susan",
    "joseph", "jessica", "thomas", "sarah", "charles", "karen", "chris",
    "daniel", "nancy", "matthew", "lisa", "anthony", "betty", "mark", "dorothy",
    "andrew", "sandra", "steven", "ashley", "paul", "kimberly", "joshua", "emily",
]
LAST_NAMES = [
    "smith", "johnson", "williams", "brown", "jones", "garcia", "miller",
    "davis", "rodriguez", "martinez", "hernandez", "lopez", "gonzalez",
    "wilson", "anderson", "thomas", "taylor", "moore", "jackson", "martin",
    "lee", "perez", "white", "harris", "clark", "lewis", "robinson", "walker",
]


def _random_email() -> str:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    domain = random.choice(EMAIL_DOMAINS)
    num = random.randint(1, 999)
    return f"{first}.{last}{num}@{domain}"


def _encode_record(record: dict) -> str:
    """Encode a record to Confluent Avro wire format (base64)."""
    out = io.BytesIO()
    fastavro.schemaless_writer(out, PARSED_SCHEMA, record)
    avro_bytes = out.getvalue()
    wire_bytes = b'\x00' + struct.pack('>I', SCHEMA_ID) + avro_bytes
    return base64.b64encode(wire_bytes).decode()


def _generate_record(
    request_id: str,
    pickup_zone: str,
    timestamp_ms: int,
) -> dict:
    """Generate a single ride request record."""
    dropoff_zone = random.choice([z for z in ZONES if z != pickup_zone])
    return {
        "request_id": request_id,
        "customer_email": _random_email(),
        "pickup_zone": pickup_zone,
        "drop_off_zone": dropoff_zone,
        "price": round(random.uniform(35.0, 175.0), 2),
        "number_of_passengers": random.randint(1, 6),
        "request_ts": timestamp_ms,
    }


def _generate_window_records(
    window_start_ms: int,
    zone: str,
    count: int,
    id_offset: int,
) -> list[dict]:
    """Generate `count` records spread across a 5-minute window."""
    records = []
    window_duration_ms = 5 * 60 * 1000  # 5 minutes

    for i in range(count):
        ts = window_start_ms + random.randint(0, window_duration_ms - 1)
        req_id = f"REQ-{id_offset + i}"
        record = _generate_record(req_id, zone, ts)
        records.append(record)

    return records


def generate_batch(
    batch_number: int,
    base_time: datetime,
    surge_multiplier: int,
    surge_zone: str,
) -> list[dict]:
    """Generate one batch of ride request data.

    Args:
        batch_number: 1-10
        base_time: Start time for this batch
        surge_multiplier: How many times baseline for surge windows
        surge_zone: Which zone gets the surge
    """
    random.seed(42 + batch_number * 1000)

    all_records = []
    id_counter = batch_number * 1_000_000

    base_ms = int(base_time.timestamp() * 1000)
    window_ms = 5 * 60 * 1000  # 5 minutes

    # Determine which windows are surge windows
    # Place surges in the second half of the batch, spread out
    # Each batch uses different surge window positions to avoid predictability
    surge_start_window = 100 + (batch_number * 7) % 50  # varies by batch
    surge_spacing = max(5, 20 - batch_number)  # closer together in later batches
    surge_windows = set()
    for i in range(SURGE_WINDOWS_PER_BATCH):
        w = surge_start_window + i * surge_spacing
        if w < WINDOWS_PER_BATCH:
            surge_windows.add(w)

    for window_idx in range(WINDOWS_PER_BATCH):
        window_start = base_ms + window_idx * window_ms

        for zone in ZONES:
            # Baseline with some natural variation
            base_count = max(4, int(random.gauss(BASELINE_REQUESTS_PER_WINDOW, 3)))

            if zone == surge_zone and window_idx in surge_windows:
                # Surge window: multiply the count
                count = base_count * surge_multiplier
            else:
                count = base_count

            records = _generate_window_records(
                window_start, zone, count, id_counter
            )
            all_records.extend(records)
            id_counter += count

    # Sort by timestamp to simulate real streaming order
    all_records.sort(key=lambda r: r["request_ts"])
    return all_records


def write_batch_file(records: list[dict], output_path: Path, batch_number: int):
    """Write records to a JSONL file in the expected format."""
    partition_count = 6

    with open(output_path, "w", encoding="utf-8") as f:
        for i, record in enumerate(records):
            value_b64 = _encode_record(record)
            partition = i % partition_count
            line = json.dumps({
                "key": None,
                "value": value_b64,
                "partition": partition,
                "offset": i,
            })
            f.write(line + "\n")

    print(f"  Batch {batch_number:02d}: {len(records):,} records -> {output_path.name}")


def main():
    output_dir = Path(__file__).parent.parent / "assets" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Base time for batch 1; each subsequent batch starts 24h later
    base_time = datetime(2026, 2, 9, 3, 0, 0, tzinfo=timezone.utc)

    print("Generating batch data with escalating surge multipliers...")
    print(f"  Baseline: ~{BASELINE_REQUESTS_PER_WINDOW} requests/window/zone")
    print(f"  Surge windows per batch: {SURGE_WINDOWS_PER_BATCH}")
    print(f"  Surge multipliers: {SURGE_MULTIPLIERS}")
    print()

    total_records = 0

    for batch_idx, (multiplier, surge_zone) in enumerate(
        zip(SURGE_MULTIPLIERS, SURGE_ZONES), start=1
    ):
        batch_time = base_time + timedelta(days=batch_idx - 1)
        records = generate_batch(batch_idx, batch_time, multiplier, surge_zone)
        total_records += len(records)

        batch_file = output_dir / f"batch_{batch_idx:02d}.jsonl"
        write_batch_file(records, batch_file, batch_idx)

    # Also regenerate ride_requests.jsonl as the "initial bootstrap" file
    # This is batch 1's data (French Quarter, 3x surges)
    bootstrap_file = output_dir / "ride_requests.jsonl"
    bootstrap_records = generate_batch(1, base_time, SURGE_MULTIPLIERS[0], SURGE_ZONES[0])
    write_batch_file(bootstrap_records, bootstrap_file, 0)
    total_records += len(bootstrap_records)

    print(f"\nDone! Generated {total_records:,} total records across 11 files.")
    print("\nSurge plan by batch:")
    for i, (mult, zone) in enumerate(zip(SURGE_MULTIPLIERS, SURGE_ZONES), 1):
        peak = BASELINE_REQUESTS_PER_WINDOW * mult
        print(f"  Batch {i:02d}: {mult}x surge -> ~{peak} req/window in {zone}")


if __name__ == "__main__":
    main()
