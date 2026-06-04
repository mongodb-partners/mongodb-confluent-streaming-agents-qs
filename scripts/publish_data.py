#!/usr/bin/env python3
"""
CLI-based ride requests data publisher for the streaming agents quickstart.

Publishes pre-generated JSONL ride request data to eliminate ShadowTraffic/Docker
dependencies for workshop participants.

Usage:
    uv run publish_data --data-file assets/data/ride_requests.jsonl
    uv run publish_data --data-file assets/data/ride_requests.jsonl --dry-run

Traditional Python:
    python scripts/publish_data.py --data-file assets/data/ride_requests.jsonl
"""

import argparse
import base64
import io
import json
import logging
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

try:
    import fastavro
    FASTAVRO_AVAILABLE = True
except ImportError:
    FASTAVRO_AVAILABLE = False

try:
    from confluent_kafka import Producer
    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

from .common.terraform import extract_kafka_credentials, validate_terraform_state, get_project_root
from .common.logging_utils import setup_logging


def _get_current_schema_id(
    sr_endpoint: str,
    sr_api_key: str,
    sr_api_secret: str,
    subject: str,
) -> int | None:
    """Look up the latest schema ID for a subject in Schema Registry.

    Returns the numeric schema ID, or None if the subject doesn't exist.
    """
    cred = base64.b64encode(f"{sr_api_key}:{sr_api_secret}".encode()).decode()
    headers = {"Authorization": f"Basic {cred}"}
    url = f"{sr_endpoint}/subjects/{subject}/versions/latest"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            return data.get("id")
    except Exception:
        return None


def _rewrite_avro_schema_id(value_bytes: bytes, new_schema_id: int) -> bytes:
    """Rewrite the schema ID in Confluent Avro wire-format bytes.

    The wire format is: magic byte (0x00) + 4-byte big-endian schema ID + Avro payload.
    Pre-captured JSONL data embeds the schema ID from the original Schema Registry.
    When publishing to a different environment, the ID must be patched to match
    the current registry.
    """
    if len(value_bytes) < 5 or value_bytes[0] != 0:
        return value_bytes  # Not Confluent Avro wire format
    return b'\x00' + new_schema_id.to_bytes(4, 'big') + value_bytes[5:]


_RIDE_REQUESTS_SCHEMA = {
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


def _compute_time_offset(jsonl_file: Path) -> int:
    """Compute the ms offset to shift pre-generated data to start at current UTC time.

    Reads the first record's request_ts and returns the delta (in ms) between
    now and that timestamp. Adding this delta to every record makes the data
    appear as if generated right now.
    """
    if not FASTAVRO_AVAILABLE:
        return 0

    with open(jsonl_file, "r", encoding="utf-8") as f:
        first_line = f.readline().strip()
    if not first_line:
        return 0

    record = json.loads(first_line)
    value_bytes = base64.b64decode(record.get("value", ""))
    if len(value_bytes) < 6 or value_bytes[0] != 0:
        return 0

    payload = io.BytesIO(value_bytes[5:])
    try:
        parsed = fastavro.schemaless_reader(payload, fastavro.parse_schema(_RIDE_REQUESTS_SCHEMA))
    except Exception:
        return 0

    first_ts = parsed["request_ts"]
    if hasattr(first_ts, "timestamp"):
        first_ts_ms = int(first_ts.timestamp() * 1000)
    else:
        first_ts_ms = int(first_ts)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return now_ms - first_ts_ms


def _rewrite_timestamp(value_bytes: bytes, offset_ms: int, parsed_schema) -> bytes:
    """Decode Avro payload, shift request_ts by offset_ms, and re-encode."""
    if not FASTAVRO_AVAILABLE or offset_ms == 0:
        return value_bytes
    if len(value_bytes) < 6 or value_bytes[0] != 0:
        return value_bytes

    header = value_bytes[:5]
    payload = io.BytesIO(value_bytes[5:])
    try:
        record = fastavro.schemaless_reader(payload, parsed_schema)
    except Exception:
        return value_bytes

    ts = record["request_ts"]
    if hasattr(ts, "timestamp"):
        ts_ms = int(ts.timestamp() * 1000)
    else:
        ts_ms = int(ts)
    record["request_ts"] = ts_ms + offset_ms

    out = io.BytesIO()
    fastavro.schemaless_writer(out, parsed_schema, record)
    return header + out.getvalue()


def _get_kafka_rest_endpoint(project_root: Path) -> str | None:
    """Read the Kafka REST endpoint from core terraform state."""
    logger = logging.getLogger(__name__)
    try:
        state_file = project_root / "terraform" / "core" / "terraform.tfstate"
        if not state_file.exists():
            return None
        with open(state_file) as f:
            state = json.load(f)
        outputs = state.get("outputs", {})
        endpoint = outputs.get("confluent_kafka_cluster_rest_endpoint", {}).get("value")
        if endpoint:
            return endpoint
    except Exception as e:
        logger.debug(f"Failed to read Kafka REST endpoint: {e}")
    return None


def _get_topic_message_count(
    rest_endpoint: str,
    cluster_id: str,
    kafka_api_key: str,
    kafka_api_secret: str,
    topic: str,
) -> int | None:
    """Get approximate message count for a Kafka topic via the REST API.

    Returns the sum of (latest_offset - earliest_offset) across all partitions,
    0 when the topic doesn't exist (404), or None when the query fails OR
    any partition's offset fetch fails.

    per-partition atomicity. The previous implementation
    accumulated `latest_offset` and subtracted `earliest_offset` across a
    global total — when `latest` fetch failed on some partitions but
    `earliest` succeeded on others, the result clamped to 0 via
    `max(total, 0)`. The caller then treated `0` as "safe to publish",
    producing duplicates into a populated topic. Now: any partition's
    failure makes the whole count UNKNOWN (None), and the caller must
    refuse to publish.
    """
    logger = logging.getLogger(__name__)
    cred = base64.b64encode(f"{kafka_api_key}:{kafka_api_secret}".encode()).decode()
    headers = {"Authorization": f"Basic {cred}"}

    # List partitions for the topic
    url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic}/partitions"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return 0  # topic doesn't exist
        return None
    except Exception:
        return None

    partitions = data.get("data", [])
    if not partitions:
        return 0

    def _fetch_offset(pid: int, offset_type: str) -> int | None:
        offset_url = (
            f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}"
            f"/topics/{topic}/partitions/{pid}/offsets/{offset_type}"
        )
        try:
            req = urllib.request.Request(offset_url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                offset_data = json.loads(resp.read().decode())
                return int(offset_data.get("offset", 0))
        except Exception:
            return None

    total = 0
    for part in partitions:
        pid = part.get("partition_id", 0)
        lo = _fetch_offset(pid, "earliest")
        hi = _fetch_offset(pid, "latest")
        if lo is None or hi is None:
            # ANY partial-failure makes the count unknown.
            logger.warning(
                f"_get_topic_message_count: partition {pid} of '{topic}' "
                f"offset fetch failed (earliest={lo}, latest={hi}). "
                f"Returning None — caller must NOT treat as 0."
            )
            return None
        total += max(0, hi - lo)
    return total


def _ensure_topic_exists(
    rest_endpoint: str,
    cluster_id: str,
    kafka_api_key: str,
    kafka_api_secret: str,
    topic: str,
    num_partitions: int = 6,
) -> bool:
    """Pre-create a Kafka topic via the Confluent Kafka REST API if it doesn't exist.

    Returns True if the topic exists (or was created), False on failure.
    """
    logger = logging.getLogger(__name__)
    cred = base64.b64encode(f"{kafka_api_key}:{kafka_api_secret}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {cred}",
    }

    # Check if topic already exists
    check_url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic}"
    req = urllib.request.Request(check_url, headers=headers)
    try:
        urllib.request.urlopen(req, timeout=15)
        logger.info(f"Topic '{topic}' already exists")
        return True
    except urllib.error.HTTPError as e:
        if e.code != 404:
            logger.warning(f"Unexpected status checking topic: HTTP {e.code}")
    except Exception:
        pass

    # Create the topic
    create_url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"
    body = json.dumps({
        "topic_name": topic,
        "partitions_count": num_partitions,
    }).encode()
    req = urllib.request.Request(create_url, data=body, method="POST", headers=headers)
    try:
        urllib.request.urlopen(req, timeout=30)
        logger.info(f"Created topic '{topic}' with {num_partitions} partitions")
        # Wait for topic metadata to propagate
        time.sleep(5)
        return True
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode() if e.fp else ""
        if "TopicExistsException" in resp_body or e.code == 409:
            logger.info(f"Topic '{topic}' already exists (race)")
            return True
        logger.error(f"Failed to create topic '{topic}': HTTP {e.code} — {resp_body}")
        return False
    except Exception as e:
        logger.error(f"Failed to create topic '{topic}': {e}")
        return False


class DataPublisher:
    """Publisher for ride request data to Kafka using confluent-kafka library."""

    def __init__(
        self,
        bootstrap_servers: str,
        kafka_api_key: str,
        kafka_api_secret: str,
        dry_run: bool = False,
        target_schema_id: int | None = None,
        time_offset_ms: int = 0,
    ):
        """Initialize the publisher with Kafka configuration."""
        self.bootstrap_servers = bootstrap_servers
        self.kafka_api_key = kafka_api_key
        self.kafka_api_secret = kafka_api_secret
        self.dry_run = dry_run
        self.target_schema_id = target_schema_id
        self.time_offset_ms = time_offset_ms
        self._parsed_schema = (
            fastavro.parse_schema(_RIDE_REQUESTS_SCHEMA)
            if FASTAVRO_AVAILABLE and time_offset_ms != 0
            else None
        )
        self.logger = logging.getLogger(__name__)

        # Create Kafka producer config
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': kafka_api_key,
            'sasl.password': kafka_api_secret,
            'linger.ms': 10,
            'batch.size': 16384,
            'compression.type': 'snappy',
        }

        # Initialize producer (if not dry run)
        self.producer = None
        if not dry_run:
            self.producer = Producer(self.producer_config)

    def publish_message(self, record: Dict[str, Any], topic: str) -> bool:
        """
        Publish a single message to Kafka from base64-encoded JSONL format.

        Args:
            record: Message data with base64-encoded key/value
            topic: Kafka topic name

        Returns:
            True if successful, False otherwise
        """
        try:
            # Decode base64 key and value back to bytes
            key_bytes = base64.b64decode(record['key']) if record.get('key') else None
            value_bytes = base64.b64decode(record['value']) if record.get('value') else None

            # Rewrite the embedded Avro schema ID to match the current registry
            if value_bytes and self.target_schema_id is not None:
                value_bytes = _rewrite_avro_schema_id(value_bytes, self.target_schema_id)

            # Shift timestamps to current time
            if value_bytes and self.time_offset_ms and self._parsed_schema:
                value_bytes = _rewrite_timestamp(value_bytes, self.time_offset_ms, self._parsed_schema)

            # Decode headers if present
            headers_list = None
            if record.get('headers'):
                headers_list = [(k, base64.b64decode(v)) for k, v in record['headers'].items()]

            if self.dry_run:
                self.logger.debug(f"[DRY RUN] Would publish message to partition {record.get('partition')}, offset {record.get('offset')}")
                return True

            # Produce message
            self.producer.produce(
                topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers_list
            )

            return True

        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")
            return False

    def publish_jsonl_file(self, jsonl_file: Path, topic: str) -> Dict[str, int]:
        """
        Publish all messages from a JSONL file with base64-encoded Avro data.

        Args:
            jsonl_file: Path to JSONL file
            topic: Kafka topic name

        Returns:
            Dictionary with success/failure counts
        """
        results = {"success": 0, "failed": 0, "total": 0}

        # Read all lines
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.logger.error(f"Failed to read JSONL file {jsonl_file}: {e}")
            return results

        results["total"] = len(lines)
        self.logger.info(f"Found {len(lines)} messages to publish")

        # Process messages
        for idx, line in enumerate(lines, 1):
            try:
                message_data = json.loads(line)
                if self.publish_message(message_data, topic):
                    results["success"] += 1
                else:
                    results["failed"] += 1

                # Flush periodically for better performance
                if not self.dry_run and idx % 100 == 0:
                    self.producer.poll(0)

                if not self.dry_run and idx % 1000 == 0:
                    self.producer.flush()
                    self.logger.info(f"Progress: {idx}/{results['total']} messages ({results['success']} succeeded, {results['failed']} failed)")

            except json.JSONDecodeError as e:
                self.logger.error(f"Error parsing line {idx}: {e}")
                results["failed"] += 1
            except Exception as e:
                self.logger.error(f"Error processing line {idx}: {e}")
                results["failed"] += 1

        # Final flush
        if not self.dry_run and self.producer:
            self.logger.info("Flushing remaining messages...")
            self.producer.flush()

        return results

    def close(self):
        """Clean up resources."""
        if self.producer:
            self.producer.flush()


def main():
    """Main entry point for the data publisher CLI."""
    parser = argparse.ArgumentParser(
        description="Publish ride request data to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --data-file assets/data/ride_requests.jsonl
  %(prog)s --data-file assets/data/ride_requests.jsonl --dry-run
        """
    )

    parser.add_argument(
        "--data-file",
        type=Path,
        required=True,
        help="Path to JSONL data file with ride requests"
    )
    parser.add_argument(
        "--topic",
        default="ride_requests",
        help="Kafka topic name (default: ride_requests)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test without actually publishing"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force publish even if the topic already contains messages"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set up logging
    logger = setup_logging(args.verbose)

    # Verify data file exists
    if not args.data_file.exists():
        logger.error(f"Data file does not exist: {args.data_file}")
        return 1

    # Check confluent-kafka library is available
    if not CONFLUENT_KAFKA_AVAILABLE:
        logger.error("confluent-kafka library not available. Please install it with: uv pip install confluent-kafka")
        return 1

    logger.info(f"Publishing ride request data from {args.data_file}")

    # Get project root
    try:
        project_root = get_project_root()
    except Exception as e:
        logger.error(f"Could not find project root: {e}")
        return 1

    # Validate terraform state
    try:
        validate_terraform_state(project_root)
    except Exception as e:
        logger.error(f"Terraform validation failed: {e}")
        return 1

    # Extract Kafka credentials
    try:
        credentials = extract_kafka_credentials(project_root)
    except Exception as e:
        logger.error(f"Failed to extract Kafka credentials: {e}")
        return 1

    # Ensure the target topic exists before publishing
    if not args.dry_run:
        rest_endpoint = _get_kafka_rest_endpoint(project_root)
        if rest_endpoint:
            if not _ensure_topic_exists(
                rest_endpoint=rest_endpoint,
                cluster_id=credentials["cluster_id"],
                kafka_api_key=credentials["kafka_api_key"],
                kafka_api_secret=credentials["kafka_api_secret"],
                topic=args.topic,
            ):
                # Don't hard-fail: the broker may still auto-create the topic
                # on first produce. But surface it — a failed pre-create is
                # otherwise invisible and was previously silently discarded.
                logger.warning(
                    f"Could not pre-create/verify topic '{args.topic}' — "
                    "relying on broker auto-create on first produce."
                )
        else:
            logger.warning("Could not determine Kafka REST endpoint — topic will be auto-created")

    # Check if topic already has messages (duplicate data guard)
    if not args.dry_run and not args.force:
        rest_endpoint = _get_kafka_rest_endpoint(project_root)
        if rest_endpoint:
            msg_count = _get_topic_message_count(
                rest_endpoint=rest_endpoint,
                cluster_id=credentials["cluster_id"],
                kafka_api_key=credentials["kafka_api_key"],
                kafka_api_secret=credentials["kafka_api_secret"],
                topic=args.topic,
            )
            if msg_count is None:
                # unknown count (partial REST API failure
                # during the per-partition offset probe). Refuse to publish
                # rather than treat unknown as 0 — would produce duplicates
                # into a populated topic.
                logger.warning(
                    f"Could not determine message count for '{args.topic}' "
                    f"(Kafka REST API partial failure). Refusing to publish "
                    f"to avoid silent duplicates."
                )
                print(
                    f"\n  Could not determine message count for '{args.topic}'."
                )
                print(
                    "  Use --force to publish anyway (may create duplicates) "
                    "or check Kafka REST connectivity."
                )
                return 1
            if msg_count > 0:
                logger.warning(
                    f"Topic '{args.topic}' already contains ~{msg_count:,} messages. "
                    f"Publishing again will create duplicates."
                )
                print(f"\n  Topic '{args.topic}' already has ~{msg_count:,} messages.")
                print("  Use --force to publish anyway (duplicates will be created).")
                return 1

    # Look up the current schema ID so we can rewrite embedded IDs in the
    # pre-captured Avro data to match the current Schema Registry.
    target_schema_id = None
    if not args.dry_run:
        sr_endpoint = credentials.get("schema_registry_url")
        sr_key = credentials.get("schema_registry_api_key")
        sr_secret = credentials.get("schema_registry_api_secret")
        if sr_endpoint and sr_key and sr_secret:
            subject = f"{args.topic}-value"
            target_schema_id = _get_current_schema_id(sr_endpoint, sr_key, sr_secret, subject)
            # Use `is not None` (not truthiness): a valid schema ID of 0 would
            # be wrongly discarded by a bare `if target_schema_id:`, which is
            # inconsistent with DataPublisher's downstream `is not None` guard.
            if target_schema_id is not None:
                logger.info(f"Current schema ID for '{subject}': {target_schema_id}")
            else:
                logger.warning(f"Could not look up schema ID for '{subject}' — publishing with original IDs")

    # Compute time offset to shift historical timestamps to current time
    time_offset_ms = _compute_time_offset(args.data_file)
    if time_offset_ms:
        from datetime import timedelta
        delta = timedelta(milliseconds=time_offset_ms)
        logger.info(f"Shifting timestamps forward by {delta} to align with current UTC time")

    # Initialize publisher
    try:
        publisher = DataPublisher(
            bootstrap_servers=credentials["bootstrap_servers"],
            kafka_api_key=credentials["kafka_api_key"],
            kafka_api_secret=credentials["kafka_api_secret"],
            dry_run=args.dry_run,
            target_schema_id=target_schema_id,
            time_offset_ms=time_offset_ms,
        )
    except Exception as e:
        logger.error(f"Failed to initialize publisher: {e}")
        return 1

    # Publish data
    try:
        logger.info(f"Publishing ride requests to topic '{args.topic}'")
        if args.dry_run:
            logger.info("[DRY RUN MODE - No actual publishing will occur]")

        results = publisher.publish_jsonl_file(args.data_file, args.topic)

        print(f"\n{'=' * 60}")
        print("DATA PUBLISHING SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total records:    {results['total']}")
        print(f"Published:        {results['success']}")
        print(f"Failed:           {results['failed']}")
        print(f"{'=' * 60}")

        if args.dry_run:
            print("\n[DRY RUN COMPLETE - No messages were actually published]")

        return 0 if results['failed'] == 0 else 1
    finally:
        publisher.close()


if __name__ == "__main__":
    sys.exit(main())
