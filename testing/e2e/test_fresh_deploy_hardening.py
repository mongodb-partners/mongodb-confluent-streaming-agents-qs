"""Fresh-deploy hardening (2026-07-14 holistic review).

Three gaps a fresh deployer could hit:
1. A blank TF_VAR_voyage_api_key silently produced an empty knowledge base
   (RAG evidence chunks missing) — preflight now warns up front.
2. A Flink statement that goes FAILED *after* deploy is only reported by
   `uv run health`; nothing auto-resumes it — the health entry now carries
   the recovery command.
3. `uv run surge --help` claimed the window default was 5 minutes while the
   actual default (and the deployed TUMBLE interval) is 1.
"""

from __future__ import annotations

import importlib
import inspect
import json
from unittest import mock

pre = importlib.import_module("scripts.preflight")
health = importlib.import_module("scripts.health")


# --- 1. voyage_api_key preflight check ---------------------------------------


def test_voyage_key_check_warns_when_blank():
    for env in ({}, {"TF_VAR_voyage_api_key": ""}, {"TF_VAR_voyage_api_key": "  "}):
        res = pre._check_voyage_api_key(env)
        assert res.status == "warn", env
        assert "knowledge-base" in res.message
        assert res.remediation and "asp-setup" in res.remediation


def test_voyage_key_check_passes_when_set():
    res = pre._check_voyage_api_key({"TF_VAR_voyage_api_key": "pa-abc123"})
    assert res.status == "pass"


def test_voyage_key_check_registered_on_asp_setup_phase():
    # Named voyage_embeddings, NOT voyage_api_key: the redaction layer masks
    # the first message word after "<name> :" when the name matches a
    # secret-key pattern (scripts/common/redaction.py _SECRET_KEYS).
    entries = [c for c in pre.CHECKS if c.name == "voyage_embeddings"]
    assert len(entries) == 1
    c = entries[0]
    assert c.phases == ("asp_setup",)
    # warn-severity: a missing key must not abort the deploy.
    assert c.severity == "warn"
    assert c.network is False


def test_voyage_check_name_survives_redaction():
    """The rendered line '<name> : <message>' must not be treated as a
    secret assignment by the redaction layer."""
    red = importlib.import_module("scripts.common.redaction")
    line = "voyage_embeddings : voyage api key present"
    assert red.redact(line) == line


# --- 2. health remediation hint on non-RUNNING Flink statements ---------------


_FLINK_OUTPUTS = {
    "app_manager_flink_api_key": {"value": "k"},
    "app_manager_flink_api_secret": {"value": "s"},
    "confluent_organization_id": {"value": "o"},
    "confluent_environment_id": {"value": "e"},
    "confluent_flink_rest_endpoint": {"value": "https://flink"},
}


class _FakeResp:
    def __init__(self, phase):
        self._phase = phase

    def read(self):
        return json.dumps({"status": {"phase": self._phase, "detail": "x"}}).encode()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


def test_failed_statement_detail_names_recovery_command():
    with mock.patch("urllib.request.urlopen", return_value=_FakeResp("FAILED")):
        entries = health._check_flink(_FLINK_OUTPUTS)
    assert entries
    for e in entries:
        assert e["status"] in ("fail", "warn")
        assert "uv run datagen" in e["detail"], e


def test_running_statement_detail_has_no_recovery_hint():
    with mock.patch("urllib.request.urlopen", return_value=_FakeResp("RUNNING")):
        entries = health._check_flink(_FLINK_OUTPUTS)
    assert entries
    for e in entries:
        assert e["status"] == "ok"
        assert "uv run datagen" not in (e["detail"] or ""), e


# --- 3. anomalies_enriched RAG merge processor ---------------------------------
#
# Without this processor the anomalies_enriched topic (LLM anomaly_reason +
# vector-search top_chunk_*) has NO consumer: the sink path writes synthesized
# reasons with NULL chunks, and RAG context never reaches Mission Control.


def test_enriched_pipeline_merges_rag_fields_onto_anomaly_doc():
    asp = importlib.import_module("scripts.asp_setup")
    pipeline = asp._pipeline_anomalies_enriched_ingestion()

    source = pipeline[0]["$source"]
    assert source["topic"] == "anomalies_enriched"
    assert source["schemaRegistry"]["connectionName"] == "confluent_schema_registry"

    merge = pipeline[-1]["$merge"]
    assert merge["into"]["db"] == "analytics"
    assert merge["into"]["coll"] == "zone_anomalies"
    assert merge["on"] == ["pickup_zone", "window_time"]
    # merge (NOT replace): overlay RAG fields without clobbering the doc the
    # sink path already wrote.
    assert merge["whenMatched"] == "merge"
    assert merge["whenNotMatched"] == "insert"


def test_enriched_pipeline_converts_window_time_to_date():
    # The $merge "on" key must be the same BSON type as the sink path writes
    # (BSON Date), or every enriched record would insert a duplicate doc
    # instead of merging.
    asp = importlib.import_module("scripts.asp_setup")
    pipeline = asp._pipeline_anomalies_enriched_ingestion()
    add_fields = next(s["$addFields"] for s in pipeline if "$addFields" in s)
    assert add_fields["window_time"] == {"$toDate": "$window_time"}


def test_enriched_processor_registered_in_ensure_processors():
    asp = importlib.import_module("scripts.asp_setup")
    src = inspect.getsource(asp.ensure_processors)
    assert '"anomalies_enriched_ingestion"' in src
    assert "_pipeline_anomalies_enriched_ingestion()" in src


def test_pipeline_reset_bounces_all_kafka_processors_after_ddl():
    # restart_flink_dml drops + recreates catalog tables (CTAS + sink),
    # which deletes and recreates their backing Kafka topics. Every
    # Kafka-consuming ASP processor must be restarted AFTER that second
    # drop (the step-8b restart in reset_pipeline runs before it), or
    # their offsets point at the old topic generation and they silently
    # stall / fail — observed live 2026-07-14.
    pr = importlib.import_module("scripts.pipeline_reset")
    src = inspect.getsource(pr.restart_flink_dml)
    assert "restart_processors_for_topics" in src
    assert "KAFKA_SOURCE_PROCESSORS" in src


def test_deploy_bounces_enriched_processor_after_ctas():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._create_flink_dml_statements)
    # the post-CTAS restart must name the CTAS-backed topics explicitly
    assert '"anomalies_enriched", "completed_actions"' in src


def test_enriched_processor_in_kafka_topology():
    # asp_restart uses this map to bounce processors after a topic is
    # deleted+recreated; a missing entry means the processor silently stalls
    # on the old topic generation.
    topo = importlib.import_module("scripts.common.asp_topology")
    assert topo.KAFKA_SOURCE_PROCESSORS["anomalies_enriched"] == [
        "anomalies_enriched_ingestion"
    ]


# --- 4. datagen --local re-publishes after the Phase-3 table recreation --------


def test_deploy_republishes_after_flink_dml():
    # Same wipe in deploy: the flink_dml phase's DDL recreation empties the
    # ride_requests topic that the publish_data phase seeded. run_deployment
    # must re-publish after _create_flink_dml_statements succeeds.
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    dml_idx = src.index("_create_flink_dml_statements(root)")
    republish_idx = src.index("_publish_local_data(root)", dml_idx)
    assert republish_idx > dml_idx


def test_datagen_local_republishes_after_phase3():
    # Phase 3 (restart_flink_dml) drops the ride_requests catalog table to
    # clear the raw-byte phantom, which DELETES the backing Kafka topic and
    # every record Phase 2 published. Observed live 2026-07-14: datagen ended
    # with ride_requests holding 0 of the 23,289 published records. A Phase 4
    # re-publish must follow.
    dg = importlib.import_module("scripts.datagen")
    src = inspect.getsource(dg.main)
    assert "Phase 4" in src
    phase3_idx = src.index("Phase 3: Recreating Flink DDL")
    phase4_idx = src.index("Phase 4: Re-publishing ride data")
    assert phase4_idx > phase3_idx


# --- 5. surge self-heals the RAG enrichment statement ---------------------------


def test_surge_heals_rag_statement_before_publishing():
    # The RAG statement dies on vector-search timeouts and nothing auto-heals
    # it; surge is the demo trigger, so it must check/recreate the statement
    # before firing (a fresh statement starts at the latest offset and
    # enriches exactly the surge it is about to publish).
    surge = importlib.import_module("scripts.surge")
    assert surge.RAG_STATEMENT == "anomalies-enriched-insert"
    src = inspect.getsource(surge.main)
    heal_idx = src.index("heal_rag_statement()")
    publish_idx = src.index("_publish(records")
    assert heal_idx < publish_idx
    # dry runs and pytest-invoked main() must not touch live Flink
    # statements (the surge tests mock the publish layer, not the heal)
    assert '"PYTEST_CURRENT_TEST" not in os.environ' in src


def test_heal_rag_statement_skips_running(monkeypatch):
    surge = importlib.import_module("scripts.surge")

    monkeypatch.setattr(
        "scripts.pipeline_reset._get_terraform_outputs", lambda root: {"x": 1}
    )
    monkeypatch.setattr(
        "scripts.pipeline_reset._get_flink_credentials",
        lambda outputs: {"api_key": "k", "api_secret": "s", "base_url": "https://f"},
    )

    class _Resp:
        def read(self):
            return b'{"status": {"phase": "RUNNING"}}'

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    deleted = []
    monkeypatch.setattr(
        "scripts.pipeline_reset._delete_flink_statement",
        lambda name, flink: deleted.append(name),
    )
    import urllib.request

    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=10: _Resp())
    assert surge.heal_rag_statement() is True
    assert deleted == []  # RUNNING statement must be left alone


# --- 6. RAG enrichment fallback worker (live_server) ---------------------------
#
# The Flink VECTOR_SEARCH_AGG enrichment dies on federated-search timeouts
# even for a single anomaly, so the live server backfills real Atlas
# $vectorSearch evidence chunks onto anomaly docs the Flink path missed.


def _worker():
    live = importlib.import_module("scripts.live_server")
    return live.RagFallbackWorker(uri="mongodb://x", voyage_api_key="k", delay_s=0)


def test_rag_offer_queues_only_chunkless_anomalies():
    w = _worker()
    # wrong collection
    w.offer("fleet.dispatch_log", "insert", {"pickup_zone": "A", "window_time": 1})
    # delete op
    w.offer("analytics.zone_anomalies", "delete", {"pickup_zone": "A", "window_time": 1})
    # already enriched
    w.offer(
        "analytics.zone_anomalies",
        "insert",
        {"pickup_zone": "A", "window_time": 1, "top_chunk_1": "x"},
    )
    # missing keys
    w.offer("analytics.zone_anomalies", "insert", {"pickup_zone": "A"})
    assert w._queue.qsize() == 0
    # the real thing
    doc = {"pickup_zone": "Bywater", "window_time": 1, "anomaly_reason": "r"}
    w.offer("analytics.zone_anomalies", "insert", doc)
    assert w._queue.qsize() == 1
    # duplicate (re-emission) is deduped
    w.offer("analytics.zone_anomalies", "insert", doc)
    assert w._queue.qsize() == 1


def test_rag_enrich_sets_chunks_and_reason(monkeypatch):
    w = _worker()

    class _Coll:
        def __init__(self, doc):
            self.doc = doc
            self.updated = None

        def find_one(self, q):
            return self.doc

        def update_one(self, q, u):
            self.updated = u

        def aggregate(self, pipeline):
            assert pipeline[0]["$vectorSearch"]["index"] == "vector_index"
            assert pipeline[0]["$vectorSearch"]["limit"] == 3
            return [
                {"chunk": "Jazz Fest at the Fair Grounds", "event_name": "Jazz Fest",
                 "event_type": "festival", "impact_level": "high"},
                {"chunk": "Second line parade"},
            ]

    coll = _Coll({"_id": 1, "pickup_zone": "Bywater", "window_time": 1,
                  "request_count": 12, "expected_requests": 2,
                  "anomaly_reason": "Surge detected in Bywater."})

    class _DB(dict):
        def __getitem__(self, k):
            return coll

    class _Client(dict):
        def __getitem__(self, k):
            return _DB()

    monkeypatch.setattr(w, "_get_client", lambda: _Client())
    monkeypatch.setattr(w, "_embed_query", lambda text: [0.1] * 4)
    w._enrich("Bywater", 1)

    up = coll.updated["$set"]
    assert up["top_chunk_1"] == "Jazz Fest at the Fair Grounds"
    assert up["top_chunk_2"] == "Second line parade"
    assert up["enriched_by"] == "rag-fallback"
    assert "Likely cause: Jazz Fest" in up["anomaly_reason"]
    assert w.enriched_count == 1


def test_rag_enrich_skips_already_enriched(monkeypatch):
    w = _worker()

    calls = {"embed": 0}

    class _Coll:
        def find_one(self, q):
            return {"_id": 1, "top_chunk_1": "already merged by Flink"}

        def update_one(self, q, u):
            raise AssertionError("must not update an already-enriched doc")

    class _DB(dict):
        def __getitem__(self, k):
            return _Coll()

    class _Client(dict):
        def __getitem__(self, k):
            return _DB()

    monkeypatch.setattr(w, "_get_client", lambda: _Client())
    monkeypatch.setattr(
        w, "_embed_query",
        lambda text: calls.__setitem__("embed", calls["embed"] + 1) or [0.1],
    )
    w._enrich("Bywater", 1)
    assert calls["embed"] == 0  # Flink won the race — no work, no API call


def test_hub_listener_receives_raw_doc():
    live = importlib.import_module("scripts.live_server")
    hub = live.ChangeStreamHub()
    seen = []
    hub.add_listener(lambda coll, op, doc: seen.append((coll, op, doc)))
    change = {
        "ns": {"db": "analytics", "coll": "zone_anomalies"},
        "operationType": "insert",
        "fullDocument": {"pickup_zone": "Bywater", "window_time": 1},
    }
    hub._dispatch_change(change)
    assert seen == [
        ("analytics.zone_anomalies", "insert",
         {"pickup_zone": "Bywater", "window_time": 1})
    ]


def test_hub_listener_errors_do_not_break_publish():
    live = importlib.import_module("scripts.live_server")
    hub = live.ChangeStreamHub()

    def _boom(coll, op, doc):
        raise RuntimeError("listener bug")

    hub.add_listener(_boom)
    # must not raise
    hub._dispatch_change(
        {
            "ns": {"db": "analytics", "coll": "zone_anomalies"},
            "operationType": "insert",
            "fullDocument": {"pickup_zone": "A", "window_time": 1},
        }
    )


# --- 7. publish_data rebases batch timestamps to END at now --------------------


def test_time_offset_rebases_batch_to_end_at_now(tmp_path):
    # Rebasing the batch to START at now pushes 24h of event time into the
    # future; the Flink watermark races ahead and every live surge record is
    # dropped as late — no anomaly can fire until wall-clock catches up
    # (observed live 2026-07-14: traffic windows 24h in the future, surge
    # anomalies never landing). The offset must anchor the LAST record to now.
    import base64
    import datetime as dt
    import io
    import json as _json

    import fastavro

    pd = importlib.import_module("scripts.publish_data")
    schema = fastavro.parse_schema(pd._RIDE_REQUESTS_SCHEMA)

    def _record(ts_ms):
        buf = io.BytesIO()
        rec = {
            "request_id": "r1",
            "customer_email": "x@example.com",
            "pickup_zone": "Bywater",
            "drop_off_zone": "Uptown",
            "price": 12.5,
            "number_of_passengers": 2,
            "request_ts": ts_ms,
        }
        fastavro.schemaless_writer(buf, schema, rec)
        value = b"\x00" + (100008).to_bytes(4, "big") + buf.getvalue()
        return _json.dumps({"value": base64.b64encode(value).decode()})

    first_ms = 1_000_000_000_000  # 2001
    last_ms = first_ms + 24 * 3600 * 1000  # +24h
    f = tmp_path / "batch.jsonl"
    f.write_text(_record(first_ms) + "\n" + _record(last_ms) + "\n")

    offset = pd._compute_time_offset(f)
    now_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    rebased_last = last_ms + offset
    # the LAST record must land at ~now (tolerance: test runtime)
    assert abs(rebased_last - now_ms) < 10_000, (
        f"last record rebased to {rebased_last}, expected ~{now_ms}; "
        "batch must END at now, not start at now"
    )


# --- 8. surge --window-min help text matches the real default ------------------


def test_surge_window_help_text_uses_real_default():
    surge = importlib.import_module("scripts.surge")
    src = inspect.getsource(surge)
    assert "(default: 5)" not in src, (
        "surge --window-min help text must not claim a 5-minute default; "
        f"the real default is {surge.DEFAULT_WINDOW_MIN}"
    )
    assert surge.DEFAULT_WINDOW_MIN == 1
