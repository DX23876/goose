// Integration tests for external metrics ingest (EXT-01, EXT-02, EXT-03):
//   - external_samples store methods: upsert, idempotent re-upsert, window query, sort order
//   - bridge methods: external.import_samples, external.import_daily_rollups,
//     external.resolve_metric_for_day
//
// Store-level tests use an in-memory GooseStore (fast, no filesystem). Bridge-level tests
// use a temp-file database via the JSON bridge entry point, matching the convention in
// feature_flags_bridge_tests.rs, since the bridge is the real Swift call surface.

use goose_core::bridge::{BridgeResponse, handle_bridge_request_json};
use goose_core::store::GooseStore;

fn open_in_memory() -> GooseStore {
    GooseStore::open_in_memory().expect("open in-memory store")
}

fn request(value: serde_json::Value) -> BridgeResponse {
    serde_json::from_str(&handle_bridge_request_json(&value.to_string())).unwrap()
}

fn db_path(tempdir: &tempfile::TempDir) -> String {
    tempdir.path().join("goose.sqlite").display().to_string()
}

// ---------------------------------------------------------------------------
// Store layer
// ---------------------------------------------------------------------------

#[test]
fn external_sample_upsert_and_query() {
    let store = open_in_memory();

    store
        .upsert_external_sample(
            "apple_health",
            "hkquantity-hrv-001",
            "hrv_sdnn",
            45.2,
            "ms",
            1_700_000_000_000,
            1_700_000_060_000,
            "apple_watch",
        )
        .expect("upsert_external_sample failed");

    let rows = store
        .external_samples_between("hrv_sdnn", 1_699_999_999_000, 1_700_000_100_000)
        .expect("external_samples_between failed");

    assert_eq!(rows.len(), 1, "expected 1 row");
    let r = &rows[0];
    assert_eq!(r.platform, "apple_health");
    assert_eq!(r.platform_record_id, "hkquantity-hrv-001");
    assert_eq!(r.metric, "hrv_sdnn");
    assert_eq!(r.value, 45.2);
    assert_eq!(r.unit, "ms");
    assert_eq!(r.source, "apple_watch");
}

#[test]
fn external_sample_reupsert_is_idempotent_not_duplicated() {
    let store = open_in_memory();

    for _ in 0..2 {
        store
            .upsert_external_sample(
                "apple_health",
                "hkquantity-hr-001",
                "hr",
                62.0,
                "bpm",
                1_700_000_000_000,
                1_700_000_010_000,
                "apple_watch",
            )
            .expect("upsert_external_sample failed");
    }

    let rows = store
        .external_samples_between("hr", 1_699_999_999_000, 1_700_000_100_000)
        .expect("external_samples_between failed");

    assert_eq!(
        rows.len(),
        1,
        "re-importing the same (platform, platform_record_id, metric) must not duplicate rows"
    );
}

#[test]
fn external_sample_reupsert_with_changed_value_replaces_not_duplicates() {
    let store = open_in_memory();

    store
        .upsert_external_sample(
            "apple_health",
            "hkquantity-rhr-001",
            "rhr",
            58.0,
            "bpm",
            1_700_000_000_000,
            1_700_000_010_000,
            "apple_watch",
        )
        .expect("first upsert failed");

    store
        .upsert_external_sample(
            "apple_health",
            "hkquantity-rhr-001",
            "rhr",
            56.5,
            "bpm",
            1_700_000_000_000,
            1_700_000_010_000,
            "apple_watch",
        )
        .expect("second upsert failed");

    let rows = store
        .external_samples_between("rhr", 1_699_999_999_000, 1_700_000_100_000)
        .expect("external_samples_between failed");

    assert_eq!(
        rows.len(),
        1,
        "a corrected value must replace, not duplicate"
    );
    assert_eq!(
        rows[0].value, 56.5,
        "the replaced row must carry the new value"
    );
}

#[test]
fn external_samples_between_sorted_ascending() {
    let store = open_in_memory();

    let entries = [
        ("rec-3", 1_700_000_300_000i64, 1_700_000_310_000i64),
        ("rec-1", 1_700_000_100_000, 1_700_000_110_000),
        ("rec-2", 1_700_000_200_000, 1_700_000_210_000),
    ];
    for (record_id, start, end) in entries {
        store
            .upsert_external_sample(
                "apple_health",
                record_id,
                "spo2",
                97.0,
                "%",
                start,
                end,
                "apple_watch",
            )
            .expect("upsert_external_sample failed");
    }

    let rows = store
        .external_samples_between("spo2", 1_699_999_999_000, 1_700_000_400_000)
        .expect("external_samples_between failed");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].platform_record_id, "rec-1");
    assert_eq!(rows[1].platform_record_id, "rec-2");
    assert_eq!(rows[2].platform_record_id, "rec-3");
}

#[test]
fn external_samples_between_excludes_out_of_window_rows() {
    let store = open_in_memory();

    store
        .upsert_external_sample(
            "apple_health",
            "rec-in-window",
            "resp_rate",
            14.0,
            "breaths/min",
            1_700_000_100_000,
            1_700_000_110_000,
            "apple_watch",
        )
        .expect("upsert failed");
    store
        .upsert_external_sample(
            "apple_health",
            "rec-out-of-window",
            "resp_rate",
            15.0,
            "breaths/min",
            1_800_000_100_000,
            1_800_000_110_000,
            "apple_watch",
        )
        .expect("upsert failed");

    let rows = store
        .external_samples_between("resp_rate", 1_699_999_999_000, 1_700_000_200_000)
        .expect("external_samples_between failed");

    assert_eq!(rows.len(), 1, "only the in-window row must be returned");
    assert_eq!(rows[0].platform_record_id, "rec-in-window");
}

#[test]
fn latest_external_sample_in_window_returns_most_recent() {
    let store = open_in_memory();

    store
        .upsert_external_sample(
            "apple_health",
            "vo2-early",
            "vo2max",
            38.0,
            "ml/(kg*min)",
            1_700_000_100_000,
            1_700_000_110_000,
            "apple_watch",
        )
        .expect("upsert failed");
    store
        .upsert_external_sample(
            "apple_health",
            "vo2-late",
            "vo2max",
            40.5,
            "ml/(kg*min)",
            1_700_000_500_000,
            1_700_000_510_000,
            "apple_watch",
        )
        .expect("upsert failed");

    let latest = store
        .latest_external_sample_in_window("vo2max", 1_699_999_999_000, 1_700_000_600_000)
        .expect("latest_external_sample_in_window failed")
        .expect("expected a row");

    assert_eq!(latest.platform_record_id, "vo2-late");
    assert_eq!(latest.value, 40.5);
}

#[test]
fn latest_external_sample_in_window_none_when_empty() {
    let store = open_in_memory();
    let latest = store
        .latest_external_sample_in_window("vo2max", 0, 1_700_000_000_000)
        .expect("latest_external_sample_in_window failed");
    assert!(
        latest.is_none(),
        "no data must resolve to None, not a default"
    );
}

// ---------------------------------------------------------------------------
// Bridge layer
// ---------------------------------------------------------------------------

#[test]
fn bridge_import_samples_round_trip() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let resp = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "import-samples-1",
        "method": "external.import_samples",
        "args": {
            "database_path": db,
            "samples": [
                {
                    "platform": "apple_health",
                    "platform_record_id": "hr-001",
                    "metric": "hr",
                    "value": 70.0,
                    "unit": "bpm",
                    "start_unix_ms": 1_700_000_000_000i64,
                    "end_unix_ms": 1_700_000_010_000i64,
                    "source": "apple_watch"
                },
                {
                    "platform": "apple_health",
                    "platform_record_id": "hr-002",
                    "metric": "hr",
                    "value": 72.0,
                    "unit": "bpm",
                    "start_unix_ms": 1_700_000_100_000i64,
                    "end_unix_ms": 1_700_000_110_000i64,
                    "source": "apple_watch"
                }
            ]
        }
    }));

    assert!(resp.ok, "import_samples failed: {:?}", resp.error);
    assert_eq!(resp.result.unwrap()["imported"], 2);
}

#[test]
fn bridge_import_samples_is_idempotent_across_calls() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let payload = serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "import-samples-idempotent",
        "method": "external.import_samples",
        "args": {
            "database_path": db,
            "samples": [{
                "platform": "apple_health",
                "platform_record_id": "hr-dup-001",
                "metric": "hr",
                "value": 70.0,
                "unit": "bpm",
                "start_unix_ms": 1_700_000_000_000i64,
                "end_unix_ms": 1_700_000_010_000i64,
                "source": "apple_watch"
            }]
        }
    });

    let first = request(payload.clone());
    assert!(first.ok, "first import failed: {:?}", first.error);
    let second = request(payload);
    assert!(second.ok, "second import failed: {:?}", second.error);

    let resolved = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "resolve-after-dup",
        "method": "external.resolve_metric_for_day",
        "args": {
            "database_path": db,
            "metric": "hr",
            "start_unix_ms": 1_699_999_999_000i64,
            "end_unix_ms": 1_700_000_100_000i64,
            "whoop_value": null
        }
    }));
    assert!(resolved.ok, "resolve failed: {:?}", resolved.error);
    assert_eq!(
        resolved.result.unwrap()["value"],
        70.0,
        "duplicate import must not create a second row to resolve against"
    );
}

#[test]
fn bridge_import_daily_rollups_round_trip() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let resp = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "import-rollups-1",
        "method": "external.import_daily_rollups",
        "args": {
            "database_path": db,
            "rollups": [
                {
                    "date": "2026-07-01",
                    "source": "apple_health",
                    "steps": 8500,
                    "active_kcal": 320.5,
                    "vo2max": 41.2,
                    "weight_kg": 78.3
                },
                {
                    "date": "2026-07-02",
                    "source": "apple_health",
                    "steps": 9200
                }
            ]
        }
    }));

    assert!(resp.ok, "import_daily_rollups failed: {:?}", resp.error);
    assert_eq!(resp.result.unwrap()["imported"], 2);
}

#[test]
fn bridge_resolve_metric_prefers_whoop_over_apple_health() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "seed-external",
        "method": "external.import_samples",
        "args": {
            "database_path": db,
            "samples": [{
                "platform": "apple_health",
                "platform_record_id": "rhr-conflict-001",
                "metric": "rhr",
                "value": 60.0,
                "unit": "bpm",
                "start_unix_ms": 1_700_000_000_000i64,
                "end_unix_ms": 1_700_000_010_000i64,
                "source": "apple_watch"
            }]
        }
    }));

    let resolved = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "resolve-conflict",
        "method": "external.resolve_metric_for_day",
        "args": {
            "database_path": db,
            "metric": "rhr",
            "start_unix_ms": 1_699_999_999_000i64,
            "end_unix_ms": 1_700_000_100_000i64,
            "whoop_value": 55.0
        }
    }));

    assert!(resolved.ok, "resolve failed: {:?}", resolved.error);
    let result = resolved.result.unwrap();
    assert_eq!(result["value"], 55.0, "WHOOP value must win when present");
    assert_eq!(result["source"], "whoop");
}

#[test]
fn bridge_resolve_metric_falls_back_to_apple_health_when_whoop_absent() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "seed-fallback",
        "method": "external.import_samples",
        "args": {
            "database_path": db,
            "samples": [{
                "platform": "apple_health",
                "platform_record_id": "rhr-fallback-001",
                "metric": "rhr",
                "value": 61.5,
                "unit": "bpm",
                "start_unix_ms": 1_700_000_000_000i64,
                "end_unix_ms": 1_700_000_010_000i64,
                "source": "apple_watch"
            }]
        }
    }));

    let resolved = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "resolve-fallback",
        "method": "external.resolve_metric_for_day",
        "args": {
            "database_path": db,
            "metric": "rhr",
            "start_unix_ms": 1_699_999_999_000i64,
            "end_unix_ms": 1_700_000_100_000i64,
            "whoop_value": null
        }
    }));

    assert!(resolved.ok, "resolve failed: {:?}", resolved.error);
    let result = resolved.result.unwrap();
    assert_eq!(
        result["value"], 61.5,
        "Apple Health must fill the gap when WHOOP has no value"
    );
    assert_eq!(result["source"], "apple_health");
}

#[test]
fn bridge_resolve_metric_null_when_neither_source_has_data() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let resolved = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "resolve-empty",
        "method": "external.resolve_metric_for_day",
        "args": {
            "database_path": db,
            "metric": "spo2",
            "start_unix_ms": 0i64,
            "end_unix_ms": 1_700_000_000_000i64,
            "whoop_value": null
        }
    }));

    assert!(resolved.ok, "resolve failed: {:?}", resolved.error);
    let result = resolved.result.unwrap();
    assert!(
        result["value"].is_null(),
        "no data from either source must not fabricate a value"
    );
    assert!(result["source"].is_null());
}

#[test]
fn bridge_import_samples_rejects_invalid_metric() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let resp = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "import-invalid-metric",
        "method": "external.import_samples",
        "args": {
            "database_path": db,
            "samples": [{
                "platform": "apple_health",
                "platform_record_id": "bad-001",
                "metric": "not_a_real_metric",
                "value": 1.0,
                "unit": "x",
                "start_unix_ms": 0i64,
                "end_unix_ms": 1i64,
                "source": "apple_watch"
            }]
        }
    }));

    assert!(
        !resp.ok,
        "the CHECK(metric IN (...)) constraint must reject unknown metric kinds"
    );
}

#[test]
fn hrv_sdnn_is_never_conflated_with_a_different_metric_kind() {
    // Guard: healthkit_rmssd_must_not_be_written_as_sdnn. HRV from Apple Health is SDNN;
    // it must round-trip tagged as "hrv_sdnn", never merged with any other HRV metric kind.
    let store = open_in_memory();

    store
        .upsert_external_sample(
            "apple_health",
            "hrv-sdnn-001",
            "hrv_sdnn",
            50.0,
            "ms",
            1_700_000_000_000,
            1_700_000_010_000,
            "apple_watch",
        )
        .expect("upsert failed");

    let hrv_rows = store
        .external_samples_between("hrv_sdnn", 1_699_999_999_000, 1_700_000_100_000)
        .expect("query failed");
    assert_eq!(hrv_rows.len(), 1);
    assert_eq!(hrv_rows[0].metric, "hrv_sdnn");
    assert_eq!(hrv_rows[0].unit, "ms");

    // A query for an unrelated metric kind must never see the HRV row.
    let hr_rows = store
        .external_samples_between("hr", 1_699_999_999_000, 1_700_000_100_000)
        .expect("query failed");
    assert!(
        hr_rows.is_empty(),
        "hrv_sdnn rows must not leak into hr queries"
    );
}
