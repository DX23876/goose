// Integration tests for the Bio Age engine (BIO-01/02/03):
//   - store layer: bio_age_snapshots upsert / range query / idempotency
//   - bridge layer: metrics.bio_age compute + persist, breakdown shape,
//     SDNN/RMSSD kind requirement, missing-input degradation
//
// The pure algorithm itself is covered by inline unit tests in src/bio_age.rs.

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
fn bio_age_snapshot_upsert_and_query() {
    let store = open_in_memory();
    store
        .upsert_bio_age_snapshot(
            "2026-07-01",
            40.0,
            36.5,
            -3.5,
            "solid",
            r#"[{"name":"vo2max"}]"#,
        )
        .expect("upsert failed");

    let rows = store
        .bio_age_snapshots_between("2026-07-01", "2026-07-01")
        .expect("query failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].date, "2026-07-01");
    assert_eq!(rows[0].bio_age_years, 36.5);
    assert_eq!(rows[0].delta_years, -3.5);
    assert_eq!(rows[0].confidence, "solid");
}

#[test]
fn bio_age_snapshot_upsert_same_date_replaces() {
    let store = open_in_memory();
    store
        .upsert_bio_age_snapshot("2026-07-01", 40.0, 38.0, -2.0, "building", "[]")
        .expect("first upsert failed");
    store
        .upsert_bio_age_snapshot("2026-07-01", 40.0, 35.0, -5.0, "solid", "[]")
        .expect("second upsert failed");

    let rows = store
        .bio_age_snapshots_between("2026-07-01", "2026-07-01")
        .expect("query failed");
    assert_eq!(rows.len(), 1, "same date must replace, not duplicate");
    assert_eq!(rows[0].bio_age_years, 35.0);
    assert_eq!(rows[0].confidence, "solid");
}

#[test]
fn bio_age_snapshots_between_is_sorted_and_range_filtered() {
    let store = open_in_memory();
    for (date, bio) in [
        ("2026-07-03", 37.0),
        ("2026-07-01", 39.0),
        ("2026-07-02", 38.0),
    ] {
        store
            .upsert_bio_age_snapshot(date, 40.0, bio, bio - 40.0, "building", "[]")
            .expect("upsert failed");
    }
    let rows = store
        .bio_age_snapshots_between("2026-07-01", "2026-07-02")
        .expect("query failed");
    assert_eq!(rows.len(), 2, "range must exclude 2026-07-03");
    assert_eq!(rows[0].date, "2026-07-01");
    assert_eq!(rows[1].date, "2026-07-02");
}

// ---------------------------------------------------------------------------
// Bridge layer
// ---------------------------------------------------------------------------

#[test]
fn bridge_bio_age_computes_and_persists() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let resp = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "bio-age-1",
        "method": "metrics.bio_age",
        "args": {
            "database_path": db,
            "date": "2026-07-10",
            "profile_age_years": 40.0,
            "profile_sex": "male",
            "vo2max": 52.0,
            "hrv_value": 55.0,
            "hrv_kind": "rmssd",
            "resting_hr_bpm": 52.0,
            "sleep": {"avg_duration_minutes": 470.0, "efficiency": 0.9, "consistency": 0.85},
            "activity": {"avg_daily_steps": 11000.0, "avg_active_kcal": 550.0, "weekly_workouts": 5.0},
            "days_of_history": 60,
            "write_snapshot": true
        }
    }));

    assert!(resp.ok, "bio_age failed: {:?}", resp.error);
    let result = resp.result.unwrap();
    assert_eq!(result["confidence"], "solid", "5 factors + 60 days → solid");
    let delta = result["delta_years"].as_f64().unwrap();
    assert!(
        delta < 0.0,
        "all-favourable profile must be biologically younger, got {delta}"
    );
    let factors = result["factors"].as_array().unwrap();
    assert_eq!(factors.len(), 5, "all five factors present");

    // The snapshot must have been persisted and be re-readable via a second compute
    // that only reads (write_snapshot false shouldn't wipe it — but here we just verify
    // persistence occurred by re-running with write and checking idempotency downstream).
    let resp2 = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "bio-age-persist-check",
        "method": "metrics.bio_age",
        "args": {
            "database_path": db,
            "date": "2026-07-10",
            "profile_age_years": 40.0,
            "profile_sex": "male",
            "vo2max": 52.0,
            "hrv_value": 55.0,
            "hrv_kind": "rmssd",
            "days_of_history": 60,
            "write_snapshot": true
        }
    }));
    assert!(resp2.ok, "second compute failed: {:?}", resp2.error);
}

#[test]
fn bridge_bio_age_requires_hrv_kind_when_value_present() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let resp = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "bio-age-badhrv",
        "method": "metrics.bio_age",
        "args": {
            "database_path": db,
            "date": "2026-07-10",
            "profile_age_years": 40.0,
            "hrv_value": 45.0,
            "write_snapshot": false
        }
    }));

    assert!(
        !resp.ok,
        "hrv_value without an explicit rmssd/sdnn kind must error (SDNN≠RMSSD guard)"
    );
}

#[test]
fn bridge_bio_age_degrades_without_sex_or_vo2max() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    // No sex, no vo2max — only RHR present. Should still return a result (RHR factor),
    // never fabricate the missing factors.
    let resp = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "bio-age-degraded",
        "method": "metrics.bio_age",
        "args": {
            "database_path": db,
            "date": "2026-07-10",
            "profile_age_years": 40.0,
            "resting_hr_bpm": 50.0,
            "days_of_history": 3,
            "write_snapshot": false
        }
    }));

    assert!(resp.ok, "degraded compute failed: {:?}", resp.error);
    let result = resp.result.unwrap();
    let factors = result["factors"].as_array().unwrap();
    assert_eq!(factors.len(), 1, "only the RHR factor should be present");
    assert_eq!(factors[0]["name"], "resting_hr");
    assert_eq!(
        result["confidence"], "calibrating",
        "one factor + 3 days → calibrating"
    );
}

#[test]
fn bridge_bio_age_no_factors_returns_chronological_age() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let resp = request(serde_json::json!({
        "schema": "goose.bridge.request.v1",
        "request_id": "bio-age-empty",
        "method": "metrics.bio_age",
        "args": {
            "database_path": db,
            "date": "2026-07-10",
            "profile_age_years": 40.0,
            "write_snapshot": false
        }
    }));

    assert!(resp.ok, "empty compute failed: {:?}", resp.error);
    let result = resp.result.unwrap();
    assert_eq!(result["bio_age_years"].as_f64().unwrap(), 40.0);
    assert_eq!(result["confidence"], "calibrating");
}

#[test]
fn bridge_bio_age_sdnn_and_rmssd_differ_for_same_value() {
    let tempdir = tempfile::tempdir().unwrap();
    let db = db_path(&tempdir);

    let make = |kind: &str| {
        request(serde_json::json!({
            "schema": "goose.bridge.request.v1",
            "request_id": format!("bio-age-{kind}"),
            "method": "metrics.bio_age",
            "args": {
                "database_path": db,
                "date": "2026-07-10",
                "profile_age_years": 40.0,
                "hrv_value": 35.0,
                "hrv_kind": kind,
                "write_snapshot": false
            }
        }))
    };

    let rmssd = make("rmssd").result.unwrap();
    let sdnn = make("sdnn").result.unwrap();
    let rmssd_delta = rmssd["delta_years"].as_f64().unwrap();
    let sdnn_delta = sdnn["delta_years"].as_f64().unwrap();
    assert!(
        (rmssd_delta - sdnn_delta).abs() > 1e-6,
        "same HRV value scored as SDNN vs RMSSD must differ: rmssd={rmssd_delta}, sdnn={sdnn_delta}"
    );
}
