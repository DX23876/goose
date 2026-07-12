use std::{
    collections::HashSet,
    ffi::{CStr, CString},
    os::raw::c_char,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::Instant,
};

use r2d2_sqlite::SqliteConnectionManager;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    GooseError, GooseResult,
    metrics::built_in_algorithm_definitions,
    openwhoop_reference::{
        OPENWHOOP_REFERENCE_ATTRIBUTION, OPENWHOOP_REFERENCE_COMMIT,
        OPENWHOOP_REFERENCE_LICENSE_CAVEAT, OPENWHOOP_REFERENCE_REPOSITORY,
        OPENWHOOP_REFERENCE_SNAPSHOT_URL, openwhoop_history_field_references,
        whoop_generation_references,
    },
    perf_budget::DEFAULT_PERF_SCALE,
    property_tests::{DEFAULT_CASES_PER_GROUP, DEFAULT_PROPERTY_SEED},
    protocol::DeviceType,
    store::{CURRENT_SCHEMA_VERSION, GooseStore},
};

mod activity;
mod body_composition;
mod capabilities;
mod capture;
mod debug;
mod external;
mod metrics;
mod realtime;
mod sleep;

pub const BRIDGE_REQUEST_SCHEMA: &str = "goose.bridge.request.v1";
pub const BRIDGE_RESPONSE_SCHEMA: &str = "goose.bridge.response.v1";
pub const CAPTURE_ARRIVAL_PLAN_REPORT_SCHEMA: &str = "goose.capture-arrival-plan-report.v1";
pub const BRIDGE_METHODS_LIST_SCHEMA: &str = "goose.bridge.methods-list.v1";

/// Canonical list of every bridge RPC method understood by
/// [`handle_bridge_request`].
///
/// The list is kept sorted and is verified against the dispatcher match arms
/// by `tests::bridge_methods_constant_matches_dispatcher` so it cannot drift
/// when new methods are added. Exposed via the `core.list_methods` RPC for
/// discoverability by external clients (the Swift app, future Android port,
/// debug tooling).
pub const BRIDGE_METHODS: &[&str] = &[
    "activity.apply_correction",
    "activity.attach_interval",
    "activity.attach_metric",
    "activity.attach_metrics",
    "activity.correction_plans",
    "activity.create_session",
    "activity.delete_session",
    "activity.get_session",
    "activity.list_intervals",
    "activity.list_metrics",
    "activity.list_sessions",
    "activity.list_sessions_with_metrics",
    "activity.metrics_for_session_in_window",
    "activity.update_session",
    "apple_daily.upsert",
    "battery.parse_cmd26_response",
    "battery.parse_event48_payload",
    "biometrics.insert_v20v21_batch",
    "biometrics.insert_v24_batch",
    "biometrics.insert_v26_batch",
    "biometrics.optical_between",
    "biometrics.spo2_from_raw",
    "biometrics.v24_between",
    "body_composition.history_between",
    "body_composition.upsert",
    "calibration.apply",
    "calibration.evaluate_dataset",
    "calibration.evaluate_stored_labels",
    "calibration.import_labels",
    "calibration.list_labels",
    "capabilities.get_feature_flags",
    "capabilities.upsert_feature_flags",
    "capture.arrival_plan",
    "capture.correlation_report",
    "capture.finish_session",
    "capture.import_frame_batch",
    "capture.list_sessions",
    "capture.observability_timeline",
    "capture.sanitize",
    "capture.start_session",
    "capture.timeline",
    "commands.capture_plan",
    "commands.definitions",
    "commands.direct_send_gate",
    "commands.direct_send_preflight",
    "commands.evidence_from_emulator_log",
    "commands.evidence_template",
    "commands.import_validation_records",
    "commands.list_validation_records",
    "commands.promote_local_frame_matches",
    "commands.validate_evidence",
    "core.list_methods",
    "core.version",
    "debug.finish_command",
    "debug.record_event",
    "debug.session_snapshot",
    "debug.start_command",
    "debug.start_session",
    "device.capabilities",
    "diagnostics.perf_budget",
    "diagnostics.property_suite",
    "exercise.detect_sessions",
    "exercise.sessions_between",
    "export.raw_timeframe",
    "export.validate_bundle",
    "external.import_daily_rollups",
    "external.import_samples",
    "external.resolve_metric_for_day",
    "health_sync.activity_dry_run",
    "health_sync.dry_run",
    "historical_sync.dry_run",
    "historical_sync.physical_evidence_template",
    "historical_sync.validate_physical_evidence",
    "journal.upsert",
    "metric_series.query_range",
    "metric_series.upsert",
    "metrics.activity_unavailable_daily_status",
    "metrics.bio_age",
    "metrics.built_in_definitions",
    "metrics.daily_activity_metrics",
    "metrics.daily_recovery_metrics",
    "metrics.default_preferences",
    "metrics.energy_capture_validation",
    "metrics.energy_daily_rollup",
    "metrics.energy_hourly_rollup",
    "metrics.energy_unavailable_daily_status",
    "metrics.fit_strain_denominator",
    "metrics.goose_hrv_v0",
    "metrics.goose_readiness_v1",
    "metrics.goose_recovery_v0",
    "metrics.goose_recovery_v1",
    "metrics.goose_sleep_v0",
    "metrics.goose_sleep_v1",
    "metrics.goose_strain_v0",
    "metrics.goose_strain_v1",
    "metrics.goose_stress_v0",
    "metrics.heart_rate_features",
    "metrics.hourly_activity_metrics",
    "metrics.hrv_capture_validation",
    "metrics.hrv_features",
    "metrics.imu_step_count_from_decoded_frames",
    "metrics.imu_step_count_v1",
    "metrics.input_readiness",
    "metrics.motion_features",
    "metrics.oxygen_saturation_capture_validation",
    "metrics.raw_motion_step_estimate",
    "metrics.recovery_score_from_features",
    "metrics.recovery_sensor_daily_rollup",
    "metrics.recovery_sensor_discovery",
    "metrics.recovery_unavailable_daily_status",
    "metrics.reference_compare",
    "metrics.reference_definitions",
    "metrics.respiratory_rate_capture_validation",
    "metrics.resting_hr_capture_validation",
    "metrics.resting_hr_daily_rollup",
    "metrics.resting_hr_features",
    "metrics.sleep_score_from_features",
    "metrics.sleep_staging",
    "metrics.step_capture_validation",
    "metrics.step_counter_daily_rollup",
    "metrics.step_counter_hourly_rollup",
    "metrics.step_counter_ingest",
    "metrics.step_packet_discovery",
    "metrics.strain_score_from_features",
    "metrics.stress_score_from_features",
    "metrics.temperature_capture_validation",
    "metrics.vital_event_features",
    "metrics.window_features",
    "openwhoop.reference_report",
    "overnight.mirror_batch",
    "overnight.mirror_counts",
    "privacy.lint",
    "protocol.parse_frame_hex",
    "protocol.parse_frame_hex_batch",
    "realtime.insert_frame",
    "settings.apply_default_algorithm_preferences",
    "settings.get_algorithm_preference",
    "settings.list_algorithm_preferences",
    "settings.set_algorithm_preference",
    "sleep.add_correction_label",
    "sleep.compute_need",
    "sleep.import_external_history",
    "sleep.list_correction_labels",
    "sleep.validate_stage_labels",
    "sleep.validate_v1_evidence_folder",
    "sleep.validate_v1_explanation_stability",
    "sleep.validate_v1_release_gates",
    "sleep.validate_window_labels",
    "storage.check",
    "storage.compact_raw_evidence",
    "store.ewma_baseline_fold_history",
    "store.ewma_baseline_update",
    "store.gravity2_samples_between",
    "store.gravity_rows_between",
    "store.hk_hr_samples_between",
    "store.hk_sleep_sessions_between",
    "store.hk_spo2_samples_between",
    "store.insert_gravity2_batch",
    "store.insert_gravity_rows",
    "sync.backfill_streams",
    "sync.mark_synced",
    "sync.record_hps_telemetry",
    "sync.rows_pending_upload",
    "timeline.from_decoded_frames",
    "ui_coverage.audit",
    "upload.get_raw_frames_for_upload",
    "upload.get_recent_decoded_streams",
    "validation.local_health_manifest_review",
    "validation.local_health_manifest_runbook",
    "validation.local_health_manifest_scaffold",
    "workout.upsert",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeRequest {
    pub schema: String,
    pub request_id: String,
    pub method: String,
    #[serde(default)]
    pub args: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeResponse {
    pub schema: String,
    pub request_id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<BridgeError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timing: Option<BridgeTiming>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeTiming {
    pub method: String,
    pub method_elapsed_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeError {
    pub code: String,
    pub message: String,
}

// --- Battery parsing (BAT-01 / BAT-02) ---
//
// All byte-level parsing is in Rust per D-02. Swift calls these bridge methods
// and passes the result directly to applyBatteryLevel().

/// Parse Event-48 battery percentage from a full event payload.
///
/// Byte layout (absolute payload offsets, BAT-01):
///   0-1   packet_type + sequence
///   2-3   event_id (u16 LE)
///   4-7   timestamp_seconds (u32 LE)
///   8-9   timestamp_subseconds (u16 LE)
///   10-11 padding/reserved
///   12+   event data body (data_hex in parse_event_payload uses payload[12..])
///
/// The battery raw u16 sits at ABSOLUTE offset 17, which is offset 5 within the
/// data body (offset 12 + 5 = offset 17). Both anchors refer to the same byte.
/// See also: parse_event48_battery_from_data() which uses the data-body anchor (5).
///
/// Guard (D-05): raw > 1100 is rejected (battery_pct_raw = raw / 10 would exceed 110%).
fn parse_event48_battery(payload: &[u8]) -> GooseResult<u16> {
    // offset 17: u16 LE, battery_raw (÷10 = battery_pct, 0–100); max guard 1100
    //   event-48 data body starts at payload[12]; absolute offset 17 = data body byte 5
    //   empirically confirmed via hardware captures
    let raw = crate::protocol::read_u16_le(payload, 17)
        .ok_or_else(|| GooseError::message("event48 payload too short for battery offset 17"))?;
    if raw > 1100 {
        return Err(GooseError::message(format!(
            "event48 battery raw={raw} exceeds sanity guard 1100"
        )));
    }
    Ok(raw / 10)
}

/// Parse Event-48 battery percentage from the **data body** bytes (payload[12..]).
///
/// Identical logic to parse_event48_battery() but anchored at data-body offset 5
/// instead of absolute payload offset 17. Used by compact_parsed_frame_summary()
/// which receives data_hex (the data body, not the full payload) from ParsedPayload::Event.
///
/// The two offsets refer to the same physical byte:
///   absolute payload offset 17 == data body offset 5 (because data body starts at offset 12).
#[allow(dead_code)]
pub(crate) fn parse_event48_battery_from_data(data: &[u8]) -> Option<u16> {
    // offset 5 (data body): u16 LE, battery_raw (÷10 = battery_pct, 0–100)
    //   data body passed as payload[12..]; same physical byte as absolute payload offset 17
    //   empirically confirmed via hardware captures
    let raw = crate::protocol::read_u16_le(data, 5)?;
    if raw > 1100 {
        return None;
    }
    Some(raw / 10)
}

/// Parse Cmd 26 (GET_BATTERY_LEVEL) response payload.
///
/// Byte layout of the full Gen4 COMMAND_RESPONSE payload (BAT-02):
///   [0]   packet_type (36 = COMMAND_RESPONSE)
///   [1]   response sequence
///   [2]   command identifier (26 = GET_BATTERY_LEVEL)
///   [3]   origin_sequence from the sent command
///   [4]   result code (1 = SUCCESS)
///   [5-6] battery raw u16 LE (data body starts here)
///
/// Guard (D-05): payload.len() >= 7 before reading bytes[5..7].
fn parse_cmd26_battery(payload: &[u8]) -> GooseResult<u16> {
    if payload.len() < 7 {
        return Err(GooseError::message(format!(
            "cmd26 payload too short for data body: {} < 7",
            payload.len()
        )));
    }
    // payload[5..7]: data body, battery raw u16 LE (BAT-02)
    let raw = u16::from(payload[5]) | u16::from(payload[6]) << 8;
    if raw > 1000 {
        return Err(GooseError::message(format!(
            "cmd26 battery raw={raw} exceeds sanity guard 1000"
        )));
    }
    Ok(raw / 10)
}

#[derive(Debug, Deserialize)]
struct ParseEvent48BatteryArgs {
    payload_hex: String,
}

fn parse_event48_battery_bridge(args: ParseEvent48BatteryArgs) -> GooseResult<serde_json::Value> {
    let payload = hex::decode(&args.payload_hex)
        .map_err(|e| GooseError::message(format!("invalid hex: {e}")))?;
    let pct = parse_event48_battery(&payload)?;
    // Key matches what NotificationFrameParsing.swift reads at raw["event48_battery_pct"]
    Ok(json!({ "event48_battery_pct": pct }))
}

#[derive(Debug, Deserialize)]
struct ParseCmd26ResponseArgs {
    payload_hex: String,
}

fn parse_cmd26_battery_bridge(args: ParseCmd26ResponseArgs) -> GooseResult<serde_json::Value> {
    let payload = hex::decode(&args.payload_hex)
        .map_err(|e| GooseError::message(format!("invalid hex: {e}")))?;
    let pct = parse_cmd26_battery(&payload)?;
    Ok(json!({ "battery_pct": pct }))
}

pub fn core_version_payload() -> serde_json::Value {
    json!({
        "core_version": option_env!("CARGO_PKG_VERSION").unwrap_or("unknown"),
        "crate_name": option_env!("CARGO_PKG_NAME").unwrap_or("goose-core"),
        "bridge_request_schema": BRIDGE_REQUEST_SCHEMA,
        "bridge_response_schema": BRIDGE_RESPONSE_SCHEMA,
        "storage_schema_version": CURRENT_SCHEMA_VERSION,
    })
}

// Payload returned by the `core.list_methods` bridge RPC.
// Returns the canonical, alphabetically sorted list of every bridge method
/// the current build understands, alongside the methods-list schema id and
/// the count. Intended for client-side discovery: the iOS app, a future
/// Android port, debug tooling, or anyone wiring a new front end can pull
/// the live list at runtime instead of grepping the Rust source.
///
/// The list itself is the compile-time constant [`BRIDGE_METHODS`]; this
/// function exists only to wrap it in the bridge response envelope.
pub fn core_list_methods_payload() -> serde_json::Value {
    json!({
        "schema": BRIDGE_METHODS_LIST_SCHEMA,
        "count": BRIDGE_METHODS.len(),
        "methods": BRIDGE_METHODS,
    })
}

pub fn openwhoop_reference_report_payload() -> serde_json::Value {
    let service_roles = whoop_generation_references()
        .iter()
        .map(|reference| {
            json!({
                "generation": reference.generation.as_str(),
                "service_uuid": reference.service_uuid,
                "characteristic_roles": [
                    {
                        "role": "command_to_strap",
                        "uuid": reference.command_to_strap_uuid,
                    },
                    {
                        "role": "command_from_strap",
                        "uuid": reference.command_from_strap_uuid,
                    },
                    {
                        "role": "events_from_strap",
                        "uuid": reference.events_from_strap_uuid,
                    },
                    {
                        "role": "data_from_strap",
                        "uuid": reference.data_from_strap_uuid,
                    },
                    {
                        "role": "memfault",
                        "uuid": reference.memfault_uuid,
                    },
                ],
            })
        })
        .collect::<Vec<_>>();
    let history_fields = openwhoop_history_field_references()
        .iter()
        .map(|reference| {
            json!({
                "field": reference.field.as_str(),
                "gen4": reference.gen4,
                "gen5": reference.gen5,
                "goose_summary_kinds": reference.goose_summary_kinds,
                "status": reference.status.as_str(),
                "note": reference.note,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "schema": "goose.openwhoop-reference-report.v1",
        "generated_by": "goose-bridge",
        "snapshot": {
            "repository": OPENWHOOP_REFERENCE_REPOSITORY,
            "commit": OPENWHOOP_REFERENCE_COMMIT,
            "snapshot_url": OPENWHOOP_REFERENCE_SNAPSHOT_URL,
            "attribution": OPENWHOOP_REFERENCE_ATTRIBUTION,
            "license_caveat": OPENWHOOP_REFERENCE_LICENSE_CAVEAT,
        },
        "service_roles": service_roles,
        "service_role_count": service_roles.len(),
        "history_fields": history_fields,
        "history_field_count": history_fields.len(),
    })
}

pub fn handle_bridge_request_json(request_json: &str) -> String {
    let response = match serde_json::from_str::<BridgeRequest>(request_json) {
        Ok(request) => handle_bridge_request(request),
        Err(error) => BridgeResponse {
            schema: BRIDGE_RESPONSE_SCHEMA.to_string(),
            request_id: "unknown".to_string(),
            ok: false,
            result: None,
            error: Some(BridgeError {
                code: "invalid_json".to_string(),
                message: error.to_string(),
            }),
            timing: None,
        },
    };
    serialize_response(&response)
}

pub fn handle_bridge_request(request: BridgeRequest) -> BridgeResponse {
    let method = request.method.clone();
    let started = Instant::now();
    let mut response = handle_bridge_request_inner(request);
    response.timing = Some(BridgeTiming {
        method,
        method_elapsed_us: elapsed_us_u64(started),
    });
    response
}

fn handle_bridge_request_inner(request: BridgeRequest) -> BridgeResponse {
    if request.schema != BRIDGE_REQUEST_SCHEMA {
        return bridge_error(
            &request.request_id,
            "unsupported_schema",
            format!(
                "expected schema {BRIDGE_REQUEST_SCHEMA}, got {}",
                request.schema
            ),
        );
    }
    if request.request_id.trim().is_empty() {
        return bridge_error("unknown", "invalid_request", "request_id is required");
    }

    // Route by method namespace prefix to the appropriate domain dispatcher.
    // Each domain dispatcher returns bridge_error("not_implemented", ...) until
    // Wave 2 fills in the real dispatch arms (Plans 86-02 through 86-05).
    //
    // Special cases handled inline here:
    //   - core.* — thin wrappers, no domain module needed
    //   - openwhoop.* — single method, no storage, handled inline
    //   - battery.* — byte-level parsing, kept in mod.rs per BAT-01/BAT-02
    let method = request.method.as_str();

    // core.* — infrastructure methods, no domain module
    if method == "core.version" {
        return bridge_ok(&request.request_id, core_version_payload());
    }
    if method == "core.list_methods" {
        return bridge_ok(&request.request_id, core_list_methods_payload());
    }

    // openwhoop.* — single reference-data method
    if method == "openwhoop.reference_report" {
        return bridge_ok(&request.request_id, openwhoop_reference_report_payload());
    }

    // battery.* — byte-level parsing kept in mod.rs (BAT-01 / BAT-02)
    if method == "battery.parse_event48_payload" {
        return request_args::<ParseEvent48BatteryArgs>(&request)
            .and_then(parse_event48_battery_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error));
    }
    if method == "battery.parse_cmd26_response" {
        return request_args::<ParseCmd26ResponseArgs>(&request)
            .and_then(parse_cmd26_battery_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error));
    }

    // Domain prefix routing (D-01 / D-02 from Phase 86 CONTEXT.md):
    //   metrics domain: metrics.*, metric_series.*, exercise.*, biometrics.*,
    //                   battery.* (handled above), calibration.*, openwhoop.*,
    //                   diagnostics.*
    if method.starts_with("body_composition.") {
        return body_composition::dispatch_body_composition(&request);
    }

    if method.starts_with("capabilities.") {
        return capabilities::dispatch_capabilities(&request);
    }

    if method.starts_with("realtime.") {
        return realtime::dispatch_realtime(&request);
    }

    if method.starts_with("external.") {
        return external::dispatch_external(&request);
    }

    if method.starts_with("metrics.")
        || method.starts_with("metric_series.")
        || method.starts_with("exercise.")
        || method.starts_with("biometrics.")
        || method.starts_with("calibration.")
        || method.starts_with("diagnostics.")
    {
        return metrics::dispatch_metrics(&request);
    }

    //   sleep domain: sleep.*, overnight.*, health_sync.*
    if method.starts_with("sleep.")
        || method.starts_with("overnight.")
        || method.starts_with("health_sync.")
    {
        return sleep::dispatch_sleep(&request);
    }

    //   capture domain: capture.*, protocol.*, historical_sync.*, sync.*
    if method.starts_with("capture.")
        || method.starts_with("protocol.")
        || method.starts_with("historical_sync.")
        || method.starts_with("sync.")
    {
        return capture::dispatch_capture(&request);
    }

    //   activity domain: activity.*, workout.*, apple_daily.*, journal.*, timeline.*
    if method.starts_with("activity.")
        || method.starts_with("workout.")
        || method.starts_with("apple_daily.")
        || method.starts_with("journal.")
        || method.starts_with("timeline.")
    {
        return activity::dispatch_activity(&request);
    }

    //   debug domain: debug.*, commands.*, core.* (handled above), settings.*,
    //                 storage.*, store.*, export.*, upload.*, privacy.*,
    //                 ui_coverage.*, device.*, local_health.*, validation.*
    if method.starts_with("debug.")
        || method.starts_with("commands.")
        || method.starts_with("settings.")
        || method.starts_with("storage.")
        || method.starts_with("store.")
        || method.starts_with("export.")
        || method.starts_with("upload.")
        || method.starts_with("privacy.")
        || method.starts_with("ui_coverage.")
        || method.starts_with("device.")
        || method.starts_with("local_health.")
        || method.starts_with("validation.")
    {
        return debug::dispatch_debug(&request);
    }

    // Test-only arm: deterministic panic trigger for FFI catch_unwind coverage.
    // Gated on debug_assertions (true in test/dev, false in release) so it is
    // never compiled into the release static library (satisfies T-09-04 threat model).
    // Note: #[cfg(test)] is not used here because integration tests compile the crate
    // in library mode without activating cfg(test) on the dependency; debug_assertions
    // achieves the same release-exclusion guarantee for this use case.
    #[cfg(debug_assertions)]
    if method == "test.panic" {
        panic!("test.panic: intentional panic for FFI catch_unwind coverage");
    }

    bridge_error(
        &request.request_id,
        "unknown_method",
        format!("unsupported bridge method: {method}"),
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn goose_core_version_json() -> *mut c_char {
    json_to_c_string(core_version_payload())
}

/// Handle a JSON-encoded bridge request from the host platform.
///
/// Returns a newly-allocated, null-terminated UTF-8 C string containing a
/// JSON-encoded response. The caller takes ownership of the returned pointer
/// and **must** release it by passing it to [`goose_bridge_free_string`].
/// Mixing this allocation with `free(3)` or any other deallocator is
/// undefined behaviour.
///
/// # Safety
///
/// The caller must ensure that:
///
/// - `request_json` is either null **or** a valid pointer to a
///   null-terminated UTF-8 C string that remains valid (and unmodified by
///   other threads) for the duration of this call.
/// - The buffer referenced by `request_json` is not aliased by any mutable
///   reference for the duration of this call.
///
/// A null `request_json` is handled defensively and returns a structured
/// error response rather than dereferencing the pointer. Invalid UTF-8 in the
/// input is likewise reported as a structured error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn goose_bridge_handle_json(request_json: *const c_char) -> *mut c_char {
    if request_json.is_null() {
        return response_to_c_string(&bridge_error(
            "unknown",
            "null_request",
            "request_json pointer is null",
        ));
    }

    // SAFETY: request_json is non-null (checked above) and points to a valid
    //   null-terminated C string owned by the caller for the duration of this call.
    //   No mutable alias exists — caller contract in the # Safety doc comment above.
    //   Called from Swift via GooseSwift-Bridging-Header.h (iOS) and from the JNI shim (Android).
    let request = match unsafe { CStr::from_ptr(request_json) }.to_str() {
        Ok(request) => request,
        Err(error) => {
            return response_to_c_string(&bridge_error(
                "unknown",
                "invalid_utf8",
                error.to_string(),
            ));
        }
    };
    // Wrap ALL panic-prone work inside catch_unwind so that a panic in dispatch
    // is caught at the FFI boundary and returned as a structured JSON error instead
    // of aborting the process. AssertUnwindSafe is sound here because the closure
    // does not alias mutable state that would be left in an inconsistent state on
    // unwind — the bridge is stateless across calls and the only side effect is the
    // returned C string allocation.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        string_to_c_string(handle_bridge_request_json(request))
    })) {
        Ok(ptr) => ptr,
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic payload".to_string());
            response_to_c_string(&bridge_error("unknown", "panic", message))
        }
    }
}

/// Free a C string previously returned by any `goose_bridge_*` or
/// `goose_core_*` function.
///
/// # Safety
///
/// The caller must ensure that:
///
/// - `value` is either null **or** a pointer that was returned by a Goose
///   bridge entry point (e.g. [`goose_bridge_handle_json`] or
///   `goose_core_version_json`) and has not yet been freed.
/// - The pointer is not aliased by any other live reference and is not used
///   after this call returns.
///
/// Passing a pointer that was not produced by the Goose core (for example,
/// one allocated by `malloc(3)` on the host) is undefined behaviour, because
/// the Rust allocator backing [`CString`] is not guaranteed to match the
/// host's allocator. A null pointer is handled as a no-op. Calling this
/// function twice on the same pointer is a double-free.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn goose_bridge_free_string(value: *mut c_char) {
    if value.is_null() {
        return;
    }
    // Reconstructing the CString transfers ownership back to Rust so it can be dropped once.
    drop(unsafe { CString::from_raw(value) });
}

// --- Shared utility functions ---

// These utility functions are used by bridge domain files (metrics.rs, sleep.rs,
// capture.rs, activity.rs, debug.rs). The `dead_code` lint fires here because those
// files are still being filled in during Phase 86.
#[allow(dead_code)]
pub(crate) fn open_bridge_store(database_path: &str) -> GooseResult<GooseStore> {
    if database_path.trim().is_empty() {
        return Err(GooseError::message("database_path is required"));
    }
    validate_no_traversal("database_path", database_path)?;
    GooseStore::open(Path::new(database_path))
}

#[allow(dead_code)]
pub(crate) fn open_bridge_store_hot(database_path: &str) -> GooseResult<GooseStore> {
    if database_path.trim().is_empty() {
        return Err(GooseError::message("database_path is required"));
    }
    validate_no_traversal("database_path", database_path)?;
    let path = Path::new(database_path);
    GooseStore::open_existing_current(path).or_else(|_| GooseStore::open(path))
}

/// Type aliases for the r2d2 SQLite connection pool used by bridge handlers.
#[allow(dead_code)]
type BridgePool = r2d2::Pool<SqliteConnectionManager>;
/// A checked-out pooled connection. Derefs to `rusqlite::Connection`.
pub(crate) type BridgePoolConn = r2d2::PooledConnection<SqliteConnectionManager>;

/// Process-lifetime r2d2 connection pool for bridge handlers.
/// Initialised on first call to `checkout_bridge_conn`; subsequent calls
/// return a connection from the pool without re-opening the database.
/// Uses Mutex<Option<BridgePool>> because OnceLock::get_or_try_init is
/// not yet stable on Rust 1.96.
#[allow(dead_code)]
static BRIDGE_CONN_POOL: OnceLock<Mutex<Option<BridgePool>>> = OnceLock::new();

/// Initialise the r2d2 pool for `database_path`.
/// Runs schema migration once via `GooseStore::open`, then builds the pool.
#[allow(dead_code)]
fn init_bridge_pool(database_path: &str) -> GooseResult<BridgePool> {
    validate_no_traversal("database_path", database_path)?;
    // Run migration exactly once before handing the path to the pool manager.
    GooseStore::open(Path::new(database_path))?;
    let manager = SqliteConnectionManager::file(database_path)
        .with_flags(
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        )
        .with_init(|conn| {
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
            Ok(())
        });
    r2d2::Pool::builder()
        .max_size(4)
        .build(manager)
        .map_err(|e| GooseError::message(format!("r2d2 pool init: {e}")))
}

/// Acquire a pooled SQLite connection for `database_path`.
///
/// On first call for a given process lifetime, initialises the pool and runs
/// schema migration. Subsequent calls return a checked-out connection from the
/// pool without re-opening the database.
///
/// This replaces `acquire_bridge_conn` and `open_bridge_store_hot` at bridge
/// handler call sites that use raw rusqlite operations.
#[allow(dead_code)]
pub(crate) fn checkout_bridge_conn(database_path: &str) -> GooseResult<BridgePoolConn> {
    if database_path.trim().is_empty() {
        return Err(GooseError::message("database_path is required"));
    }
    let cell = BRIDGE_CONN_POOL.get_or_init(|| Mutex::new(None));
    let mut guard = cell
        .lock()
        .map_err(|_| GooseError::message("bridge pool mutex poisoned"))?;
    if guard.is_none() {
        *guard = Some(init_bridge_pool(database_path)?);
    }
    guard
        .as_ref()
        .expect("invariant: pool was just initialised above")
        .get()
        .map_err(|e| GooseError::message(format!("pool checkout: {e}")))
}

/// Set of database paths that have been opened and migrated at least once in this
/// process lifetime. Used by `acquire_bridge_conn` to skip redundant migrations
/// on subsequent opens of the same path, reducing per-call overhead.
static BRIDGE_MIGRATED_PATHS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Acquire a `GooseStore` for `database_path`, skipping the schema migration on
/// subsequent calls for the same path within this process lifetime.
///
/// First call for a given path: validates the path, opens the store, and runs
/// `migrate()` (same as `open_bridge_store`). Records the path as migrated.
///
/// Subsequent calls for the same path: opens via `open_existing_current` (no
/// migration), falling back to a full `open` only if the schema version check
/// fails — matching the behaviour of `open_bridge_store_hot`.
///
/// This is a drop-in replacement for `open_bridge_store` at call sites that are
/// called on every bridge request: it eliminates per-call migration overhead while
/// keeping the first-open correctness guarantee.
#[allow(dead_code)]
pub(crate) fn acquire_bridge_conn(database_path: &str) -> GooseResult<GooseStore> {
    if database_path.trim().is_empty() {
        return Err(GooseError::message("database_path is required"));
    }
    validate_no_traversal("database_path", database_path)?;

    let migrated = BRIDGE_MIGRATED_PATHS.get_or_init(|| Mutex::new(HashSet::new()));
    let already_migrated = migrated
        .lock()
        .map_err(|_| GooseError::message("bridge migrated-paths lock poisoned"))?
        .contains(database_path);

    if already_migrated {
        // Fast path: schema is current — skip migration.
        let path = Path::new(database_path);
        GooseStore::open_existing_current(path).or_else(|_| GooseStore::open(path))
    } else {
        // First open for this path: run full open + migrate, then record as migrated.
        let store = GooseStore::open(Path::new(database_path))?;
        migrated
            .lock()
            .map_err(|_| GooseError::message("bridge migrated-paths lock poisoned"))?
            .insert(database_path.to_string());
        Ok(store)
    }
}

#[allow(dead_code)]
pub(crate) fn json_object_string(
    field_name: &str,
    value: &serde_json::Value,
) -> GooseResult<String> {
    if !value.is_object() {
        return Err(GooseError::message(format!(
            "{field_name} must be a JSON object"
        )));
    }
    serde_json::to_string(value)
        .map_err(|error| GooseError::message(format!("cannot serialize {field_name}: {error}")))
}

pub(crate) fn register_built_in_definitions(store: &GooseStore) -> GooseResult<()> {
    for definition in built_in_algorithm_definitions() {
        store.upsert_algorithm_definition(&definition)?;
    }
    Ok(())
}

pub(crate) fn request_args<T>(request: &BridgeRequest) -> GooseResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(request.args.clone())
        .map_err(|error| GooseError::message(format!("invalid args: {error}")))
}

pub(crate) fn validate_no_traversal(field: &str, value: &str) -> GooseResult<()> {
    if value.contains("..") {
        return Err(GooseError::message(format!(
            "{field} must not contain path traversal sequences"
        )));
    }
    Ok(())
}

pub(crate) fn metric_result_to_value<T: serde::Serialize>(
    result: T,
) -> GooseResult<serde_json::Value> {
    serde_json::to_value(result)
        .map_err(|e| GooseError::message(format!("cannot serialize metric result: {e}")))
}

#[allow(dead_code)]
pub(crate) fn parse_device_type(value: &str) -> GooseResult<DeviceType> {
    match value {
        "GEN4" | "GEN_4" | "Gen4" | "gen4" => Ok(DeviceType::Gen4),
        "GOOSE" | "Goose" | "goose" => Ok(DeviceType::Goose),
        "HR_MONITOR" | "hr_monitor" => Ok(DeviceType::HrMonitor),
        other => Err(GooseError::message(format!(
            "unsupported device_type: {other}"
        ))),
    }
}

#[allow(dead_code)]
pub(crate) fn default_device_type() -> String {
    "GOOSE".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_algorithm_scope() -> String {
    "global".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_true() -> bool {
    true
}

#[allow(dead_code)]
pub(crate) fn default_raw_export_app_version() -> String {
    "goose-app/bridge".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_raw_export_core_version() -> String {
    format!(
        "goose-core/{}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("unknown")
    )
}

#[allow(dead_code)]
pub(crate) fn default_parser_version() -> String {
    format!(
        "goose-core/{}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("unknown")
    )
}

#[allow(dead_code)]
pub(crate) fn default_overnight_mode() -> String {
    "overnight_guard".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_active_status() -> String {
    "active".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_raw_notification_source() -> String {
    "ios.corebluetooth.raw_notification".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_decode_status() -> String {
    "not_decoded".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_capture_sanitize_salt() -> String {
    "goose-capture-sanitize-v1".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_ui_coverage_map_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../apk-ui-inventory/coverage-map.json")
}

#[allow(dead_code)]
pub(crate) fn default_perf_scale() -> usize {
    DEFAULT_PERF_SCALE
}

#[allow(dead_code)]
pub(crate) fn default_property_seed() -> u64 {
    DEFAULT_PROPERTY_SEED
}

#[allow(dead_code)]
pub(crate) fn default_property_cases() -> usize {
    DEFAULT_CASES_PER_GROUP
}

#[allow(dead_code)]
pub(crate) fn default_manual_source() -> String {
    "manual".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_correlation_start() -> String {
    "0000".to_string()
}

#[allow(dead_code)]
pub(crate) fn default_correlation_end() -> String {
    "9999".to_string()
}

#[allow(dead_code)]
pub(crate) fn empty_json_array() -> serde_json::Value {
    json!([])
}

#[allow(dead_code)]
pub(crate) fn empty_json_object() -> serde_json::Value {
    json!({})
}

fn elapsed_us_u64(started: Instant) -> u64 {
    let elapsed = started.elapsed().as_micros();
    if elapsed > u64::MAX as u128 {
        u64::MAX
    } else {
        elapsed as u64
    }
}

pub(crate) fn bridge_ok(request_id: &str, result: serde_json::Value) -> BridgeResponse {
    BridgeResponse {
        schema: BRIDGE_RESPONSE_SCHEMA.to_string(),
        request_id: request_id.to_string(),
        ok: true,
        result: Some(result),
        error: None,
        timing: None,
    }
}

pub(crate) fn bridge_error(
    request_id: &str,
    code: impl Into<String>,
    message: impl ToString,
) -> BridgeResponse {
    BridgeResponse {
        schema: BRIDGE_RESPONSE_SCHEMA.to_string(),
        request_id: request_id.to_string(),
        ok: false,
        result: None,
        error: Some(BridgeError {
            code: code.into(),
            message: message.to_string(),
        }),
        timing: None,
    }
}

fn response_to_c_string(response: &BridgeResponse) -> *mut c_char {
    string_to_c_string(serialize_response(response))
}

fn json_to_c_string(value: serde_json::Value) -> *mut c_char {
    match serde_json::to_string(&value) {
        Ok(value) => string_to_c_string(value),
        Err(error) => string_to_c_string(serialize_response(&bridge_error(
            "unknown",
            "serialization_error",
            error.to_string(),
        ))),
    }
}

fn string_to_c_string(value: String) -> *mut c_char {
    // Sanitize any interior null bytes before handing to CString; after this
    // replacement the string is guaranteed null-free so the unwrap is sound.
    let safe = value.replace('\0', "\\u0000");
    CString::new(safe)
        .expect("sanitized string cannot contain null bytes")
        .into_raw()
}

fn serialize_response(response: &BridgeResponse) -> String {
    serde_json::to_string(response).unwrap_or_else(|error| {
        format!(
            r#"{{"schema":"{BRIDGE_RESPONSE_SCHEMA}","request_id":"unknown","ok":false,"error":{{"code":"serialization_error","message":"{}"}}}}"#,
            escape_json_string(&error.to_string())
        )
    })
}

fn escape_json_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\0', "\\u0000")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Guard against drift between [`BRIDGE_METHODS`] and the actual dispatch arms
    /// across all 8 domain files.
    ///
    /// Reads all 8 domain files via `include_str!` (compile-time, zero runtime I/O)
    /// and scans for Rust match arm lines of the form `"namespace.method" =>` or the
    /// multi-line variant where the method name is alone on its line followed by `|`
    /// or `=>` on the next. mod.rs is excluded from the scan because the test block
    /// itself contains quoted strings in comments that would produce false positives;
    /// the small set of methods handled inline in mod.rs is enumerated explicitly.
    ///
    /// Asserts set-equality with `BRIDGE_METHODS`, catching both:
    ///   - methods listed in BRIDGE_METHODS with no dispatch arm (silent drop)
    ///   - dispatch arms not registered in BRIDGE_METHODS (undocumented method)
    #[test]
    fn bridge_methods_constant_matches_dispatcher() {
        // Domain files contain explicit match arms for their namespaces.
        // mod.rs is intentionally excluded — scanning it would pick up quoted
        // strings from comments and docstrings in the test block itself.
        let domain_source = concat!(
            include_str!("body_composition.rs"),
            include_str!("capabilities.rs"),
            include_str!("external.rs"),
            include_str!("metrics.rs"),
            include_str!("realtime.rs"),
            include_str!("sleep.rs"),
            include_str!("capture.rs"),
            include_str!("activity.rs"),
            include_str!("debug.rs"),
        );

        // Methods handled inline in mod.rs via equality guards rather than
        // delegated match arms in a domain file. Update this list when
        // handle_bridge_request_inner gains a new inline equality guard.
        let inline_methods: std::collections::HashSet<&str> = [
            "core.version",
            "core.list_methods",
            "openwhoop.reference_report",
            "battery.parse_event48_payload",
            "battery.parse_cmd26_response",
        ]
        .into_iter()
        .collect();

        // Scan domain files for match arm lines.
        //
        // Pattern A (single-line):   "namespace.method" =>
        //   trimmed starts with `"`, content after first closing `"` is `=>` or `|`
        //
        // Pattern A2 (multi-line first token):
        //   "namespace.method"
        //   | "alias" =>
        //   trimmed starts with `"`, and the rest after the closing `"` is empty
        //   (the `|` / `=>` is on the next line).
        //
        // Only strings containing `.` are collected (namespace.method pattern).
        let mut found_methods: std::collections::HashSet<&str> = Default::default();
        for line in domain_source.lines() {
            let trimmed = line.trim_start();
            if !trimmed.starts_with('"') {
                continue;
            }
            let after_first_quote = &trimmed[1..];
            let close = match after_first_quote.find('"') {
                Some(i) => i,
                None => continue,
            };
            let method = &after_first_quote[..close];
            if !method.contains('.') {
                continue;
            }
            let rest = after_first_quote[close + 1..].trim();
            if rest.starts_with("=>") || rest.starts_with('|') || rest.is_empty() {
                found_methods.insert(method);
            }
        }

        // Merge domain-scanned methods with the inline-mod.rs set.
        let found_methods: std::collections::HashSet<&str> =
            found_methods.union(&inline_methods).copied().collect();

        // Every BRIDGE_METHODS entry must have a dispatch arm and vice-versa.
        let bridge_methods: std::collections::HashSet<&str> =
            BRIDGE_METHODS.iter().copied().collect();

        let mut missing_arms: Vec<&&str> = bridge_methods.difference(&found_methods).collect();
        missing_arms.sort();

        let mut extra_arms: Vec<&&str> = found_methods.difference(&bridge_methods).collect();
        extra_arms.sort();

        assert!(
            missing_arms.is_empty(),
            "Methods in BRIDGE_METHODS with no dispatch arm: {missing_arms:?}"
        );
        assert!(
            extra_arms.is_empty(),
            "Dispatch arms not in BRIDGE_METHODS: {extra_arms:?}"
        );
    }

    /// Belt-and-braces: `BRIDGE_METHODS` is documented as sorted; verify it.
    #[test]
    fn bridge_methods_constant_is_sorted_and_unique() {
        let mut sorted = BRIDGE_METHODS.to_vec();
        sorted.sort();
        assert_eq!(
            BRIDGE_METHODS,
            sorted.as_slice(),
            "BRIDGE_METHODS must be sorted"
        );
        let mut deduped = sorted.clone();
        deduped.dedup();
        assert_eq!(sorted.len(), deduped.len(), "BRIDGE_METHODS must be unique");
    }

    /// The `core.list_methods` RPC must round-trip through the bridge envelope
    /// and return the exact same list as the constant.
    #[test]
    fn core_list_methods_rpc_returns_full_method_set() {
        let request = BridgeRequest {
            schema: BRIDGE_REQUEST_SCHEMA.to_string(),
            request_id: "test-list-methods".to_string(),
            method: "core.list_methods".to_string(),
            args: serde_json::Value::Null,
        };
        let response = handle_bridge_request(request);
        assert!(
            response.ok,
            "core.list_methods should succeed: {:?}",
            response.error
        );
        let result = response.result.expect("result payload");
        assert_eq!(result["schema"], BRIDGE_METHODS_LIST_SCHEMA);
        assert_eq!(
            result["count"]
                .as_u64()
                .expect("result.count must be a u64 integer") as usize,
            BRIDGE_METHODS.len()
        );
        let methods: Vec<String> = result["methods"]
            .as_array()
            .expect("result.methods must be a JSON array")
            .iter()
            .map(|v| {
                v.as_str()
                    .expect("each method entry must be a JSON string")
                    .to_string()
            })
            .collect();
        let expected: Vec<String> = BRIDGE_METHODS.iter().map(|s| s.to_string()).collect();
        assert_eq!(methods, expected);
        // `core.list_methods` must itself appear in the list it advertises.
        assert!(methods.iter().any(|m| m == "core.list_methods"));
    }

    fn event48_payload(seq: u8, raw_battery: u16) -> Vec<u8> {
        let mut p = vec![0u8; 20];
        p[0] = 0x30; // packet_type
        p[1] = seq;
        p[2] = 48;
        p[3] = 0;
        // timestamp at offset 4..8
        // battery raw at absolute offset 17 (data body offset 5)
        p[17] = (raw_battery & 0xFF) as u8;
        p[18] = (raw_battery >> 8) as u8;
        p
    }

    fn cmd26_payload(len: usize, raw_battery: u16) -> Vec<u8> {
        let mut p = vec![0u8; len.max(7)];
        p[0] = 36; // COMMAND_RESPONSE
        p[2] = 26; // GET_BATTERY_LEVEL
        p[4] = 1; // SUCCESS
        p[5] = (raw_battery & 0xFF) as u8;
        p[6] = (raw_battery >> 8) as u8;
        p.truncate(len);
        p
    }

    // BAT-01 valid: raw=850 at absolute offset 17 → battery_pct = 85.
    #[test]
    fn event48_valid_85() {
        let payload = event48_payload(1, 850);
        let pct = parse_event48_battery(&payload).expect("should parse");
        assert_eq!(pct, 85);
    }

    // BAT-01 edge: raw=1100 is exactly at the guard boundary → pct=110.
    #[test]
    fn event48_boundary_110() {
        let payload = event48_payload(2, 1100);
        let pct = parse_event48_battery(&payload).expect("boundary should be accepted");
        assert_eq!(pct, 110);
    }

    // BAT-01 guard reject: raw=1101 exceeds the guard → Err.
    #[test]
    fn event48_rejects_over_1100() {
        let payload = event48_payload(30, 1101);
        assert!(
            parse_event48_battery(&payload).is_err(),
            "raw=1101 should be rejected by guard"
        );
    }

    // BAT-01 too short: payload shorter than 19 bytes cannot supply offset 17 → Err.
    #[test]
    fn event48_rejects_too_short() {
        let payload = vec![0u8; 18]; // only 18 bytes; offset 17 needs bytes[17] and bytes[18]
        assert!(
            parse_event48_battery(&payload).is_err(),
            "payload of 18 bytes should be rejected (cannot read offset 17+1)"
        );
    }

    // BAT-02 valid: real COMMAND_RESPONSE layout, raw=850 at payload[5..7] → battery_pct=85.
    #[test]
    fn cmd26_valid_85() {
        let payload = cmd26_payload(10, 850);
        let pct = parse_cmd26_battery(&payload).expect("should parse");
        assert_eq!(pct, 85);
    }

    // BAT-02 guard reject: payload of 6 bytes (too short to contain data body) → Err.
    #[test]
    fn cmd26_rejects_short() {
        let payload = cmd26_payload(6, 0);
        assert!(
            parse_cmd26_battery(&payload).is_err(),
            "payload.len()=6 should be rejected (need >= 7)"
        );
    }

    // Bridge round-trip: hex-encode a valid Event-48 payload and call the bridge wrapper,
    // asserting the returned JSON contains the expected event48_battery_pct.
    #[test]
    fn event48_bridge_round_trip() {
        let raw_payload = event48_payload(30, 850);
        let payload_hex = hex::encode(&raw_payload);
        let args = ParseEvent48BatteryArgs { payload_hex };
        let result = parse_event48_battery_bridge(args).expect("bridge should succeed");
        let battery_pct = result["event48_battery_pct"]
            .as_u64()
            .expect("event48_battery_pct must be present");
        assert_eq!(battery_pct, 85);
    }

    /// PROTO-11: COMMAND_DEFINITIONS must serialise to a non-empty JSON array without error.
    ///
    /// This mirrors the "commands.definitions" bridge dispatch arm (debug.rs) which calls
    /// serde_json::to_value(COMMAND_DEFINITIONS) at runtime. If the type ever loses its
    /// Serialize impl, this test fails at compile time / test time — catching registry
    /// drift before it reaches the bridge.
    #[test]
    fn commands_definitions_serialises_without_error() {
        use crate::commands::COMMAND_DEFINITIONS;
        let value = serde_json::to_value(COMMAND_DEFINITIONS)
            .expect("COMMAND_DEFINITIONS must serialise to JSON without error");
        let arr = value
            .as_array()
            .expect("COMMAND_DEFINITIONS must serialise as a JSON array");
        assert!(!arr.is_empty(), "COMMAND_DEFINITIONS must not be empty");
    }
}

#[cfg(target_os = "android")]
pub mod android {
    use jni::JNIEnv;
    use jni::objects::{JClass, JString};
    use jni::sys::jstring;
    use std::ptr;

    /// JNI entry point for com.goose.core.GooseBridge.handle(String) -> String.
    ///
    /// Converts the Java string to a Rust str, delegates to the existing
    /// handle_bridge_request_json dispatch function, and returns the response
    /// as a new Java string. Never panics — all errors are returned as JSON.
    #[unsafe(no_mangle)]
    pub extern "system" fn Java_com_goose_core_GooseBridge_handle(
        mut env: JNIEnv,
        _class: JClass,
        request_json: JString,
    ) -> jstring {
        let request = match env.get_string(&request_json) {
            Ok(s) => s.to_string_lossy().into_owned(),
            Err(_) => {
                return env
                    .new_string("{\"ok\":false,\"error\":\"jni_string_conversion_error\"}")
                    .map(|s| s.into_raw())
                    .unwrap_or(ptr::null_mut());
            }
        };
        let response = super::handle_bridge_request_json(&request);
        env.new_string(response)
            .map(|s| s.into_raw())
            .unwrap_or(ptr::null_mut())
    }
}
