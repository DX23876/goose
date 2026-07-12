use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

mod activity;
mod capture;
mod metrics;
mod realtime;
mod sleep;

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    GooseError, GooseResult,
    protocol::{DeviceType, ParsedFrame},
    validation_labels::OFFICIAL_WHOOP_LABEL_POLICY,
};

pub const CURRENT_SCHEMA_VERSION: i64 = 25;
pub const DEFAULT_RAW_EVIDENCE_PAYLOAD_RETENTION_LIMIT_BYTES: i64 = 512 * 1024 * 1024;

const ALLOWED_METRIC_SOURCE_KINDS: [&str; 4] = [
    "device_counter",
    "device_sensor",
    "local_estimate",
    "unavailable",
];

const ALLOWED_METRIC_PROVENANCE_SCOPES: [&str; 3] =
    ["daily_activity", "daily_recovery", "hourly_activity"];

const ALLOWED_ACTIVITY_SYNC_STATUSES: [&str; 6] = [
    "candidate",
    "verified",
    "user_confirmed",
    "synced",
    "blocked",
    "discarded",
];

const ALLOWED_ACTIVITY_TYPES: [&str; 48] = [
    "unknown",
    "running",
    "walking",
    "cycling",
    "jogging",
    "strength",
    "weightlifting",
    "powerlifting",
    "swimming",
    "rowing",
    "hiit",
    "hiking",
    "hiking_rucking",
    "functional_fitness",
    "machine_workout",
    "martial_arts",
    "boxing",
    "kickboxing",
    "rock_climbing",
    "climber",
    "pilates",
    "yoga",
    "hot_yoga",
    "restorative_yoga",
    "meditation",
    "breathwork",
    "non_sleep_deep_rest",
    "ice_bath",
    "sauna",
    "manual",
    "manual_labor",
    "commuting",
    "cleaning",
    "cooking",
    "driving",
    "dog_walking",
    "stroller_walking",
    "stroller_jogging",
    "race_walking",
    "spinning",
    "elliptical",
    "team_sport",
    "padel",
    "barre",
    "barre3",
    "other",
    "other_recovery",
    "nap",
];

const ALLOWED_ACTIVITY_DETECTION_METHODS: [&str; 9] = [
    "user_assigned",
    "heuristic_motion",
    "heuristic_hr_motion",
    "machine_learning",
    "official_capture",
    "imported",
    "manual_split",
    "manual_merge",
    "manual_annotation",
];

const ALLOWED_ACTIVITY_INTERVAL_TYPES: [&str; 6] =
    ["lap", "pause", "work", "rest", "window", "split"];

const ALLOWED_ACTIVITY_LABEL_TYPES: [&str; 4] = [
    "user",
    "official_app_comparison",
    "calibration",
    "candidate",
];

const ALLOWED_ACTIVITY_METRIC_UNITS: [&str; 25] = [
    "raw", "bpm", "ms", "hz", "count", "steps", "m", "km", "mi", "kcal", "m/s", "km/h", "min", "s",
    "percent", "ratio", "load", "joule", "w", "kg", "m/s2", "c", "f", "degrees", "n/a",
];

const ALLOWED_EXTERNAL_SLEEP_PLATFORMS: [&str; 5] = [
    "healthkit",
    "health_connect",
    "manual",
    "import",
    "goose_ble",
];

const ALLOWED_EXTERNAL_SLEEP_STAGE_KINDS: [&str; 8] = [
    "in_bed",
    "asleep",
    "awake",
    "core",
    "deep",
    "rem",
    "unknown",
    "not_applicable",
];

const ALLOWED_EXTERNAL_SLEEP_STAGE_SUMMARY_KEYS: [&str; 21] = [
    "in_bed",
    "inbed",
    "unknown",
    "not_applicable",
    "not_applicable_sleep",
    "awake",
    "asleep_awake",
    "sleep_awake",
    "out_of_bed",
    "asleep",
    "asleep_unspecified",
    "core",
    "light",
    "asleep_core",
    "sleep_light",
    "deep",
    "asleep_deep",
    "sleep_deep",
    "rem",
    "asleep_rem",
    "sleep_rem",
];

const ALLOWED_SLEEP_CORRECTION_LABEL_TYPES: [&str; 5] = [
    "sleep_start",
    "sleep_end",
    "sleep_window",
    "sleep_stage",
    "nap",
];

#[derive(Debug)]
pub struct GooseStore {
    pub(super) conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct RawEvidenceInput<'a> {
    pub evidence_id: &'a str,
    pub source: &'a str,
    pub captured_at: &'a str,
    pub device_model: &'a str,
    pub payload: &'a [u8],
    pub sensitivity: &'a str,
    pub capture_session_id: Option<&'a str>,
    pub device_uuid: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawEvidenceRow {
    pub evidence_id: String,
    pub source: String,
    pub captured_at: String,
    pub device_model: String,
    pub payload_hex: String,
    pub sha256: String,
    pub sensitivity: String,
    #[serde(default)]
    pub capture_session_id: Option<String>,
    #[serde(default)]
    pub device_uuid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawEvidencePayloadRetentionReport {
    pub limit_bytes: i64,
    pub before_bytes: i64,
    pub after_bytes: i64,
    pub compacted_rows: i64,
    pub freed_bytes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecodedFrameRow {
    pub frame_id: String,
    pub evidence_id: String,
    pub captured_at: String,
    pub device_type: String,
    pub raw_len: i64,
    pub header_len: i64,
    pub declared_len: i64,
    pub payload_hex: String,
    pub payload_crc_hex: String,
    pub header_crc_valid: bool,
    pub payload_crc_valid: bool,
    pub packet_type: Option<i64>,
    pub packet_type_name: Option<String>,
    pub sequence: Option<i64>,
    pub command_or_event: Option<i64>,
    pub parsed_payload_json: String,
    pub parser_version: String,
    pub warnings_json: String,
    #[serde(default)]
    pub device_uuid: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DecodedFrameInput<'a> {
    pub frame_id: &'a str,
    pub evidence_id: &'a str,
    pub parsed: &'a ParsedFrame,
    pub parser_version: &'a str,
    pub device_uuid: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureSessionInput<'a> {
    pub session_id: &'a str,
    pub source: &'a str,
    pub started_at_unix_ms: i64,
    pub device_model: &'a str,
    pub active_device_id: Option<&'a str>,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureSessionRow {
    pub session_id: String,
    pub source: String,
    pub started_at_unix_ms: i64,
    pub ended_at_unix_ms: Option<i64>,
    pub device_model: String,
    pub active_device_id: Option<String>,
    pub status: String,
    pub frame_count: i64,
    pub provenance_json: String,
}

#[derive(Debug, Clone)]
pub struct OvernightSyncSessionInput<'a> {
    pub session_id: &'a str,
    pub started_at: &'a str,
    pub ended_at: Option<&'a str>,
    pub band_identifier: Option<&'a str>,
    pub app_version: Option<&'a str>,
    pub mode: &'a str,
    pub final_status: &'a str,
    pub raw_frame_count: i64,
    pub historical_frame_count: i64,
    pub k18_count: i64,
    pub k24_count: i64,
    pub k25_count: i64,
    pub k26_count: i64,
    pub packet47_count: i64,
    pub event17_count: i64,
    pub event29_count: i64,
    pub metadata49_count: i64,
    pub metadata56_count: i64,
    pub range_poll_count: i64,
    pub successful_range_poll_count: i64,
    pub event_log_count: i64,
    pub readiness_status: Option<&'a str>,
    pub readiness: Option<&'a str>,
    pub error_count: i64,
    pub notes: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct OvernightRawNotificationInput<'a> {
    pub session_id: &'a str,
    pub captured_at: &'a str,
    pub source: &'a str,
    pub device_id: Option<&'a str>,
    pub active_device_name: Option<&'a str>,
    pub connection_state: Option<&'a str>,
    pub service_uuid: Option<&'a str>,
    pub characteristic_uuid: &'a str,
    pub device_type: Option<&'a str>,
    pub command_or_event: Option<i64>,
    pub packet_type: Option<i64>,
    pub k_revision: Option<i64>,
    pub sequence: Option<i64>,
    pub frame_hex: &'a str,
    pub payload_hex: Option<&'a str>,
    pub byte_count: i64,
    pub decode_status: &'a str,
}

#[derive(Debug, Clone)]
pub struct OvernightHistoricalRangePollInput<'a> {
    pub session_id: &'a str,
    pub captured_at: &'a str,
    pub status: &'a str,
    pub command_sequence: i64,
    pub result_code: i64,
    pub result_name: &'a str,
    pub raw_payload_hex: &'a str,
    pub raw_body_hex: &'a str,
    pub revision_or_status: Option<i64>,
    pub page_current: Option<i64>,
    pub page_oldest: Option<i64>,
    pub page_end: Option<i64>,
    pub pages_behind: Option<i64>,
    pub pending_response_count: i64,
    pub retry_count: i64,
    pub notes: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OvernightMirrorReport {
    pub schema: String,
    pub session_upserted: usize,
    pub raw_inserted: usize,
    pub raw_existing: usize,
    pub historical_range_inserted: usize,
    pub historical_range_existing: usize,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OvernightMirrorCounts {
    pub schema: String,
    pub session_id: String,
    pub session_exists: bool,
    pub raw_notification_count: i64,
    pub historical_range_poll_count: i64,
    pub successful_historical_range_poll_count: i64,
}

#[derive(Debug, Clone)]
pub struct ActivitySessionInput<'a> {
    pub session_id: &'a str,
    pub source: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub activity_type: &'a str,
    pub external_activity_type_code: Option<&'a str>,
    pub external_activity_type_name: Option<&'a str>,
    pub custom_label: Option<&'a str>,
    pub confidence: f64,
    pub detection_method: &'a str,
    pub sync_status: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivitySessionRow {
    pub session_id: String,
    pub source: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub duration_ms: i64,
    pub activity_type: String,
    pub external_activity_type_code: Option<String>,
    pub external_activity_type_name: Option<String>,
    pub custom_label: Option<String>,
    pub confidence: f64,
    pub detection_method: String,
    pub sync_status: String,
    pub provenance_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct ActivityMetricInput<'a> {
    pub metric_id: &'a str,
    pub activity_session_id: &'a str,
    pub metric_name: &'a str,
    pub value: f64,
    pub unit: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub quality_flags_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityMetricRow {
    pub metric_id: String,
    pub activity_session_id: String,
    pub metric_name: String,
    pub value: f64,
    pub unit: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub quality_flags_json: String,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct DailyActivityMetricInput<'a> {
    pub daily_metric_id: &'a str,
    pub date_key: &'a str,
    pub timezone: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub steps: Option<i64>,
    pub active_kcal: Option<f64>,
    pub resting_kcal: Option<f64>,
    pub total_kcal: Option<f64>,
    pub average_cadence_spm: Option<f64>,
    pub source_kind: &'a str,
    pub confidence: f64,
    pub inputs_json: &'a str,
    pub quality_flags_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyActivityMetricRow {
    pub daily_metric_id: String,
    pub date_key: String,
    pub timezone: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub steps: Option<i64>,
    pub active_kcal: Option<f64>,
    pub resting_kcal: Option<f64>,
    pub total_kcal: Option<f64>,
    pub average_cadence_spm: Option<f64>,
    pub source_kind: String,
    pub confidence: f64,
    pub inputs_json: String,
    pub quality_flags_json: String,
    pub provenance_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct HourlyActivityMetricInput<'a> {
    pub hourly_metric_id: &'a str,
    pub date_key: &'a str,
    pub timezone: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub steps: Option<i64>,
    pub active_kcal: Option<f64>,
    pub resting_kcal: Option<f64>,
    pub total_kcal: Option<f64>,
    pub average_cadence_spm: Option<f64>,
    pub source_kind: &'a str,
    pub confidence: f64,
    pub inputs_json: &'a str,
    pub quality_flags_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HourlyActivityMetricRow {
    pub hourly_metric_id: String,
    pub date_key: String,
    pub timezone: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub steps: Option<i64>,
    pub active_kcal: Option<f64>,
    pub resting_kcal: Option<f64>,
    pub total_kcal: Option<f64>,
    pub average_cadence_spm: Option<f64>,
    pub source_kind: String,
    pub confidence: f64,
    pub inputs_json: String,
    pub quality_flags_json: String,
    pub provenance_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct DailyRecoveryMetricInput<'a> {
    pub daily_metric_id: &'a str,
    pub date_key: &'a str,
    pub timezone: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub resting_hr_bpm: Option<f64>,
    pub hrv_rmssd_ms: Option<f64>,
    pub respiratory_rate_rpm: Option<f64>,
    pub oxygen_saturation_percent: Option<f64>,
    pub skin_temperature_delta_c: Option<f64>,
    pub source_kind: &'a str,
    pub confidence: f64,
    pub inputs_json: &'a str,
    pub quality_flags_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyRecoveryMetricRow {
    pub daily_metric_id: String,
    pub date_key: String,
    pub timezone: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub resting_hr_bpm: Option<f64>,
    pub hrv_rmssd_ms: Option<f64>,
    pub respiratory_rate_rpm: Option<f64>,
    pub oxygen_saturation_percent: Option<f64>,
    pub skin_temperature_delta_c: Option<f64>,
    pub source_kind: String,
    pub confidence: f64,
    pub inputs_json: String,
    pub quality_flags_json: String,
    pub provenance_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct MetricProvenanceInput<'a> {
    pub provenance_id: &'a str,
    pub metric_scope: &'a str,
    pub metric_id: &'a str,
    pub source_kind: &'a str,
    pub source_detail: &'a str,
    pub confidence: Option<f64>,
    pub inputs_json: &'a str,
    pub quality_flags_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricProvenanceRow {
    pub provenance_id: String,
    pub metric_scope: String,
    pub metric_id: String,
    pub source_kind: String,
    pub source_detail: String,
    pub confidence: Option<f64>,
    pub inputs_json: String,
    pub quality_flags_json: String,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct MetricDebugFeatureInput<'a> {
    pub feature_id: &'a str,
    pub metric_family: &'a str,
    pub feature_name: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub source_kind: &'a str,
    pub confidence: Option<f64>,
    pub feature_json: &'a str,
    pub inputs_json: &'a str,
    pub quality_flags_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricDebugFeatureRow {
    pub feature_id: String,
    pub metric_family: String,
    pub feature_name: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub source_kind: String,
    pub confidence: Option<f64>,
    pub feature_json: String,
    pub inputs_json: String,
    pub quality_flags_json: String,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct StepCounterSampleInput<'a> {
    pub sample_id: &'a str,
    pub sample_time_unix_ms: i64,
    pub counter_value: i64,
    pub cadence_spm: Option<f64>,
    pub activity_state: Option<&'a str>,
    pub source_kind: &'a str,
    pub packet_family: &'a str,
    pub json_path: &'a str,
    pub frame_id: Option<&'a str>,
    pub evidence_id: Option<&'a str>,
    pub capture_session_id: Option<&'a str>,
    pub quality_flags_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StepCounterSampleRow {
    pub sample_id: String,
    pub sample_time_unix_ms: i64,
    pub counter_value: i64,
    pub cadence_spm: Option<f64>,
    pub activity_state: Option<String>,
    pub source_kind: String,
    pub packet_family: String,
    pub json_path: String,
    pub frame_id: Option<String>,
    pub evidence_id: Option<String>,
    pub capture_session_id: Option<String>,
    pub quality_flags_json: String,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GravityRow {
    pub device_id: String,
    pub ts: f64,
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Spo2SampleRow {
    pub device_id: String,
    pub ts: f64,
    pub red: i64,
    pub ir: i64,
    pub contact: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkinTempSampleRow {
    pub device_id: String,
    pub ts: f64,
    pub raw: i64,
    pub contact: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RespSampleRow {
    pub device_id: String,
    pub ts: f64,
    pub raw: i64,
    pub contact: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SigQualitySampleRow {
    pub device_id: String,
    pub ts: f64,
    pub quality: i64,
    pub contact: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HrSampleRow {
    pub device_id: String,
    pub ts: f64,
    pub bpm: i64,
    pub synced: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RrIntervalRow {
    pub device_id: String,
    pub ts: f64,
    pub interval_ms: i64,
    pub synced: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRow {
    pub device_id: String,
    pub ts: f64,
    pub event_id: i64,
    pub event_name: String,
    pub synced: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatteryRow {
    pub device_id: String,
    pub ts: f64,
    pub level_pct: i64,
    pub synced: i64,
}

/// Stream tables that accept synced-flag operations (mark_synced_rows, rows_pending_upload,
/// prune_synced_stream_rows). Any stream name not in this list is rejected to prevent SQL injection.
const STREAM_ALLOWLIST: &[&str] = &[
    "battery",
    "events",
    "exercise_sessions",
    "gravity",
    "gravity2_samples",
    "hr_samples",
    "resp_samples",
    "rr_intervals",
    "skin_temp_samples",
    "spo2_samples",
];

/// Summary returned by backfill_streams_from_decoded_frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillReport {
    pub hr_inserted: usize,
    pub rr_inserted: usize,
    pub events_inserted: usize,
    pub battery_inserted: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExerciseSessionRow {
    pub device_id: String,
    pub start_ts: f64,
    pub end_ts: f64,
    pub duration_s: f64,
    pub avg_hr: f64,
    pub peak_hr: f64,
    pub strain: f64,
    pub calories_kcal: f64,
    pub zone_time_pct_json: String,
    pub hrmax_source: String,
    pub rhr_source: String,
    pub avg_hrr_pct: f64,
}

#[derive(Debug, Clone)]
pub struct V24BiometricBatch {
    pub spo2: Vec<(f64, i64, i64, i64)>,   // (ts, red, ir, contact)
    pub skin_temp: Vec<(f64, i64, i64)>,   // (ts, raw, contact)
    pub resp: Vec<(f64, i64, i64)>,        // (ts, raw, contact)
    pub sig_quality: Vec<(f64, i64, i64)>, // (ts, quality, contact)
}

#[derive(Debug, Clone)]
pub struct V24BiometricWindow {
    pub spo2: Vec<Spo2SampleRow>,
    pub skin_temp: Vec<SkinTempSampleRow>,
    pub resp: Vec<RespSampleRow>,
    pub sig_quality: Vec<SigQualitySampleRow>,
}

#[derive(Debug, Clone)]
pub struct ActivityIntervalInput<'a> {
    pub interval_id: &'a str,
    pub activity_session_id: &'a str,
    pub interval_type: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub sequence: i64,
    pub metadata_json: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityIntervalRow {
    pub interval_id: String,
    pub activity_session_id: String,
    pub interval_type: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub duration_ms: i64,
    pub sequence: i64,
    pub metadata_json: String,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct ActivityLabelInput<'a> {
    pub label_id: &'a str,
    pub activity_session_id: &'a str,
    pub label_type: &'a str,
    pub value: &'a str,
    pub source: &'a str,
    pub confidence: Option<f64>,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityLabelRow {
    pub label_id: String,
    pub activity_session_id: String,
    pub label_type: String,
    pub value: String,
    pub source: String,
    pub confidence: Option<f64>,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct ExternalSleepSessionInput<'a> {
    pub sleep_id: &'a str,
    pub source: &'a str,
    pub platform: &'a str,
    pub platform_record_id: Option<&'a str>,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub timezone: Option<&'a str>,
    pub stage_summary_json: &'a str,
    pub confidence: f64,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExternalSleepSessionRow {
    pub sleep_id: String,
    pub source: String,
    pub platform: String,
    pub platform_record_id: Option<String>,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub duration_ms: i64,
    pub timezone: Option<String>,
    pub stage_summary_json: String,
    pub confidence: f64,
    pub provenance_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct ExternalSleepStageInput<'a> {
    pub stage_id: &'a str,
    pub sleep_id: &'a str,
    pub stage_kind: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub confidence: f64,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExternalSleepStageRow {
    pub stage_id: String,
    pub sleep_id: String,
    pub stage_kind: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub duration_ms: i64,
    pub confidence: f64,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct SleepCorrectionLabelInput<'a> {
    pub label_id: &'a str,
    pub sleep_id: Option<&'a str>,
    pub label_type: &'a str,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub value_json: &'a str,
    pub source: &'a str,
    pub confidence: Option<f64>,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SleepCorrectionLabelRow {
    pub label_id: String,
    pub sleep_id: Option<String>,
    pub label_type: String,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms: i64,
    pub value_json: String,
    pub source: String,
    pub confidence: Option<f64>,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlgorithmDefinitionRecord {
    pub algorithm_id: String,
    pub version: String,
    pub metric_family: String,
    pub display_name: String,
    pub implementation: String,
    pub license: String,
    pub input_schema: String,
    pub output_schema: String,
    pub input_requirements_json: String,
    pub params_json: String,
    pub quality_gates_json: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlgorithmRunRecord {
    pub run_id: String,
    pub algorithm_id: String,
    pub version: String,
    pub start_time: String,
    pub end_time: String,
    pub output_json: String,
    pub quality_flags_json: String,
    pub provenance_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricValueRecord {
    pub metric_value_id: String,
    pub run_id: String,
    pub metric_family: String,
    pub name: String,
    pub value: f64,
    pub unit: String,
    pub start_time: String,
    pub end_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricComponentRecord {
    pub metric_component_id: String,
    pub run_id: String,
    pub component_name: String,
    pub value: f64,
    pub unit: String,
    pub contribution_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlgorithmPreferenceRecord {
    pub scope: String,
    pub metric_family: String,
    pub algorithm_id: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CalibrationRunTimes {
    pub train_start: String,
    pub train_end: String,
    pub holdout_start: String,
    pub holdout_end: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CalibrationRunRecord {
    pub calibration_run_id: String,
    pub algorithm_id: String,
    pub version: String,
    pub times: CalibrationRunTimes,
    pub metrics_json: String,
    pub params_json: String,
}

#[derive(Debug, Clone)]
pub struct CalibrationLabelInput<'a> {
    pub label_id: &'a str,
    pub metric_family: &'a str,
    pub label_source: &'a str,
    pub captured_at: &'a str,
    pub value: f64,
    pub unit: &'a str,
    pub provenance_json: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CalibrationLabelRow {
    pub label_id: String,
    pub metric_family: String,
    pub label_source: String,
    pub captured_at: String,
    pub value: f64,
    pub unit: String,
    pub provenance_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandValidationRecord {
    pub command: String,
    pub risk_gate: String,
    pub direct_send_ready: bool,
    pub report_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugSessionRow {
    pub session_id: String,
    pub started_at_unix_ms: i64,
    pub bridge_url: String,
    pub bind_host: String,
    pub token_required: bool,
    pub token_present: bool,
    pub remote_bind_enabled: bool,
    pub visible_remote_bind_toggle: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugCommandRow {
    pub command_id: String,
    pub session_id: String,
    pub schema: String,
    pub command: String,
    pub args_json: String,
    pub dry_run: bool,
    pub received_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugEventRow {
    pub session_id: String,
    pub sequence: i64,
    pub schema: String,
    pub time_unix_ms: i64,
    pub source: String,
    pub level: String,
    pub topic: String,
    pub message: String,
    pub command_id: Option<String>,
    pub data_json: String,
}

#[derive(Debug)]
pub struct OpticalSampleRow {
    pub device_id: String,
    pub ts: f64,
    pub packet_k: i64,
    pub version: i64,
    pub channel_index: i64,
    pub samples_json: String,
}

#[derive(Debug)]
pub struct FeatureFlagRow {
    pub device_id: String,
    pub flag_index: i64,
    pub flag_value: i64,
    pub discovered_at: String,
}

/// BODY-01: One row from body_composition_history.
pub struct BodyCompositionRow {
    pub date: String,
    pub source: String,
    pub weight_kg: Option<f64>,
    pub bmi: Option<f64>,
    pub body_fat_pct: Option<f64>,
    pub muscle_mass_kg: Option<f64>,
    pub water_pct: Option<f64>,
}

/// EXT-01: One row from external_samples (Apple Health / Apple Watch import).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExternalSampleRow {
    pub platform: String,
    pub platform_record_id: String,
    pub metric: String,
    pub value: f64,
    pub unit: String,
    pub start_unix_ms: i64,
    pub end_unix_ms: i64,
    pub source: String,
    pub created_at: String,
}

/// EXT-01: Source-priority resolver. WHOOP-derived values always win when present;
/// an externally sourced value (Apple Health / Apple Watch) is used only as a fallback
/// for windows where no WHOOP value exists. Pure combinator — callers supply both
/// candidates already resolved for the same metric+window so this function has no
/// knowledge of where either value came from.
pub fn resolve_metric_priority<T>(
    whoop_value: Option<T>,
    external_value: Option<T>,
) -> Option<(T, &'static str)> {
    match (whoop_value, external_value) {
        (Some(v), _) => Some((v, "whoop")),
        (None, Some(v)) => Some((v, "apple_health")),
        (None, None) => None,
    }
}

#[cfg(test)]
mod resolve_metric_priority_tests {
    use super::resolve_metric_priority;

    #[test]
    fn whoop_wins_when_both_present() {
        let resolved = resolve_metric_priority(Some(42.0), Some(99.0));
        assert_eq!(
            resolved,
            Some((42.0, "whoop")),
            "WHOOP value must win over Apple Health when both exist"
        );
    }

    #[test]
    fn external_is_fallback_when_whoop_absent() {
        let resolved = resolve_metric_priority(None, Some(99.0));
        assert_eq!(
            resolved,
            Some((99.0, "apple_health")),
            "Apple Health must fill the gap when WHOOP has no value"
        );
    }

    #[test]
    fn none_when_neither_present() {
        let resolved: Option<(f64, &str)> = resolve_metric_priority(None, None);
        assert_eq!(
            resolved, None,
            "No data from either source must resolve to None, not a fabricated value"
        );
    }

    #[test]
    fn whoop_wins_even_without_external_value() {
        let resolved = resolve_metric_priority(Some(10.0), None);
        assert_eq!(resolved, Some((10.0, "whoop")));
    }
}

fn configure_read_write_connection(conn: &Connection) -> GooseResult<()> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000;
        "#,
    )?;
    Ok(())
}

fn configure_read_only_connection(conn: &Connection) -> GooseResult<()> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 5000;
        "#,
    )?;
    Ok(())
}

impl GooseStore {
    pub fn open(path: &Path) -> GooseResult<Self> {
        let conn = Connection::open(path)?;
        configure_read_write_connection(&conn)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_existing_current(path: &Path) -> GooseResult<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        configure_read_write_connection(&conn)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        let schema_version = store.schema_version()?;
        if schema_version != CURRENT_SCHEMA_VERSION {
            return Err(GooseError::message(format!(
                "database schema version {schema_version} is not current {CURRENT_SCHEMA_VERSION}"
            )));
        }
        Ok(store)
    }

    pub fn open_read_only(path: &Path) -> GooseResult<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        configure_read_only_connection(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn open_in_memory() -> GooseResult<Self> {
        let conn = Connection::open_in_memory()?;
        configure_read_write_connection(&conn)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn immediate_transaction<F, T>(&self, operation: F) -> GooseResult<T>
    where
        F: FnOnce(&Connection) -> GooseResult<T>,
    {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        match operation(&conn) {
            Ok(value) => {
                conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub fn migrate(&self) -> GooseResult<()> {
        // Lock conn only for the schema SQL, then drop it before calling ensure_* helpers
        // (each of which re-acquires the lock independently — holding it here would deadlock).
        {
            let conn = self
                .conn
                .lock()
                .map_err(|_| GooseError::message("store mutex poisoned"))?;
            conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS goose_schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS raw_evidence (
                evidence_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                device_model TEXT NOT NULL,
                payload_hex TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                sensitivity TEXT NOT NULL,
                capture_session_id TEXT REFERENCES capture_sessions(session_id) ON DELETE SET NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS decoded_frames (
                frame_id TEXT PRIMARY KEY,
                evidence_id TEXT NOT NULL REFERENCES raw_evidence(evidence_id) ON DELETE CASCADE,
                device_type TEXT NOT NULL,
                raw_len INTEGER NOT NULL,
                header_len INTEGER NOT NULL,
                declared_len INTEGER NOT NULL,
                payload_hex TEXT NOT NULL,
                payload_crc_hex TEXT NOT NULL,
                header_crc_valid INTEGER NOT NULL,
                payload_crc_valid INTEGER NOT NULL,
                packet_type INTEGER,
                packet_type_name TEXT,
                sequence INTEGER,
                command_or_event INTEGER,
                parsed_payload_json TEXT NOT NULL DEFAULT 'null',
                parser_version TEXT NOT NULL,
                warnings_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_raw_evidence_by_captured_at
                ON raw_evidence(captured_at, evidence_id);

            CREATE INDEX IF NOT EXISTS idx_decoded_frames_by_evidence
                ON decoded_frames(evidence_id);

            CREATE TABLE IF NOT EXISTS algorithm_definitions (
                algorithm_id TEXT NOT NULL,
                version TEXT NOT NULL,
                metric_family TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                implementation TEXT NOT NULL DEFAULT '',
                license TEXT NOT NULL DEFAULT '',
                input_schema TEXT NOT NULL,
                output_schema TEXT NOT NULL,
                input_requirements_json TEXT NOT NULL DEFAULT '{}',
                params_json TEXT NOT NULL,
                quality_gates_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'experimental',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (algorithm_id, version)
            );

            CREATE TABLE IF NOT EXISTS algorithm_runs (
                run_id TEXT PRIMARY KEY,
                algorithm_id TEXT NOT NULL,
                version TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                output_json TEXT NOT NULL,
                quality_flags_json TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                FOREIGN KEY (algorithm_id, version)
                    REFERENCES algorithm_definitions(algorithm_id, version)
            );

            CREATE TABLE IF NOT EXISTS command_validation_records (
                command TEXT PRIMARY KEY,
                risk_gate TEXT NOT NULL,
                direct_send_ready INTEGER NOT NULL,
                report_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS capture_sessions (
                session_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                started_at_unix_ms INTEGER NOT NULL,
                ended_at_unix_ms INTEGER,
                device_model TEXT NOT NULL,
                active_device_id TEXT,
                status TEXT NOT NULL,
                frame_count INTEGER NOT NULL DEFAULT 0,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_capture_sessions_by_started_at
                ON capture_sessions(started_at_unix_ms);

            CREATE TABLE IF NOT EXISTS activity_sessions (
                session_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL,
                activity_type TEXT NOT NULL,
                external_activity_type_code TEXT,
                external_activity_type_name TEXT,
                custom_label TEXT,
                confidence REAL NOT NULL,
                detection_method TEXT NOT NULL,
                sync_status TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_activity_sessions_by_window
                ON activity_sessions(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_activity_sessions_by_type
                ON activity_sessions(activity_type);
            CREATE INDEX IF NOT EXISTS idx_activity_sessions_by_source
                ON activity_sessions(source);
            CREATE INDEX IF NOT EXISTS idx_activity_sessions_by_sync_status
                ON activity_sessions(sync_status);

            CREATE TABLE IF NOT EXISTS activity_metrics (
                metric_id TEXT PRIMARY KEY,
                activity_session_id TEXT NOT NULL REFERENCES activity_sessions(session_id) ON DELETE CASCADE,
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_activity_metrics_by_session
                ON activity_metrics(activity_session_id);
            CREATE INDEX IF NOT EXISTS idx_activity_metrics_by_name
                ON activity_metrics(metric_name);

            CREATE TABLE IF NOT EXISTS daily_activity_metrics (
                daily_metric_id TEXT PRIMARY KEY,
                date_key TEXT NOT NULL,
                timezone TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                steps INTEGER,
                active_kcal REAL,
                resting_kcal REAL,
                total_kcal REAL,
                average_cadence_spm REAL,
                source_kind TEXT NOT NULL,
                confidence REAL NOT NULL,
                inputs_json TEXT NOT NULL DEFAULT '{}',
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_daily_activity_metrics_by_date
                ON daily_activity_metrics(date_key);
            CREATE INDEX IF NOT EXISTS idx_daily_activity_metrics_by_window
                ON daily_activity_metrics(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_daily_activity_metrics_by_source_kind
                ON daily_activity_metrics(source_kind);

            CREATE TABLE IF NOT EXISTS hourly_activity_metrics (
                hourly_metric_id TEXT PRIMARY KEY,
                date_key TEXT NOT NULL,
                timezone TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                steps INTEGER,
                active_kcal REAL,
                resting_kcal REAL,
                total_kcal REAL,
                average_cadence_spm REAL,
                source_kind TEXT NOT NULL,
                confidence REAL NOT NULL,
                inputs_json TEXT NOT NULL DEFAULT '{}',
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_hourly_activity_metrics_by_date
                ON hourly_activity_metrics(date_key);
            CREATE INDEX IF NOT EXISTS idx_hourly_activity_metrics_by_window
                ON hourly_activity_metrics(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_hourly_activity_metrics_by_source_kind
                ON hourly_activity_metrics(source_kind);

            CREATE TABLE IF NOT EXISTS daily_recovery_metrics (
                daily_metric_id TEXT PRIMARY KEY,
                date_key TEXT NOT NULL,
                timezone TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                resting_hr_bpm REAL,
                hrv_rmssd_ms REAL,
                respiratory_rate_rpm REAL,
                oxygen_saturation_percent REAL,
                skin_temperature_delta_c REAL,
                source_kind TEXT NOT NULL,
                confidence REAL NOT NULL,
                inputs_json TEXT NOT NULL DEFAULT '{}',
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_daily_recovery_metrics_by_date
                ON daily_recovery_metrics(date_key);
            CREATE INDEX IF NOT EXISTS idx_daily_recovery_metrics_by_window
                ON daily_recovery_metrics(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_daily_recovery_metrics_by_source_kind
                ON daily_recovery_metrics(source_kind);

            CREATE TABLE IF NOT EXISTS metric_provenance (
                provenance_id TEXT PRIMARY KEY,
                metric_scope TEXT NOT NULL,
                metric_id TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_detail TEXT NOT NULL DEFAULT '',
                confidence REAL,
                inputs_json TEXT NOT NULL DEFAULT '{}',
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_metric_provenance_by_metric
                ON metric_provenance(metric_scope, metric_id);
            CREATE INDEX IF NOT EXISTS idx_metric_provenance_by_source_kind
                ON metric_provenance(source_kind);

            CREATE TABLE IF NOT EXISTS metric_debug_features (
                feature_id TEXT PRIMARY KEY,
                metric_family TEXT NOT NULL,
                feature_name TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                source_kind TEXT NOT NULL,
                confidence REAL,
                feature_json TEXT NOT NULL DEFAULT '{}',
                inputs_json TEXT NOT NULL DEFAULT '{}',
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_metric_debug_features_by_family
                ON metric_debug_features(metric_family, feature_name);
            CREATE INDEX IF NOT EXISTS idx_metric_debug_features_by_window
                ON metric_debug_features(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_metric_debug_features_by_source_kind
                ON metric_debug_features(source_kind);

            CREATE TABLE IF NOT EXISTS step_counter_samples (
                sample_id TEXT PRIMARY KEY,
                sample_time_unix_ms INTEGER NOT NULL,
                counter_value INTEGER NOT NULL,
                cadence_spm REAL,
                activity_state TEXT,
                source_kind TEXT NOT NULL,
                packet_family TEXT NOT NULL DEFAULT '',
                json_path TEXT NOT NULL DEFAULT '',
                frame_id TEXT,
                evidence_id TEXT,
                capture_session_id TEXT,
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_step_counter_samples_by_time
                ON step_counter_samples(sample_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_step_counter_samples_by_field
                ON step_counter_samples(packet_family, json_path, sample_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_step_counter_samples_by_source_kind
                ON step_counter_samples(source_kind);

            CREATE TABLE IF NOT EXISTS activity_intervals (
                interval_id TEXT PRIMARY KEY,
                activity_session_id TEXT NOT NULL REFERENCES activity_sessions(session_id) ON DELETE CASCADE,
                interval_type TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_activity_intervals_by_session
                ON activity_intervals(activity_session_id);
            CREATE INDEX IF NOT EXISTS idx_activity_intervals_by_type
                ON activity_intervals(interval_type);

            CREATE TABLE IF NOT EXISTS activity_labels (
                label_id TEXT PRIMARY KEY,
                activity_session_id TEXT NOT NULL REFERENCES activity_sessions(session_id) ON DELETE CASCADE,
                label_type TEXT NOT NULL,
                value TEXT NOT NULL,
                source TEXT NOT NULL,
                confidence REAL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_activity_labels_by_session
                ON activity_labels(activity_session_id);
            CREATE INDEX IF NOT EXISTS idx_activity_labels_by_type
                ON activity_labels(label_type);

            CREATE TABLE IF NOT EXISTS external_sleep_sessions (
                sleep_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                platform TEXT NOT NULL,
                platform_record_id TEXT,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL,
                timezone TEXT,
                stage_summary_json TEXT NOT NULL DEFAULT '{}',
                confidence REAL NOT NULL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(platform, platform_record_id)
            );

            CREATE INDEX IF NOT EXISTS idx_external_sleep_sessions_by_window
                ON external_sleep_sessions(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_external_sleep_sessions_by_platform
                ON external_sleep_sessions(platform);
            CREATE INDEX IF NOT EXISTS idx_external_sleep_sessions_by_source
                ON external_sleep_sessions(source);

            CREATE TABLE IF NOT EXISTS external_sleep_stages (
                stage_id TEXT PRIMARY KEY,
                sleep_id TEXT NOT NULL REFERENCES external_sleep_sessions(sleep_id) ON DELETE CASCADE,
                stage_kind TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL,
                confidence REAL NOT NULL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_external_sleep_stages_by_sleep
                ON external_sleep_stages(sleep_id);
            CREATE INDEX IF NOT EXISTS idx_external_sleep_stages_by_window
                ON external_sleep_stages(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_external_sleep_stages_by_kind
                ON external_sleep_stages(stage_kind);

            CREATE TABLE IF NOT EXISTS sleep_correction_labels (
                label_id TEXT PRIMARY KEY,
                sleep_id TEXT,
                label_type TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                value_json TEXT NOT NULL,
                source TEXT NOT NULL,
                confidence REAL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_sleep_correction_labels_by_sleep
                ON sleep_correction_labels(sleep_id);
            CREATE INDEX IF NOT EXISTS idx_sleep_correction_labels_by_type
                ON sleep_correction_labels(label_type);
            CREATE INDEX IF NOT EXISTS idx_sleep_correction_labels_by_window
                ON sleep_correction_labels(start_time_unix_ms, end_time_unix_ms);

            CREATE TABLE IF NOT EXISTS metric_values (
                metric_value_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES algorithm_runs(run_id) ON DELETE CASCADE,
                metric_family TEXT NOT NULL,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS metric_components (
                metric_component_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES algorithm_runs(run_id) ON DELETE CASCADE,
                component_name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                contribution_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS calibration_labels (
                label_id TEXT PRIMARY KEY,
                metric_family TEXT NOT NULL,
                label_source TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS calibration_runs (
                calibration_run_id TEXT PRIMARY KEY,
                algorithm_id TEXT NOT NULL,
                version TEXT NOT NULL,
                train_start TEXT NOT NULL,
                train_end TEXT NOT NULL,
                holdout_start TEXT NOT NULL,
                holdout_end TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                params_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                FOREIGN KEY (algorithm_id, version)
                    REFERENCES algorithm_definitions(algorithm_id, version)
            );

            CREATE TABLE IF NOT EXISTS algorithm_preferences (
                scope TEXT NOT NULL,
                metric_family TEXT NOT NULL,
                algorithm_id TEXT NOT NULL,
                version TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (scope, metric_family),
                FOREIGN KEY (algorithm_id, version)
                    REFERENCES algorithm_definitions(algorithm_id, version)
            );

            CREATE TABLE IF NOT EXISTS debug_sessions (
                session_id TEXT PRIMARY KEY,
                started_at_unix_ms INTEGER NOT NULL,
                bridge_url TEXT NOT NULL,
                bind_host TEXT NOT NULL,
                token_required INTEGER NOT NULL,
                token_present INTEGER NOT NULL,
                remote_bind_enabled INTEGER NOT NULL,
                visible_remote_bind_toggle INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS debug_commands (
                command_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES debug_sessions(session_id) ON DELETE CASCADE,
                schema TEXT NOT NULL,
                command TEXT NOT NULL,
                args_json TEXT NOT NULL,
                dry_run INTEGER NOT NULL,
                received_at_unix_ms INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS debug_events (
                session_id TEXT NOT NULL REFERENCES debug_sessions(session_id) ON DELETE CASCADE,
                sequence INTEGER NOT NULL,
                schema TEXT NOT NULL,
                time_unix_ms INTEGER NOT NULL,
                source TEXT NOT NULL,
                level TEXT NOT NULL,
                topic TEXT NOT NULL,
                message TEXT NOT NULL,
                command_id TEXT REFERENCES debug_commands(command_id),
                data_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (session_id, sequence)
            );

            CREATE TABLE IF NOT EXISTS gravity (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                x REAL NOT NULL,
                y REAL NOT NULL,
                z REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_gravity_device_ts ON gravity(device_id, ts);

            CREATE TABLE IF NOT EXISTS gravity2_samples (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                x REAL NOT NULL,
                y REAL NOT NULL,
                z REAL NOT NULL,
                synced INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_gravity2_samples_device_ts ON gravity2_samples(device_id, ts);

            CREATE TABLE IF NOT EXISTS spo2_samples (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                red INTEGER NOT NULL,
                ir INTEGER NOT NULL,
                contact INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_spo2_samples_device_ts ON spo2_samples(device_id, ts);

            CREATE TABLE IF NOT EXISTS skin_temp_samples (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                raw INTEGER NOT NULL,
                contact INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_skin_temp_samples_device_ts ON skin_temp_samples(device_id, ts);

            CREATE TABLE IF NOT EXISTS resp_samples (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                raw INTEGER NOT NULL,
                contact INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_resp_samples_device_ts ON resp_samples(device_id, ts);

            CREATE TABLE IF NOT EXISTS sig_quality_samples (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                quality INTEGER NOT NULL,
                contact INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_sig_quality_samples_device_ts ON sig_quality_samples(device_id, ts);

            CREATE TABLE IF NOT EXISTS exercise_sessions (
                session_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
                device_id TEXT NOT NULL,
                start_ts REAL NOT NULL,
                end_ts REAL NOT NULL,
                duration_s REAL NOT NULL,
                avg_hr REAL NOT NULL,
                peak_hr REAL NOT NULL,
                strain REAL NOT NULL DEFAULT 0.0,
                calories_kcal REAL NOT NULL DEFAULT 0.0,
                zone_time_pct_json TEXT NOT NULL DEFAULT '{}',
                hrmax_source TEXT NOT NULL DEFAULT 'fallback',
                rhr_source TEXT NOT NULL DEFAULT 'daily_p10',
                avg_hrr_pct REAL NOT NULL DEFAULT 0.0,
                synced INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, start_ts)
            );

            CREATE INDEX IF NOT EXISTS idx_exercise_sessions_device_ts ON exercise_sessions(device_id, start_ts);

            CREATE TABLE IF NOT EXISTS hr_samples (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                bpm INTEGER NOT NULL,
                synced INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_hr_samples_device_ts ON hr_samples(device_id, ts);
            CREATE INDEX IF NOT EXISTS idx_hr_samples_synced_ts ON hr_samples(synced, ts);

            CREATE TABLE IF NOT EXISTS rr_intervals (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                interval_ms INTEGER NOT NULL,
                synced INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_rr_intervals_device_ts ON rr_intervals(device_id, ts);
            CREATE INDEX IF NOT EXISTS idx_rr_intervals_synced_ts ON rr_intervals(synced, ts);

            CREATE TABLE IF NOT EXISTS events (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                event_id INTEGER NOT NULL,
                event_name TEXT NOT NULL DEFAULT '',
                synced INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_events_device_ts ON events(device_id, ts);
            CREATE INDEX IF NOT EXISTS idx_events_synced_ts ON events(synced, ts);

            CREATE TABLE IF NOT EXISTS battery (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                level_pct INTEGER NOT NULL,
                synced INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts)
            );

            CREATE INDEX IF NOT EXISTS idx_battery_device_ts ON battery(device_id, ts);
            CREATE INDEX IF NOT EXISTS idx_battery_synced_ts ON battery(synced, ts);

            CREATE TABLE IF NOT EXISTS upload_cursors (
                namespace TEXT NOT NULL,
                stream TEXT NOT NULL,
                value TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (namespace, stream)
            );

            CREATE TABLE IF NOT EXISTS journal (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                date           TEXT NOT NULL,
                source         TEXT NOT NULL DEFAULT 'goose',
                behaviors_json TEXT NOT NULL DEFAULT '{}',
                notes          TEXT,
                created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(source, date)
            );

            CREATE TABLE IF NOT EXISTS workout (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_session_id TEXT REFERENCES activity_sessions(session_id) ON DELETE SET NULL,
                date                TEXT NOT NULL,
                source              TEXT NOT NULL,
                sport               TEXT NOT NULL,
                start_time          TEXT NOT NULL,
                end_time            TEXT NOT NULL,
                duration_s          REAL NOT NULL,
                avg_hr_bpm          REAL,
                max_hr_bpm          REAL,
                strain              REAL,
                calories_kcal       REAL,
                distance_m          REAL,
                notes               TEXT,
                provenance_json     TEXT NOT NULL DEFAULT '{}',
                created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(source, start_time)
            );

            CREATE TABLE IF NOT EXISTS apple_daily (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL,
                source      TEXT NOT NULL DEFAULT 'healthkit',
                steps       INTEGER,
                active_kcal REAL,
                basal_kcal  REAL,
                avg_hr_bpm  REAL,
                max_hr_bpm  REAL,
                vo2max      REAL,
                weight_kg   REAL,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(source, date)
            );

            CREATE TABLE IF NOT EXISTS metric_series (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                date        TEXT NOT NULL,
                value       REAL NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(source, metric_name, date)
            );

            CREATE INDEX IF NOT EXISTS idx_metric_series_lookup ON metric_series (source, metric_name, date);
            CREATE INDEX IF NOT EXISTS idx_journal_date ON journal (date);
            CREATE INDEX IF NOT EXISTS idx_workout_date ON workout (date);
            CREATE INDEX IF NOT EXISTS idx_apple_daily_date ON apple_daily (date);

            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (1);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (2);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (3);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (4);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (5);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (6);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (7);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (8);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (9);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (10);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (11);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (12);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (13);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (14);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (15);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (16);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (17);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (18);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (19);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (20);
            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (21);

            UPDATE decoded_frames SET device_type = 'GOOSE'
            WHERE device_type IN ('MAVERICK', 'PUFFIN');

            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (22);
            PRAGMA user_version = 22;

            CREATE TABLE IF NOT EXISTS sync_telemetry (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id       TEXT NOT NULL,
                burst_index      INTEGER NOT NULL,
                bytes_received   INTEGER NOT NULL,
                duration_ms      INTEGER NOT NULL,
                missing_packets  INTEGER NOT NULL DEFAULT 0,
                sequence_gaps    INTEGER NOT NULL DEFAULT 0,
                result           TEXT NOT NULL DEFAULT 'ok',
                created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE INDEX IF NOT EXISTS idx_sync_telemetry_session
                ON sync_telemetry(session_id);

            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (23);
            PRAGMA user_version = 23;

            CREATE TABLE IF NOT EXISTS optical_channel_samples (
                device_id TEXT NOT NULL,
                ts REAL NOT NULL,
                packet_k INTEGER NOT NULL,
                version INTEGER NOT NULL,
                channel_index INTEGER NOT NULL,
                samples_json TEXT NOT NULL,
                captured_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(device_id, ts, packet_k, channel_index)
            );
            CREATE INDEX IF NOT EXISTS idx_optical_channel_samples_device_ts
                ON optical_channel_samples(device_id, ts);

            CREATE TABLE IF NOT EXISTS device_feature_flags (
                device_id TEXT NOT NULL,
                flag_index INTEGER NOT NULL,
                flag_value INTEGER NOT NULL,
                discovered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY(device_id, flag_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS body_composition_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                weight_kg REAL,
                bmi REAL,
                body_fat_pct REAL,
                muscle_mass_kg REAL,
                water_pct REAL,
                source TEXT NOT NULL CHECK(source IN ('manual','healthkit','scale')),
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(source, date)
            );

            CREATE TABLE IF NOT EXISTS realtime_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_uuid TEXT NOT NULL,
                frame_hex TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'realtime_pip',
                synced INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_realtime_frames_device_captured
                ON realtime_frames(device_uuid, captured_at);

            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (24);
            PRAGMA user_version = 24;

            -- EXT-01: externally sourced point samples (Apple Health / Apple Watch via
            -- HealthKit). Idempotent on (platform, platform_record_id, metric) so a
            -- HealthKit re-import never duplicates rows; a changed value from the same
            -- record simply replaces the row (INSERT OR REPLACE in upsert_external_sample).
            -- HRV is stored as its own metric kind (hrv_sdnn) — never conflated with the
            -- WHOOP-derived RMSSD metric family (healthkit_rmssd_must_not_be_written_as_sdnn).
            CREATE TABLE IF NOT EXISTS external_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                platform_record_id TEXT NOT NULL,
                metric TEXT NOT NULL CHECK(metric IN (
                    'hr', 'hrv_sdnn', 'rhr', 'spo2', 'resp_rate', 'vo2max', 'skin_temp'
                )),
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                start_unix_ms INTEGER NOT NULL,
                end_unix_ms INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(platform, platform_record_id, metric)
            );

            CREATE INDEX IF NOT EXISTS idx_external_samples_metric_window
                ON external_samples(metric, start_unix_ms, end_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_external_samples_by_platform
                ON external_samples(platform, platform_record_id);

            INSERT OR IGNORE INTO goose_schema_migrations(version) VALUES (25);
            PRAGMA user_version = 25;
            "#,
        )?;
        } // conn lock released here — ensure_* each re-acquire independently
        self.ensure_raw_evidence_columns()?;
        self.ensure_decoded_frame_columns()?;
        self.ensure_algorithm_definition_columns()?;
        self.ensure_daily_activity_metric_multi_row_source_kind()?;
        self.ensure_daily_recovery_metric_multi_row_source_kind()?;
        self.ensure_step_counter_sample_columns()?;
        self.ensure_synced_columns()?;
        self.ensure_realtime_source_column()?;
        Ok(())
    }

    pub fn schema_version(&self) -> GooseResult<i64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        Ok(conn.query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    /// SYNC-12: Insert one row into sync_telemetry for a completed HPS burst.
    pub fn insert_sync_telemetry(
        &self,
        session_id: &str,
        burst_index: i64,
        bytes_received: i64,
        duration_ms: i64,
        missing_packets: i64,
        sequence_gaps: i64,
        result: &str,
    ) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        conn.execute(
            "INSERT INTO sync_telemetry \
             (session_id, burst_index, bytes_received, duration_ms, missing_packets, sequence_gaps, result) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![session_id, burst_index, bytes_received, duration_ms, missing_packets, sequence_gaps, result],
        )?;
        Ok(())
    }

    /// BODY-01: Upsert one row into body_composition_history.
    /// Uses INSERT OR REPLACE on UNIQUE(source, date) — a second call with the
    /// same (source, date) pair replaces the existing row with the new values.
    pub fn upsert_body_composition(
        &self,
        date: &str,
        source: &str,
        weight_kg: Option<f64>,
        bmi: Option<f64>,
        body_fat_pct: Option<f64>,
        muscle_mass_kg: Option<f64>,
        water_pct: Option<f64>,
    ) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        conn.execute(
            "INSERT OR REPLACE INTO body_composition_history \
             (date, source, weight_kg, bmi, body_fat_pct, muscle_mass_kg, water_pct) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                date,
                source,
                weight_kg,
                bmi,
                body_fat_pct,
                muscle_mass_kg,
                water_pct
            ],
        )?;
        Ok(())
    }

    /// BODY-01: Return all body composition rows in the inclusive date range
    /// [start_date, end_date], across ALL sources, ordered by date ascending (D-01).
    pub fn body_composition_history_between(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> GooseResult<Vec<BodyCompositionRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT date, source, weight_kg, bmi, body_fat_pct, muscle_mass_kg, water_pct \
             FROM body_composition_history \
             WHERE date >= ?1 AND date <= ?2 \
             ORDER BY date ASC",
        )?;
        let rows = stmt
            .query_map(params![start_date, end_date], |row| {
                Ok(BodyCompositionRow {
                    date: row.get(0)?,
                    source: row.get(1)?,
                    weight_kg: row.get(2)?,
                    bmi: row.get(3)?,
                    body_fat_pct: row.get(4)?,
                    muscle_mass_kg: row.get(5)?,
                    water_pct: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// EXT-01: Upsert one externally sourced sample (Apple Health / Apple Watch).
    /// Idempotent on (platform, platform_record_id, metric) via INSERT OR REPLACE — a
    /// re-import of the same HealthKit record never duplicates rows, and a corrected
    /// value from the same record replaces the prior one.
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_external_sample(
        &self,
        platform: &str,
        platform_record_id: &str,
        metric: &str,
        value: f64,
        unit: &str,
        start_unix_ms: i64,
        end_unix_ms: i64,
        source: &str,
    ) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        conn.execute(
            "INSERT OR REPLACE INTO external_samples \
             (platform, platform_record_id, metric, value, unit, start_unix_ms, end_unix_ms, source) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                platform,
                platform_record_id,
                metric,
                value,
                unit,
                start_unix_ms,
                end_unix_ms,
                source
            ],
        )?;
        Ok(())
    }

    /// EXT-01: Return all external_samples rows for one metric overlapping
    /// [start_unix_ms, end_unix_ms), sorted by start_unix_ms ascending.
    pub fn external_samples_between(
        &self,
        metric: &str,
        start_unix_ms: i64,
        end_unix_ms: i64,
    ) -> GooseResult<Vec<ExternalSampleRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT platform, platform_record_id, metric, value, unit, start_unix_ms, end_unix_ms, source, created_at \
             FROM external_samples \
             WHERE metric = ?1 AND start_unix_ms < ?3 AND end_unix_ms > ?2 \
             ORDER BY start_unix_ms ASC",
        )?;
        let rows = stmt
            .query_map(
                params![metric, start_unix_ms, end_unix_ms],
                external_sample_from_row,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// EXT-01: The most recent external_samples row for one metric within
    /// [start_unix_ms, end_unix_ms), by start_unix_ms. Used by the source-priority
    /// resolver (resolve_metric_priority) as the Apple Health fallback candidate.
    pub fn latest_external_sample_in_window(
        &self,
        metric: &str,
        start_unix_ms: i64,
        end_unix_ms: i64,
    ) -> GooseResult<Option<ExternalSampleRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        conn.query_row(
            "SELECT platform, platform_record_id, metric, value, unit, start_unix_ms, end_unix_ms, source, created_at \
             FROM external_samples \
             WHERE metric = ?1 AND start_unix_ms < ?3 AND end_unix_ms > ?2 \
             ORDER BY start_unix_ms DESC LIMIT 1",
            params![metric, start_unix_ms, end_unix_ms],
            external_sample_from_row,
        )
        .optional()
        .map_err(GooseError::from)
    }

    pub(super) fn ensure_overnight_mirror_tables(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS overnight_sync_sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                band_identifier TEXT,
                app_version TEXT,
                mode TEXT NOT NULL,
                final_status TEXT NOT NULL,
                raw_frame_count INTEGER NOT NULL DEFAULT 0,
                historical_frame_count INTEGER NOT NULL DEFAULT 0,
                k18_count INTEGER NOT NULL DEFAULT 0,
                k24_count INTEGER NOT NULL DEFAULT 0,
                k25_count INTEGER NOT NULL DEFAULT 0,
                k26_count INTEGER NOT NULL DEFAULT 0,
                packet47_count INTEGER NOT NULL DEFAULT 0,
                event17_count INTEGER NOT NULL DEFAULT 0,
                event29_count INTEGER NOT NULL DEFAULT 0,
                metadata49_count INTEGER NOT NULL DEFAULT 0,
                metadata56_count INTEGER NOT NULL DEFAULT 0,
                range_poll_count INTEGER NOT NULL DEFAULT 0,
                successful_range_poll_count INTEGER NOT NULL DEFAULT 0,
                event_log_count INTEGER NOT NULL DEFAULT 0,
                readiness_status TEXT,
                readiness TEXT,
                error_count INTEGER NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            CREATE TABLE IF NOT EXISTS ble_raw_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                source TEXT NOT NULL,
                device_id TEXT,
                active_device_name TEXT,
                connection_state TEXT,
                service_uuid TEXT,
                characteristic_uuid TEXT NOT NULL,
                device_type TEXT,
                command_or_event INTEGER,
                packet_type INTEGER,
                k_revision INTEGER,
                sequence INTEGER,
                frame_hex TEXT NOT NULL,
                payload_hex TEXT,
                byte_count INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                decode_status TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(session_id, captured_at, characteristic_uuid, sha256)
            );

            CREATE INDEX IF NOT EXISTS idx_ble_raw_notifications_session_time
                ON ble_raw_notifications(session_id, captured_at);
            CREATE INDEX IF NOT EXISTS idx_ble_raw_notifications_packet_type
                ON ble_raw_notifications(packet_type);

            CREATE TABLE IF NOT EXISTS historical_range_polls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                status TEXT NOT NULL,
                command_sequence INTEGER NOT NULL,
                result_code INTEGER NOT NULL,
                result_name TEXT NOT NULL,
                raw_payload_hex TEXT NOT NULL,
                raw_body_hex TEXT NOT NULL,
                revision_or_status INTEGER,
                page_current INTEGER,
                page_oldest INTEGER,
                page_end INTEGER,
                pages_behind INTEGER,
                pending_response_count INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE(session_id, captured_at, command_sequence, status, result_code, raw_body_hex)
            );

            CREATE INDEX IF NOT EXISTS idx_historical_range_polls_session_time
                ON historical_range_polls(session_id, captured_at);
            CREATE INDEX IF NOT EXISTS idx_historical_range_polls_status
                ON historical_range_polls(status);
            "#,
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    fn upsert_overnight_sync_session(
        &self,
        input: &OvernightSyncSessionInput<'_>,
    ) -> GooseResult<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        Self::upsert_overnight_sync_session_with_conn(&conn, input)
    }

    #[allow(dead_code)]
    pub(super) fn upsert_overnight_sync_session_with_conn(
        conn: &Connection,
        input: &OvernightSyncSessionInput<'_>,
    ) -> GooseResult<bool> {
        validate_required("session_id", input.session_id)?;
        validate_required("started_at", input.started_at)?;
        validate_required("mode", input.mode)?;
        validate_required("final_status", input.final_status)?;
        validate_non_negative("raw_frame_count", input.raw_frame_count)?;
        validate_non_negative("historical_frame_count", input.historical_frame_count)?;
        validate_non_negative("range_poll_count", input.range_poll_count)?;
        validate_non_negative(
            "successful_range_poll_count",
            input.successful_range_poll_count,
        )?;
        validate_non_negative("event_log_count", input.event_log_count)?;

        let changed = conn.execute(
            r#"
            INSERT INTO overnight_sync_sessions (
                session_id,
                started_at,
                ended_at,
                band_identifier,
                app_version,
                mode,
                final_status,
                raw_frame_count,
                historical_frame_count,
                k18_count,
                k24_count,
                k25_count,
                k26_count,
                packet47_count,
                event17_count,
                event29_count,
                metadata49_count,
                metadata56_count,
                range_poll_count,
                successful_range_poll_count,
                event_log_count,
                readiness_status,
                readiness,
                error_count,
                notes
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)
            ON CONFLICT(session_id) DO UPDATE SET
                ended_at = excluded.ended_at,
                band_identifier = excluded.band_identifier,
                app_version = excluded.app_version,
                mode = excluded.mode,
                final_status = excluded.final_status,
                raw_frame_count = excluded.raw_frame_count,
                historical_frame_count = excluded.historical_frame_count,
                k18_count = excluded.k18_count,
                k24_count = excluded.k24_count,
                k25_count = excluded.k25_count,
                k26_count = excluded.k26_count,
                packet47_count = excluded.packet47_count,
                event17_count = excluded.event17_count,
                event29_count = excluded.event29_count,
                metadata49_count = excluded.metadata49_count,
                metadata56_count = excluded.metadata56_count,
                range_poll_count = excluded.range_poll_count,
                successful_range_poll_count = excluded.successful_range_poll_count,
                event_log_count = excluded.event_log_count,
                readiness_status = excluded.readiness_status,
                readiness = excluded.readiness,
                error_count = excluded.error_count,
                notes = excluded.notes,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            "#,
            params![
                input.session_id,
                input.started_at,
                input.ended_at,
                input.band_identifier,
                input.app_version,
                input.mode,
                input.final_status,
                input.raw_frame_count,
                input.historical_frame_count,
                input.k18_count,
                input.k24_count,
                input.k25_count,
                input.k26_count,
                input.packet47_count,
                input.event17_count,
                input.event29_count,
                input.metadata49_count,
                input.metadata56_count,
                input.range_poll_count,
                input.successful_range_poll_count,
                input.event_log_count,
                input.readiness_status,
                input.readiness,
                input.error_count,
                input.notes,
            ],
        )?;
        Ok(changed > 0)
    }

    #[allow(dead_code)]
    fn insert_overnight_raw_notification(
        &self,
        input: &OvernightRawNotificationInput<'_>,
    ) -> GooseResult<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        Self::insert_overnight_raw_notification_with_conn(&conn, input)
    }

    #[allow(dead_code)]
    pub(super) fn insert_overnight_raw_notification_with_conn(
        conn: &Connection,
        input: &OvernightRawNotificationInput<'_>,
    ) -> GooseResult<bool> {
        validate_required("session_id", input.session_id)?;
        validate_required("captured_at", input.captured_at)?;
        validate_required("source", input.source)?;
        validate_required("characteristic_uuid", input.characteristic_uuid)?;
        validate_required("frame_hex", input.frame_hex)?;
        validate_required("decode_status", input.decode_status)?;
        validate_non_negative("byte_count", input.byte_count)?;

        let payload = hex::decode(input.frame_hex).map_err(|error| {
            GooseError::message(format!("frame_hex is not valid hexadecimal: {error}"))
        })?;
        let sha256 = sha256_hex(&payload);

        let changed = conn.execute(
            r#"
            INSERT OR IGNORE INTO ble_raw_notifications (
                session_id,
                captured_at,
                source,
                device_id,
                active_device_name,
                connection_state,
                service_uuid,
                characteristic_uuid,
                device_type,
                command_or_event,
                packet_type,
                k_revision,
                sequence,
                frame_hex,
                payload_hex,
                byte_count,
                sha256,
                decode_status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
            "#,
            params![
                input.session_id,
                input.captured_at,
                input.source,
                input.device_id,
                input.active_device_name,
                input.connection_state,
                input.service_uuid,
                input.characteristic_uuid,
                input.device_type,
                input.command_or_event,
                input.packet_type,
                input.k_revision,
                input.sequence,
                input.frame_hex,
                input.payload_hex,
                input.byte_count,
                sha256,
                input.decode_status,
            ],
        )?;
        Ok(changed > 0)
    }

    #[allow(dead_code)]
    fn insert_overnight_historical_range_poll(
        &self,
        input: &OvernightHistoricalRangePollInput<'_>,
    ) -> GooseResult<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        Self::insert_overnight_historical_range_poll_with_conn(&conn, input)
    }

    #[allow(dead_code)]
    pub(super) fn insert_overnight_historical_range_poll_with_conn(
        conn: &Connection,
        input: &OvernightHistoricalRangePollInput<'_>,
    ) -> GooseResult<bool> {
        validate_required("session_id", input.session_id)?;
        validate_required("captured_at", input.captured_at)?;
        validate_required("status", input.status)?;
        validate_required("result_name", input.result_name)?;
        validate_required("raw_payload_hex", input.raw_payload_hex)?;
        validate_required("raw_body_hex", input.raw_body_hex)?;
        validate_non_negative("command_sequence", input.command_sequence)?;
        validate_non_negative("result_code", input.result_code)?;
        validate_non_negative("pending_response_count", input.pending_response_count)?;
        validate_non_negative("retry_count", input.retry_count)?;

        let changed = conn.execute(
            r#"
            INSERT OR IGNORE INTO historical_range_polls (
                session_id,
                captured_at,
                status,
                command_sequence,
                result_code,
                result_name,
                raw_payload_hex,
                raw_body_hex,
                revision_or_status,
                page_current,
                page_oldest,
                page_end,
                pages_behind,
                pending_response_count,
                retry_count,
                notes
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            "#,
            params![
                input.session_id,
                input.captured_at,
                input.status,
                input.command_sequence,
                input.result_code,
                input.result_name,
                input.raw_payload_hex,
                input.raw_body_hex,
                input.revision_or_status,
                input.page_current,
                input.page_oldest,
                input.page_end,
                input.pages_behind,
                input.pending_response_count,
                input.retry_count,
                input.notes,
            ],
        )?;
        Ok(changed > 0)
    }

    /// OPT-03: Insert optical channel sample rows (v20/v21 packet format).
    /// Uses INSERT OR IGNORE — idempotent on (device_id, ts, packet_k, channel_index).
    pub fn insert_optical_samples(&self, rows: &[OpticalSampleRow]) -> GooseResult<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        for row in rows {
            conn.execute(
                "INSERT OR IGNORE INTO optical_channel_samples \
                 (device_id, ts, packet_k, version, channel_index, samples_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    row.device_id,
                    row.ts,
                    row.packet_k,
                    row.version,
                    row.channel_index,
                    row.samples_json,
                ],
            )?;
        }
        Ok(rows.len())
    }

    /// OPT-03: Query optical channel samples within a time range.
    pub fn query_optical_between(
        &self,
        device_id: &str,
        packet_k: i64,
        start_ts: f64,
        end_ts: f64,
    ) -> GooseResult<Vec<OpticalSampleRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT device_id, ts, packet_k, version, channel_index, samples_json \
             FROM optical_channel_samples \
             WHERE device_id = ?1 AND packet_k = ?2 AND ts >= ?3 AND ts <= ?4 \
             ORDER BY ts ASC",
        )?;
        let rows = stmt
            .query_map(params![device_id, packet_k, start_ts, end_ts], |row| {
                Ok(OpticalSampleRow {
                    device_id: row.get(0)?,
                    ts: row.get(1)?,
                    packet_k: row.get(2)?,
                    version: row.get(3)?,
                    channel_index: row.get(4)?,
                    samples_json: row.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// FF-03: Upsert device feature flags. Latest value wins (INSERT OR REPLACE).
    pub fn upsert_feature_flags(
        &self,
        device_id: &str,
        flags: &[(i64, i64)],
    ) -> GooseResult<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        for (flag_index, flag_value) in flags {
            conn.execute(
                "INSERT OR REPLACE INTO device_feature_flags \
                 (device_id, flag_index, flag_value) \
                 VALUES (?1, ?2, ?3)",
                params![device_id, flag_index, flag_value],
            )?;
        }
        Ok(flags.len())
    }

    /// FF-03: Get all feature flags for a device, ordered by flag_index.
    pub fn get_feature_flags(&self, device_id: &str) -> GooseResult<Vec<FeatureFlagRow>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let mut stmt = conn.prepare(
            "SELECT device_id, flag_index, flag_value, discovered_at \
             FROM device_feature_flags \
             WHERE device_id = ?1 \
             ORDER BY flag_index ASC",
        )?;
        let rows = stmt
            .query_map(params![device_id], |row| {
                Ok(FeatureFlagRow {
                    device_id: row.get(0)?,
                    flag_index: row.get(1)?,
                    flag_value: row.get(2)?,
                    discovered_at: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

impl GooseStore {
    fn ensure_raw_evidence_columns(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let columns = Self::table_columns_from_conn(&conn, "raw_evidence")?;
        for (column, ddl) in [
            (
                "capture_session_id",
                "capture_session_id TEXT REFERENCES capture_sessions(session_id) ON DELETE SET NULL",
            ),
            ("device_uuid", "device_uuid TEXT"),
        ] {
            if !columns.contains(column) {
                conn.execute(&format!("ALTER TABLE raw_evidence ADD COLUMN {ddl}"), [])?;
            }
        }
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_raw_evidence_by_device_uuid \
             ON raw_evidence(device_uuid, captured_at)",
            [],
        )?;
        Ok(())
    }

    fn ensure_decoded_frame_columns(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let columns = Self::table_columns_from_conn(&conn, "decoded_frames")?;
        for (column, ddl) in [
            ("packet_type_name", "packet_type_name TEXT"),
            (
                "parsed_payload_json",
                "parsed_payload_json TEXT NOT NULL DEFAULT 'null'",
            ),
            ("device_uuid", "device_uuid TEXT"),
        ] {
            if !columns.contains(column) {
                conn.execute(&format!("ALTER TABLE decoded_frames ADD COLUMN {ddl}"), [])?;
            }
        }
        Ok(())
    }

    fn ensure_algorithm_definition_columns(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let columns = Self::table_columns_from_conn(&conn, "algorithm_definitions")?;
        for (column, ddl) in [
            ("display_name", "display_name TEXT NOT NULL DEFAULT ''"),
            ("implementation", "implementation TEXT NOT NULL DEFAULT ''"),
            ("license", "license TEXT NOT NULL DEFAULT ''"),
            (
                "input_requirements_json",
                "input_requirements_json TEXT NOT NULL DEFAULT '{}'",
            ),
            (
                "quality_gates_json",
                "quality_gates_json TEXT NOT NULL DEFAULT '[]'",
            ),
            ("status", "status TEXT NOT NULL DEFAULT 'experimental'"),
        ] {
            if !columns.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE algorithm_definitions ADD COLUMN {ddl}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn ensure_daily_activity_metric_multi_row_source_kind(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        if !Self::table_has_source_kind_constraint_with_conn(&conn, "daily_activity_metrics")? {
            return Ok(());
        }

        conn.execute_batch(
            r#"
            ALTER TABLE daily_activity_metrics RENAME TO daily_activity_metrics_v12_source_unique;

            CREATE TABLE daily_activity_metrics (
                daily_metric_id TEXT PRIMARY KEY,
                date_key TEXT NOT NULL,
                timezone TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                steps INTEGER,
                active_kcal REAL,
                resting_kcal REAL,
                total_kcal REAL,
                average_cadence_spm REAL,
                source_kind TEXT NOT NULL,
                confidence REAL NOT NULL,
                inputs_json TEXT NOT NULL DEFAULT '{}',
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            INSERT INTO daily_activity_metrics (
                daily_metric_id,
                date_key,
                timezone,
                start_time_unix_ms,
                end_time_unix_ms,
                steps,
                active_kcal,
                resting_kcal,
                total_kcal,
                average_cadence_spm,
                source_kind,
                confidence,
                inputs_json,
                quality_flags_json,
                provenance_json,
                created_at,
                updated_at
            )
            SELECT
                daily_metric_id,
                date_key,
                timezone,
                start_time_unix_ms,
                end_time_unix_ms,
                steps,
                active_kcal,
                resting_kcal,
                total_kcal,
                average_cadence_spm,
                source_kind,
                confidence,
                inputs_json,
                quality_flags_json,
                provenance_json,
                created_at,
                updated_at
            FROM daily_activity_metrics_v12_source_unique;

            DROP TABLE daily_activity_metrics_v12_source_unique;

            CREATE INDEX IF NOT EXISTS idx_daily_activity_metrics_by_date
                ON daily_activity_metrics(date_key);
            CREATE INDEX IF NOT EXISTS idx_daily_activity_metrics_by_window
                ON daily_activity_metrics(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_daily_activity_metrics_by_source_kind
                ON daily_activity_metrics(source_kind);
            "#,
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    fn daily_activity_metrics_has_source_kind_unique_constraint(&self) -> GooseResult<bool> {
        self.table_has_source_kind_unique_constraint("daily_activity_metrics")
    }

    fn ensure_daily_recovery_metric_multi_row_source_kind(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        if !Self::table_has_source_kind_constraint_with_conn(&conn, "daily_recovery_metrics")? {
            return Ok(());
        }

        conn.execute_batch(
            r#"
            ALTER TABLE daily_recovery_metrics RENAME TO daily_recovery_metrics_source_unique;

            CREATE TABLE daily_recovery_metrics (
                daily_metric_id TEXT PRIMARY KEY,
                date_key TEXT NOT NULL,
                timezone TEXT NOT NULL,
                start_time_unix_ms INTEGER NOT NULL,
                end_time_unix_ms INTEGER NOT NULL,
                resting_hr_bpm REAL,
                hrv_rmssd_ms REAL,
                respiratory_rate_rpm REAL,
                oxygen_saturation_percent REAL,
                skin_temperature_delta_c REAL,
                source_kind TEXT NOT NULL,
                confidence REAL NOT NULL,
                inputs_json TEXT NOT NULL DEFAULT '{}',
                quality_flags_json TEXT NOT NULL DEFAULT '[]',
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            INSERT INTO daily_recovery_metrics (
                daily_metric_id,
                date_key,
                timezone,
                start_time_unix_ms,
                end_time_unix_ms,
                resting_hr_bpm,
                hrv_rmssd_ms,
                respiratory_rate_rpm,
                oxygen_saturation_percent,
                skin_temperature_delta_c,
                source_kind,
                confidence,
                inputs_json,
                quality_flags_json,
                provenance_json,
                created_at,
                updated_at
            )
            SELECT
                daily_metric_id,
                date_key,
                timezone,
                start_time_unix_ms,
                end_time_unix_ms,
                resting_hr_bpm,
                hrv_rmssd_ms,
                respiratory_rate_rpm,
                oxygen_saturation_percent,
                skin_temperature_delta_c,
                source_kind,
                confidence,
                inputs_json,
                quality_flags_json,
                provenance_json,
                created_at,
                updated_at
            FROM daily_recovery_metrics_source_unique;

            DROP TABLE daily_recovery_metrics_source_unique;

            CREATE INDEX IF NOT EXISTS idx_daily_recovery_metrics_by_date
                ON daily_recovery_metrics(date_key);
            CREATE INDEX IF NOT EXISTS idx_daily_recovery_metrics_by_window
                ON daily_recovery_metrics(start_time_unix_ms, end_time_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_daily_recovery_metrics_by_source_kind
                ON daily_recovery_metrics(source_kind);
            "#,
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    fn daily_recovery_metrics_has_source_kind_unique_constraint(&self) -> GooseResult<bool> {
        self.table_has_source_kind_unique_constraint("daily_recovery_metrics")
    }

    #[allow(dead_code)]
    fn table_has_source_kind_unique_constraint(&self, table: &str) -> GooseResult<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let mut statement = conn.prepare(&format!("PRAGMA index_list({table})"))?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(2)? != 0))
        })?;
        for row in rows {
            let (index_name, unique) = row?;
            if !unique {
                continue;
            }
            let columns = Self::index_columns_from_conn(&conn, &index_name)?;
            let column_names = columns.iter().map(String::as_str).collect::<Vec<_>>();
            if column_names == ["date_key", "timezone", "source_kind"] {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn ensure_step_counter_sample_columns(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let columns = Self::table_columns_from_conn(&conn, "step_counter_samples")?;
        for (column, ddl) in [
            ("cadence_spm", "cadence_spm REAL"),
            ("activity_state", "activity_state TEXT"),
        ] {
            if !columns.contains(column) {
                conn.execute(
                    &format!("ALTER TABLE step_counter_samples ADD COLUMN {ddl}"),
                    [],
                )?;
            }
        }
        Ok(())
    }

    fn table_columns_unchecked(&self, table: &str) -> GooseResult<BTreeSet<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        Self::table_columns_from_conn(&conn, table)
    }

    #[allow(dead_code)]
    fn index_columns_unchecked(&self, index_name: &str) -> GooseResult<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        Self::index_columns_from_conn(&conn, index_name)
    }

    // Connection-taking variants — used inside methods that already hold self.conn.lock()
    // to avoid mutex re-entrancy deadlocks.
    fn table_columns_from_conn(conn: &Connection, table: &str) -> GooseResult<BTreeSet<String>> {
        let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
        rows.collect::<Result<BTreeSet<_>, _>>()
            .map_err(GooseError::from)
    }

    fn index_columns_from_conn(conn: &Connection, index_name: &str) -> GooseResult<Vec<String>> {
        let escaped = index_name.replace('\'', "''");
        let mut statement = conn.prepare(&format!("PRAGMA index_info('{escaped}')"))?;
        let rows = statement.query_map([], |row| row.get::<_, String>(2))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(GooseError::from)
    }

    fn table_has_source_kind_constraint_with_conn(
        conn: &Connection,
        table: &str,
    ) -> GooseResult<bool> {
        let mut statement = conn.prepare(&format!("PRAGMA index_list({table})"))?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(2)? != 0))
        })?;
        for row in rows {
            let (index_name, unique) = row?;
            if !unique {
                continue;
            }
            let columns = Self::index_columns_from_conn(conn, &index_name)?;
            let column_names = columns.iter().map(String::as_str).collect::<Vec<_>>();
            if column_names == ["date_key", "timezone", "source_kind"] {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn ensure_synced_columns(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let synced_ddl = "synced INTEGER NOT NULL DEFAULT 0";
        for table in &[
            "spo2_samples",
            "skin_temp_samples",
            "resp_samples",
            "gravity",
            "gravity2_samples",
            "exercise_sessions",
        ] {
            let columns = Self::table_columns_from_conn(&conn, table)?;
            if !columns.contains("synced") {
                conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {synced_ddl}"), [])?;
            }
        }
        Ok(())
    }

    fn ensure_realtime_source_column(&self) -> GooseResult<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| GooseError::message("store mutex poisoned"))?;
        let columns = Self::table_columns_from_conn(&conn, "realtime_frames")?;
        if !columns.contains("source") {
            conn.execute(
                "ALTER TABLE realtime_frames ADD COLUMN source TEXT NOT NULL DEFAULT 'realtime_pip'",
                [],
            )?;
        }
        Ok(())
    }
}

pub(super) fn finite_json_number(value: &Value) -> Option<f64> {
    let value = value.as_f64()?;
    value.is_finite().then_some(value)
}

/// Convert a Unix timestamp (f64 seconds since epoch) to an ISO-8601 string
/// compatible with the format used in decoded_frames captured_at column.
/// Mirrors the chrono_from_unix helper in bridge.rs without adding a chrono dependency.
pub(super) fn unix_f64_to_iso8601(ts: f64) -> String {
    let secs = ts as u64;
    let ms = ((ts - secs as f64) * 1000.0) as u64;
    let h = (secs / 3600) % 24;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    let days = secs / 86400;
    let (year, month, day) = days_to_ymd_store(days as u32);
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{ms:03}Z")
}

/// Gregorian date from days-since-epoch (1970-01-01 = day 0).
/// Matches the logic used in the bridge-side days_to_ymd helper.
fn days_to_ymd_store(days: u32) -> (u32, u32, u32) {
    let days = days as i64;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as u32, m as u32, d as u32)
}

pub(super) fn metric_output_unit(name: &str) -> &'static str {
    if name.ends_with("_0_to_100") {
        "score_0_to_100"
    } else if name.ends_with("_0_to_21") {
        "score_0_to_21"
    } else if name.ends_with("_ms") {
        "ms"
    } else if name.ends_with("_minutes") {
        "minutes"
    } else if name.ends_with("_bpm") {
        "bpm"
    } else if name.ends_with("_rpm") {
        "rpm"
    } else if name.ends_with("_c") {
        "celsius"
    } else if name.ends_with("_fraction") {
        "fraction"
    } else if name.ends_with("_count") || name == "interval_count" || name == "disturbance_count" {
        "count"
    } else if name.ends_with("_per_hour") {
        "per_hour"
    } else if name.contains("load") {
        "load"
    } else {
        "raw"
    }
}

pub(super) fn validate_required(name: &str, value: &str) -> GooseResult<()> {
    if value.trim().is_empty() {
        Err(GooseError::message(format!("{name} is required")))
    } else {
        Ok(())
    }
}

fn validate_optional_required(name: &str, value: Option<&str>) -> GooseResult<()> {
    if let Some(value) = value {
        validate_required(name, value)?;
    }
    Ok(())
}

pub(super) fn validate_json(name: &str, value: &str) -> GooseResult<()> {
    serde_json::from_str::<serde_json::Value>(value)
        .map_err(|error| GooseError::message(format!("{name} must be valid JSON: {error}")))?;
    Ok(())
}

pub(super) fn validate_command_report_json(record: &CommandValidationRecord) -> GooseResult<()> {
    let parsed = serde_json::from_str::<serde_json::Value>(&record.report_json)
        .map_err(|error| GooseError::message(format!("report_json must be valid JSON: {error}")))?;
    let Some(report_command) = parsed.get("command").and_then(serde_json::Value::as_str) else {
        return Err(GooseError::message("report_json must contain command"));
    };
    if report_command != record.command {
        return Err(GooseError::message(format!(
            "report_json command {report_command} does not match record command {}",
            record.command
        )));
    }

    let Some(report_risk_gate) = parsed.get("risk_gate").and_then(serde_json::Value::as_str) else {
        return Err(GooseError::message("report_json must contain risk_gate"));
    };
    if report_risk_gate != record.risk_gate {
        return Err(GooseError::message(format!(
            "report_json risk_gate {report_risk_gate} does not match record risk_gate {}",
            record.risk_gate
        )));
    }

    let Some(report_ready) = parsed
        .get("direct_send_ready")
        .and_then(serde_json::Value::as_bool)
    else {
        return Err(GooseError::message(
            "report_json must contain direct_send_ready",
        ));
    };
    if report_ready != record.direct_send_ready {
        return Err(GooseError::message(format!(
            "report_json direct_send_ready {report_ready} does not match record direct_send_ready {}",
            record.direct_send_ready
        )));
    }
    Ok(())
}

pub(super) fn validate_json_object(name: &str, value: &str) -> GooseResult<()> {
    let parsed = serde_json::from_str::<serde_json::Value>(value)
        .map_err(|error| GooseError::message(format!("{name} must be valid JSON: {error}")))?;
    if !parsed.is_object() {
        return Err(GooseError::message(format!("{name} must be a JSON object")));
    }
    Ok(())
}

fn validate_no_official_whoop_label_marker(name: &str, value: &str) -> GooseResult<()> {
    let parsed = serde_json::from_str::<Value>(value)
        .map_err(|error| GooseError::message(format!("{name} must be valid JSON: {error}")))?;
    if value_contains_official_whoop_label_marker(&parsed) {
        return Err(GooseError::message(format!(
            "{name} must not contain official WHOOP label markers for formatted local metrics",
        )));
    }
    Ok(())
}

fn validate_no_official_whoop_label_text(name: &str, value: &str) -> GooseResult<()> {
    if is_official_whoop_label_token(value) {
        return Err(GooseError::message(format!(
            "{name} must not identify official WHOOP labels as a formatted metric source",
        )));
    }
    Ok(())
}

fn validate_no_platform_metric_source_marker(name: &str, value: &str) -> GooseResult<()> {
    let parsed = serde_json::from_str::<Value>(value)
        .map_err(|error| GooseError::message(format!("{name} must be valid JSON: {error}")))?;
    if value_contains_platform_metric_source_marker(&parsed, None) {
        return Err(GooseError::message(format!(
            "{name} must not contain HealthKit, Health Connect, Apple Health, or platform-import markers as formatted metric sources",
        )));
    }
    Ok(())
}

fn validate_no_platform_metric_source_text(name: &str, value: &str) -> GooseResult<()> {
    if is_platform_metric_source_token(value, None) {
        return Err(GooseError::message(format!(
            "{name} must not identify HealthKit, Health Connect, Apple Health, or platform imports as a formatted metric source",
        )));
    }
    Ok(())
}

fn value_contains_official_whoop_label_marker(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            if matches!(
                normalized_marker(key).as_str(),
                "official_whoop_label" | "whoop_label"
            ) && child.as_bool().unwrap_or(true)
            {
                return true;
            }
            value_contains_official_whoop_label_marker(child)
        }),
        Value::Array(values) => values
            .iter()
            .any(value_contains_official_whoop_label_marker),
        Value::String(text) => is_official_whoop_label_token(text),
        _ => false,
    }
}

fn is_official_whoop_label_token(value: &str) -> bool {
    let normalized = normalized_marker(value);
    // The official-label compliance policy declaration explicitly documents that
    // official WHOOP values are validation labels, never metric inputs. It is
    // compliance metadata, not a source-identity claim, so it must not trip the
    // marker guard even though it shares the `official_whoop_` prefix.
    if normalized == normalized_marker(OFFICIAL_WHOOP_LABEL_POLICY) {
        return false;
    }
    matches!(
        normalized.as_str(),
        "whoop"
            | "whoop_app"
            | "whoop_backend"
            | "official_whoop"
            | "official_whoop_label"
            | "official_whoop_app"
            | "official_whoop_backend"
            | "official_whoop_value"
            | "official_whoop_values"
            | "validation_label_from_whoop"
    ) || normalized.starts_with("official_whoop_")
        || normalized.starts_with("whoop_backend_")
}

fn value_contains_platform_metric_source_marker(value: &Value, parent_key: Option<&str>) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            if is_platform_metric_source_token(key, None) {
                return true;
            }
            value_contains_platform_metric_source_marker(child, Some(key))
        }),
        Value::Array(values) => values
            .iter()
            .any(|child| value_contains_platform_metric_source_marker(child, parent_key)),
        Value::String(text) => is_platform_metric_source_token(text, parent_key),
        _ => false,
    }
}

fn is_platform_metric_source_token(value: &str, parent_key: Option<&str>) -> bool {
    let normalized = normalized_marker(value);
    if !contains_platform_metric_source_marker(&normalized) {
        return false;
    }
    let parent_context = parent_key.map(normalized_marker);
    if parent_context
        .as_deref()
        .is_some_and(is_allowed_profile_platform_context)
        || is_allowed_profile_platform_context(&normalized)
    {
        return false;
    }
    true
}

fn contains_platform_metric_source_marker(normalized: &str) -> bool {
    normalized.contains("healthkit")
        || normalized.contains("health_kit")
        || normalized.contains("apple_health")
        || normalized.contains("applehealth")
        || normalized.contains("health_connect")
        || normalized.contains("healthconnect")
        || normalized.contains("android_health")
        || normalized.contains("platform_import")
        || normalized.contains("platform_imported")
        || normalized.contains("imported_platform")
        || normalized.contains("external_history_context_only")
        || normalized.contains("hkquantitytypeidentifier")
        || normalized.contains("hksample")
}

fn is_allowed_profile_platform_context(normalized: &str) -> bool {
    normalized.contains("profile")
        || normalized.contains("weight")
        || normalized.contains("body_mass")
        || normalized.contains("bodymass")
}

fn normalized_marker(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '.', ':'], "_")
}

fn validate_external_sleep_stage_summary_json(value: &str) -> GooseResult<()> {
    let parsed = serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
        GooseError::message(format!("stage_summary_json must be valid JSON: {error}"))
    })?;
    let Some(object) = parsed.as_object() else {
        return Err(GooseError::message(
            "stage_summary_json must be a JSON object",
        ));
    };
    if object.is_empty() {
        return Ok(());
    }
    let Some(minutes_by_stage) = object
        .get("minutes_by_stage")
        .and_then(serde_json::Value::as_object)
    else {
        return Err(GooseError::message(
            "stage_summary_json must contain minutes_by_stage object",
        ));
    };
    if minutes_by_stage.is_empty() {
        return Err(GooseError::message(
            "stage_summary_json minutes_by_stage must not be empty",
        ));
    }
    for (stage, minutes) in minutes_by_stage {
        if stage.trim().is_empty() {
            return Err(GooseError::message(
                "stage_summary_json stage names must not be empty",
            ));
        }
        validate_external_sleep_stage_summary_key(stage)?;
        let Some(minutes) = minutes.as_f64() else {
            return Err(GooseError::message(format!(
                "stage_summary_json minutes_by_stage.{stage} must be a number",
            )));
        };
        if !minutes.is_finite() || minutes < 0.0 {
            return Err(GooseError::message(format!(
                "stage_summary_json minutes_by_stage.{stage} must be finite and non-negative",
            )));
        }
    }
    Ok(())
}

fn validate_external_sleep_stage_summary_key(stage: &str) -> GooseResult<()> {
    let normalized = stage.trim().to_ascii_lowercase().replace([' ', '-'], "_");
    if ALLOWED_EXTERNAL_SLEEP_STAGE_SUMMARY_KEYS.contains(&normalized.as_str()) {
        Ok(())
    } else {
        Err(GooseError::message(format!(
            "stage_summary_json minutes_by_stage.{stage} stage must be recognized"
        )))
    }
}

pub(super) fn validate_non_negative(name: &str, value: i64) -> GooseResult<()> {
    if value < 0 {
        Err(GooseError::message(format!("{name} must be non-negative")))
    } else {
        Ok(())
    }
}

fn validate_optional_non_negative_i64(name: &str, value: Option<i64>) -> GooseResult<()> {
    if let Some(value) = value {
        validate_non_negative(name, value)?;
    }
    Ok(())
}

fn validate_optional_finite_f64(name: &str, value: Option<f64>) -> GooseResult<()> {
    if let Some(value) = value
        && !value.is_finite()
    {
        return Err(GooseError::message(format!("{name} must be finite")));
    }
    Ok(())
}

fn validate_optional_non_negative_f64(name: &str, value: Option<f64>) -> GooseResult<()> {
    if let Some(value) = value
        && (!value.is_finite() || value < 0.0)
    {
        return Err(GooseError::message(format!(
            "{name} must be finite and non-negative",
        )));
    }
    Ok(())
}

pub(super) fn validate_window_order(
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
) -> GooseResult<()> {
    if end_time_unix_ms <= start_time_unix_ms {
        Err(GooseError::message(
            "end_time_unix_ms must be greater than start_time_unix_ms",
        ))
    } else {
        Ok(())
    }
}

fn validate_allowed(name: &str, value: &str, allowed: &[&str]) -> GooseResult<()> {
    if allowed.contains(&value) {
        Ok(())
    } else {
        Err(GooseError::message(format!(
            "{name} must be one of: {}",
            allowed.join(", ")
        )))
    }
}

pub(super) fn validate_metric_source_kind(source_kind: &str) -> GooseResult<()> {
    validate_allowed("source_kind", source_kind, &ALLOWED_METRIC_SOURCE_KINDS)
}

pub(super) fn validate_metric_provenance_scope(metric_scope: &str) -> GooseResult<()> {
    validate_allowed(
        "metric_scope",
        metric_scope,
        &ALLOWED_METRIC_PROVENANCE_SCOPES,
    )
}

fn validate_external_sleep_platform(platform: &str) -> GooseResult<()> {
    validate_allowed("platform", platform, &ALLOWED_EXTERNAL_SLEEP_PLATFORMS)
}

fn validate_external_sleep_stage_kind(stage_kind: &str) -> GooseResult<()> {
    validate_allowed(
        "stage_kind",
        stage_kind,
        &ALLOWED_EXTERNAL_SLEEP_STAGE_KINDS,
    )
}

fn validate_sleep_correction_label_type(label_type: &str) -> GooseResult<()> {
    validate_allowed(
        "label_type",
        label_type,
        &ALLOWED_SLEEP_CORRECTION_LABEL_TYPES,
    )
}

pub(super) fn validate_confidence(name: &str, confidence: f64) -> GooseResult<()> {
    if !confidence.is_finite() {
        return Err(GooseError::message(format!("{name} must be finite")));
    }
    if !(0.0..=1.0).contains(&confidence) {
        return Err(GooseError::message(format!(
            "{name} must be between 0.0 and 1.0",
        )));
    }
    Ok(())
}

pub(super) fn validate_unavailable_metric_confidence(
    source_kind: &str,
    confidence: f64,
) -> GooseResult<()> {
    if source_kind == "unavailable" && confidence != 0.0 {
        return Err(GooseError::message(
            "unavailable formatted metrics must have confidence 0.0",
        ));
    }
    Ok(())
}

pub(super) fn validate_unavailable_metric_provenance_confidence(
    source_kind: &str,
    confidence: Option<f64>,
) -> GooseResult<()> {
    if source_kind == "unavailable" && confidence.unwrap_or(0.0) != 0.0 {
        return Err(GooseError::message(
            "unavailable metric provenance must have confidence 0.0",
        ));
    }
    Ok(())
}

pub(super) fn validate_daily_activity_metric_input(
    input: &DailyActivityMetricInput<'_>,
) -> GooseResult<()> {
    validate_required("daily_metric_id", input.daily_metric_id)?;
    validate_required("date_key", input.date_key)?;
    validate_required("timezone", input.timezone)?;
    validate_non_negative("start_time_unix_ms", input.start_time_unix_ms)?;
    validate_non_negative("end_time_unix_ms", input.end_time_unix_ms)?;
    validate_window_order(input.start_time_unix_ms, input.end_time_unix_ms)?;
    validate_optional_non_negative_i64("steps", input.steps)?;
    validate_optional_non_negative_f64("active_kcal", input.active_kcal)?;
    validate_optional_non_negative_f64("resting_kcal", input.resting_kcal)?;
    validate_optional_non_negative_f64("total_kcal", input.total_kcal)?;
    validate_optional_non_negative_f64("average_cadence_spm", input.average_cadence_spm)?;
    validate_required("source_kind", input.source_kind)?;
    validate_metric_source_kind(input.source_kind)?;
    validate_confidence("confidence", input.confidence)?;
    validate_unavailable_metric_confidence(input.source_kind, input.confidence)?;
    validate_activity_formatted_metric_values(
        input.source_kind,
        input.steps,
        input.active_kcal,
        input.resting_kcal,
        input.total_kcal,
        input.average_cadence_spm,
    )?;
    validate_json_object("inputs_json", input.inputs_json)?;
    validate_json("quality_flags_json", input.quality_flags_json)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    validate_no_official_whoop_label_marker("inputs_json", input.inputs_json)?;
    validate_no_official_whoop_label_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_official_whoop_label_marker("provenance_json", input.provenance_json)?;
    validate_no_platform_metric_source_marker("inputs_json", input.inputs_json)?;
    validate_no_platform_metric_source_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_platform_metric_source_marker("provenance_json", input.provenance_json)?;
    Ok(())
}

pub(super) fn validate_hourly_activity_metric_input(
    input: &HourlyActivityMetricInput<'_>,
) -> GooseResult<()> {
    validate_required("hourly_metric_id", input.hourly_metric_id)?;
    validate_required("date_key", input.date_key)?;
    validate_required("timezone", input.timezone)?;
    validate_non_negative("start_time_unix_ms", input.start_time_unix_ms)?;
    validate_non_negative("end_time_unix_ms", input.end_time_unix_ms)?;
    validate_window_order(input.start_time_unix_ms, input.end_time_unix_ms)?;
    validate_optional_non_negative_i64("steps", input.steps)?;
    validate_optional_non_negative_f64("active_kcal", input.active_kcal)?;
    validate_optional_non_negative_f64("resting_kcal", input.resting_kcal)?;
    validate_optional_non_negative_f64("total_kcal", input.total_kcal)?;
    validate_optional_non_negative_f64("average_cadence_spm", input.average_cadence_spm)?;
    validate_required("source_kind", input.source_kind)?;
    validate_metric_source_kind(input.source_kind)?;
    validate_confidence("confidence", input.confidence)?;
    validate_unavailable_metric_confidence(input.source_kind, input.confidence)?;
    validate_activity_formatted_metric_values(
        input.source_kind,
        input.steps,
        input.active_kcal,
        input.resting_kcal,
        input.total_kcal,
        input.average_cadence_spm,
    )?;
    validate_json_object("inputs_json", input.inputs_json)?;
    validate_json("quality_flags_json", input.quality_flags_json)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    validate_no_official_whoop_label_marker("inputs_json", input.inputs_json)?;
    validate_no_official_whoop_label_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_official_whoop_label_marker("provenance_json", input.provenance_json)?;
    validate_no_platform_metric_source_marker("inputs_json", input.inputs_json)?;
    validate_no_platform_metric_source_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_platform_metric_source_marker("provenance_json", input.provenance_json)?;
    Ok(())
}

pub(super) fn validate_daily_recovery_metric_input(
    input: &DailyRecoveryMetricInput<'_>,
) -> GooseResult<()> {
    validate_required("daily_metric_id", input.daily_metric_id)?;
    validate_required("date_key", input.date_key)?;
    validate_required("timezone", input.timezone)?;
    validate_non_negative("start_time_unix_ms", input.start_time_unix_ms)?;
    validate_non_negative("end_time_unix_ms", input.end_time_unix_ms)?;
    validate_window_order(input.start_time_unix_ms, input.end_time_unix_ms)?;
    validate_optional_non_negative_f64("resting_hr_bpm", input.resting_hr_bpm)?;
    validate_optional_non_negative_f64("hrv_rmssd_ms", input.hrv_rmssd_ms)?;
    validate_optional_non_negative_f64("respiratory_rate_rpm", input.respiratory_rate_rpm)?;
    validate_optional_non_negative_f64(
        "oxygen_saturation_percent",
        input.oxygen_saturation_percent,
    )?;
    validate_optional_finite_f64("skin_temperature_delta_c", input.skin_temperature_delta_c)?;
    validate_required("source_kind", input.source_kind)?;
    validate_metric_source_kind(input.source_kind)?;
    validate_confidence("confidence", input.confidence)?;
    validate_unavailable_metric_confidence(input.source_kind, input.confidence)?;
    validate_recovery_formatted_metric_values(
        input.source_kind,
        input.resting_hr_bpm,
        input.hrv_rmssd_ms,
        input.respiratory_rate_rpm,
        input.oxygen_saturation_percent,
        input.skin_temperature_delta_c,
    )?;
    validate_json_object("inputs_json", input.inputs_json)?;
    validate_json("quality_flags_json", input.quality_flags_json)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    validate_no_official_whoop_label_marker("inputs_json", input.inputs_json)?;
    validate_no_official_whoop_label_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_official_whoop_label_marker("provenance_json", input.provenance_json)?;
    validate_no_platform_metric_source_marker("inputs_json", input.inputs_json)?;
    validate_no_platform_metric_source_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_platform_metric_source_marker("provenance_json", input.provenance_json)?;
    Ok(())
}

fn validate_activity_formatted_metric_values(
    source_kind: &str,
    steps: Option<i64>,
    active_kcal: Option<f64>,
    resting_kcal: Option<f64>,
    total_kcal: Option<f64>,
    average_cadence_spm: Option<f64>,
) -> GooseResult<()> {
    let has_metric_value =
        steps.is_some() || active_kcal.is_some() || resting_kcal.is_some() || total_kcal.is_some();
    let has_any_value = has_metric_value || average_cadence_spm.is_some();
    if source_kind == "unavailable" {
        if has_any_value {
            return Err(GooseError::message(
                "unavailable activity metrics must not carry metric values",
            ));
        }
    } else if !has_metric_value {
        return Err(GooseError::message(
            "available activity metrics must include steps or calorie values",
        ));
    }
    Ok(())
}

fn validate_recovery_formatted_metric_values(
    source_kind: &str,
    resting_hr_bpm: Option<f64>,
    hrv_rmssd_ms: Option<f64>,
    respiratory_rate_rpm: Option<f64>,
    oxygen_saturation_percent: Option<f64>,
    skin_temperature_delta_c: Option<f64>,
) -> GooseResult<()> {
    let has_metric_value = resting_hr_bpm.is_some()
        || hrv_rmssd_ms.is_some()
        || respiratory_rate_rpm.is_some()
        || oxygen_saturation_percent.is_some()
        || skin_temperature_delta_c.is_some();
    if source_kind == "unavailable" {
        if has_metric_value {
            return Err(GooseError::message(
                "unavailable recovery metrics must not carry metric values",
            ));
        }
    } else if !has_metric_value {
        return Err(GooseError::message(
            "available recovery metrics must include at least one recovery value",
        ));
    }
    Ok(())
}

pub(super) fn validate_metric_provenance_input(
    store: &GooseStore,
    input: &MetricProvenanceInput<'_>,
) -> GooseResult<()> {
    validate_required("provenance_id", input.provenance_id)?;
    validate_required("metric_scope", input.metric_scope)?;
    validate_metric_provenance_scope(input.metric_scope)?;
    validate_required("metric_id", input.metric_id)?;
    validate_required("source_kind", input.source_kind)?;
    validate_metric_source_kind(input.source_kind)?;
    validate_required("source_detail", input.source_detail)?;
    validate_no_official_whoop_label_text("source_detail", input.source_detail)?;
    validate_no_platform_metric_source_text("source_detail", input.source_detail)?;
    if let Some(confidence) = input.confidence {
        validate_confidence("confidence", confidence)?;
    }
    validate_unavailable_metric_provenance_confidence(input.source_kind, input.confidence)?;
    validate_json_object("inputs_json", input.inputs_json)?;
    validate_json("quality_flags_json", input.quality_flags_json)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    validate_no_official_whoop_label_marker("inputs_json", input.inputs_json)?;
    validate_no_official_whoop_label_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_official_whoop_label_marker("provenance_json", input.provenance_json)?;
    validate_no_platform_metric_source_marker("inputs_json", input.inputs_json)?;
    validate_no_platform_metric_source_marker("quality_flags_json", input.quality_flags_json)?;
    validate_no_platform_metric_source_marker("provenance_json", input.provenance_json)?;
    validate_metric_provenance_target(store, input)?;
    Ok(())
}

fn validate_metric_provenance_target(
    store: &GooseStore,
    input: &MetricProvenanceInput<'_>,
) -> GooseResult<()> {
    let metric_source_kind = match input.metric_scope {
        "daily_activity" => store
            .daily_activity_metric(input.metric_id)?
            .map(|metric| metric.source_kind)
            .ok_or_else(|| {
                GooseError::message(
                    "metric_provenance metric_id must reference existing daily_activity metric",
                )
            })?,
        "daily_recovery" => store
            .daily_recovery_metric(input.metric_id)?
            .map(|metric| metric.source_kind)
            .ok_or_else(|| {
                GooseError::message(
                    "metric_provenance metric_id must reference existing daily_recovery metric",
                )
            })?,
        "hourly_activity" => store
            .hourly_activity_metric(input.metric_id)?
            .map(|metric| metric.source_kind)
            .ok_or_else(|| {
                GooseError::message(
                    "metric_provenance metric_id must reference existing hourly_activity metric",
                )
            })?,
        _ => unreachable!("metric_scope was validated before target lookup"),
    };
    if metric_source_kind != input.source_kind {
        return Err(GooseError::message(format!(
            "metric_provenance source_kind must match {} metric source_kind",
            input.metric_scope
        )));
    }
    Ok(())
}

pub(super) fn validate_metric_debug_feature_input(
    input: &MetricDebugFeatureInput<'_>,
) -> GooseResult<()> {
    validate_required("feature_id", input.feature_id)?;
    validate_required("metric_family", input.metric_family)?;
    validate_required("feature_name", input.feature_name)?;
    validate_non_negative("start_time_unix_ms", input.start_time_unix_ms)?;
    validate_non_negative("end_time_unix_ms", input.end_time_unix_ms)?;
    validate_window_order(input.start_time_unix_ms, input.end_time_unix_ms)?;
    validate_required("source_kind", input.source_kind)?;
    validate_metric_source_kind(input.source_kind)?;
    if let Some(confidence) = input.confidence {
        validate_confidence("confidence", confidence)?;
    }
    validate_json_object("feature_json", input.feature_json)?;
    validate_json_object("inputs_json", input.inputs_json)?;
    validate_json("quality_flags_json", input.quality_flags_json)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    Ok(())
}

pub(super) fn validate_step_counter_sample_input(
    input: &StepCounterSampleInput<'_>,
) -> GooseResult<()> {
    validate_required("sample_id", input.sample_id)?;
    validate_non_negative("sample_time_unix_ms", input.sample_time_unix_ms)?;
    validate_non_negative("counter_value", input.counter_value)?;
    validate_optional_non_negative_f64("cadence_spm", input.cadence_spm)?;
    validate_optional_required("activity_state", input.activity_state)?;
    validate_required("source_kind", input.source_kind)?;
    validate_metric_source_kind(input.source_kind)?;
    if input.source_kind != "device_counter" {
        return Err(GooseError::message(
            "source_kind for step_counter_samples must be device_counter",
        ));
    }
    validate_required("packet_family", input.packet_family)?;
    validate_required("json_path", input.json_path)?;
    validate_optional_required("frame_id", input.frame_id)?;
    validate_optional_required("evidence_id", input.evidence_id)?;
    validate_optional_required("capture_session_id", input.capture_session_id)?;
    validate_json("quality_flags_json", input.quality_flags_json)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    Ok(())
}

pub(super) fn validate_external_sleep_session_input(
    input: &ExternalSleepSessionInput<'_>,
) -> GooseResult<()> {
    validate_required("sleep_id", input.sleep_id)?;
    validate_required("source", input.source)?;
    validate_required("platform", input.platform)?;
    validate_external_sleep_platform(input.platform)?;
    validate_optional_required("platform_record_id", input.platform_record_id)?;
    validate_non_negative("start_time_unix_ms", input.start_time_unix_ms)?;
    validate_non_negative("end_time_unix_ms", input.end_time_unix_ms)?;
    validate_window_order(input.start_time_unix_ms, input.end_time_unix_ms)?;
    validate_optional_required("timezone", input.timezone)?;
    validate_external_sleep_stage_summary_json(input.stage_summary_json)?;
    validate_confidence("confidence", input.confidence)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    Ok(())
}

pub(super) fn validate_external_sleep_stage_input(
    store: &GooseStore,
    input: &ExternalSleepStageInput<'_>,
) -> GooseResult<()> {
    validate_required("stage_id", input.stage_id)?;
    validate_required("sleep_id", input.sleep_id)?;
    let Some(session) = store.external_sleep_session(input.sleep_id)? else {
        return Err(GooseError::message(format!(
            "external sleep session {} not found",
            input.sleep_id
        )));
    };
    validate_required("stage_kind", input.stage_kind)?;
    validate_external_sleep_stage_kind(input.stage_kind)?;
    validate_non_negative("start_time_unix_ms", input.start_time_unix_ms)?;
    validate_non_negative("end_time_unix_ms", input.end_time_unix_ms)?;
    validate_window_order(input.start_time_unix_ms, input.end_time_unix_ms)?;
    if input.start_time_unix_ms < session.start_time_unix_ms
        || input.end_time_unix_ms > session.end_time_unix_ms
    {
        return Err(GooseError::message(format!(
            "external sleep stage {} must be within parent sleep session {}",
            input.stage_id, input.sleep_id
        )));
    }
    validate_confidence("confidence", input.confidence)?;
    validate_json_object("provenance_json", input.provenance_json)?;
    Ok(())
}

pub(super) fn validate_sleep_correction_label_input(
    input: &SleepCorrectionLabelInput<'_>,
) -> GooseResult<()> {
    validate_required("label_id", input.label_id)?;
    validate_optional_required("sleep_id", input.sleep_id)?;
    validate_required("label_type", input.label_type)?;
    validate_sleep_correction_label_type(input.label_type)?;
    validate_non_negative("start_time_unix_ms", input.start_time_unix_ms)?;
    validate_non_negative("end_time_unix_ms", input.end_time_unix_ms)?;
    validate_window_order(input.start_time_unix_ms, input.end_time_unix_ms)?;
    validate_json_object("value_json", input.value_json)?;
    validate_required("source", input.source)?;
    if let Some(confidence) = input.confidence {
        validate_confidence("confidence", confidence)?;
    }
    validate_json_object("provenance_json", input.provenance_json)?;
    Ok(())
}

pub(super) fn is_allowed_calibration_label_source(source: &str) -> bool {
    matches!(
        source,
        "manual" | "passive_official_capture" | "user_export" | "screenshot_import" | "synthetic"
    )
}

pub(super) fn algorithm_preference_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<AlgorithmPreferenceRecord> {
    Ok(AlgorithmPreferenceRecord {
        scope: row.get(0)?,
        metric_family: row.get(1)?,
        algorithm_id: row.get(2)?,
        version: row.get(3)?,
    })
}

pub(super) fn decoded_frame_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DecodedFrameRow> {
    Ok(DecodedFrameRow {
        frame_id: row.get(0)?,
        evidence_id: row.get(1)?,
        captured_at: row.get(2)?,
        device_type: row.get(3)?,
        raw_len: row.get(4)?,
        header_len: row.get(5)?,
        declared_len: row.get(6)?,
        payload_hex: row.get(7)?,
        payload_crc_hex: row.get(8)?,
        header_crc_valid: row.get::<_, i64>(9)? != 0,
        payload_crc_valid: row.get::<_, i64>(10)? != 0,
        packet_type: row.get(11)?,
        packet_type_name: row.get(12)?,
        sequence: row.get(13)?,
        command_or_event: row.get(14)?,
        parsed_payload_json: row.get(15)?,
        parser_version: row.get(16)?,
        warnings_json: row.get(17)?,
        device_uuid: row.get(18)?,
    })
}

pub(super) fn command_validation_record_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<CommandValidationRecord> {
    Ok(CommandValidationRecord {
        command: row.get(0)?,
        risk_gate: row.get(1)?,
        direct_send_ready: i64_to_bool(row.get(2)?),
        report_json: row.get(3)?,
    })
}

pub(super) fn calibration_label_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<CalibrationLabelRow> {
    Ok(CalibrationLabelRow {
        label_id: row.get(0)?,
        metric_family: row.get(1)?,
        label_source: row.get(2)?,
        captured_at: row.get(3)?,
        value: row.get(4)?,
        unit: row.get(5)?,
        provenance_json: row.get(6)?,
    })
}

pub(super) fn capture_session_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<CaptureSessionRow> {
    Ok(CaptureSessionRow {
        session_id: row.get(0)?,
        source: row.get(1)?,
        started_at_unix_ms: row.get(2)?,
        ended_at_unix_ms: row.get(3)?,
        device_model: row.get(4)?,
        active_device_id: row.get(5)?,
        status: row.get(6)?,
        frame_count: row.get(7)?,
        provenance_json: row.get(8)?,
    })
}

pub(super) fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn i64_to_bool(value: i64) -> bool {
    value != 0
}

pub(super) fn daily_activity_metric_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<DailyActivityMetricRow> {
    Ok(DailyActivityMetricRow {
        daily_metric_id: row.get(0)?,
        date_key: row.get(1)?,
        timezone: row.get(2)?,
        start_time_unix_ms: row.get(3)?,
        end_time_unix_ms: row.get(4)?,
        steps: row.get(5)?,
        active_kcal: row.get(6)?,
        resting_kcal: row.get(7)?,
        total_kcal: row.get(8)?,
        average_cadence_spm: row.get(9)?,
        source_kind: row.get(10)?,
        confidence: row.get(11)?,
        inputs_json: row.get(12)?,
        quality_flags_json: row.get(13)?,
        provenance_json: row.get(14)?,
        created_at: row.get(15)?,
        updated_at: row.get(16)?,
    })
}

pub(super) fn hourly_activity_metric_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<HourlyActivityMetricRow> {
    Ok(HourlyActivityMetricRow {
        hourly_metric_id: row.get(0)?,
        date_key: row.get(1)?,
        timezone: row.get(2)?,
        start_time_unix_ms: row.get(3)?,
        end_time_unix_ms: row.get(4)?,
        steps: row.get(5)?,
        active_kcal: row.get(6)?,
        resting_kcal: row.get(7)?,
        total_kcal: row.get(8)?,
        average_cadence_spm: row.get(9)?,
        source_kind: row.get(10)?,
        confidence: row.get(11)?,
        inputs_json: row.get(12)?,
        quality_flags_json: row.get(13)?,
        provenance_json: row.get(14)?,
        created_at: row.get(15)?,
        updated_at: row.get(16)?,
    })
}

pub(super) fn daily_recovery_metric_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<DailyRecoveryMetricRow> {
    Ok(DailyRecoveryMetricRow {
        daily_metric_id: row.get(0)?,
        date_key: row.get(1)?,
        timezone: row.get(2)?,
        start_time_unix_ms: row.get(3)?,
        end_time_unix_ms: row.get(4)?,
        resting_hr_bpm: row.get(5)?,
        hrv_rmssd_ms: row.get(6)?,
        respiratory_rate_rpm: row.get(7)?,
        oxygen_saturation_percent: row.get(8)?,
        skin_temperature_delta_c: row.get(9)?,
        source_kind: row.get(10)?,
        confidence: row.get(11)?,
        inputs_json: row.get(12)?,
        quality_flags_json: row.get(13)?,
        provenance_json: row.get(14)?,
        created_at: row.get(15)?,
        updated_at: row.get(16)?,
    })
}

pub(super) fn metric_provenance_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MetricProvenanceRow> {
    Ok(MetricProvenanceRow {
        provenance_id: row.get(0)?,
        metric_scope: row.get(1)?,
        metric_id: row.get(2)?,
        source_kind: row.get(3)?,
        source_detail: row.get(4)?,
        confidence: row.get(5)?,
        inputs_json: row.get(6)?,
        quality_flags_json: row.get(7)?,
        provenance_json: row.get(8)?,
        created_at: row.get(9)?,
    })
}

pub(super) fn metric_debug_feature_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MetricDebugFeatureRow> {
    Ok(MetricDebugFeatureRow {
        feature_id: row.get(0)?,
        metric_family: row.get(1)?,
        feature_name: row.get(2)?,
        start_time_unix_ms: row.get(3)?,
        end_time_unix_ms: row.get(4)?,
        source_kind: row.get(5)?,
        confidence: row.get(6)?,
        feature_json: row.get(7)?,
        inputs_json: row.get(8)?,
        quality_flags_json: row.get(9)?,
        provenance_json: row.get(10)?,
        created_at: row.get(11)?,
    })
}

pub(super) fn step_counter_sample_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<StepCounterSampleRow> {
    Ok(StepCounterSampleRow {
        sample_id: row.get(0)?,
        sample_time_unix_ms: row.get(1)?,
        counter_value: row.get(2)?,
        cadence_spm: row.get(3)?,
        activity_state: row.get(4)?,
        source_kind: row.get(5)?,
        packet_family: row.get(6)?,
        json_path: row.get(7)?,
        frame_id: row.get(8)?,
        evidence_id: row.get(9)?,
        capture_session_id: row.get(10)?,
        quality_flags_json: row.get(11)?,
        provenance_json: row.get(12)?,
        created_at: row.get(13)?,
    })
}

pub(super) fn external_sleep_session_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExternalSleepSessionRow> {
    Ok(ExternalSleepSessionRow {
        sleep_id: row.get(0)?,
        source: row.get(1)?,
        platform: row.get(2)?,
        platform_record_id: row.get(3)?,
        start_time_unix_ms: row.get(4)?,
        end_time_unix_ms: row.get(5)?,
        duration_ms: row.get(6)?,
        timezone: row.get(7)?,
        stage_summary_json: row.get(8)?,
        confidence: row.get(9)?,
        provenance_json: row.get(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
    })
}

pub(super) fn external_sleep_stage_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExternalSleepStageRow> {
    Ok(ExternalSleepStageRow {
        stage_id: row.get(0)?,
        sleep_id: row.get(1)?,
        stage_kind: row.get(2)?,
        start_time_unix_ms: row.get(3)?,
        end_time_unix_ms: row.get(4)?,
        duration_ms: row.get(5)?,
        confidence: row.get(6)?,
        provenance_json: row.get(7)?,
        created_at: row.get(8)?,
    })
}

fn external_sample_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExternalSampleRow> {
    Ok(ExternalSampleRow {
        platform: row.get(0)?,
        platform_record_id: row.get(1)?,
        metric: row.get(2)?,
        value: row.get(3)?,
        unit: row.get(4)?,
        start_unix_ms: row.get(5)?,
        end_unix_ms: row.get(6)?,
        source: row.get(7)?,
        created_at: row.get(8)?,
    })
}

pub(super) fn sleep_correction_label_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SleepCorrectionLabelRow> {
    Ok(SleepCorrectionLabelRow {
        label_id: row.get(0)?,
        sleep_id: row.get(1)?,
        label_type: row.get(2)?,
        start_time_unix_ms: row.get(3)?,
        end_time_unix_ms: row.get(4)?,
        value_json: row.get(5)?,
        source: row.get(6)?,
        confidence: row.get(7)?,
        provenance_json: row.get(8)?,
        created_at: row.get(9)?,
    })
}

pub(super) fn device_type_name(device_type: DeviceType) -> &'static str {
    match device_type {
        DeviceType::Gen4 => "GEN_4",
        DeviceType::Maverick => "MAVERICK",
        DeviceType::Puffin => "PUFFIN",
        DeviceType::Goose => "GOOSE",
        DeviceType::HrMonitor => "HR_MONITOR",
    }
}

fn is_known_table(table: &str) -> bool {
    known_tables().contains(&table)
}

pub fn known_tables() -> &'static [&'static str] {
    &[
        "goose_schema_migrations",
        "raw_evidence",
        "decoded_frames",
        "algorithm_definitions",
        "algorithm_runs",
        "metric_values",
        "metric_components",
        "calibration_labels",
        "calibration_runs",
        "algorithm_preferences",
        "command_validation_records",
        "capture_sessions",
        "activity_sessions",
        "activity_metrics",
        "daily_activity_metrics",
        "hourly_activity_metrics",
        "daily_recovery_metrics",
        "metric_provenance",
        "metric_debug_features",
        "step_counter_samples",
        "activity_intervals",
        "activity_labels",
        "external_sleep_sessions",
        "external_sleep_stages",
        "sleep_correction_labels",
        "debug_sessions",
        "debug_commands",
        "debug_events",
        "exercise_sessions",
        "gravity",
        "gravity2_samples",
        "spo2_samples",
        "skin_temp_samples",
        "resp_samples",
        "sig_quality_samples",
        "hr_samples",
        "rr_intervals",
        "events",
        "battery",
        "upload_cursors",
        "journal",
        "workout",
        "apple_daily",
        "metric_series",
        "optical_channel_samples",
        "device_feature_flags",
        "body_composition_history",
        "realtime_frames",
        "sync_telemetry",
    ]
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod v24_biometric_tests {
    use super::*;

    fn make_store() -> GooseStore {
        GooseStore::open_in_memory().expect("failed to open in-memory store")
    }

    fn make_batch() -> V24BiometricBatch {
        V24BiometricBatch {
            spo2: vec![(1000.0_f64, 60000_i64, 55000_i64, 1_i64)],
            skin_temp: vec![(1000.0_f64, 930_i64, 1_i64)],
            resp: vec![(1000.0_f64, 12345_i64, 1_i64)],
            sig_quality: vec![(1000.0_f64, 3_i64, 1_i64)],
        }
    }

    #[test]
    fn test_insert_v24_batch_roundtrip() {
        let store = make_store();
        let batch = make_batch();

        store
            .insert_v24_biometric_batch("device-A", &batch)
            .expect("v24 biometric batch insert should succeed");

        let window = store
            .v24_biometric_samples_between("device-A", 0.0, 2000.0)
            .expect("v24 biometric samples query should succeed");

        assert_eq!(window.spo2.len(), 1);
        assert_eq!(window.spo2[0].ts, 1000.0);
        assert_eq!(window.spo2[0].red, 60000);
        assert_eq!(window.spo2[0].ir, 55000);
        assert_eq!(window.spo2[0].contact, 1);
        assert_eq!(window.spo2[0].device_id, "device-A");

        assert_eq!(window.skin_temp.len(), 1);
        assert_eq!(window.skin_temp[0].ts, 1000.0);
        assert_eq!(window.skin_temp[0].raw, 930);
        assert_eq!(window.skin_temp[0].contact, 1);

        assert_eq!(window.resp.len(), 1);
        assert_eq!(window.resp[0].ts, 1000.0);
        assert_eq!(window.resp[0].raw, 12345);
        assert_eq!(window.resp[0].contact, 1);

        assert_eq!(window.sig_quality.len(), 1);
        assert_eq!(window.sig_quality[0].ts, 1000.0);
        assert_eq!(window.sig_quality[0].quality, 3);
        assert_eq!(window.sig_quality[0].contact, 1);
    }

    #[test]
    fn test_insert_v24_batch_idempotent() {
        let store = make_store();
        let batch = make_batch();

        // Insert twice — second INSERT OR IGNORE should be a no-op.
        store
            .insert_v24_biometric_batch("device-A", &batch)
            .expect("v24 biometric batch first insert should succeed");
        store
            .insert_v24_biometric_batch("device-A", &batch)
            .expect("v24 biometric batch second insert (idempotent) should succeed");

        let window = store
            .v24_biometric_samples_between("device-A", 0.0, 2000.0)
            .expect("v24 biometric samples query should succeed");

        // Each table should have exactly 1 row.
        assert_eq!(
            window.spo2.len(),
            1,
            "spo2 should have exactly 1 row after idempotent insert"
        );
        assert_eq!(
            window.skin_temp.len(),
            1,
            "skin_temp should have exactly 1 row"
        );
        assert_eq!(window.resp.len(), 1, "resp should have exactly 1 row");
        assert_eq!(
            window.sig_quality.len(),
            1,
            "sig_quality should have exactly 1 row"
        );
    }

    #[test]
    fn test_insert_v24_batch_contact_zero() {
        let store = make_store();
        let batch = V24BiometricBatch {
            spo2: vec![(2000.0_f64, 50000_i64, 45000_i64, 0_i64)],
            skin_temp: vec![(2000.0_f64, 800_i64, 0_i64)],
            resp: vec![(2000.0_f64, 9999_i64, 0_i64)],
            sig_quality: vec![(2000.0_f64, 0_i64, 0_i64)],
        };

        store
            .insert_v24_biometric_batch("device-A", &batch)
            .expect("v24 biometric batch insert with contact=0 should succeed");

        let window = store
            .v24_biometric_samples_between("device-A", 0.0, 3000.0)
            .expect("v24 biometric samples query should succeed");

        // Rows with contact=0 are stored; downstream gating is consumer responsibility.
        assert_eq!(window.spo2.len(), 1);
        assert_eq!(window.spo2[0].contact, 0);

        assert_eq!(window.skin_temp.len(), 1);
        assert_eq!(window.skin_temp[0].contact, 0);

        assert_eq!(window.resp.len(), 1);
        assert_eq!(window.resp[0].contact, 0);

        assert_eq!(window.sig_quality.len(), 1);
        assert_eq!(window.sig_quality[0].contact, 0);
    }
}

#[cfg(test)]
mod exercise_session_tests {
    use super::*;

    fn make_store() -> GooseStore {
        GooseStore::open_in_memory().expect("failed to open in-memory store")
    }

    fn make_row(start_ts: f64, end_ts: f64) -> ExerciseSessionRow {
        ExerciseSessionRow {
            device_id: "device-X".to_string(),
            start_ts,
            end_ts,
            duration_s: end_ts - start_ts,
            avg_hr: 145.0,
            peak_hr: 182.0,
            strain: 12.5,
            calories_kcal: 420.0,
            zone_time_pct_json: r#"{"1":10,"2":20,"3":30,"4":30,"5":10}"#.to_string(),
            hrmax_source: "220_minus_age".to_string(),
            rhr_source: "daily_p10".to_string(),
            avg_hrr_pct: 65.0,
        }
    }

    #[test]
    fn test_exercise_sessions_table_exists() {
        let store = make_store();
        let count: i64 = store
            .conn
            .lock().unwrap()
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='exercise_sessions'",
                [],
                |row| row.get(0),
            )
            .expect("failed to query sqlite_master");
        assert_eq!(
            count, 1,
            "exercise_sessions table should exist after migration"
        );
    }

    #[test]
    fn test_exercise_sessions_schema_version() {
        let store = make_store();
        let version: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("failed to read user_version");
        assert_eq!(
            version, CURRENT_SCHEMA_VERSION,
            "PRAGMA user_version should equal CURRENT_SCHEMA_VERSION after migration"
        );
    }

    #[test]
    fn test_insert_exercise_session_roundtrip() {
        let store = make_store();
        let row = make_row(1_000_000.0, 1_003_600.0);

        let inserted = store
            .insert_exercise_session(&row)
            .expect("exercise session insert should succeed");
        assert!(inserted, "first insert should return true");

        let results = store
            .exercise_sessions_between("device-X", 900_000.0, 2_000_000.0)
            .expect("exercise sessions query should succeed");
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.device_id, row.device_id);
        assert_eq!(r.start_ts, row.start_ts);
        assert_eq!(r.end_ts, row.end_ts);
        assert_eq!(r.duration_s, row.duration_s);
        assert_eq!(r.avg_hr, row.avg_hr);
        assert_eq!(r.peak_hr, row.peak_hr);
        assert_eq!(r.strain, row.strain);
        assert_eq!(r.calories_kcal, row.calories_kcal);
        assert_eq!(r.zone_time_pct_json, row.zone_time_pct_json);
        assert_eq!(r.hrmax_source, row.hrmax_source);
        assert_eq!(r.rhr_source, row.rhr_source);
        assert_eq!(r.avg_hrr_pct, row.avg_hrr_pct);
    }

    #[test]
    fn test_insert_exercise_session_idempotent() {
        let store = make_store();
        let row = make_row(2_000_000.0, 2_003_600.0);

        let first = store
            .insert_exercise_session(&row)
            .expect("first exercise session insert should succeed");
        let second = store
            .insert_exercise_session(&row)
            .expect("second exercise session insert (idempotent) should succeed");

        assert!(first, "first insert should return true");
        assert!(!second, "duplicate insert should return false (OR IGNORE)");

        let results = store
            .exercise_sessions_between("device-X", 1_900_000.0, 3_000_000.0)
            .expect("exercise sessions query should succeed");
        assert_eq!(
            results.len(),
            1,
            "only one row should exist after idempotent insert"
        );
    }

    #[test]
    fn test_exercise_sessions_between_ordering() {
        let store = make_store();
        // Insert 3 rows out of chronological order.
        store
            .insert_exercise_session(&make_row(3_000.0, 3_600.0))
            .expect("exercise session insert for ordering test should succeed");
        store
            .insert_exercise_session(&make_row(1_000.0, 1_600.0))
            .expect("exercise session insert for ordering test should succeed");
        store
            .insert_exercise_session(&make_row(2_000.0, 2_600.0))
            .expect("exercise session insert for ordering test should succeed");

        let results = store
            .exercise_sessions_between("device-X", 0.0, 10_000.0)
            .expect("exercise sessions ordering query should succeed");
        assert_eq!(results.len(), 3);
        assert!(
            results[0].start_ts < results[1].start_ts && results[1].start_ts < results[2].start_ts,
            "results should be ordered by start_ts ascending"
        );
        assert_eq!(results[0].start_ts, 1_000.0);
        assert_eq!(results[1].start_ts, 2_000.0);
        assert_eq!(results[2].start_ts, 3_000.0);
    }
}

#[cfg(test)]
mod sync_schema_tests {
    use super::*;

    fn make_store() -> GooseStore {
        GooseStore::open_in_memory().expect("failed to open in-memory store")
    }

    #[test]
    fn test_schema_version_is_current() {
        let store = make_store();
        assert_eq!(
            store
                .schema_version()
                .expect("schema_version query should succeed"),
            CURRENT_SCHEMA_VERSION
        );
    }

    #[test]
    fn test_hr_samples_table_exists() {
        let store = make_store();
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='hr_samples'",
                [],
                |row| row.get(0),
            )
            .expect("sqlite_master count for hr_samples should succeed");
        assert_eq!(count, 1, "hr_samples table should exist");
    }

    #[test]
    fn test_rr_intervals_table_exists() {
        let store = make_store();
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='rr_intervals'",
                [],
                |row| row.get(0),
            )
            .expect("sqlite_master count for rr_intervals should succeed");
        assert_eq!(count, 1, "rr_intervals table should exist");
    }

    #[test]
    fn test_events_table_exists() {
        let store = make_store();
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='events'",
                [],
                |row| row.get(0),
            )
            .expect("sqlite_master count for events should succeed");
        assert_eq!(count, 1, "events table should exist");
    }

    #[test]
    fn test_battery_table_exists() {
        let store = make_store();
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='battery'",
                [],
                |row| row.get(0),
            )
            .expect("sqlite_master count for battery should succeed");
        assert_eq!(count, 1, "battery table should exist");
    }

    #[test]
    fn test_upload_cursors_table_exists() {
        let store = make_store();
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='upload_cursors'",
                [],
                |row| row.get(0),
            )
            .expect("sqlite_master count for upload_cursors should succeed");
        assert_eq!(count, 1, "upload_cursors table should exist");
    }

    #[test]
    fn test_synced_column_on_new_tables() {
        let store = make_store();
        let cols = store
            .table_columns_unchecked("hr_samples")
            .expect("table_columns_unchecked should succeed for hr_samples");
        assert!(
            cols.contains("synced"),
            "hr_samples should have synced column"
        );
    }

    #[test]
    fn test_synced_column_added_to_existing() {
        let store = make_store();
        for table in &[
            "spo2_samples",
            "skin_temp_samples",
            "resp_samples",
            "gravity",
        ] {
            let cols = store
                .table_columns_unchecked(table)
                .expect("table_columns_unchecked should succeed for known table");
            assert!(
                cols.contains("synced"),
                "{table} should have synced column after migration"
            );
        }
    }

    #[test]
    fn test_existing_rows_default_zero() {
        let store = make_store();
        store
            .conn
            .lock().unwrap()
            .execute(
                "INSERT OR IGNORE INTO gravity (device_id, ts, x, y, z) VALUES ('dev-1', 1000.0, 1.0, 2.0, 3.0)",
                [],
            )
            .expect("conn execute insert gravity row should succeed");
        let synced: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT synced FROM gravity WHERE device_id='dev-1' AND ts=1000.0",
                [],
                |row| row.get(0),
            )
            .expect("conn query_row for gravity synced should succeed");
        assert_eq!(synced, 0, "synced should default to 0 for existing rows");
    }

    #[test]
    fn test_upload_cursors_namespace_isolation() {
        let store = make_store();
        store
            .upsert_upload_cursor("highwater", "hr_samples", "1000.0")
            .expect("upsert_upload_cursor highwater should succeed");
        store
            .upsert_upload_cursor("read", "hr_samples", "500.0")
            .expect("upsert_upload_cursor read should succeed");

        let hw = store
            .get_upload_cursor("highwater", "hr_samples")
            .expect("get_upload_cursor highwater should succeed");
        let rd = store
            .get_upload_cursor("read", "hr_samples")
            .expect("get_upload_cursor read should succeed");

        assert_eq!(hw.as_deref(), Some("1000.0"));
        assert_eq!(rd.as_deref(), Some("500.0"));
    }
}

#[cfg(test)]
mod sync_methods_tests {
    use super::*;

    fn make_store() -> GooseStore {
        GooseStore::open_in_memory().expect("failed to open in-memory store")
    }

    /// Insert a minimal decoded frame row (with a NormalHistory HR packet) for backfill tests.
    /// Uses the raw SQL path because the store's public API requires evidence rows too.
    fn insert_test_hr_frame(store: &GooseStore, device_id: &str, ts_unix: u32, bpm: u8) {
        // Insert a synthetic raw_evidence row — all NOT NULL columns must be provided
        let evidence_id = format!("evidence-{ts_unix}");
        let captured_at = format!("1970-01-01T00:{:02}:{:02}.000Z", ts_unix / 60, ts_unix % 60);
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT OR IGNORE INTO raw_evidence \
             (evidence_id, source, captured_at, device_model, payload_hex, sha256, sensitivity) \
             VALUES (?1, 'test', ?2, 'test-device', '', '', 'standard')",
                params![evidence_id, captured_at],
            )
            .expect("conn execute insert raw_evidence should succeed");
        // Build the ParsedPayload JSON for a NormalHistory DataPacket.
        // ParsedPayload uses #[serde(tag = "kind", rename_all = "snake_case")], so
        // DataPacket serialises as {"kind":"data_packet", <fields flat>} (internally tagged).
        let payload_json = format!(
            r#"{{"kind":"data_packet","packet_k":40,"domain":null,"status_or_stream":null,"counter_or_page":null,"timestamp_seconds":{ts_unix},"timestamp_subseconds":null,"hr_marker_offset":null,"hr_present_marker":null,"body_offset":0,"body_hex":"","body_summary":{{"kind":"normal_history","hr_present":true,"marker_offset":null,"marker_value":{bpm}}},"warnings":[]}}"#
        );
        let frame_id = format!("frame-{ts_unix}");
        store.conn.lock().unwrap().execute(
            "INSERT OR IGNORE INTO decoded_frames \
             (frame_id, evidence_id, device_type, raw_len, header_len, declared_len, \
              payload_hex, payload_crc_hex, header_crc_valid, payload_crc_valid, \
              packet_type, packet_type_name, sequence, command_or_event, \
              parsed_payload_json, parser_version, warnings_json) \
             VALUES (?1, ?2, 'whoop5', 0, 0, 0, '', '', 1, 1, 40, 'REALTIME_DATA', 0, 0, ?3, 'test', '[]')",
            params![frame_id, evidence_id, payload_json],
        ).expect("conn execute insert decoded_frames should succeed");
    }

    #[test]
    fn test_mark_synced_sets_flag() {
        let store = make_store();
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO hr_samples (device_id, ts, bpm) VALUES ('dev-1', 1000.0, 75)",
                [],
            )
            .expect("conn execute insert hr_samples should succeed");
        let rowid: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT rowid FROM hr_samples WHERE device_id='dev-1' AND ts=1000.0",
                [],
                |r| r.get(0),
            )
            .expect("conn query_row for rowid should succeed");
        let affected = store
            .mark_synced_rows("hr_samples", &[rowid])
            .expect("mark_synced_rows should succeed");
        assert_eq!(affected, 1);
        let synced: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT synced FROM hr_samples WHERE rowid=?1",
                params![rowid],
                |r| r.get(0),
            )
            .expect("conn query_row for synced flag should succeed");
        assert_eq!(synced, 1, "synced should be 1 after mark_synced_rows");
    }

    #[test]
    fn test_mark_synced_unknown_table_rejected() {
        let store = make_store();
        let result = store.mark_synced_rows("nonexistent_table", &[1]);
        assert!(result.is_err(), "unknown stream should return Err");
        let msg = format!("{:?}", result.unwrap_err());
        assert!(
            msg.contains("unknown stream"),
            "error should mention unknown stream"
        );
    }

    #[test]
    fn test_rows_pending_upload_returns_unsynced() {
        let store = make_store();
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO hr_samples (device_id, ts, bpm, synced) VALUES ('d', 1.0, 60, 0)",
                [],
            )
            .expect("conn execute insert hr_samples synced=0 should succeed");
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO hr_samples (device_id, ts, bpm, synced) VALUES ('d', 2.0, 61, 0)",
                [],
            )
            .expect("conn execute insert hr_samples synced=0 should succeed");
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO hr_samples (device_id, ts, bpm, synced) VALUES ('d', 3.0, 62, 1)",
                [],
            )
            .expect("conn execute insert hr_samples synced=1 should succeed");
        let rows = store
            .rows_pending_upload("hr_samples", 10)
            .expect("rows_pending_upload should succeed");
        assert_eq!(rows.len(), 2, "only synced=0 rows should be returned");
    }

    #[test]
    fn test_rows_pending_upload_respects_limit() {
        let store = make_store();
        for i in 0..5i64 {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO hr_samples (device_id, ts, bpm, synced) VALUES ('d', ?1, 70, 0)",
                    params![i as f64],
                )
                .expect("conn execute insert hr_samples for limit test should succeed");
        }
        let rows = store
            .rows_pending_upload("hr_samples", 3)
            .expect("rows_pending_upload with limit should succeed");
        assert_eq!(rows.len(), 3, "limit=3 should return exactly 3 rows");
    }

    #[test]
    fn test_sync_backfill_creates_hr_rows() {
        let store = make_store();
        insert_test_hr_frame(&store, "dev-1", 1000, 75);
        let report = store
            .backfill_streams_from_decoded_frames("dev-1", 900.0, 1100.0)
            .expect("backfill_streams_from_decoded_frames should succeed");
        assert_eq!(report.hr_inserted, 1, "one HR row should be inserted");
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM hr_samples WHERE synced=0", [], |r| {
                r.get(0)
            })
            .expect("conn query_row for hr_samples count after backfill should succeed");
        assert_eq!(count, 1, "backfilled row must have synced=0 (not stranded)");
    }

    #[test]
    fn test_sync_backfill_is_idempotent() {
        let store = make_store();
        insert_test_hr_frame(&store, "dev-1", 2000, 80);
        let r1 = store
            .backfill_streams_from_decoded_frames("dev-1", 1900.0, 2100.0)
            .expect("backfill_streams_from_decoded_frames first call should succeed");
        let r2 = store
            .backfill_streams_from_decoded_frames("dev-1", 1900.0, 2100.0)
            .expect("backfill_streams_from_decoded_frames second call (idempotent) should succeed");
        assert_eq!(r1.hr_inserted, 1);
        assert_eq!(
            r2.hr_inserted, 0,
            "second backfill should insert 0 rows (idempotent via INSERT OR IGNORE)"
        );
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM hr_samples", [], |r| r.get(0))
            .expect("conn query_row for hr_samples count after two backfills should succeed");
        assert_eq!(count, 1, "exactly one row after two backfill calls");
    }

    #[test]
    fn test_sync_prune_respects_synced_flag() {
        let store = make_store();
        // Insert one synced=0 row and one synced=1 row
        store.conn.lock().unwrap().execute(
            "INSERT INTO gravity (device_id, ts, x, y, z, synced) VALUES ('d', 500.0, 0.0, 0.0, 1.0, 0)",
            [],
        ).expect("conn execute insert gravity synced=0 row should succeed");
        store.conn.lock().unwrap().execute(
            "INSERT INTO gravity (device_id, ts, x, y, z, synced) VALUES ('d', 600.0, 0.0, 0.0, 1.0, 1)",
            [],
        ).expect("conn execute insert gravity synced=1 row should succeed");
        // Prune all synced=1 rows older than ts=10000
        let pruned = store
            .prune_synced_stream_rows("gravity", 10000.0)
            .expect("prune_synced_stream_rows should succeed");
        assert_eq!(pruned, 1, "should prune exactly 1 synced=1 row");
        let remaining: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM gravity", [], |r| r.get(0))
            .expect("conn query_row for gravity count after prune should succeed");
        assert_eq!(remaining, 1, "synced=0 row must survive prune");
        let synced: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT synced FROM gravity WHERE ts=500.0", [], |r| {
                r.get(0)
            })
            .expect("conn query_row for surviving gravity synced flag should succeed");
        assert_eq!(synced, 0, "surviving row should still be synced=0");
    }

    #[test]
    fn test_sync_invalid_stream_rejected() {
        let store = make_store();
        // All three stream methods must reject unknown table names
        assert!(
            store
                .mark_synced_rows("'; DROP TABLE hr_samples; --", &[1])
                .is_err()
        );
        assert!(store.rows_pending_upload("malicious_table", 10).is_err());
        assert!(store.prune_synced_stream_rows("notastream", 0.0).is_err());
    }

    #[test]
    fn test_sync_cursor_namespace_isolation() {
        let store = make_store();
        store
            .upsert_upload_cursor("highwater", "hr_samples", "1000")
            .expect("upsert_upload_cursor highwater should succeed");
        store
            .upsert_upload_cursor("read", "hr_samples", "2000")
            .expect("upsert_upload_cursor read should succeed");
        let hw = store
            .get_upload_cursor("highwater", "hr_samples")
            .expect("get_upload_cursor highwater should succeed");
        let rd = store
            .get_upload_cursor("read", "hr_samples")
            .expect("get_upload_cursor read should succeed");
        assert_eq!(
            hw.as_deref(),
            Some("1000"),
            "highwater cursor should return 1000"
        );
        assert_eq!(
            rd.as_deref(),
            Some("2000"),
            "read cursor should return 2000"
        );
    }

    /// D-06 contract test: rows inserted AFTER rows_pending_upload captures IDs must remain
    /// synced=0 after mark_synced_rows is called with only the pre-captured IDs.
    ///
    /// Scenario: a BLE frame arrives during the HTTP round-trip (race window). The pre-capture
    /// pattern used in GooseUploadService means only rows visible BEFORE the upload request are
    /// marked. Any row arriving between pre-capture and mark_synced_rows must stay synced=0 and
    /// be included in the next upload cycle.
    #[test]
    fn test_pre_capture_does_not_mark_rows_inserted_during_race_window() {
        let store = make_store();

        // Step 1: insert the "pre-upload" row — exists before the HTTP request begins.
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO hr_samples (device_id, ts, bpm) VALUES ('dev-race', 1.0, 70)",
                [],
            )
            .expect("conn execute insert pre-upload hr_samples should succeed");

        // Step 2: pre-capture — simulates what GooseUploadService does before building the
        // HTTP payload. rows_pending_upload returns all synced=0 rows at this moment.
        let pending_before: Vec<serde_json::Value> = store
            .rows_pending_upload("hr_samples", 500)
            .expect("rows_pending_upload before race window should succeed");
        let captured_ids: Vec<i64> = pending_before
            .iter()
            .filter_map(|r| r["rowid"].as_i64())
            .collect();
        assert_eq!(
            captured_ids.len(),
            1,
            "exactly one row should be pending before upload"
        );

        // Step 3: race-window row — arrives while the HTTP request is in-flight, after
        // pre-capture but before mark_synced_rows is called.
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO hr_samples (device_id, ts, bpm) VALUES ('dev-race', 2.0, 72)",
                [],
            )
            .expect("conn execute insert race-window hr_samples should succeed");

        // Step 4: mark only the pre-captured IDs (simulates post-2xx mark).
        let affected = store
            .mark_synced_rows("hr_samples", &captured_ids)
            .expect("mark_synced_rows for pre-captured IDs should succeed");
        assert_eq!(
            affected, 1,
            "exactly the pre-captured row should be marked synced"
        );

        // Assertion A: exactly one row remains pending — the race-window row (ts=2.0).
        let pending_after: Vec<serde_json::Value> = store
            .rows_pending_upload("hr_samples", 10)
            .expect("rows_pending_upload after mark should succeed");
        assert_eq!(
            pending_after.len(),
            1,
            "race-window row must remain pending (synced=0)"
        );
        let ts = pending_after[0]["ts"].as_f64();
        assert_eq!(
            ts,
            Some(2.0),
            "pending row must be the race-window row (ts=2.0)"
        );

        // Assertion B: the pre-captured row is now synced=1.
        let synced_flag: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT synced FROM hr_samples WHERE ts=1.0", [], |r| {
                r.get(0)
            })
            .expect("conn query_row for synced flag of pre-captured row should succeed");
        assert_eq!(
            synced_flag, 1i64,
            "pre-captured row must be synced=1 after mark_synced_rows"
        );
    }
}

#[cfg(test)]
mod v20_migration_tests {
    use super::*;

    fn open_migrated_store() -> GooseStore {
        let store = GooseStore::open_in_memory().expect("open in-memory store");
        store.migrate().expect("migrate");
        store
    }

    #[test]
    fn test_schema_version_is_current() {
        let store = open_migrated_store();
        let version: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .expect("user_version");
        assert_eq!(
            version, CURRENT_SCHEMA_VERSION,
            "PRAGMA user_version must equal CURRENT_SCHEMA_VERSION after migration"
        );
    }

    #[test]
    fn test_v20_tables_exist() {
        let store = open_migrated_store();
        for table in &["journal", "workout", "apple_daily", "metric_series"] {
            let count: i64 = store
                .conn
                .lock()
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                    |r| r.get(0),
                )
                .expect("sqlite_master query");
            assert_eq!(count, 1, "table '{}' must exist after v20 migration", table);
        }
    }

    #[test]
    fn test_migration_is_idempotent() {
        let store = GooseStore::open_in_memory().expect("open in-memory store");
        store.migrate().expect("first migration");
        store.migrate().expect("second migration must not error");
        let count: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM goose_schema_migrations WHERE version=20",
                [],
                |r| r.get(0),
            )
            .expect("count version=20");
        assert_eq!(
            count, 1,
            "version=20 must appear exactly once after two migrate() calls"
        );
    }

    #[test]
    fn test_metric_series_unique_constraint() {
        let store = open_migrated_store();
        store
            .conn
            .lock().unwrap()
            .execute(
                "INSERT OR IGNORE INTO metric_series (source, metric_name, date, value) VALUES ('goose', 'hrv', '2026-06-01', 42.0)",
                [],
            )
            .expect("first insert");
        store
            .conn
            .lock().unwrap()
            .execute(
                "INSERT OR IGNORE INTO metric_series (source, metric_name, date, value) VALUES ('goose', 'hrv', '2026-06-01', 99.0)",
                [],
            )
            .expect("second insert (should be ignored)");
        let count: i64 = store
            .conn
            .lock().unwrap()
            .query_row(
                "SELECT COUNT(*) FROM metric_series WHERE source='goose' AND metric_name='hrv' AND date='2026-06-01'",
                [],
                |r| r.get(0),
            )
            .expect("count");
        assert_eq!(
            count, 1,
            "UNIQUE(source, metric_name, date) constraint must prevent duplicate rows"
        );
    }
}

#[cfg(test)]
mod migration_step_22_tests {
    use super::*;

    // Seed an in-memory store with MAVERICK and PUFFIN decoded_frames rows,
    // as if the DB were at schema version 21 before migration step 22 ran.
    // Returns the store after migrate() has run (i.e., step 22 applied).
    fn store_with_legacy_device_type_rows() -> GooseStore {
        let store = GooseStore::open_in_memory().expect("open in-memory store");
        // Insert test rows directly via the internal connection.
        // FK enforcement is off for the in-memory DB at this point because
        // migrate() already ran; use PRAGMA to insert without raw_evidence parent.
        store
            .conn
            .lock()
            .unwrap()
            .execute_batch("PRAGMA foreign_keys = OFF;")
            .expect("disable fk");
        store
            .conn
            .lock().unwrap()
            .execute_batch(
                r#"
                INSERT INTO decoded_frames (
                    frame_id, evidence_id, device_type,
                    raw_len, header_len, declared_len,
                    payload_hex, payload_crc_hex,
                    header_crc_valid, payload_crc_valid,
                    parser_version, warnings_json
                ) VALUES
                    ('frame-mav-1', 'ev-mav-1', 'MAVERICK', 16, 8, 8, 'aa', 'bb', 1, 1, 'test/0.1', '[]'),
                    ('frame-mav-2', 'ev-mav-1', 'MAVERICK', 16, 8, 8, 'aa', 'bb', 1, 1, 'test/0.1', '[]'),
                    ('frame-puf-1', 'ev-puf-1', 'PUFFIN',   16, 8, 8, 'bb', 'cc', 1, 1, 'test/0.1', '[]'),
                    ('frame-goose-1', 'ev-goose-1', 'GOOSE', 16, 8, 8, 'cc', 'dd', 1, 1, 'test/0.1', '[]');
                "#,
            )
            .expect("insert legacy device_type rows");
        store
            .conn
            .lock()
            .unwrap()
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("re-enable fk");

        // Run migrate() again — step 22 UPDATE is the target; INSERT OR IGNORE makes it safe.
        store.migrate().expect("second migrate for step 22");
        store
    }

    fn count_device_type(store: &GooseStore, device_type: &str) -> i64 {
        store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM decoded_frames WHERE device_type = ?1",
                [device_type],
                |row| row.get(0),
            )
            .expect("count device_type")
    }

    #[test]
    fn test_migration_step_22_maverick_puffin_to_goose() {
        let store = store_with_legacy_device_type_rows();

        // Schema version must be 22 after migration.
        assert_eq!(
            store.schema_version().expect("schema_version"),
            CURRENT_SCHEMA_VERSION
        );

        // All MAVERICK and PUFFIN rows are replaced with GOOSE.
        assert_eq!(
            count_device_type(&store, "MAVERICK"),
            0,
            "MAVERICK rows must be 0 after migration step 22"
        );
        assert_eq!(
            count_device_type(&store, "PUFFIN"),
            0,
            "PUFFIN rows must be 0 after migration step 22"
        );

        // 2 MAVERICK + 1 PUFFIN rows become GOOSE; 1 pre-existing GOOSE row is unchanged.
        assert_eq!(
            count_device_type(&store, "GOOSE"),
            4,
            "3 migrated (2 MAVERICK + 1 PUFFIN) + 1 pre-existing GOOSE row"
        );
    }

    #[test]
    fn test_migration_step_22_idempotent() {
        let store = store_with_legacy_device_type_rows();

        // After first migrate() (already run in helper), MAVERICK and PUFFIN are 0.
        assert_eq!(count_device_type(&store, "MAVERICK"), 0);
        assert_eq!(count_device_type(&store, "PUFFIN"), 0);

        // Run migrate() a second time — UPDATE WHERE IN matches nothing; INSERT OR IGNORE is a no-op.
        store.migrate().expect("second migrate must not error");

        assert_eq!(
            count_device_type(&store, "MAVERICK"),
            0,
            "MAVERICK count must remain 0 after second migrate() call"
        );
        assert_eq!(
            count_device_type(&store, "PUFFIN"),
            0,
            "PUFFIN count must remain 0 after second migrate() call"
        );
        assert_eq!(
            count_device_type(&store, "GOOSE"),
            4,
            "GOOSE count unchanged after second migrate() call"
        );
    }
}
