use serde::Deserialize;

use super::{
    BridgeRequest, BridgeResponse, acquire_bridge_conn, bridge_error, bridge_ok, request_args,
};
use crate::{GooseResult, store::resolve_metric_priority};

pub(crate) fn dispatch_external(request: &BridgeRequest) -> BridgeResponse {
    match request.method.as_str() {
        "external.import_samples" => request_args::<ExternalImportSamplesArgs>(request)
            .and_then(import_samples_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "external.import_daily_rollups" => request_args::<ExternalImportDailyRollupsArgs>(request)
            .and_then(import_daily_rollups_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "external.resolve_metric_for_day" => request_args::<ExternalResolveMetricArgs>(request)
            .and_then(resolve_metric_for_day_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        _ => unreachable!(
            "dispatch_external called with non-external method: {}",
            request.method
        ),
    }
}

#[derive(Debug, Deserialize)]
struct ExternalSampleInput {
    platform: String,
    platform_record_id: String,
    metric: String,
    value: f64,
    unit: String,
    start_unix_ms: i64,
    end_unix_ms: i64,
    source: String,
}

#[derive(Debug, Deserialize)]
struct ExternalImportSamplesArgs {
    database_path: String,
    #[serde(default)]
    samples: Vec<ExternalSampleInput>,
}

#[derive(Debug, Deserialize)]
struct DailyRollupInput {
    date: String,
    source: String,
    #[serde(default)]
    steps: Option<i64>,
    #[serde(default)]
    active_kcal: Option<f64>,
    #[serde(default)]
    basal_kcal: Option<f64>,
    #[serde(default)]
    avg_hr_bpm: Option<f64>,
    #[serde(default)]
    max_hr_bpm: Option<f64>,
    #[serde(default)]
    vo2max: Option<f64>,
    #[serde(default)]
    weight_kg: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ExternalImportDailyRollupsArgs {
    database_path: String,
    #[serde(default)]
    rollups: Vec<DailyRollupInput>,
}

#[derive(Debug, Deserialize)]
struct ExternalResolveMetricArgs {
    database_path: String,
    metric: String,
    start_unix_ms: i64,
    end_unix_ms: i64,
    #[serde(default)]
    whoop_value: Option<f64>,
}

/// EXT-01: Batch import of externally sourced samples (Apple Health / Apple Watch).
/// Idempotent — re-running with the same records never duplicates rows (see
/// upsert_external_sample). Returns the count actually processed, not a per-row diff,
/// since INSERT OR REPLACE does not distinguish insert from replace.
fn import_samples_bridge(args: ExternalImportSamplesArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let mut imported = 0i64;
    for sample in &args.samples {
        store.upsert_external_sample(
            &sample.platform,
            &sample.platform_record_id,
            &sample.metric,
            sample.value,
            &sample.unit,
            sample.start_unix_ms,
            sample.end_unix_ms,
            &sample.source,
        )?;
        imported += 1;
    }
    Ok(serde_json::json!({"imported": imported}))
}

/// EXT-01: Batch wrapper over the existing apple_daily.upsert store method (insert_apple_daily),
/// so Swift can send a full date-range backfill in one bridge round-trip instead of one call
/// per day. Reuses the existing apple_daily table — no new schema for daily rollups.
fn import_daily_rollups_bridge(
    args: ExternalImportDailyRollupsArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let mut imported = 0i64;
    for rollup in &args.rollups {
        store.insert_apple_daily(
            &rollup.date,
            &rollup.source,
            rollup.steps,
            rollup.active_kcal,
            rollup.basal_kcal,
            rollup.avg_hr_bpm,
            rollup.max_hr_bpm,
            rollup.vo2max,
            rollup.weight_kg,
        )?;
        imported += 1;
    }
    Ok(serde_json::json!({"imported": imported}))
}

/// EXT-01/EXT-03: Resolve one metric for one window, applying the WHOOP-first /
/// Apple-Health-fallback priority rule. The caller (Swift dashboard/metric query layer)
/// supplies whatever WHOOP-derived value it already computed for the same window; this
/// method only decides whether to use it or fall back to the latest external sample.
fn resolve_metric_for_day_bridge(
    args: ExternalResolveMetricArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let external = store.latest_external_sample_in_window(
        &args.metric,
        args.start_unix_ms,
        args.end_unix_ms,
    )?;
    let external_value = external.as_ref().map(|row| row.value);
    let resolved = resolve_metric_priority(args.whoop_value, external_value);
    match resolved {
        Some((value, source)) => Ok(serde_json::json!({
            "value": value,
            "source": source,
            "unit": external.map(|row| row.unit),
        })),
        None => Ok(serde_json::json!({
            "value": null,
            "source": null,
            "unit": null,
        })),
    }
}
