use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    BridgeRequest, BridgeResponse, acquire_bridge_conn, bridge_error, bridge_ok,
    default_correlation_end, default_correlation_start, metric_result_to_value,
    register_built_in_definitions, request_args,
};
use crate::{
    GooseError, GooseResult,
    algorithm_compare::{
        compare_hrv_goose_to_reference, compare_sleep_goose_to_external_reference_report,
        compare_sleep_goose_to_reference, compare_sleep_v1_goose_to_external_reference_report,
        compare_sleep_v1_goose_to_reference, compare_strain_goose_to_reference,
        compare_stress_goose_to_reference,
    },
    baselines::EwmaBaseline,
    bio_age::{
        ActivityFactorInput, BioAgeInputs, ConfidenceTier, HrvKind, Sex, SleepFactorInput,
        compute_bio_age,
    },
    calibration::{
        CalibrationApplicationInput, CalibrationDataset, CalibrationOptions, CalibrationRecord,
        CalibrationReport, apply_calibration, calibration_run_record, evaluate_linear_calibration,
    },
    capture_correlation::{
        CaptureCorrelationOptions, DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY,
        run_capture_correlation_for_store,
    },
    energy_rollup::{
        EnergyCaptureValidationOptions, EnergyDailyRollupOptions, EnergyHourlyRollupOptions,
        rollup_energy_day_for_store, rollup_energy_hour_for_store,
        rollup_energy_unavailable_daily_status_for_store, validate_energy_capture_for_store,
    },
    metric_features::{
        HeartRateFeatureOptions, HrvCaptureValidationOptions, HrvFeatureOptions,
        MetricWindowFeatureOptions, MotionFeatureOptions, OxygenSaturationCaptureValidationOptions,
        RecoveryFeatureScoreOptions, RecoverySensorDiscoveryOptions,
        RespiratoryRateCaptureValidationOptions, RestingHeartRateFeatureOptions,
        SleepFeatureScoreOptions, SleepFeatureScoreReport, SleepStageKind,
        StrainFeatureScoreOptions, StressFeatureScoreOptions, TemperatureCaptureValidationOptions,
        VitalEventFeatureOptions, run_heart_rate_feature_report_for_store,
        run_hrv_capture_validation_for_store, run_hrv_feature_report_for_store,
        run_metric_window_feature_report_for_store, run_motion_feature_report_for_store,
        run_oxygen_saturation_capture_validation_for_store,
        run_recovery_feature_score_report_for_store,
        run_recovery_sensor_discovery_report_for_store,
        run_respiratory_rate_capture_validation_for_store,
        run_resting_heart_rate_feature_report_for_store, run_sleep_feature_score_report_for_store,
        run_strain_feature_score_report_for_store, run_stress_feature_score_report_for_store,
        run_temperature_capture_validation_for_store, run_vital_event_feature_report_for_store,
    },
    metric_readiness::{MetricInputReadinessOptions, run_metric_input_readiness},
    metrics::{
        AlgorithmRunResult, GOOSE_HRV_V0_ID, GOOSE_HRV_V0_VERSION, GOOSE_RECOVERY_V0_ID,
        GOOSE_RECOVERY_V0_VERSION, GOOSE_SLEEP_V0_ID, GOOSE_SLEEP_V0_VERSION, GOOSE_SLEEP_V1_ID,
        GOOSE_SLEEP_V1_VERSION, GOOSE_STRAIN_V0_ID, GOOSE_STRAIN_V0_VERSION, GOOSE_STRESS_V0_ID,
        GOOSE_STRESS_V0_VERSION, HrvInput, ImuStepCountInput, ReadinessInput, RecoveryInput,
        RecoveryV1Input, SleepInput, SleepModelStatusInput, SleepNightHistoryInput,
        SleepStageSegment, SleepV1Input, StrainInput, StressInput, algorithm_run_record,
        built_in_algorithm_definitions, built_in_default_algorithm_preferences,
        fit_strain_denominator, goose_hrv_v0, goose_readiness_v1, goose_recovery_v0,
        goose_recovery_v1, goose_sleep_v0, goose_sleep_v1, goose_strain_v0, goose_strain_v1,
        goose_stress_v0, imu_step_count_v1, sleep_history_night_is_usable,
    },
    perf_budget::{PerfBudgetOptions, PerfBudgets, run_perf_budget},
    property_tests::{PropertySuiteOptions, run_property_suite},
    protocol::{DataPacketBodySummary, ParsedPayload},
    recovery_rollup::{
        RecoverySensorDailyRollupOptions, RecoveryUnavailableDailyStatusOptions,
        RestingHeartRateCaptureValidationOptions, RestingHeartRateDailyRollupOptions,
        rollup_recovery_sensor_daily_for_store, rollup_recovery_unavailable_daily_status_for_store,
        rollup_resting_heart_rate_day_for_store, validate_resting_heart_rate_capture_for_store,
    },
    reference::reference_algorithm_definitions,
    sleep_need::compute_sleep_need_with_store,
    sleep_staging::{
        EpochHrFeature, SleepStagingInput, SleepStagingOutput, stage_sleep_four_class,
    },
    step_counter::{
        ActivityUnavailableDailyStatusOptions, StepCounterDailyRollupOptions,
        StepCounterHourlyRollupOptions, StepCounterIngestOptions,
        rollup_activity_unavailable_daily_status_for_store, rollup_device_step_counter_day,
        rollup_device_step_counter_hour, run_step_counter_ingest_for_store,
    },
    step_discovery::{
        StepCaptureValidationOptions, StepPacketDiscoveryOptions,
        run_step_capture_validation_for_store, run_step_packet_discovery_for_store,
    },
    step_motion_estimator::{RawMotionStepEstimateOptions, run_raw_motion_step_estimate_for_store},
    store::{
        AlgorithmRunRecord, CalibrationLabelInput, CalibrationLabelRow, ExerciseSessionRow,
        ExternalSleepSessionRow, ExternalSleepStageRow, GooseStore, GravityRow,
    },
};

pub(crate) fn dispatch_metrics(request: &BridgeRequest) -> BridgeResponse {
    match request.method.as_str() {
        "metrics.bio_age" => request_args::<BioAgeArgs>(request)
            .and_then(bio_age_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.built_in_definitions" => serde_json::to_value(built_in_algorithm_definitions())
            .map_err(|error| GooseError::message(error.to_string()))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.reference_definitions" => serde_json::to_value(reference_algorithm_definitions())
            .map_err(|error| GooseError::message(error.to_string()))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.reference_compare" => request_args::<ReferenceCompareArgs>(request)
            .and_then(reference_compare_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.default_preferences" => {
            serde_json::to_value(built_in_default_algorithm_preferences())
                .map_err(|error| GooseError::message(error.to_string()))
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.goose_hrv_v0" => request_args::<HrvInput>(request)
            .and_then(|input| metric_result_to_value(goose_hrv_v0(&input)))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_sleep_v0" => request_args::<SleepInput>(request)
            .and_then(|input| metric_result_to_value(goose_sleep_v0(&input)))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_sleep_v1" => request_args::<SleepV1Input>(request)
            .and_then(|input| metric_result_to_value(goose_sleep_v1(&input)))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_strain_v0" => request_args::<StrainInput>(request)
            .and_then(|input| metric_result_to_value(goose_strain_v0(&input)))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_strain_v1" => request_args::<StrainInput>(request)
            .and_then(|input| metric_result_to_value(goose_strain_v1(&input)))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.fit_strain_denominator" => request_args::<FitStrainDenominatorArgs>(request)
            .and_then(|args| match fit_strain_denominator(&args.pairs) {
                Some(d) => Ok(serde_json::json!({ "denominator": d })),
                None => Err(GooseError::message(
                    "insufficient_or_degenerate_pairs".to_string(),
                )),
            })
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_recovery_v0" => request_args::<RecoveryInput>(request)
            .and_then(|input| metric_result_to_value(goose_recovery_v0(&input)))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_recovery_v1" => request_args::<RecoveryV1BridgeArgs>(request)
            .and_then(goose_recovery_v1_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_stress_v0" => request_args::<StressInput>(request)
            .and_then(|input| metric_result_to_value(goose_stress_v0(&input)))
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.goose_readiness_v1" => request_args::<ReadinessInput>(request)
            .and_then(|input| {
                serde_json::to_value(goose_readiness_v1(&input)).map_err(|e| {
                    GooseError::message(format!("cannot serialize readiness_v1 output: {e}"))
                })
            })
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.imu_step_count_from_decoded_frames" => {
            request_args::<ImuStepCountFromDecodedFramesArgs>(request)
                .and_then(imu_step_count_from_decoded_frames_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.imu_step_count_v1" => request_args::<ImuStepCountInput>(request)
            .and_then(|input| {
                serde_json::to_value(imu_step_count_v1(&input)).map_err(|e| {
                    GooseError::message(format!("cannot serialize imu_step_count output: {e}"))
                })
            })
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.input_readiness" => request_args::<MetricInputReadinessArgs>(request)
            .and_then(metric_input_readiness_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.motion_features" => request_args::<MotionFeaturesArgs>(request)
            .and_then(motion_features_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.heart_rate_features" => request_args::<HeartRateFeaturesArgs>(request)
            .and_then(heart_rate_features_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.vital_event_features" => request_args::<VitalEventFeaturesArgs>(request)
            .and_then(vital_event_features_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.step_packet_discovery" => request_args::<StepPacketDiscoveryArgs>(request)
            .and_then(step_packet_discovery_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.step_capture_validation" => request_args::<StepCaptureValidationArgs>(request)
            .and_then(step_capture_validation_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.raw_motion_step_estimate" => request_args::<RawMotionStepEstimateArgs>(request)
            .and_then(raw_motion_step_estimate_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.step_counter_ingest" => request_args::<StepCounterIngestArgs>(request)
            .and_then(step_counter_ingest_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.step_counter_daily_rollup" => request_args::<StepCounterDailyRollupArgs>(request)
            .and_then(step_counter_daily_rollup_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.step_counter_hourly_rollup" => {
            request_args::<StepCounterHourlyRollupArgs>(request)
                .and_then(step_counter_hourly_rollup_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.activity_unavailable_daily_status" => {
            request_args::<ActivityUnavailableDailyStatusArgs>(request)
                .and_then(activity_unavailable_daily_status_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.daily_activity_metrics" => request_args::<DailyActivityMetricListArgs>(request)
            .and_then(daily_activity_metrics_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.hourly_activity_metrics" => request_args::<HourlyActivityMetricListArgs>(request)
            .and_then(hourly_activity_metrics_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.daily_recovery_metrics" => request_args::<DailyRecoveryMetricListArgs>(request)
            .and_then(daily_recovery_metrics_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.energy_daily_rollup" => request_args::<EnergyDailyRollupArgs>(request)
            .and_then(energy_daily_rollup_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.energy_unavailable_daily_status" => request_args::<EnergyDailyRollupArgs>(request)
            .and_then(energy_unavailable_daily_status_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.energy_hourly_rollup" => request_args::<EnergyHourlyRollupArgs>(request)
            .and_then(energy_hourly_rollup_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.energy_capture_validation" => request_args::<EnergyCaptureValidationArgs>(request)
            .and_then(energy_capture_validation_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.hrv_features" => request_args::<HrvFeaturesArgs>(request)
            .and_then(hrv_features_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.hrv_capture_validation" => request_args::<HrvCaptureValidationArgs>(request)
            .and_then(hrv_capture_validation_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.respiratory_rate_capture_validation" => {
            request_args::<RespiratoryRateCaptureValidationArgs>(request)
                .and_then(respiratory_rate_capture_validation_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.oxygen_saturation_capture_validation" => {
            request_args::<OxygenSaturationCaptureValidationArgs>(request)
                .and_then(oxygen_saturation_capture_validation_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.temperature_capture_validation" => {
            request_args::<TemperatureCaptureValidationArgs>(request)
                .and_then(temperature_capture_validation_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.recovery_sensor_discovery" => request_args::<RecoverySensorDiscoveryArgs>(request)
            .and_then(recovery_sensor_discovery_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.recovery_unavailable_daily_status" => {
            request_args::<RecoveryUnavailableDailyStatusArgs>(request)
                .and_then(recovery_unavailable_daily_status_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.recovery_sensor_daily_rollup" => {
            request_args::<RecoverySensorDailyRollupArgs>(request)
                .and_then(recovery_sensor_daily_rollup_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.window_features" => request_args::<MetricWindowFeaturesArgs>(request)
            .and_then(metric_window_features_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.resting_hr_features" => request_args::<RestingHeartRateFeaturesArgs>(request)
            .and_then(resting_heart_rate_features_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.resting_hr_daily_rollup" => {
            request_args::<RestingHeartRateDailyRollupArgs>(request)
                .and_then(resting_heart_rate_daily_rollup_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.resting_hr_capture_validation" => {
            request_args::<RestingHeartRateCaptureValidationArgs>(request)
                .and_then(resting_heart_rate_capture_validation_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "metrics.sleep_score_from_features" => request_args::<SleepFeatureScoreArgs>(request)
            .and_then(sleep_feature_score_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.sleep_staging" => request_args::<SleepStagingBridgeArgs>(request)
            .and_then(sleep_staging_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.recovery_score_from_features" => request_args::<RecoveryFeatureScoreArgs>(request)
            .and_then(recovery_feature_score_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.strain_score_from_features" => request_args::<StrainFeatureScoreArgs>(request)
            .and_then(strain_feature_score_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metrics.stress_score_from_features" => request_args::<StressFeatureScoreArgs>(request)
            .and_then(stress_feature_score_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "calibration.evaluate_dataset" => request_args::<EvaluateCalibrationDatasetArgs>(request)
            .and_then(evaluate_calibration_dataset_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "calibration.evaluate_stored_labels" => {
            request_args::<EvaluateStoredCalibrationLabelsArgs>(request)
                .and_then(evaluate_stored_calibration_labels_bridge)
                .map(|value| bridge_ok(&request.request_id, value))
                .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error))
        }
        "calibration.import_labels" => request_args::<ImportCalibrationLabelsArgs>(request)
            .and_then(import_calibration_labels_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "calibration.list_labels" => request_args::<ListCalibrationLabelsArgs>(request)
            .and_then(list_calibration_labels_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "calibration.apply" => request_args::<ApplyCalibrationArgs>(request)
            .and_then(apply_calibration_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "exercise.detect_sessions" => request_args::<DetectExerciseSessionsArgs>(request)
            .and_then(exercise_detect_sessions_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "exercise.sessions_between" => request_args::<ExerciseSessionsBetweenArgs>(request)
            .and_then(exercise_sessions_between_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "diagnostics.perf_budget" => request_args::<PerfBudgetArgs>(request)
            .and_then(perf_budget_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "diagnostics.property_suite" => request_args::<PropertySuiteArgs>(request)
            .and_then(property_suite_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metric_series.query_range" => request_args::<MetricSeriesQueryRangeArgs>(request)
            .and_then(metric_series_query_range_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "metric_series.upsert" => request_args::<MetricSeriesUpsertArgs>(request)
            .and_then(metric_series_upsert_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "biometrics.insert_v20v21_batch" => request_args::<InsertV20V21BatchArgs>(request)
            .and_then(insert_v20v21_batch_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "biometrics.insert_v24_batch" => request_args::<InsertV24BatchArgs>(request)
            .and_then(insert_v24_biometric_batch_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "biometrics.insert_v26_batch" => request_args::<InsertV26BatchArgs>(request)
            .and_then(insert_v26_batch_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "biometrics.optical_between" => request_args::<OpticalBetweenArgs>(request)
            .and_then(optical_between_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "biometrics.v24_between" => request_args::<V24BetweenArgs>(request)
            .and_then(v24_biometric_samples_between_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        "biometrics.spo2_from_raw" => request_args::<Spo2FromRawArgs>(request)
            .and_then(spo2_from_raw_bridge)
            .map(|value| bridge_ok(&request.request_id, value))
            .unwrap_or_else(|error| bridge_error(&request.request_id, "method_error", error)),
        _ => unreachable!(
            "dispatch_metrics called with non-metrics method: {}",
            request.method
        ),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ApplyCalibrationArgs {
    database_path: String,
    metric_family: String,
    algorithm_id: String,
    algorithm_version: String,
    raw_score: f64,
    #[serde(default)]
    input_run_id: Option<String>,
    #[serde(default)]
    calibration_run_id: Option<String>,
    score_min: f64,
    score_max: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct EvaluateCalibrationDatasetArgs {
    dataset: CalibrationDataset,
    options: CalibrationOptions,
    #[serde(default)]
    database_path: Option<String>,
    #[serde(default)]
    persist: bool,
    #[serde(default)]
    calibration_run_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct EvaluateStoredCalibrationLabelsArgs {
    database_path: String,
    start: String,
    end: String,
    options: CalibrationOptions,
    #[serde(default)]
    persist: bool,
    #[serde(default)]
    calibration_run_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ImportCalibrationLabelsArgs {
    database_path: String,
    labels: Vec<CalibrationLabelBridgeInput>,
}

#[derive(Debug, Clone, Deserialize)]
struct ListCalibrationLabelsArgs {
    database_path: String,
    start: String,
    end: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CalibrationLabelBridgeInput {
    label_id: String,
    metric_family: String,
    label_source: String,
    captured_at: String,
    value: f64,
    unit: String,
    provenance: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct PerfBudgetArgs {
    #[serde(default = "default_perf_scale")]
    scale: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct PropertySuiteArgs {
    #[serde(default = "default_property_seed")]
    seed: u64,
    #[serde(default = "default_property_cases")]
    cases_per_group: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct FitStrainDenominatorArgs {
    pairs: Vec<(f64, f64)>,
    // Included for API consistency with DB-backed methods; unused (pure computation).
    #[serde(default)]
    #[allow(dead_code)]
    database_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MetricInputReadinessArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_owned_captures: bool,
    #[serde(default)]
    require_scores_ready: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct StepPacketDiscoveryArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    max_candidate_fields: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct StepCaptureValidationArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    max_candidate_fields: Option<usize>,
    #[serde(default)]
    capture_kind: Option<String>,
    #[serde(default)]
    manual_step_delta: Option<i64>,
    #[serde(default)]
    official_whoop_step_delta: Option<i64>,
    #[serde(default)]
    tolerance_steps: Option<i64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawMotionStepEstimateArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    sample_rate_hz: Option<f64>,
    #[serde(default)]
    peak_threshold_i16: Option<f64>,
    #[serde(default)]
    min_peak_spacing_samples: Option<usize>,
    #[serde(default)]
    manual_step_delta: Option<i64>,
    #[serde(default)]
    official_whoop_step_delta: Option<i64>,
    #[serde(default)]
    tolerance_steps: Option<i64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
    #[serde(default)]
    date_key: Option<String>,
    #[serde(default)]
    timezone: Option<String>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct StepCounterIngestArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    max_candidate_fields: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct StepCounterDailyRollupArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
    #[serde(default)]
    min_sample_count: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct StepCounterHourlyRollupArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
    #[serde(default)]
    min_sample_count: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ActivityUnavailableDailyStatusArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
    #[serde(default)]
    min_sample_count: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct DailyActivityMetricListArgs {
    database_path: String,
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct HourlyActivityMetricListArgs {
    database_path: String,
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct DailyRecoveryMetricListArgs {
    database_path: String,
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct EnergyDailyRollupArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    profile_weight_kg: Option<f64>,
    #[serde(default)]
    profile_age_years: Option<u32>,
    #[serde(default)]
    profile_sex: Option<String>,
    #[serde(default)]
    profile_height_cm: Option<f64>,
    #[serde(default)]
    resting_hr_bpm: Option<f64>,
    #[serde(default)]
    max_hr_bpm: Option<f64>,
    #[serde(default)]
    min_heart_rate_samples: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct EnergyHourlyRollupArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    profile_weight_kg: Option<f64>,
    #[serde(default)]
    profile_age_years: Option<u32>,
    #[serde(default)]
    profile_sex: Option<String>,
    #[serde(default)]
    resting_hr_bpm: Option<f64>,
    #[serde(default)]
    max_hr_bpm: Option<f64>,
    #[serde(default)]
    min_heart_rate_samples: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct EnergyCaptureValidationArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    profile_weight_kg: Option<f64>,
    #[serde(default)]
    profile_age_years: Option<u32>,
    #[serde(default)]
    profile_sex: Option<String>,
    #[serde(default)]
    profile_height_cm: Option<f64>,
    #[serde(default)]
    resting_hr_bpm: Option<f64>,
    #[serde(default)]
    max_hr_bpm: Option<f64>,
    #[serde(default)]
    min_heart_rate_samples: Option<usize>,
    #[serde(default)]
    capture_kind: Option<String>,
    #[serde(default)]
    official_whoop_active_kcal: Option<f64>,
    #[serde(default)]
    official_whoop_resting_kcal: Option<f64>,
    #[serde(default)]
    official_whoop_total_kcal: Option<f64>,
    #[serde(default)]
    tolerance_kcal: Option<f64>,
    #[serde(default)]
    relative_tolerance_fraction: Option<f64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReferenceCompareArgs {
    family: String,
    input: serde_json::Value,
    #[serde(default)]
    reference_report: Option<serde_json::Value>,
    #[serde(default)]
    goose_algorithm_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MotionFeaturesArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct HeartRateFeaturesArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct VitalEventFeaturesArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RespiratoryRateCaptureValidationArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    capture_kind: Option<String>,
    #[serde(default)]
    official_whoop_respiratory_rate_rpm: Option<f64>,
    #[serde(default)]
    tolerance_rpm: Option<f64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct OxygenSaturationCaptureValidationArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    capture_kind: Option<String>,
    #[serde(default)]
    official_whoop_oxygen_saturation_percent: Option<f64>,
    #[serde(default)]
    tolerance_percent: Option<f64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct TemperatureCaptureValidationArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    capture_kind: Option<String>,
    #[serde(default)]
    official_whoop_skin_temperature_delta_c: Option<f64>,
    #[serde(default)]
    tolerance_c: Option<f64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct HrvFeaturesArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    min_rr_intervals_to_compute: Option<usize>,
    #[serde(default)]
    baseline_min_days: Option<usize>,
    #[serde(default)]
    require_baseline: bool,
    #[serde(default)]
    persist_algorithm_run: bool,
    #[serde(default)]
    algorithm_run_id: Option<String>,
    #[serde(default)]
    algorithm_id: Option<String>,
    #[serde(default)]
    algorithm_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RecoverySensorDiscoveryArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    min_rr_intervals_to_compute: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct RecoveryUnavailableDailyStatusArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    min_rr_intervals_to_compute: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RecoverySensorDailyRollupArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    min_rr_intervals_to_compute: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct HrvCaptureValidationArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    min_rr_intervals_to_compute: Option<usize>,
    #[serde(default)]
    capture_kind: Option<String>,
    #[serde(default)]
    official_whoop_hrv_rmssd_ms: Option<f64>,
    #[serde(default)]
    tolerance_ms: Option<f64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct MetricWindowFeaturesArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    resting_hr_bpm: Option<f64>,
    #[serde(default)]
    max_hr_bpm: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct RestingHeartRateFeaturesArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    baseline_min_days: Option<usize>,
    #[serde(default)]
    require_baseline: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RestingHeartRateDailyRollupArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    baseline_min_days: Option<usize>,
    #[serde(default)]
    require_baseline: bool,
    #[serde(default)]
    min_sample_count: Option<usize>,
    #[serde(default)]
    write_metric: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RestingHeartRateCaptureValidationArgs {
    database_path: String,
    date_key: String,
    timezone: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    baseline_min_days: Option<usize>,
    #[serde(default)]
    require_baseline: bool,
    #[serde(default)]
    min_sample_count: Option<usize>,
    #[serde(default)]
    capture_kind: Option<String>,
    #[serde(default)]
    official_whoop_resting_hr_bpm: Option<f64>,
    #[serde(default)]
    tolerance_bpm: Option<f64>,
    #[serde(default)]
    label_provenance: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct StrainFeatureScoreArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    resting_start: Option<String>,
    #[serde(default)]
    resting_end: Option<String>,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    resting_baseline_min_days: Option<usize>,
    #[serde(default)]
    max_hr_bpm: Option<f64>,
    #[serde(default)]
    persist_algorithm_run: bool,
    #[serde(default)]
    algorithm_run_id: Option<String>,
    #[serde(default)]
    algorithm_id: Option<String>,
    #[serde(default)]
    algorithm_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SleepFeatureScoreArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    sleep_need_minutes: Option<f64>,
    #[serde(default)]
    age_years: Option<u8>,
    #[serde(default)]
    low_motion_threshold_0_to_1: Option<f64>,
    #[serde(default)]
    disturbance_motion_threshold_0_to_1: Option<f64>,
    #[serde(default)]
    target_midpoint_minutes_since_midnight: Option<f64>,
    #[serde(default)]
    history_import_in_progress: bool,
    #[serde(default)]
    persist_algorithm_run: bool,
    #[serde(default)]
    algorithm_run_id: Option<String>,
    #[serde(default)]
    algorithm_id: Option<String>,
    #[serde(default)]
    algorithm_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RecoveryFeatureScoreArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default)]
    hrv_start: Option<String>,
    #[serde(default)]
    hrv_end: Option<String>,
    #[serde(default = "default_correlation_start")]
    hrv_baseline_start: String,
    #[serde(default = "default_correlation_end")]
    hrv_baseline_end: String,
    #[serde(default = "default_correlation_start")]
    resting_start: String,
    #[serde(default = "default_correlation_end")]
    resting_end: String,
    #[serde(default)]
    sleep_start: Option<String>,
    #[serde(default)]
    sleep_end: Option<String>,
    #[serde(default)]
    prior_strain_start: Option<String>,
    #[serde(default)]
    prior_strain_end: Option<String>,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    resting_baseline_min_days: Option<usize>,
    #[serde(default)]
    hrv_min_rr_intervals_to_compute: Option<usize>,
    #[serde(default)]
    hrv_baseline_min_days: Option<usize>,
    #[serde(default)]
    sleep_need_minutes: Option<f64>,
    #[serde(default)]
    age_years: Option<u8>,
    #[serde(default)]
    low_motion_threshold_0_to_1: Option<f64>,
    #[serde(default)]
    disturbance_motion_threshold_0_to_1: Option<f64>,
    #[serde(default)]
    target_midpoint_minutes_since_midnight: Option<f64>,
    #[serde(default)]
    prior_strain_resting_baseline_min_days: Option<usize>,
    #[serde(default)]
    prior_strain_max_hr_bpm: Option<f64>,
    #[serde(default)]
    respiratory_rate_rpm: Option<f64>,
    #[serde(default)]
    respiratory_rate_baseline_rpm: Option<f64>,
    #[serde(default)]
    skin_temp_delta_c: Option<f64>,
    #[serde(default)]
    provided_vitals_source: Option<String>,
    #[serde(default)]
    provided_vitals_provenance_json: Option<String>,
    #[serde(default)]
    persist_algorithm_run: bool,
    #[serde(default)]
    algorithm_run_id: Option<String>,
    #[serde(default)]
    algorithm_id: Option<String>,
    #[serde(default)]
    algorithm_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct StressFeatureScoreArgs {
    database_path: String,
    #[serde(default = "default_correlation_start")]
    start: String,
    #[serde(default = "default_correlation_end")]
    end: String,
    #[serde(default = "default_correlation_start")]
    resting_start: String,
    #[serde(default = "default_correlation_end")]
    resting_end: String,
    #[serde(default)]
    hrv_start: Option<String>,
    #[serde(default)]
    hrv_end: Option<String>,
    #[serde(default = "default_correlation_start")]
    hrv_baseline_start: String,
    #[serde(default = "default_correlation_end")]
    hrv_baseline_end: String,
    #[serde(default)]
    min_owned_captures: Option<usize>,
    #[serde(default)]
    require_trusted_evidence: bool,
    #[serde(default)]
    resting_baseline_min_days: Option<usize>,
    #[serde(default)]
    hrv_min_rr_intervals_to_compute: Option<usize>,
    #[serde(default)]
    hrv_baseline_min_days: Option<usize>,
    #[serde(default)]
    persist_algorithm_run: bool,
    #[serde(default)]
    algorithm_run_id: Option<String>,
    #[serde(default)]
    algorithm_id: Option<String>,
    #[serde(default)]
    algorithm_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct MetricSeriesUpsertArgs {
    database_path: String,
    source: String,
    metric_name: String,
    date: String,
    value: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct MetricSeriesQueryRangeArgs {
    database_path: String,
    metric_name: String,
    start_date: String,
    end_date: String,
    #[serde(default)]
    source: Option<String>,
}

const IMU_LSB_PER_G: f64 = 3900.0;

// TOGGLE_IMU_MODE (command 106) is already sent in production by
// SensorStreamCommandKind.startPhysiologyCapture / stopPhysiologyCapture
// in the Swift layer. No Rust toggle is needed — this pipeline only stores
// and converts the resulting K10/K21 motion data. (IMU-04 — no code change
// required beyond this documentation comment.)

#[derive(Debug, Deserialize)]
struct ImuStepCountFromDecodedFramesArgs {
    database_path: String,
    start_ts: f64,
    end_ts: f64,
}

#[derive(Debug, Deserialize)]
struct RecoveryV1BridgeArgs {
    database_path: String,
    device_id: String,
    date_key: String,
    hrv_rmssd_ms: f64,
    resting_hr_bpm: f64,
    #[serde(default)]
    resp_rate_rpm: Option<f64>,
    #[serde(default)]
    sleep_performance_fraction: Option<f64>,
}

fn goose_recovery_v1_bridge(args: RecoveryV1BridgeArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let baseline = EwmaBaseline::fold_history(&store)?;
    let input = RecoveryV1Input {
        device_id: args.device_id,
        date_key: args.date_key,
        hrv_rmssd_ms: args.hrv_rmssd_ms,
        resting_hr_bpm: args.resting_hr_bpm,
        resp_rate_rpm: args.resp_rate_rpm,
        sleep_performance_fraction: args.sleep_performance_fraction,
    };
    let output = goose_recovery_v1(&input, &baseline);
    serde_json::to_value(output)
        .map_err(|e| GooseError::message(format!("cannot serialize recovery_v1 output: {e}")))
}

#[derive(Debug, Deserialize)]
struct HrSampleArg {
    ts: f64,
    bpm: u8,
}

#[derive(Debug, Deserialize)]
struct ExerciseProfileArg {
    resting_hr: Option<f64>,
    max_hr: Option<f64>,
    age: Option<u8>,
    sex: Option<String>,
    weight_kg: Option<f64>,
    height_cm: Option<f64>,
    daily_hr_p10: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DetectExerciseSessionsArgs {
    database_path: String,
    device_id: String,
    hr_samples: Vec<HrSampleArg>,
    gravity_rows: Vec<GravityRow>,
    profile: ExerciseProfileArg,
}

#[derive(Debug, Deserialize)]
struct ExerciseSessionsBetweenArgs {
    database_path: String,
    device_id: String,
    ts_start: f64,
    ts_end: f64,
}

fn exercise_detect_sessions_bridge(
    args: DetectExerciseSessionsArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let hr: Vec<crate::exercise_detection::HrSample> = args
        .hr_samples
        .iter()
        .map(|s| crate::exercise_detection::HrSample {
            ts: s.ts,
            bpm: s.bpm,
        })
        .collect();
    let profile = crate::exercise_detection::ExerciseProfile {
        resting_hr: args.profile.resting_hr,
        max_hr: args.profile.max_hr,
        age: args.profile.age,
        sex: args.profile.sex.clone(),
        weight_kg: args.profile.weight_kg,
        height_cm: args.profile.height_cm,
        daily_hr_p10: args.profile.daily_hr_p10,
    };
    let sessions =
        crate::exercise_detection::detect_exercise_sessions(&hr, &args.gravity_rows, &profile);
    let mut warnings: Vec<String> = Vec::new();

    // Build rows and insert all sessions in a single transaction (PERF-03).
    let rows: Vec<ExerciseSessionRow> = sessions
        .iter()
        .map(|session| ExerciseSessionRow {
            device_id: args.device_id.clone(),
            start_ts: session.start_ts,
            end_ts: session.end_ts,
            duration_s: session.duration_s,
            avg_hr: session.avg_hr,
            peak_hr: session.peak_hr,
            strain: session.strain,
            calories_kcal: session.calories_kcal,
            zone_time_pct_json: serde_json::to_string(&session.zone_time_pct).unwrap_or_default(),
            hrmax_source: session.hrmax_source.clone(),
            rhr_source: session.rhr_source.clone(),
            avg_hrr_pct: session.avg_hrr_pct,
        })
        .collect();

    let inserted = store
        .insert_exercise_sessions_batch(&rows)
        .unwrap_or_else(|e| {
            warnings.push(format!("batch insert failed: {e}"));
            0
        });

    Ok(json!({
        "sessions_detected": sessions.len(),
        "sessions_inserted": inserted,
        "warnings": warnings,
    }))
}

fn exercise_sessions_between_bridge(
    args: ExerciseSessionsBetweenArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let rows = store.exercise_sessions_between(&args.device_id, args.ts_start, args.ts_end)?;
    Ok(json!({ "sessions": rows }))
}

#[derive(Debug, Deserialize)]
struct Spo2RawArg {
    ts: f64,
    red: u16,
    ir: u16,
    contact: i64,
}

#[derive(Debug, Deserialize)]
struct SkinTempRawArg {
    ts: f64,
    raw: u16,
    contact: i64,
}

#[derive(Debug, Deserialize)]
struct RespRawArg {
    ts: f64,
    raw: u16,
    contact: i64,
}

#[derive(Debug, Deserialize)]
struct SigQualityArg {
    ts: f64,
    quality: u16,
    contact: i64,
}

#[derive(Debug, Deserialize)]
struct InsertV24BatchArgs {
    database_path: String,
    device_id: String,
    #[serde(default)]
    spo2: Vec<Spo2RawArg>,
    #[serde(default)]
    skin_temp: Vec<SkinTempRawArg>,
    #[serde(default)]
    resp: Vec<RespRawArg>,
    #[serde(default)]
    sig_quality: Vec<SigQualityArg>,
}

#[derive(Debug, Deserialize)]
struct V24BetweenArgs {
    database_path: String,
    device_id: String,
    start_ts: f64,
    end_ts: f64,
}

#[derive(Debug, Deserialize)]
struct Spo2FromRawArgs {
    red: u16,
    ir: u16,
}

fn insert_v24_biometric_batch_bridge(args: InsertV24BatchArgs) -> GooseResult<serde_json::Value> {
    use crate::store::V24BiometricBatch;

    let store = acquire_bridge_conn(&args.database_path)?;
    let mut warnings: Vec<String> = Vec::new();

    // Build SpO2 tuples with plausibility gate
    let spo2_tuples: Vec<(f64, i64, i64, i64)> = args
        .spo2
        .iter()
        .filter_map(|row| match spo2_from_raw_uncalibrated(row.red, row.ir) {
            Some(_) => Some((row.ts, row.red as i64, row.ir as i64, row.contact)),
            None => {
                warnings.push(format!(
                    "spo2_plausibility_reject: ts={} red={} ir={} (out of range [70,100]%)",
                    row.ts, row.red, row.ir
                ));
                None
            }
        })
        .collect();

    // Build skin_temp tuples with plausibility gate
    let skin_temp_tuples: Vec<(f64, i64, i64)> = args
        .skin_temp
        .iter()
        .filter_map(|row| match skin_temp_celsius_from_raw(row.raw) {
            Some(_) => Some((row.ts, row.raw as i64, row.contact)),
            None => {
                warnings.push(format!(
                    "skin_temp_plausibility_reject: ts={} raw={} (celsius outside [25,40])",
                    row.ts, row.raw
                ));
                None
            }
        })
        .collect();

    // Build resp tuples (raw u16 is always <= 65535; gate is a no-op by type)
    let resp_tuples: Vec<(f64, i64, i64)> = args
        .resp
        .iter()
        .map(|row| (row.ts, row.raw as i64, row.contact))
        .collect();

    // Build sig_quality tuples (no plausibility gate — quality is a dimensionless score)
    let sig_quality_tuples: Vec<(f64, i64, i64)> = args
        .sig_quality
        .iter()
        .map(|row| (row.ts, row.quality as i64, row.contact))
        .collect();

    let batch = V24BiometricBatch {
        spo2: spo2_tuples,
        skin_temp: skin_temp_tuples,
        resp: resp_tuples,
        sig_quality: sig_quality_tuples,
    };

    store.insert_v24_biometric_batch(&args.device_id, &batch)?;

    Ok(json!({"inserted": true, "warnings": warnings}))
}

fn v24_biometric_samples_between_bridge(args: V24BetweenArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let window =
        store.v24_biometric_samples_between(&args.device_id, args.start_ts, args.end_ts)?;

    let spo2: Vec<serde_json::Value> = window
        .spo2
        .iter()
        .map(|r| json!({"ts": r.ts, "red": r.red, "ir": r.ir, "contact": r.contact}))
        .collect();
    let skin_temp: Vec<serde_json::Value> = window
        .skin_temp
        .iter()
        .map(|r| json!({"ts": r.ts, "raw": r.raw, "contact": r.contact}))
        .collect();
    let resp: Vec<serde_json::Value> = window
        .resp
        .iter()
        .map(|r| json!({"ts": r.ts, "raw": r.raw, "contact": r.contact}))
        .collect();
    let sig_quality: Vec<serde_json::Value> = window
        .sig_quality
        .iter()
        .map(|r| json!({"ts": r.ts, "quality": r.quality, "contact": r.contact}))
        .collect();

    Ok(json!({"spo2": spo2, "skin_temp": skin_temp, "resp": resp, "sig_quality": sig_quality}))
}

fn spo2_from_raw_bridge(args: Spo2FromRawArgs) -> GooseResult<serde_json::Value> {
    match spo2_from_raw_uncalibrated(args.red, args.ir) {
        Some(spo2_pct) => Ok(json!({"spo2_pct": spo2_pct, "quality_flag": "uncalibrated"})),
        None => Ok(json!({"spo2_pct": null, "quality_flag": "uncalibrated", "rejected": true})),
    }
}

/// HR feature argument as received from Swift (mirrors EpochHrFeature in sleep_staging.rs).
#[derive(Debug, Deserialize)]
struct HrFeatureArg {
    ts: f64,
    hr_bpm: f64,
}

#[derive(Debug, Deserialize)]
struct SleepStagingBridgeArgs {
    database_path: String,
    device_id: String,
    sleep_start_ts: f64,
    sleep_end_ts: f64,
    /// Optional per-epoch HR features for the 4-class classifier.
    /// Absent or empty → 4-class output still valid (light fallback).
    #[serde(default)]
    hr_features: Vec<HrFeatureArg>,
    /// Whether resp_samples data is available for this session. When false,
    /// REM classification is suppressed (graceful degradation — see PROTO-03).
    /// Defaults to true for backwards compatibility with existing callers.
    #[serde(default = "default_resp_available")]
    resp_available: bool,
}

fn default_resp_available() -> bool {
    true
}

fn sleep_staging_bridge(args: SleepStagingBridgeArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let gravity_rows: Vec<GravityRow> =
        store.gravity_rows_between(&args.device_id, args.sleep_start_ts, args.sleep_end_ts)?;
    let tuples: Vec<(f64, f64, f64, f64)> =
        gravity_rows.iter().map(|r| (r.ts, r.x, r.y, r.z)).collect();
    let input = SleepStagingInput {
        device_id: args.device_id.clone(),
        sleep_start_ts: args.sleep_start_ts,
        sleep_end_ts: args.sleep_end_ts,
    };
    let hr_feats: Vec<EpochHrFeature> = args
        .hr_features
        .iter()
        .map(|f| EpochHrFeature {
            ts: f.ts,
            hr_bpm: f.hr_bpm,
        })
        .collect();
    // Determine resp availability: if caller did not pass resp_available=false explicitly,
    // check whether there are any resp rows in the window (lazy check).
    let resp_available = if args.resp_available {
        let resp_count = store
            .resp_samples_between(&args.device_id, args.sleep_start_ts, args.sleep_end_ts)
            .map(|rows| rows.len())
            .unwrap_or(0);
        resp_count > 0
    } else {
        false
    };
    let output: SleepStagingOutput =
        stage_sleep_four_class(&input, &tuples, &hr_feats, resp_available);
    serde_json::to_value(output)
        .map_err(|e| GooseError::message(format!("cannot serialize sleep_staging output: {e}")))
}

fn reference_compare_bridge(args: ReferenceCompareArgs) -> GooseResult<serde_json::Value> {
    let report = match args.family.as_str() {
        "hrv" => {
            let input: HrvInput = serde_json::from_value(args.input)
                .map_err(|error| GooseError::message(format!("invalid HRV input: {error}")))?;
            compare_hrv_goose_to_reference(&input)?
        }
        "sleep" => {
            let use_sleep_v1 = args
                .goose_algorithm_id
                .as_deref()
                .is_some_and(|id| id == crate::metrics::GOOSE_SLEEP_V1_ID)
                || args
                    .input
                    .get("sleep")
                    .is_some_and(|value| value.is_object());
            if use_sleep_v1 {
                let input: SleepV1Input = serde_json::from_value(normalize_sleep_v1_input_value(
                    args.input,
                ))
                .map_err(|error| GooseError::message(format!("invalid sleep v1 input: {error}")))?;
                if let Some(reference_report) = args.reference_report {
                    compare_sleep_v1_goose_to_external_reference_report(&input, &reference_report)?
                } else {
                    compare_sleep_v1_goose_to_reference(&input)?
                }
            } else {
                let input: SleepInput = serde_json::from_value(args.input).map_err(|error| {
                    GooseError::message(format!("invalid sleep input: {error}"))
                })?;
                if let Some(reference_report) = args.reference_report {
                    compare_sleep_goose_to_external_reference_report(&input, &reference_report)?
                } else {
                    compare_sleep_goose_to_reference(&input)?
                }
            }
        }
        "strain" => {
            let input: StrainInput = serde_json::from_value(args.input)
                .map_err(|error| GooseError::message(format!("invalid strain input: {error}")))?;
            compare_strain_goose_to_reference(&input)?
        }
        "stress" => {
            let input: StressInput = serde_json::from_value(args.input)
                .map_err(|error| GooseError::message(format!("invalid stress input: {error}")))?;
            compare_stress_goose_to_reference(&input)?
        }
        other => {
            return Err(GooseError::message(format!(
                "unsupported reference comparison family {other}; use hrv|sleep|strain|stress"
            )));
        }
    };
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize reference comparison report: {error}"
        ))
    })
}

fn normalize_sleep_v1_input_value(input: serde_json::Value) -> serde_json::Value {
    let serde_json::Value::Object(mut object) = input else {
        return input;
    };
    let Some(serde_json::Value::Object(sleep)) = object.remove("sleep") else {
        return serde_json::Value::Object(object);
    };
    let mut merged = sleep;
    for (key, value) in object {
        merged.insert(key, value);
    }
    serde_json::Value::Object(merged)
}

fn metric_input_readiness_bridge(args: MetricInputReadinessArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let correlation = run_capture_correlation_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        CaptureCorrelationOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_owned_captures: args.require_owned_captures,
        },
    )?;
    let report = run_metric_input_readiness(
        &correlation,
        MetricInputReadinessOptions {
            require_scores_ready: args.require_scores_ready,
        },
    );
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize metric input readiness report: {error}"
        ))
    })
}

fn motion_features_bridge(args: MotionFeaturesArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_motion_feature_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        MotionFeatureOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!("cannot serialize motion feature report: {error}"))
    })
}

fn heart_rate_features_bridge(args: HeartRateFeaturesArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_heart_rate_feature_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        HeartRateFeatureOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize heart-rate feature report: {error}"
        ))
    })
}

fn vital_event_features_bridge(args: VitalEventFeaturesArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_vital_event_feature_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        VitalEventFeatureOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize vital event feature report: {error}"
        ))
    })
}

fn step_packet_discovery_bridge(args: StepPacketDiscoveryArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_step_packet_discovery_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        StepPacketDiscoveryOptions {
            max_candidate_fields: args.max_candidate_fields.unwrap_or(250),
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize step packet discovery report: {error}"
        ))
    })
}

fn step_capture_validation_bridge(
    args: StepCaptureValidationArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_step_capture_validation_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        StepCaptureValidationOptions {
            max_candidate_fields: args.max_candidate_fields.unwrap_or(1000),
            capture_kind: args.capture_kind,
            manual_step_delta: args.manual_step_delta,
            official_whoop_step_delta: args.official_whoop_step_delta,
            tolerance_steps: args.tolerance_steps.unwrap_or(10).max(0),
            label_provenance: args.label_provenance,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize step capture validation report: {error}"
        ))
    })
}

fn raw_motion_step_estimate_bridge(
    args: RawMotionStepEstimateArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_raw_motion_step_estimate_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        RawMotionStepEstimateOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            sample_rate_hz: args.sample_rate_hz.unwrap_or(50.0),
            peak_threshold_i16: args.peak_threshold_i16.unwrap_or(1_200.0),
            min_peak_spacing_samples: args.min_peak_spacing_samples.unwrap_or(10),
            manual_step_delta: args.manual_step_delta,
            official_whoop_step_delta: args.official_whoop_step_delta,
            tolerance_steps: args.tolerance_steps.unwrap_or(10),
            label_provenance: args.label_provenance,
            date_key: args.date_key,
            timezone: args.timezone,
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize raw-motion step estimate report: {error}"
        ))
    })
}

fn step_counter_ingest_bridge(args: StepCounterIngestArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_step_counter_ingest_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        StepCounterIngestOptions {
            max_candidate_fields: args.max_candidate_fields.unwrap_or(1000),
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize step counter ingest report: {error}"
        ))
    })
}

fn step_counter_daily_rollup_bridge(
    args: StepCounterDailyRollupArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_device_step_counter_day(
        &store,
        StepCounterDailyRollupOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start_time_unix_ms: args.start_time_unix_ms,
            end_time_unix_ms: args.end_time_unix_ms,
            min_sample_count: args.min_sample_count.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize step counter daily rollup report: {error}"
        ))
    })
}

fn step_counter_hourly_rollup_bridge(
    args: StepCounterHourlyRollupArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_device_step_counter_hour(
        &store,
        StepCounterHourlyRollupOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start_time_unix_ms: args.start_time_unix_ms,
            end_time_unix_ms: args.end_time_unix_ms,
            min_sample_count: args.min_sample_count.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize step counter hourly rollup report: {error}"
        ))
    })
}

fn activity_unavailable_daily_status_bridge(
    args: ActivityUnavailableDailyStatusArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_activity_unavailable_daily_status_for_store(
        &store,
        ActivityUnavailableDailyStatusOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start_time_unix_ms: args.start_time_unix_ms,
            end_time_unix_ms: args.end_time_unix_ms,
            min_sample_count: args.min_sample_count.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize activity unavailable daily status report: {error}"
        ))
    })
}

fn daily_activity_metrics_bridge(
    args: DailyActivityMetricListArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let metrics =
        store.daily_activity_metrics_between(args.start_time_unix_ms, args.end_time_unix_ms)?;
    Ok(json!({
        "schema": "goose.daily-activity-metric-list.v1",
        "generated_by": "goose-bridge",
        "start_time_unix_ms": args.start_time_unix_ms,
        "end_time_unix_ms": args.end_time_unix_ms,
        "metric_count": metrics.len(),
        "metrics": metrics,
    }))
}

fn hourly_activity_metrics_bridge(
    args: HourlyActivityMetricListArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let metrics =
        store.hourly_activity_metrics_between(args.start_time_unix_ms, args.end_time_unix_ms)?;
    Ok(json!({
        "schema": "goose.hourly-activity-metric-list.v1",
        "generated_by": "goose-bridge",
        "start_time_unix_ms": args.start_time_unix_ms,
        "end_time_unix_ms": args.end_time_unix_ms,
        "metric_count": metrics.len(),
        "metrics": metrics,
    }))
}

fn daily_recovery_metrics_bridge(
    args: DailyRecoveryMetricListArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let metrics =
        store.daily_recovery_metrics_between(args.start_time_unix_ms, args.end_time_unix_ms)?;
    Ok(json!({
        "schema": "goose.daily-recovery-metric-list.v1",
        "generated_by": "goose-bridge",
        "start_time_unix_ms": args.start_time_unix_ms,
        "end_time_unix_ms": args.end_time_unix_ms,
        "metric_count": metrics.len(),
        "metrics": metrics,
    }))
}

fn energy_daily_rollup_bridge(args: EnergyDailyRollupArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_energy_day_for_store(
        &store,
        &args.database_path,
        EnergyDailyRollupOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start: &args.start,
            end: &args.end,
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            profile_weight_kg: args.profile_weight_kg,
            profile_age_years: args.profile_age_years,
            profile_sex: args.profile_sex.as_deref(),
            profile_height_cm: args.profile_height_cm,
            resting_hr_bpm: args.resting_hr_bpm,
            max_hr_bpm: args.max_hr_bpm,
            min_heart_rate_samples: args.min_heart_rate_samples.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize energy daily rollup report: {error}"
        ))
    })
}

fn energy_unavailable_daily_status_bridge(
    args: EnergyDailyRollupArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_energy_unavailable_daily_status_for_store(
        &store,
        &args.database_path,
        EnergyDailyRollupOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start: &args.start,
            end: &args.end,
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            profile_weight_kg: args.profile_weight_kg,
            profile_age_years: args.profile_age_years,
            profile_sex: args.profile_sex.as_deref(),
            profile_height_cm: args.profile_height_cm,
            resting_hr_bpm: args.resting_hr_bpm,
            max_hr_bpm: args.max_hr_bpm,
            min_heart_rate_samples: args.min_heart_rate_samples.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize energy unavailable daily status report: {error}"
        ))
    })
}

fn energy_hourly_rollup_bridge(args: EnergyHourlyRollupArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_energy_hour_for_store(
        &store,
        &args.database_path,
        EnergyHourlyRollupOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start: &args.start,
            end: &args.end,
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            profile_weight_kg: args.profile_weight_kg,
            profile_age_years: args.profile_age_years,
            profile_sex: args.profile_sex.as_deref(),
            resting_hr_bpm: args.resting_hr_bpm,
            max_hr_bpm: args.max_hr_bpm,
            min_heart_rate_samples: args.min_heart_rate_samples.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize energy hourly rollup report: {error}"
        ))
    })
}

fn energy_capture_validation_bridge(
    args: EnergyCaptureValidationArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = validate_energy_capture_for_store(
        &store,
        &args.database_path,
        EnergyCaptureValidationOptions {
            rollup_options: EnergyDailyRollupOptions {
                date_key: &args.date_key,
                timezone: &args.timezone,
                start: &args.start,
                end: &args.end,
                min_owned_captures_per_summary: args
                    .min_owned_captures
                    .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
                require_trusted_evidence: args.require_trusted_evidence,
                profile_weight_kg: args.profile_weight_kg,
                profile_age_years: args.profile_age_years,
                profile_sex: args.profile_sex.as_deref(),
                profile_height_cm: args.profile_height_cm,
                resting_hr_bpm: args.resting_hr_bpm,
                max_hr_bpm: args.max_hr_bpm,
                min_heart_rate_samples: args.min_heart_rate_samples.unwrap_or(2),
                write_metric: false,
            },
            capture_kind: args.capture_kind,
            official_whoop_active_kcal: args.official_whoop_active_kcal,
            official_whoop_resting_kcal: args.official_whoop_resting_kcal,
            official_whoop_total_kcal: args.official_whoop_total_kcal,
            tolerance_kcal: args.tolerance_kcal.unwrap_or(75.0),
            relative_tolerance_fraction: args.relative_tolerance_fraction.unwrap_or(0.25),
            label_provenance: args.label_provenance,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize energy capture validation report: {error}"
        ))
    })
}

fn validate_requested_primary_algorithm(
    metric_family: &str,
    requested_algorithm_id: Option<&str>,
    requested_algorithm_version: Option<&str>,
    supported_algorithm_id: &str,
    supported_algorithm_version: &str,
) -> GooseResult<()> {
    let Some(requested_id) = requested_algorithm_id else {
        return Ok(());
    };
    let requested_id = requested_id.trim();
    if requested_id.is_empty() {
        return Err(GooseError::message(
            "algorithm_id must be non-empty when provided",
        ));
    }
    let requested_version = requested_algorithm_version
        .map(str::trim)
        .unwrap_or(supported_algorithm_version);
    if requested_version.is_empty() {
        return Err(GooseError::message(
            "algorithm_version must be non-empty when provided",
        ));
    }
    if requested_id != supported_algorithm_id || requested_version != supported_algorithm_version {
        return Err(GooseError::message(format!(
            "unsupported primary algorithm {requested_id}@{requested_version} for {metric_family}; this packet-derived scorer currently supports {supported_algorithm_id}@{supported_algorithm_version}"
        )));
    }
    Ok(())
}

fn hrv_features_bridge(args: HrvFeaturesArgs) -> GooseResult<serde_json::Value> {
    validate_requested_primary_algorithm(
        "hrv",
        args.algorithm_id.as_deref(),
        args.algorithm_version.as_deref(),
        GOOSE_HRV_V0_ID,
        GOOSE_HRV_V0_VERSION,
    )?;
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_hrv_feature_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        HrvFeatureOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            min_rr_intervals_to_compute: args.min_rr_intervals_to_compute.unwrap_or(2),
            baseline_min_days: args.baseline_min_days.unwrap_or(3),
            require_baseline: args.require_baseline,
        },
    )?;
    let mut value = serde_json::to_value(&report).map_err(|error| {
        GooseError::message(format!("cannot serialize HRV feature report: {error}"))
    })?;
    maybe_persist_algorithm_run(
        &store,
        &mut value,
        args.persist_algorithm_run,
        args.algorithm_run_id.as_deref(),
        "packet-derived-hrv",
        report.score_result.as_ref(),
    )?;
    Ok(value)
}

fn hrv_capture_validation_bridge(args: HrvCaptureValidationArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_hrv_capture_validation_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        HrvCaptureValidationOptions {
            feature_options: HrvFeatureOptions {
                min_owned_captures_per_summary: args
                    .min_owned_captures
                    .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
                require_trusted_evidence: args.require_trusted_evidence,
                min_rr_intervals_to_compute: args.min_rr_intervals_to_compute.unwrap_or(2),
                baseline_min_days: 1,
                require_baseline: false,
            },
            capture_kind: args.capture_kind,
            official_whoop_hrv_rmssd_ms: args.official_whoop_hrv_rmssd_ms,
            tolerance_ms: args.tolerance_ms.unwrap_or(10.0),
            label_provenance: args.label_provenance,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize HRV capture validation report: {error}"
        ))
    })
}

fn respiratory_rate_capture_validation_bridge(
    args: RespiratoryRateCaptureValidationArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_respiratory_rate_capture_validation_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        RespiratoryRateCaptureValidationOptions {
            feature_options: VitalEventFeatureOptions {
                min_owned_captures_per_summary: args
                    .min_owned_captures
                    .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
                require_trusted_evidence: args.require_trusted_evidence,
            },
            capture_kind: args.capture_kind,
            official_whoop_respiratory_rate_rpm: args.official_whoop_respiratory_rate_rpm,
            tolerance_rpm: args.tolerance_rpm.unwrap_or(1.0),
            label_provenance: args.label_provenance,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize respiratory-rate capture validation report: {error}"
        ))
    })
}

fn oxygen_saturation_capture_validation_bridge(
    args: OxygenSaturationCaptureValidationArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_oxygen_saturation_capture_validation_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        OxygenSaturationCaptureValidationOptions {
            feature_options: VitalEventFeatureOptions {
                min_owned_captures_per_summary: args
                    .min_owned_captures
                    .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
                require_trusted_evidence: args.require_trusted_evidence,
            },
            capture_kind: args.capture_kind,
            official_whoop_oxygen_saturation_percent: args.official_whoop_oxygen_saturation_percent,
            tolerance_percent: args.tolerance_percent.unwrap_or(2.0),
            label_provenance: args.label_provenance,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize oxygen-saturation capture validation report: {error}"
        ))
    })
}

fn temperature_capture_validation_bridge(
    args: TemperatureCaptureValidationArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_temperature_capture_validation_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        TemperatureCaptureValidationOptions {
            feature_options: VitalEventFeatureOptions {
                min_owned_captures_per_summary: args
                    .min_owned_captures
                    .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
                require_trusted_evidence: args.require_trusted_evidence,
            },
            capture_kind: args.capture_kind,
            official_whoop_skin_temperature_delta_c: args.official_whoop_skin_temperature_delta_c,
            tolerance_c: args.tolerance_c.unwrap_or(0.3),
            label_provenance: args.label_provenance,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize temperature capture validation report: {error}"
        ))
    })
}

fn recovery_sensor_discovery_bridge(
    args: RecoverySensorDiscoveryArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_recovery_sensor_discovery_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        RecoverySensorDiscoveryOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            min_rr_intervals_to_compute: args.min_rr_intervals_to_compute.unwrap_or(2),
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize recovery sensor discovery report: {error}"
        ))
    })
}

fn recovery_unavailable_daily_status_bridge(
    args: RecoveryUnavailableDailyStatusArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_recovery_unavailable_daily_status_for_store(
        &store,
        &args.database_path,
        RecoveryUnavailableDailyStatusOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start: &args.start,
            end: &args.end,
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            min_rr_intervals_to_compute: args.min_rr_intervals_to_compute.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize recovery unavailable daily status report: {error}"
        ))
    })
}

fn recovery_sensor_daily_rollup_bridge(
    args: RecoverySensorDailyRollupArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_recovery_sensor_daily_for_store(
        &store,
        &args.database_path,
        RecoverySensorDailyRollupOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start: &args.start,
            end: &args.end,
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            min_rr_intervals_to_compute: args.min_rr_intervals_to_compute.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize recovery sensor daily rollup report: {error}"
        ))
    })
}

fn metric_window_features_bridge(args: MetricWindowFeaturesArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_metric_window_feature_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        MetricWindowFeatureOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            resting_hr_bpm: args.resting_hr_bpm,
            max_hr_bpm: args.max_hr_bpm,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize metric window feature report: {error}"
        ))
    })
}

fn resting_heart_rate_features_bridge(
    args: RestingHeartRateFeaturesArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_resting_heart_rate_feature_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        RestingHeartRateFeatureOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            baseline_min_days: args.baseline_min_days.unwrap_or(3),
            require_baseline: args.require_baseline,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize resting heart-rate feature report: {error}"
        ))
    })
}

fn resting_heart_rate_daily_rollup_bridge(
    args: RestingHeartRateDailyRollupArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = rollup_resting_heart_rate_day_for_store(
        &store,
        &args.database_path,
        RestingHeartRateDailyRollupOptions {
            date_key: &args.date_key,
            timezone: &args.timezone,
            start: &args.start,
            end: &args.end,
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            baseline_min_days: args.baseline_min_days.unwrap_or(3),
            require_baseline: args.require_baseline,
            min_sample_count: args.min_sample_count.unwrap_or(2),
            write_metric: args.write_metric,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize resting heart-rate daily rollup report: {error}"
        ))
    })
}

fn resting_heart_rate_capture_validation_bridge(
    args: RestingHeartRateCaptureValidationArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = validate_resting_heart_rate_capture_for_store(
        &store,
        &args.database_path,
        RestingHeartRateCaptureValidationOptions {
            rollup_options: RestingHeartRateDailyRollupOptions {
                date_key: &args.date_key,
                timezone: &args.timezone,
                start: &args.start,
                end: &args.end,
                min_owned_captures_per_summary: args
                    .min_owned_captures
                    .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
                require_trusted_evidence: args.require_trusted_evidence,
                baseline_min_days: args.baseline_min_days.unwrap_or(3),
                require_baseline: args.require_baseline,
                min_sample_count: args.min_sample_count.unwrap_or(2),
                write_metric: false,
            },
            capture_kind: args.capture_kind,
            official_whoop_resting_hr_bpm: args.official_whoop_resting_hr_bpm,
            tolerance_bpm: args.tolerance_bpm.unwrap_or(3.0),
            label_provenance: args.label_provenance,
        },
    )?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize resting heart-rate capture validation report: {error}"
        ))
    })
}

fn sleep_v1_input_from_feature_score(
    store: &GooseStore,
    sleep_input: &SleepInput,
    report: &SleepFeatureScoreReport,
    history_import_in_progress: bool,
) -> GooseResult<SleepV1Input> {
    let prior_history_end_unix_ms = sleep_time_unix_ms(&sleep_input.start_time)
        .ok_or_else(|| GooseError::message("sleep_v1_input_start_time_invalid"))?;
    let prior_nights = external_sleep_history_nights_for_sleep_v1(
        store,
        sleep_input.sleep_need_minutes,
        prior_history_end_unix_ms,
    )?;
    let naps_minutes = external_sleep_naps_before_sleep(store, sleep_input)?;
    let schedule_baseline = sleep_history_schedule_baseline(&prior_nights);
    let imported_sleep_history_seen = !prior_nights.is_empty();
    let imported_platform_sleep_nights = prior_nights
        .iter()
        .filter(|night| sleep_history_night_is_usable(night))
        .count() as u32;
    let excluded_sleep_nights = prior_nights
        .iter()
        .filter(|night| !sleep_history_night_is_usable(night))
        .count() as u32;
    let repeated_low_confidence_nights = prior_nights
        .iter()
        .filter(|night| night.confidence_0_to_1 < 0.50)
        .count()
        >= 3;
    let days_since_last_valid_night = days_since_last_valid_sleep_night(sleep_input, &prior_nights);
    let trusted_goose_sleep_nights = u32::from(
        report
            .sleep_window
            .as_ref()
            .is_some_and(|window| window.trusted_metric_input),
    );
    let stage_segments = report
        .sleep_window
        .as_ref()
        .map(|window| {
            window
                .stage_segments
                .iter()
                .map(|segment| SleepStageSegment {
                    stage_kind: sleep_stage_kind_label(&segment.stage).to_string(),
                    start_time: segment.start_time.clone(),
                    end_time: segment.end_time.clone(),
                    duration_minutes: segment.duration_minutes,
                    confidence_0_to_1: segment.confidence_0_to_1,
                    stage_probabilities: if segment.stage_probabilities.is_empty() {
                        BTreeMap::from([(
                            sleep_stage_kind_label(&segment.stage).to_string(),
                            segment.confidence_0_to_1,
                        )])
                    } else {
                        segment.stage_probabilities.clone()
                    },
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let data_coverage_fraction = report.sleep_window.as_ref().map(|window| {
        (window.motion_coverage_fraction + window.heart_rate_coverage_fraction) / 2.0
    });

    Ok(SleepV1Input {
        sleep: sleep_input.clone(),
        model_status: SleepModelStatusInput {
            sleep_permission_granted: imported_sleep_history_seen,
            history_import_in_progress,
            imported_platform_sleep_nights,
            excluded_sleep_nights,
            trusted_goose_sleep_nights,
            days_since_last_valid_night,
            repeated_low_confidence_nights,
            motion_coverage_fraction: report
                .sleep_window
                .as_ref()
                .map(|window| window.motion_coverage_fraction),
            heart_rate_coverage_fraction: report
                .sleep_window
                .as_ref()
                .map(|window| window.heart_rate_coverage_fraction),
            ..Default::default()
        },
        prior_nights,
        stage_segments,
        sleep_hr_average_bpm: report
            .sleep_window
            .as_ref()
            .and_then(|window| window.average_sleep_hr_bpm),
        sleep_hr_min_bpm: report
            .sleep_window
            .as_ref()
            .and_then(|window| window.lowest_sleep_hr_bpm),
        pre_sleep_awake_hr_average_bpm: report
            .sleep_window
            .as_ref()
            .and_then(|window| window.baseline_awake_hr_bpm),
        sleep_hr_trend_bpm_per_hour: report
            .sleep_window
            .as_ref()
            .and_then(|window| window.sleep_hr_trend_bpm_per_hour),
        bedtime_deviation_minutes: schedule_baseline
            .and_then(|(typical_bedtime, _)| {
                sleep_time_minute_of_day(&sleep_input.start_time)
                    .map(|bedtime| circular_minute_deviation(bedtime, typical_bedtime))
            })
            .unwrap_or(0.0),
        wake_time_deviation_minutes: schedule_baseline
            .and_then(|(_, typical_wake_time)| {
                sleep_time_minute_of_day(&sleep_input.end_time)
                    .map(|wake_time| circular_minute_deviation(wake_time, typical_wake_time))
            })
            .unwrap_or(0.0),
        naps_minutes,
        data_coverage_fraction,
        ..Default::default()
    })
}

fn days_since_last_valid_sleep_night(
    sleep_input: &SleepInput,
    prior_nights: &[SleepNightHistoryInput],
) -> Option<u32> {
    let current_start_unix_ms = sleep_time_unix_ms(&sleep_input.start_time)?;
    let latest_valid_end_unix_ms = prior_nights
        .iter()
        .filter(|night| sleep_history_night_is_usable(night))
        .filter_map(|night| sleep_time_unix_ms(&night.end_time))
        .max()?;
    let elapsed_ms = current_start_unix_ms.saturating_sub(latest_valid_end_unix_ms);
    Some((elapsed_ms / (24 * 60 * 60 * 1_000)) as u32)
}

fn external_sleep_history_nights_for_sleep_v1(
    store: &GooseStore,
    sleep_need_minutes: f64,
    before_unix_ms: i64,
) -> GooseResult<Vec<SleepNightHistoryInput>> {
    let sessions = store.external_sleep_sessions_between(0, before_unix_ms)?;
    let mut nights = Vec::new();
    for session in sessions
        .into_iter()
        .filter(|session| session.end_time_unix_ms <= before_unix_ms)
    {
        let detailed_stages = store.external_sleep_stages_for_session(&session.sleep_id)?;
        let maybe_night = (|| {
            let (mut stage_minutes, has_stage_summary_minutes) =
                external_sleep_stage_minutes_from_rows_or_summary(
                    &detailed_stages,
                    &session.stage_summary_json,
                );
            let time_in_bed_minutes = session.duration_ms as f64 / 60_000.0;
            if time_in_bed_minutes <= 0.0 || !time_in_bed_minutes.is_finite() {
                return None;
            }
            let stage_minutes_normalized = normalize_external_stage_minutes_to_time_in_bed(
                &mut stage_minutes,
                time_in_bed_minutes,
            );
            let sleep_duration_minutes = external_sleep_duration_minutes_or_empty_summary_fallback(
                &stage_minutes,
                time_in_bed_minutes,
                has_stage_summary_minutes,
            )?;
            if sleep_duration_minutes <= 0.0 {
                return None;
            }
            let is_nap = external_sleep_session_is_nap(
                session.start_time_unix_ms,
                session.end_time_unix_ms,
                sleep_duration_minutes,
            );
            if is_nap {
                return None;
            }
            let awake_minutes = stage_minutes
                .get("awake")
                .copied()
                .unwrap_or((time_in_bed_minutes - sleep_duration_minutes).max(0.0));
            let excluded_from_baseline = stage_minutes_normalized
                || external_sleep_session_has_platform_import_marker(&session)
                || external_sleep_session_excluded_from_baseline(
                    session.confidence,
                    &session.provenance_json,
                )
                || external_sleep_stage_rows_excluded_from_baseline(&detailed_stages);
            Some(SleepNightHistoryInput {
                night_id: session.sleep_id,
                start_time: format!("unix_ms:{}", session.start_time_unix_ms),
                end_time: format!("unix_ms:{}", session.end_time_unix_ms),
                sleep_duration_minutes,
                sleep_need_minutes,
                time_in_bed_minutes,
                awake_minutes,
                sleep_latency_minutes: 0.0,
                wake_after_sleep_onset_minutes: awake_minutes,
                wake_episode_count: 0,
                stage_minutes,
                heart_rate_dip_percent: None,
                sleep_hr_average_bpm: None,
                sleep_hr_min_bpm: None,
                pre_sleep_awake_hr_average_bpm: None,
                sleep_hr_trend_bpm_per_hour: None,
                bedtime_deviation_minutes: 0.0,
                wake_time_deviation_minutes: 0.0,
                midpoint_deviation_minutes: 0.0,
                naps_minutes: 0.0,
                confidence_0_to_1: session.confidence,
                source: session.platform,
                excluded_from_baseline,
            })
        })();
        if let Some(night) = maybe_night {
            nights.push(night);
        }
    }
    if let Some((typical_bedtime, typical_wake_time)) = sleep_history_schedule_baseline(&nights) {
        for night in &mut nights {
            if let Some(bedtime) = sleep_time_minute_of_day(&night.start_time) {
                night.bedtime_deviation_minutes =
                    circular_minute_deviation(bedtime, typical_bedtime);
            }
            if let Some(wake_time) = sleep_time_minute_of_day(&night.end_time) {
                night.wake_time_deviation_minutes =
                    circular_minute_deviation(wake_time, typical_wake_time);
            }
            night.midpoint_deviation_minutes =
                (night.bedtime_deviation_minutes + night.wake_time_deviation_minutes) / 2.0;
        }
    }
    Ok(nights)
}

fn external_sleep_session_excluded_from_baseline(confidence: f64, provenance_json: &str) -> bool {
    if confidence < 0.50 {
        return true;
    }
    let Ok(provenance) = serde_json::from_str::<Value>(provenance_json) else {
        return true;
    };
    provenance
        .get("overlap_conflict")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || provenance
            .get("excluded_from_baseline")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || provenance_has_baseline_exclusion_context(&provenance)
}

fn external_sleep_session_has_platform_import_marker(session: &ExternalSleepSessionRow) -> bool {
    external_sleep_platform_import_token(&session.platform)
        || external_sleep_platform_import_token(&session.source)
        || external_sleep_provenance_has_platform_import_marker(&session.provenance_json)
}

fn external_sleep_stage_rows_excluded_from_baseline(stages: &[ExternalSleepStageRow]) -> bool {
    stages.iter().any(|stage| {
        stage.confidence < 0.50
            || serde_json::from_str::<Value>(&stage.provenance_json).map_or(true, |provenance| {
                provenance
                    .get("overlap_conflict")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    || provenance
                        .get("excluded_from_baseline")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    || provenance_has_baseline_exclusion_context(&provenance)
                    || value_has_platform_import_marker(&provenance)
            })
    })
}

fn provenance_has_baseline_exclusion_context(provenance: &Value) -> bool {
    const BOOL_KEYS: &[&str] = &[
        "travel",
        "sickness",
        "illness",
        "manual_entry",
        "manual_correction",
        "manually_corrected",
    ];
    const STRING_KEYS: &[&str] = &[
        "detected_context",
        "context",
        "journal_tag",
        "tag",
        "source",
        "correction_source",
    ];
    const ARRAY_KEYS: &[&str] = &["journal_tags", "tags", "context_tags", "quality_flags"];

    if BOOL_KEYS.iter().any(|key| {
        provenance
            .get(*key)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }) {
        return true;
    }

    if STRING_KEYS.iter().any(|key| {
        provenance
            .get(*key)
            .and_then(Value::as_str)
            .is_some_and(baseline_exclusion_context_token)
    }) {
        return true;
    }

    ARRAY_KEYS.iter().any(|key| {
        provenance
            .get(*key)
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .any(baseline_exclusion_context_token)
            })
    })
}

fn baseline_exclusion_context_token(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase().replace([' ', '-'], "_");
    matches!(
        normalized.as_str(),
        "travel"
            | "sick"
            | "sickness"
            | "illness"
            | "manual_entry"
            | "manual_correction"
            | "manual_edit"
            | "manual_sleep_edit"
            | "manually_corrected"
    )
}

fn external_sleep_provenance_has_platform_import_marker(provenance_json: &str) -> bool {
    serde_json::from_str::<Value>(provenance_json)
        .map(|provenance| value_has_platform_import_marker(&provenance))
        .unwrap_or(true)
}

fn value_has_platform_import_marker(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            external_sleep_platform_import_token(key) || value_has_platform_import_marker(child)
        }),
        Value::Array(values) => values.iter().any(value_has_platform_import_marker),
        Value::String(text) => external_sleep_platform_import_token(text),
        _ => false,
    }
}

fn external_sleep_platform_import_token(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase().replace([' ', '-'], "_");
    matches!(
        normalized.as_str(),
        "healthkit"
            | "health_kit"
            | "apple_health"
            | "apple_healthkit"
            | "hkhealthstore"
            | "healthkit_sleep_analysis"
            | "health_connect"
            | "google_health_connect"
            | "health_connect_sleep_session"
            | "health_connect_sleep_stage"
            | "imported_platform_sleep"
            | "sleep_history_import"
            | "external_history_context_only"
    ) || normalized.starts_with("healthkit_")
        || normalized.starts_with("health_kit_")
        || normalized.contains("_healthkit_")
        || normalized.contains("_health_connect_")
}

fn external_sleep_naps_before_sleep(
    store: &GooseStore,
    sleep_input: &SleepInput,
) -> GooseResult<f64> {
    let Some(sleep_start_unix_ms) = sleep_time_unix_ms(&sleep_input.start_time) else {
        return Ok(0.0);
    };
    let lookback_start_unix_ms = sleep_start_unix_ms.saturating_sub(18 * 60 * 60 * 1000);
    let sessions =
        store.external_sleep_sessions_between(lookback_start_unix_ms, sleep_start_unix_ms)?;
    let mut naps_minutes = 0.0;
    for session in sessions
        .into_iter()
        .filter(|session| session.end_time_unix_ms <= sleep_start_unix_ms)
    {
        let detailed_stages = store.external_sleep_stages_for_session(&session.sleep_id)?;
        let maybe_nap_minutes = (|| {
            let duration_minutes = session.duration_ms as f64 / 60_000.0;
            if duration_minutes <= 0.0 || !duration_minutes.is_finite() {
                return None;
            }
            let (mut stage_minutes, has_stage_summary_minutes) =
                external_sleep_stage_minutes_from_rows_or_summary(
                    &detailed_stages,
                    &session.stage_summary_json,
                );
            let stage_minutes_normalized = normalize_external_stage_minutes_to_time_in_bed(
                &mut stage_minutes,
                duration_minutes,
            );
            if stage_minutes_normalized
                || external_sleep_session_has_platform_import_marker(&session)
                || external_sleep_session_excluded_from_baseline(
                    session.confidence,
                    &session.provenance_json,
                )
                || external_sleep_stage_rows_excluded_from_baseline(&detailed_stages)
            {
                return None;
            }
            let sleep_duration_minutes = external_sleep_duration_minutes_or_empty_summary_fallback(
                &stage_minutes,
                duration_minutes,
                has_stage_summary_minutes,
            )?;
            external_sleep_session_is_nap(
                session.start_time_unix_ms,
                session.end_time_unix_ms,
                sleep_duration_minutes,
            )
            .then_some(sleep_duration_minutes)
        })();
        if let Some(minutes) = maybe_nap_minutes {
            naps_minutes += minutes;
        }
    }
    Ok(naps_minutes)
}

fn external_sleep_session_is_nap(
    start_time_unix_ms: i64,
    end_time_unix_ms: i64,
    sleep_duration_minutes: f64,
) -> bool {
    if !(20.0..=180.0).contains(&sleep_duration_minutes) {
        return false;
    }
    let midpoint_unix_ms = start_time_unix_ms + (end_time_unix_ms - start_time_unix_ms) / 2;
    let midpoint_minute = unix_ms_minute_of_day(midpoint_unix_ms);
    (9.0 * 60.0..=20.0 * 60.0).contains(&midpoint_minute)
}

fn sleep_history_schedule_baseline(nights: &[SleepNightHistoryInput]) -> Option<(f64, f64)> {
    let mut bedtime_minutes = nights
        .iter()
        .filter(|night| sleep_history_night_is_usable(night))
        .filter_map(|night| sleep_time_minute_of_day(&night.start_time))
        .collect::<Vec<_>>();
    let mut wake_time_minutes = nights
        .iter()
        .filter(|night| sleep_history_night_is_usable(night))
        .filter_map(|night| sleep_time_minute_of_day(&night.end_time))
        .collect::<Vec<_>>();
    if bedtime_minutes.is_empty() || wake_time_minutes.is_empty() {
        return None;
    }
    Some((
        typical_minute_of_day(&mut bedtime_minutes),
        typical_minute_of_day(&mut wake_time_minutes),
    ))
}

fn sleep_time_minute_of_day(value: &str) -> Option<f64> {
    if let Some(unix_ms) = value
        .strip_prefix("unix_ms:")
        .and_then(|text| text.parse::<i64>().ok())
    {
        return Some(unix_ms_minute_of_day(unix_ms));
    }
    rfc3339_minute_of_day(value)
}

fn sleep_time_unix_ms(value: &str) -> Option<i64> {
    if let Some(unix_ms) = value
        .strip_prefix("unix_ms:")
        .and_then(|text| text.parse::<i64>().ok())
    {
        return Some(unix_ms);
    }
    parse_rfc3339_utc_unix_ms(value)
}

fn unix_ms_minute_of_day(unix_ms: i64) -> f64 {
    ((unix_ms / 60_000).rem_euclid(24 * 60)) as f64
}

fn rfc3339_minute_of_day(value: &str) -> Option<f64> {
    let (_, time) = value.split_once('T')?;
    let mut parts = time.split(':');
    let hour = parts.next()?.parse::<u32>().ok()?;
    let minute = parts.next()?.parse::<u32>().ok()?;
    if hour >= 24 || minute >= 60 {
        return None;
    }
    Some((hour * 60 + minute) as f64)
}

fn parse_rfc3339_utc_unix_ms(value: &str) -> Option<i64> {
    let value = value.strip_suffix('Z')?;
    let (date, time) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i32>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let seconds_part = time_parts.next()?;
    if time_parts.next().is_some() {
        return None;
    }
    let second = seconds_part
        .split_once('.')
        .map(|(second, _)| second)
        .unwrap_or(seconds_part)
        .parse::<u32>()
        .ok()?;
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour >= 24
        || minute >= 60
        || second >= 60
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some((days * 86_400 + hour as i64 * 3_600 + minute as i64 * 60 + second as i64) * 1_000)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn typical_minute_of_day(values: &mut [f64]) -> f64 {
    values.sort_by(|left, right| left.total_cmp(right));
    values
        .iter()
        .copied()
        .min_by(|left, right| {
            let left_distance = values
                .iter()
                .map(|value| circular_minute_deviation(*left, *value))
                .sum::<f64>();
            let right_distance = values
                .iter()
                .map(|value| circular_minute_deviation(*right, *value))
                .sum::<f64>();
            left_distance.total_cmp(&right_distance)
        })
        .unwrap_or(0.0)
}

fn circular_minute_deviation(left: f64, right: f64) -> f64 {
    let difference = (left - right).abs().rem_euclid(24.0 * 60.0);
    difference.min(24.0 * 60.0 - difference)
}

fn external_sleep_stage_minutes_from_rows_or_summary(
    stages: &[ExternalSleepStageRow],
    stage_summary_json: &str,
) -> (BTreeMap<String, f64>, bool) {
    if !stages.is_empty() {
        let mut stage_minutes = BTreeMap::new();
        for stage in stages {
            let Some(stage_kind) = canonical_external_sleep_stage(&stage.stage_kind) else {
                continue;
            };
            let minutes = stage.duration_ms as f64 / 60_000.0;
            if minutes.is_finite() && minutes >= 0.0 {
                *stage_minutes.entry(stage_kind.to_string()).or_insert(0.0) += minutes;
            }
        }
        return (stage_minutes, true);
    }
    external_sleep_stage_minutes(stage_summary_json)
}

fn external_sleep_stage_minutes(stage_summary_json: &str) -> (BTreeMap<String, f64>, bool) {
    let Ok(summary) = serde_json::from_str::<Value>(stage_summary_json) else {
        return (BTreeMap::new(), false);
    };
    let Some(values) = summary.get("minutes_by_stage").and_then(Value::as_object) else {
        return (BTreeMap::new(), false);
    };
    let has_stage_summary_minutes = !values.is_empty();
    let stage_minutes = values
        .iter()
        .fold(BTreeMap::new(), |mut acc, (stage, minutes)| {
            if let (Some(stage), Some(minutes)) = (
                canonical_external_sleep_stage(stage),
                minutes
                    .as_f64()
                    .filter(|minutes| minutes.is_finite() && *minutes >= 0.0),
            ) {
                *acc.entry(stage.to_string()).or_insert(0.0) += minutes;
            }
            acc
        });
    (stage_minutes, has_stage_summary_minutes)
}

fn external_sleep_duration_minutes(stage_minutes: &BTreeMap<String, f64>) -> Option<f64> {
    let asleep = ["core", "deep", "rem"]
        .iter()
        .filter_map(|stage| stage_minutes.get(*stage))
        .copied()
        .sum::<f64>();
    (asleep > 0.0).then_some(asleep)
}

fn external_sleep_duration_minutes_or_empty_summary_fallback(
    stage_minutes: &BTreeMap<String, f64>,
    time_in_bed_minutes: f64,
    has_stage_summary_minutes: bool,
) -> Option<f64> {
    if !has_stage_summary_minutes {
        Some(time_in_bed_minutes)
    } else {
        external_sleep_duration_minutes(stage_minutes)
            .map(|minutes| minutes.min(time_in_bed_minutes))
    }
}

fn normalize_external_stage_minutes_to_time_in_bed(
    stage_minutes: &mut BTreeMap<String, f64>,
    time_in_bed_minutes: f64,
) -> bool {
    let total = stage_minutes.values().copied().sum::<f64>();
    if total <= time_in_bed_minutes || total <= 0.0 {
        return false;
    }
    let scale = time_in_bed_minutes / total;
    for minutes in stage_minutes.values_mut() {
        *minutes *= scale;
    }
    true
}

fn canonical_external_sleep_stage(stage: &str) -> Option<&'static str> {
    match stage
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "awake" | "asleep_awake" | "sleep_awake" | "out_of_bed" => Some("awake"),
        "asleep" | "asleep_unspecified" | "core" | "light" | "asleep_core" | "sleep_light" => {
            Some("core")
        }
        "deep" | "asleep_deep" | "sleep_deep" => Some("deep"),
        "rem" | "asleep_rem" | "sleep_rem" => Some("rem"),
        "in_bed" | "inbed" => None,
        _ => None,
    }
}

#[allow(dead_code)]
fn canonical_external_sleep_stage_row(stage: &str) -> Option<&'static str> {
    match stage
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "in_bed" | "inbed" => Some("in_bed"),
        "unknown" => Some("unknown"),
        "not_applicable" | "not_applicable_sleep" => Some("not_applicable"),
        value => canonical_external_sleep_stage(value),
    }
}

fn sleep_stage_kind_label(stage: &SleepStageKind) -> &'static str {
    match stage {
        SleepStageKind::Awake => "awake",
        SleepStageKind::Core => "core",
        SleepStageKind::Deep => "deep",
        SleepStageKind::Rem => "rem",
    }
}

fn sleep_feature_score_bridge(args: SleepFeatureScoreArgs) -> GooseResult<serde_json::Value> {
    let requested_algorithm_id = args
        .algorithm_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(GOOSE_SLEEP_V0_ID);
    let requested_algorithm_version = args
        .algorithm_version
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(if requested_algorithm_id == GOOSE_SLEEP_V1_ID {
            GOOSE_SLEEP_V1_VERSION
        } else {
            GOOSE_SLEEP_V0_VERSION
        });
    let sleep_v1_requested = match (requested_algorithm_id, requested_algorithm_version) {
        (GOOSE_SLEEP_V0_ID, GOOSE_SLEEP_V0_VERSION) => false,
        (GOOSE_SLEEP_V1_ID, GOOSE_SLEEP_V1_VERSION) => true,
        _ => {
            return Err(GooseError::message(format!(
                "unsupported primary algorithm {requested_algorithm_id}@{requested_algorithm_version} for sleep; this packet-derived scorer currently supports {GOOSE_SLEEP_V0_ID}@{GOOSE_SLEEP_V0_VERSION} and {GOOSE_SLEEP_V1_ID}@{GOOSE_SLEEP_V1_VERSION}"
            )));
        }
    };
    let store = acquire_bridge_conn(&args.database_path)?;
    let report = run_sleep_feature_score_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        SleepFeatureScoreOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            sleep_need_minutes: args.sleep_need_minutes.unwrap_or_else(|| {
                compute_sleep_need_with_store(&store, args.age_years, None)
                    .map(|r| r.total_need_minutes)
                    .unwrap_or(450.0)
            }),
            age_years: args.age_years,
            low_motion_threshold_0_to_1: args.low_motion_threshold_0_to_1.unwrap_or(0.05),
            disturbance_motion_threshold_0_to_1: args
                .disturbance_motion_threshold_0_to_1
                .unwrap_or(0.20),
            target_midpoint_minutes_since_midnight: args
                .target_midpoint_minutes_since_midnight
                .unwrap_or(180.0),
        },
    )?;
    let mut value = serde_json::to_value(&report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize sleep feature score report: {error}"
        ))
    })?;
    if sleep_v1_requested {
        if let Some(sleep_input) = report.sleep_input.as_ref() {
            let sleep_v1_input = sleep_v1_input_from_feature_score(
                &store,
                sleep_input,
                &report,
                args.history_import_in_progress,
            )?;
            let sleep_v1_result = goose_sleep_v1(&sleep_v1_input);
            value["sleep_v1_input"] = serde_json::to_value(&sleep_v1_input).map_err(|error| {
                GooseError::message(format!("cannot serialize sleep v1 input: {error}"))
            })?;
            value["score_result"] = metric_result_to_value(&sleep_v1_result)?;
            maybe_persist_algorithm_run(
                &store,
                &mut value,
                args.persist_algorithm_run,
                args.algorithm_run_id.as_deref(),
                "packet-derived-sleep-v1",
                Some(&sleep_v1_result),
            )?;
        } else {
            value["score_result"] = Value::Null;
            maybe_persist_algorithm_run::<crate::metrics::SleepV1Output>(
                &store,
                &mut value,
                args.persist_algorithm_run,
                args.algorithm_run_id.as_deref(),
                "packet-derived-sleep-v1",
                None,
            )?;
        }
    } else {
        maybe_persist_algorithm_run(
            &store,
            &mut value,
            args.persist_algorithm_run,
            args.algorithm_run_id.as_deref(),
            "packet-derived-sleep",
            report.score_result.as_ref(),
        )?;
    }
    Ok(value)
}

fn recovery_feature_score_bridge(args: RecoveryFeatureScoreArgs) -> GooseResult<serde_json::Value> {
    validate_requested_primary_algorithm(
        "recovery",
        args.algorithm_id.as_deref(),
        args.algorithm_version.as_deref(),
        GOOSE_RECOVERY_V0_ID,
        GOOSE_RECOVERY_V0_VERSION,
    )?;
    let store = acquire_bridge_conn(&args.database_path)?;
    let hrv_start = args.hrv_start.as_deref().unwrap_or(&args.start);
    let hrv_end = args.hrv_end.as_deref().unwrap_or(&args.end);
    let sleep_start = args.sleep_start.as_deref().unwrap_or(&args.start);
    let sleep_end = args.sleep_end.as_deref().unwrap_or(&args.end);
    let prior_strain_start = args.prior_strain_start.as_deref().unwrap_or(&args.start);
    let prior_strain_end = args.prior_strain_end.as_deref().unwrap_or(&args.end);
    let report = run_recovery_feature_score_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        hrv_start,
        hrv_end,
        &args.hrv_baseline_start,
        &args.hrv_baseline_end,
        &args.resting_start,
        &args.resting_end,
        sleep_start,
        sleep_end,
        prior_strain_start,
        prior_strain_end,
        RecoveryFeatureScoreOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            resting_baseline_min_days: args.resting_baseline_min_days.unwrap_or(3),
            hrv_min_rr_intervals_to_compute: args.hrv_min_rr_intervals_to_compute.unwrap_or(2),
            hrv_baseline_min_days: args.hrv_baseline_min_days.unwrap_or(3),
            sleep_need_minutes: args.sleep_need_minutes.unwrap_or_else(|| {
                compute_sleep_need_with_store(&store, args.age_years, None)
                    .map(|r| r.total_need_minutes)
                    .unwrap_or(450.0)
            }),
            age_years: args.age_years,
            low_motion_threshold_0_to_1: args.low_motion_threshold_0_to_1.unwrap_or(0.05),
            disturbance_motion_threshold_0_to_1: args
                .disturbance_motion_threshold_0_to_1
                .unwrap_or(0.20),
            target_midpoint_minutes_since_midnight: args
                .target_midpoint_minutes_since_midnight
                .unwrap_or(180.0),
            prior_strain_resting_baseline_min_days: args
                .prior_strain_resting_baseline_min_days
                .unwrap_or(3),
            prior_strain_max_hr_bpm: args.prior_strain_max_hr_bpm,
            respiratory_rate_rpm: args.respiratory_rate_rpm,
            respiratory_rate_baseline_rpm: args.respiratory_rate_baseline_rpm,
            skin_temp_delta_c: args.skin_temp_delta_c,
            provided_vitals_source: args.provided_vitals_source,
            provided_vitals_provenance_json: args.provided_vitals_provenance_json,
        },
    )?;
    let mut value = serde_json::to_value(&report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize recovery feature score report: {error}"
        ))
    })?;
    if args.persist_algorithm_run && !report.pass {
        value["persisted_algorithm_run"] = json!({
            "persist_requested": true,
            "inserted": false,
            "blocked_reason": "report_not_passed",
            "issues": &report.issues,
        });
    } else {
        maybe_persist_algorithm_run(
            &store,
            &mut value,
            args.persist_algorithm_run,
            args.algorithm_run_id.as_deref(),
            "packet-derived-recovery",
            report.score_result.as_ref(),
        )?;
    }
    Ok(value)
}

fn strain_feature_score_bridge(args: StrainFeatureScoreArgs) -> GooseResult<serde_json::Value> {
    validate_requested_primary_algorithm(
        "strain",
        args.algorithm_id.as_deref(),
        args.algorithm_version.as_deref(),
        GOOSE_STRAIN_V0_ID,
        GOOSE_STRAIN_V0_VERSION,
    )?;
    let store = acquire_bridge_conn(&args.database_path)?;
    let resting_start = args.resting_start.as_deref().unwrap_or(&args.start);
    let resting_end = args.resting_end.as_deref().unwrap_or(&args.end);
    let report = run_strain_feature_score_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        resting_start,
        resting_end,
        StrainFeatureScoreOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            resting_baseline_min_days: args.resting_baseline_min_days.unwrap_or(3),
            max_hr_bpm: args.max_hr_bpm,
        },
    )?;
    let mut value = serde_json::to_value(&report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize strain feature score report: {error}"
        ))
    })?;
    maybe_persist_algorithm_run(
        &store,
        &mut value,
        args.persist_algorithm_run,
        args.algorithm_run_id.as_deref(),
        "packet-derived-strain",
        report.score_result.as_ref(),
    )?;
    Ok(value)
}

fn stress_feature_score_bridge(args: StressFeatureScoreArgs) -> GooseResult<serde_json::Value> {
    validate_requested_primary_algorithm(
        "stress",
        args.algorithm_id.as_deref(),
        args.algorithm_version.as_deref(),
        GOOSE_STRESS_V0_ID,
        GOOSE_STRESS_V0_VERSION,
    )?;
    let store = acquire_bridge_conn(&args.database_path)?;
    let hrv_start = args.hrv_start.as_deref().unwrap_or(&args.start);
    let hrv_end = args.hrv_end.as_deref().unwrap_or(&args.end);
    let report = run_stress_feature_score_report_for_store(
        &store,
        &args.database_path,
        &args.start,
        &args.end,
        &args.resting_start,
        &args.resting_end,
        hrv_start,
        hrv_end,
        &args.hrv_baseline_start,
        &args.hrv_baseline_end,
        StressFeatureScoreOptions {
            min_owned_captures_per_summary: args
                .min_owned_captures
                .unwrap_or(DEFAULT_MIN_OWNED_CAPTURES_PER_SUMMARY),
            require_trusted_evidence: args.require_trusted_evidence,
            resting_baseline_min_days: args.resting_baseline_min_days.unwrap_or(3),
            hrv_min_rr_intervals_to_compute: args.hrv_min_rr_intervals_to_compute.unwrap_or(2),
            hrv_baseline_min_days: args.hrv_baseline_min_days.unwrap_or(3),
        },
    )?;
    let mut value = serde_json::to_value(&report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize stress feature score report: {error}"
        ))
    })?;
    maybe_persist_algorithm_run(
        &store,
        &mut value,
        args.persist_algorithm_run,
        args.algorithm_run_id.as_deref(),
        "packet-derived-stress",
        report.score_result.as_ref(),
    )?;
    Ok(value)
}

fn evaluate_calibration_dataset_bridge(
    args: EvaluateCalibrationDatasetArgs,
) -> GooseResult<serde_json::Value> {
    let report = evaluate_linear_calibration(&args.dataset, &args.options);
    let calibration_run_id = args.calibration_run_id.clone();
    let persisted = maybe_persist_calibration_report(
        &report,
        args.database_path.as_deref(),
        args.persist,
        calibration_run_id.as_deref(),
    )?;

    let mut value = serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!("cannot serialize calibration report: {error}"))
    })?;
    if let Some(object) = value.as_object_mut() {
        object.insert("persisted".to_string(), json!(persisted));
        object.insert("calibration_run_id".to_string(), json!(calibration_run_id));
    }
    Ok(value)
}

fn evaluate_stored_calibration_labels_bridge(
    args: EvaluateStoredCalibrationLabelsArgs,
) -> GooseResult<serde_json::Value> {
    if args.start.trim().is_empty() {
        return Err(GooseError::message("start is required"));
    }
    if args.end.trim().is_empty() {
        return Err(GooseError::message("end is required"));
    }
    if args.start >= args.end {
        return Err(GooseError::message("start must be earlier than end"));
    }

    let store = acquire_bridge_conn(&args.database_path)?;
    let algorithm_runs = store.algorithm_runs_overlapping(&args.start, &args.end)?;
    let labels = store.calibration_labels_between(&args.start, &args.end)?;
    let (dataset, matched_records, dataset_issues) =
        stored_calibration_dataset(&algorithm_runs, &labels, &args.options);
    let report = evaluate_linear_calibration(&dataset, &args.options);
    let calibration_run_id = args.calibration_run_id.clone();
    let persisted = maybe_persist_calibration_report(
        &report,
        Some(&args.database_path),
        args.persist,
        calibration_run_id.as_deref(),
    )?;

    let mut value = serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!(
            "cannot serialize stored calibration report: {error}"
        ))
    })?;
    if let Some(object) = value.as_object_mut() {
        object.insert("persisted".to_string(), json!(persisted));
        object.insert("calibration_run_id".to_string(), json!(calibration_run_id));
        object.insert(
            "dataset_schema".to_string(),
            json!("goose.calibration-dataset.v1"),
        );
        object.insert(
            "dataset_record_count".to_string(),
            json!(dataset.records.len()),
        );
        object.insert(
            "algorithm_run_count".to_string(),
            json!(algorithm_runs.len()),
        );
        object.insert("label_count".to_string(), json!(labels.len()));
        object.insert(
            "matched_record_count".to_string(),
            json!(matched_records.len()),
        );
        object.insert("matched_records".to_string(), json!(matched_records));
        object.insert("dataset_issues".to_string(), json!(dataset_issues));
        object.insert("official_labels_are_labels".to_string(), json!(true));
    }
    Ok(value)
}

fn import_calibration_labels_bridge(
    args: ImportCalibrationLabelsArgs,
) -> GooseResult<serde_json::Value> {
    if args.labels.is_empty() {
        return Err(GooseError::message(
            "at least one calibration label is required",
        ));
    }
    let store = acquire_bridge_conn(&args.database_path)?;
    let mut inserted = 0usize;
    let mut existing = 0usize;
    let mut labels = Vec::new();
    for label in args.labels {
        let provenance_json = serde_json::to_string(&label.provenance).map_err(|error| {
            GooseError::message(format!("cannot serialize label provenance: {error}"))
        })?;
        let changed = store.insert_calibration_label(CalibrationLabelInput {
            label_id: &label.label_id,
            metric_family: &label.metric_family,
            label_source: &label.label_source,
            captured_at: &label.captured_at,
            value: label.value,
            unit: &label.unit,
            provenance_json: &provenance_json,
        })?;
        if changed {
            inserted += 1;
        } else {
            existing += 1;
        }
        if let Some(row) = store.calibration_label(&label.label_id)? {
            labels.push(row);
        }
    }
    Ok(json!({
        "schema": "goose.calibration-label-import-report.v1",
        "generated_by": "goose-bridge",
        "pass": true,
        "label_count": inserted + existing,
        "inserted": inserted,
        "existing": existing,
        "official_labels_are_labels": true,
        "labels": labels,
        "issues": []
    }))
}

fn list_calibration_labels_bridge(
    args: ListCalibrationLabelsArgs,
) -> GooseResult<serde_json::Value> {
    if args.start.trim().is_empty() {
        return Err(GooseError::message("start is required"));
    }
    if args.end.trim().is_empty() {
        return Err(GooseError::message("end is required"));
    }
    if args.start >= args.end {
        return Err(GooseError::message("start must be earlier than end"));
    }
    let store = acquire_bridge_conn(&args.database_path)?;
    let labels = store.calibration_labels_between(&args.start, &args.end)?;
    Ok(json!({
        "schema": "goose.calibration-label-list.v1",
        "generated_by": "goose-bridge",
        "start": args.start,
        "end": args.end,
        "label_count": labels.len(),
        "official_labels_are_labels": true,
        "labels": labels
    }))
}

fn apply_calibration_bridge(args: ApplyCalibrationArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let calibration_run = match args.calibration_run_id.as_deref() {
        Some(calibration_run_id) if !calibration_run_id.trim().is_empty() => {
            store.calibration_run(calibration_run_id)?.ok_or_else(|| {
                GooseError::message(format!("calibration run {calibration_run_id} not found"))
            })?
        }
        _ => latest_matching_calibration_run(&store, &args.algorithm_id, &args.algorithm_version)?
            .ok_or_else(|| {
                GooseError::message(format!(
                    "no calibration run found for {}@{}",
                    args.algorithm_id, args.algorithm_version
                ))
            })?,
    };
    let report = apply_calibration(&CalibrationApplicationInput {
        metric_family: args.metric_family,
        algorithm_id: args.algorithm_id,
        algorithm_version: args.algorithm_version,
        raw_score: args.raw_score,
        input_run_id: args.input_run_id,
        score_min: args.score_min,
        score_max: args.score_max,
        calibration_run,
    });
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!("cannot serialize calibration application: {error}"))
    })
}

fn maybe_persist_calibration_report(
    report: &CalibrationReport,
    database_path: Option<&str>,
    persist_requested: bool,
    calibration_run_id: Option<&str>,
) -> GooseResult<bool> {
    if !persist_requested {
        return Ok(false);
    }
    if !report.pass {
        return Err(GooseError::message(
            "calibration report did not pass; refusing to persist",
        ));
    }
    let database_path = database_path
        .ok_or_else(|| GooseError::message("database_path is required when persist is true"))?;
    let calibration_run_id = calibration_run_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            GooseError::message("calibration_run_id is required when persist is true")
        })?;
    let store = acquire_bridge_conn(database_path)?;
    register_built_in_definitions(&store)?;
    let record = calibration_run_record(calibration_run_id, report)?;
    store.insert_calibration_run(&record)
}

fn stored_calibration_dataset(
    algorithm_runs: &[AlgorithmRunRecord],
    labels: &[CalibrationLabelRow],
    options: &CalibrationOptions,
) -> (CalibrationDataset, Vec<serde_json::Value>, Vec<String>) {
    let expected_unit = expected_calibration_label_unit(&options.metric_family);
    let mut records = Vec::new();
    let mut matched_records = Vec::new();
    let mut issues = Vec::new();

    for label in labels
        .iter()
        .filter(|label| label.metric_family.as_str() == options.metric_family.as_str())
    {
        if label.unit != expected_unit {
            issues.push(format!(
                "{} skipped: unit {} does not match {}",
                label.label_id, label.unit, expected_unit
            ));
            continue;
        }
        let provenance = serde_json::from_str::<serde_json::Value>(&label.provenance_json)
            .unwrap_or_else(|_| json!({}));
        let Some(run) =
            matching_calibration_algorithm_run(algorithm_runs, label, &provenance, options)
        else {
            issues.push(format!(
                "{} skipped: no matching algorithm run",
                label.label_id
            ));
            continue;
        };
        let Some(prediction) = prediction_from_algorithm_run(run, &options.metric_family) else {
            issues.push(format!(
                "{} skipped: algorithm run {} has no score field for {}",
                label.label_id, run.run_id, options.metric_family
            ));
            continue;
        };

        let label_provenance = calibration_label_provenance(provenance, label, run);
        let record_id = format!("stored.{}.{}", run.run_id, label.label_id);
        let session_id = label_provenance
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| run.run_id.clone());
        records.push(CalibrationRecord {
            record_id: record_id.clone(),
            captured_at: label.captured_at.clone(),
            session_id: Some(session_id),
            metric_family: label.metric_family.clone(),
            algorithm_id: run.algorithm_id.clone(),
            algorithm_version: run.version.clone(),
            prediction,
            label: label.value,
            label_source: label.label_source.clone(),
            label_provenance,
        });
        matched_records.push(json!({
            "record_id": record_id,
            "label_id": &label.label_id,
            "algorithm_run_id": &run.run_id,
            "captured_at": &label.captured_at,
            "prediction": prediction,
            "label": label.value,
            "unit": &label.unit
        }));
    }

    (
        CalibrationDataset {
            schema: "goose.calibration-dataset.v1".to_string(),
            records,
        },
        matched_records,
        issues,
    )
}

fn matching_calibration_algorithm_run<'a>(
    algorithm_runs: &'a [AlgorithmRunRecord],
    label: &CalibrationLabelRow,
    provenance: &serde_json::Value,
    options: &CalibrationOptions,
) -> Option<&'a AlgorithmRunRecord> {
    if let Some(run_id) = provenance_algorithm_run_id(provenance)
        && let Some(run) = algorithm_runs.iter().find(|run| {
            run.run_id.as_str() == run_id
                && run.algorithm_id.as_str() == options.algorithm_id.as_str()
                && run.version.as_str() == options.algorithm_version.as_str()
        })
    {
        return Some(run);
    }

    algorithm_runs.iter().find(|run| {
        run.algorithm_id.as_str() == options.algorithm_id.as_str()
            && run.version.as_str() == options.algorithm_version.as_str()
            && run.start_time.as_str() <= label.captured_at.as_str()
            && run.end_time.as_str() >= label.captured_at.as_str()
    })
}

fn provenance_algorithm_run_id(provenance: &serde_json::Value) -> Option<&str> {
    ["algorithm_run_id", "run_id", "input_run_id"]
        .into_iter()
        .find_map(|key| provenance.get(key).and_then(serde_json::Value::as_str))
        .filter(|value| !value.trim().is_empty())
}

fn prediction_from_algorithm_run(run: &AlgorithmRunRecord, metric_family: &str) -> Option<f64> {
    let output = serde_json::from_str::<serde_json::Value>(&run.output_json).ok()?;
    let field = score_field_for_metric_family(metric_family);
    output
        .get(field)
        .and_then(serde_json::Value::as_f64)
        .or_else(|| {
            output
                .get("output")
                .and_then(|nested| nested.get(field))
                .and_then(serde_json::Value::as_f64)
        })
}

fn score_field_for_metric_family(metric_family: &str) -> &'static str {
    match metric_family {
        "strain" => "score_0_to_21",
        "hrv" => "rmssd_ms",
        _ => "score_0_to_100",
    }
}

fn expected_calibration_label_unit(metric_family: &str) -> &'static str {
    match metric_family {
        "strain" => "score_0_to_21",
        "hrv" => "ms",
        _ => "score_0_to_100",
    }
}

fn calibration_label_provenance(
    provenance: serde_json::Value,
    label: &CalibrationLabelRow,
    run: &AlgorithmRunRecord,
) -> serde_json::Value {
    let mut provenance = provenance;
    if !provenance.is_object() || provenance == json!({}) {
        provenance = json!({
            "source": "stored_calibration_label",
            "official_labels_are_labels": true
        });
    }
    if let Some(object) = provenance.as_object_mut() {
        object.insert("label_id".to_string(), json!(&label.label_id));
        object.insert("algorithm_run_id".to_string(), json!(&run.run_id));
        object.insert("official_labels_are_labels".to_string(), json!(true));
    }
    provenance
}

fn metric_series_upsert_bridge(args: MetricSeriesUpsertArgs) -> GooseResult<serde_json::Value> {
    // T-69-01: validate metric_name is non-empty and matches [a-z0-9._-]+
    if args.metric_name.is_empty()
        || !args.metric_name.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        })
    {
        return Err(GooseError::message(format!(
            "invalid metric_name '{}': must be non-empty and match [a-z0-9._-]+",
            args.metric_name
        )));
    }
    let store = acquire_bridge_conn(&args.database_path)?;
    let inserted =
        store.insert_metric_series(&args.source, &args.metric_name, &args.date, args.value)?;
    Ok(json!({
        "schema": "goose.metric-series-upsert-result.v1",
        "generated_by": "goose-bridge",
        "inserted": inserted,
    }))
}

fn metric_series_query_range_bridge(
    args: MetricSeriesQueryRangeArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let rows = store.query_metric_series_range(
        &args.metric_name,
        &args.start_date,
        &args.end_date,
        args.source.as_deref(),
    )?;
    Ok(json!({
        "schema": "goose.metric-series-query-range-result.v1",
        "metric_name": args.metric_name,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "rows": rows,
    }))
}

fn maybe_persist_algorithm_run<T: Serialize>(
    store: &GooseStore,
    report_value: &mut serde_json::Value,
    persist_requested: bool,
    requested_run_id: Option<&str>,
    default_run_prefix: &str,
    score_result: Option<&AlgorithmRunResult<T>>,
) -> GooseResult<()> {
    if !persist_requested {
        return Ok(());
    }
    let Some(score_result) = score_result else {
        report_value["persisted_algorithm_run"] = json!({
            "persist_requested": true,
            "inserted": false,
            "blocked_reason": "score_result_missing",
        });
        return Ok(());
    };
    if score_result.output.is_none() {
        report_value["persisted_algorithm_run"] = json!({
            "persist_requested": true,
            "inserted": false,
            "algorithm_id": &score_result.algorithm_id,
            "algorithm_version": &score_result.algorithm_version,
            "blocked_reason": "score_output_missing",
        });
        return Ok(());
    }
    let run_id = requested_run_id
        .filter(|run_id| !run_id.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| packet_derived_algorithm_run_id(default_run_prefix, score_result));
    for definition in built_in_algorithm_definitions()
        .into_iter()
        .filter(|definition| {
            definition.algorithm_id == score_result.algorithm_id
                && definition.version == score_result.algorithm_version
        })
    {
        store.upsert_algorithm_definition(&definition)?;
    }
    let record = algorithm_run_record(&run_id, score_result)?;
    let inserted = store.insert_algorithm_run(&record)?;
    report_value["persisted_algorithm_run"] = json!({
        "persist_requested": true,
        "inserted": inserted,
        "run_id": run_id,
        "algorithm_id": &score_result.algorithm_id,
        "algorithm_version": &score_result.algorithm_version,
        "start_time": &score_result.start_time,
        "end_time": &score_result.end_time,
    });
    Ok(())
}

fn packet_derived_algorithm_run_id<T>(prefix: &str, result: &AlgorithmRunResult<T>) -> String {
    format!(
        "{}.{}.{}.{}",
        prefix, result.algorithm_id, result.start_time, result.end_time
    )
    .chars()
    .map(|ch| {
        if ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-' {
            ch
        } else {
            '-'
        }
    })
    .collect()
}

fn latest_matching_calibration_run(
    store: &GooseStore,
    algorithm_id: &str,
    algorithm_version: &str,
) -> GooseResult<Option<crate::store::CalibrationRunRecord>> {
    let runs = store.calibration_runs_overlapping("0000", "9999")?;
    Ok(runs
        .into_iter()
        .filter(|run| run.algorithm_id == algorithm_id && run.version == algorithm_version)
        .max_by(|left, right| {
            left.times
                .holdout_end
                .cmp(&right.times.holdout_end)
                .then_with(|| left.calibration_run_id.cmp(&right.calibration_run_id))
        }))
}

/// Returns None if ir == 0 or if the computed value is outside the plausible
/// physiological range [70, 100] % (clamp applied after plausibility gate).
///
/// SpO2 from raw ratio-of-ratios (pre-calibration, photoplethysmography standard):
///   R = AC_red / DC_red / (AC_ir / DC_ir)
///   Here approximated as: R = red_adc / ir_adc (single-sample ratio; no AC/DC separation)
///   SpO2 ≈ 110 − 25 × R (empirical linear approximation)
///   gate: 70–100 % — values outside indicate sensor error, motion artifact, or off-wrist
///   empirically verified via hardware captures and app readout comparison
fn spo2_from_raw_uncalibrated(red: u16, ir: u16) -> Option<f64> {
    if ir == 0 {
        return None;
    }
    let r = (red as f64) / (ir as f64);
    let spo2 = 110.0 - 25.0 * r;
    if !(70.0..=100.0).contains(&spo2) {
        // Outside plausible range — reject; caller emits a warning.
        return None;
    }
    Some(spo2)
}

/// Skin temperature (uncalibrated linear approximation).
/// Linear model: raw=930 → 33 °C, slope 30 ADC units per °C.
/// Returns None if the result is outside the plausible range [25, 40] °C.
///
/// Skin temperature conversion from NTC thermistor ADC reading:
///   degC = (raw_u16 − 930) / 30.0 + 33.0
///   anchor: raw=930 maps to 33 °C (typical resting wrist temperature)
///   slope: 30 ADC units per °C (empirical coefficient from NTC linearisation curve)
///   gate: 25–40 °C (below 25 = device off-wrist or cold shock, above 40 = sensor error)
///   LSB-per-degC coefficient empirically verified via payload regression
fn skin_temp_celsius_from_raw(raw: u16) -> Option<f64> {
    let celsius = (raw as f64 - 930.0) / 30.0 + 33.0;
    if !(25.0..=40.0).contains(&celsius) {
        // Outside plausible range — reject.
        return None;
    }
    Some(celsius)
}

/// Respiratory rate estimate (uncalibrated) using zero-crossing count on a
/// raw window sampled at 1 Hz. Returns None if window_len < 10 or if the
/// computed rate is outside the plausible range (0, 60] breaths/min.
///
/// Zero-crossing algorithm for respiratory rate estimation:
///   1. Compute mean of window to centre signal around zero.
///   2. Count sign changes (zero-crossings) in the mean-subtracted signal.
///   3. rate_bpm = (crossings / 2) / window_seconds × 60
///      Each full breath cycle produces 2 crossings (one inhale, one exhale).
// sampling rate: 1 Hz (resp_raw field from V24/V18 body, one value per second)
// window: minimum 10 samples required for a stable estimate
// gate: (0, 60] breaths/min — 0 and negative rejected; above 60 indicates noise or motion artifact
// algorithm: standard zero-crossing rate estimator; no patent claims
// empirically verified 2026-06-14 via comparison to reference waveform at known breathing rates
#[allow(dead_code)] // deferred to Phase 31/33 (zero-crossing rate not yet wired into insert path)
fn resp_rate_bpm_zero_crossing(window: &[u16]) -> Option<f64> {
    if window.len() < 10 {
        return None;
    }
    let mean = window.iter().map(|&v| v as f64).sum::<f64>() / window.len() as f64;
    let centered: Vec<f64> = window.iter().map(|&v| v as f64 - mean).collect();
    let crossings = centered
        .windows(2)
        .filter(|w| (w[0] >= 0.0) != (w[1] >= 0.0))
        .count();
    let rate = (crossings as f64 / 2.0) / (window.len() as f64) * 60.0;
    if rate <= 0.0 || rate > 60.0 {
        return None;
    }
    Some(rate)
}

// Local default helpers for serde attributes in this module (serde requires bare fn names,
// not super:: paths, so these are defined locally even though mod.rs has similar functions).
fn default_perf_scale() -> usize {
    crate::perf_budget::DEFAULT_PERF_SCALE
}

fn default_property_seed() -> u64 {
    crate::property_tests::DEFAULT_PROPERTY_SEED
}

fn default_property_cases() -> usize {
    crate::property_tests::DEFAULT_CASES_PER_GROUP
}

fn imu_step_count_from_decoded_frames_bridge(
    args: ImuStepCountFromDecodedFramesArgs,
) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;

    let start_dt = chrono_from_unix(args.start_ts);
    let end_dt = chrono_from_unix(args.end_ts);

    let frames = store.decoded_frames_between(&start_dt, &end_dt)?;

    let mut gravity_samples: Vec<[f64; 3]> = Vec::new();

    for frame in &frames {
        if !frame.header_crc_valid || !frame.payload_crc_valid {
            continue;
        }

        let parsed: Option<ParsedPayload> =
            serde_json::from_str(&frame.parsed_payload_json).unwrap_or(None);

        if let Some(ParsedPayload::DataPacket {
            body_summary: Some(DataPacketBodySummary::RawMotionK10 { axes, .. }),
            ..
        }) = parsed
        {
            let ax = axes
                .iter()
                .find(|a| a.name == "accelerometer_x")
                .and_then(|a| a.full_samples.as_ref());
            let ay = axes
                .iter()
                .find(|a| a.name == "accelerometer_y")
                .and_then(|a| a.full_samples.as_ref());
            let az = axes
                .iter()
                .find(|a| a.name == "accelerometer_z")
                .and_then(|a| a.full_samples.as_ref());

            if let (Some(xs), Some(ys), Some(zs)) = (ax, ay, az) {
                let n = xs.len().min(ys.len()).min(zs.len());
                for i in 0..n {
                    gravity_samples.push([
                        xs[i] as f64 / IMU_LSB_PER_G,
                        ys[i] as f64 / IMU_LSB_PER_G,
                        zs[i] as f64 / IMU_LSB_PER_G,
                    ]);
                }
            }
        } else if let Some(ParsedPayload::DataPacket {
            body_summary:
                Some(DataPacketBodySummary::V18History {
                    gravity_x: Some(x),
                    gravity_y: Some(y),
                    gravity_z: Some(z),
                    ..
                }),
            ..
        }) = parsed
        {
            // V18History gravity fields are already in g-units (f32 LE) — no IMU_LSB_PER_G
            // conversion, unlike K10 raw LSB values. This is the WHOOP 5.0 (Gen5 v18) path;
            // without this arm, step count returns zero for all Gen5 devices.
            gravity_samples.push([x as f64, y as f64, z as f64]);
        }
    }

    let input = ImuStepCountInput { gravity_samples };
    let output = imu_step_count_v1(&input);
    serde_json::to_value(output)
        .map_err(|e| GooseError::message(format!("cannot serialize imu_step_count output: {e}")))
}

fn perf_budget_bridge(args: PerfBudgetArgs) -> GooseResult<serde_json::Value> {
    let report = run_perf_budget(PerfBudgetOptions {
        scale: args.scale,
        budgets: PerfBudgets::default(),
    })?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!("cannot serialize perf budget report: {error}"))
    })
}

fn property_suite_bridge(args: PropertySuiteArgs) -> GooseResult<serde_json::Value> {
    let report = run_property_suite(PropertySuiteOptions {
        seed: args.seed,
        cases_per_group: args.cases_per_group,
    })?;
    serde_json::to_value(report).map_err(|error| {
        GooseError::message(format!("cannot serialize property suite report: {error}"))
    })
}

fn chrono_from_unix(ts: f64) -> String {
    // Clamp to epoch; callers should validate before this point, but a negative ts
    // would wrap secs as u64 to ~u64::MAX producing a wildly incorrect date string.
    let ts = ts.max(0.0);
    let secs = ts as i64;
    let nanos = ((ts - secs as f64) * 1_000_000_000.0) as u32;
    let dt = std::time::UNIX_EPOCH + std::time::Duration::new(secs as u64, nanos);
    // Format as ISO-8601 with millisecond precision, matching SQLite stored format
    let elapsed = dt.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let total_secs = elapsed.as_secs();
    let ms = elapsed.subsec_millis();
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    let days_since_epoch = total_secs / 86400;
    // Simple ISO-8601 formatting without chrono dependency
    // epoch = 1970-01-01; compute year/month/day from days_since_epoch
    let (year, month, day) = days_to_ymd(days_since_epoch as u32);
    format!(
        "{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{ms:03}Z",
        h = h % 24
    )
}

/// Return the current UTC time as an ISO-8601 string for use as an upper bound.
#[allow(dead_code)]
fn chrono_now() -> String {
    use std::time::SystemTime;
    let since_epoch = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    chrono_from_unix(since_epoch.as_secs_f64())
}

/// Convert days since Unix epoch (1970-01-01) to (year, month, day).
/// Gregorian calendar implementation without external dependencies.
fn days_to_ymd(days: u32) -> (u32, u32, u32) {
    // Algorithm: Julian Day Number from epoch offset
    let jd = days + 2440588; // Julian Day Number of 1970-01-01 is 2440588
    let l = jd + 68569;
    let n = 4 * l / 146097;
    let l = l - (146097 * n).div_ceil(4);
    let i = 4000 * (l + 1) / 1461001;
    let l = l - 1461 * i / 4 + 31;
    let j = 80 * l / 2447;
    let day = l - 2447 * j / 80;
    let l = j / 11;
    let month = j + 2 - 12 * l;
    let year = 100 * (n - 49) + i + l;
    (year, month, day)
}

/// Parse the codebase's ISO-8601 format "YYYY-MM-DDTHH:MM:SS.mmmZ" to unix seconds (f64).
/// D-03: inverse of chrono_from_unix / days_to_ymd — no chrono dependency.
/// Returns None on any malformed component (T-08.1-02: no panic on bad timestamps).
#[allow(dead_code)]
fn unix_from_iso8601(s: &str) -> Option<f64> {
    // Expected format: "YYYY-MM-DDTHH:MM:SS.mmmZ" (may omit milliseconds or Z suffix)
    // Minimum: "YYYY-MM-DDTHH:MM:SS" (19 chars)
    if s.len() < 19 {
        return None;
    }
    let year: u32 = s[0..4].parse().ok()?;
    let month: u32 = s[5..7].parse().ok()?;
    let day: u32 = s[8..10].parse().ok()?;
    let hour: u32 = s[11..13].parse().ok()?;
    let minute: u32 = s[14..16].parse().ok()?;
    let sec: u32 = s[17..19].parse().ok()?;
    // Milliseconds: optional, after "." if present
    let millis: f64 = if s.len() > 20 && s.as_bytes().get(19) == Some(&b'.') {
        // Collect digits after "."
        let frac: &str = s[20..].trim_end_matches('Z');
        let frac_digits: &str = frac
            .split_once(|c: char| !c.is_ascii_digit())
            .map_or(frac, |(d, _)| d);
        if frac_digits.is_empty() {
            0.0
        } else {
            let raw: f64 = frac_digits.parse().ok()?;
            // Normalise to milliseconds (e.g. "123" → 123 ms, "12" → 12 ms, "1" → 1 ms)
            raw / 10f64.powi(frac_digits.len() as i32 - 3)
        }
    } else {
        0.0
    };

    // Validate calendar ranges
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || sec > 59
    {
        return None;
    }

    // Convert calendar date to days since Unix epoch via inverse Julian-day math
    // (mirror of days_to_ymd, which implements the same Gregorian algorithm)
    let a = (14u32.wrapping_sub(month)) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    // Julian Day Number of 1970-01-01 is 2440588
    let days_since_epoch = jdn.checked_sub(2_440_588)?;

    let secs = days_since_epoch as f64 * 86400.0
        + hour as f64 * 3600.0
        + minute as f64 * 60.0
        + sec as f64
        + millis / 1000.0;
    Some(secs)
}

// ---------------------------------------------------------------------------
// OPT-03: optical_channel_samples bridge methods
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OpticalChannelArg {
    index: u8,
    samples: Vec<i64>,
}

#[derive(Debug, Deserialize)]
struct OpticalPacketArg {
    ts: f64,
    packet_k: u8,
    version: u8,
    channels: Vec<OpticalChannelArg>,
}

#[derive(Debug, Deserialize)]
struct InsertV20V21BatchArgs {
    database_path: String,
    device_id: String,
    packets: Vec<OpticalPacketArg>,
}

#[derive(Debug, Deserialize)]
struct V26PacketArg {
    ts: f64,
    packet_k: u8,
    version: u8,
    ppg: Vec<i64>,
    #[allow(dead_code)]
    num_channels: u8,
}

#[derive(Debug, Deserialize)]
struct InsertV26BatchArgs {
    database_path: String,
    device_id: String,
    packets: Vec<V26PacketArg>,
}

#[derive(Debug, Deserialize)]
struct OpticalBetweenArgs {
    database_path: String,
    device_id: String,
    packet_k: i64,
    start_ts: f64,
    end_ts: f64,
}

fn insert_v20v21_batch_bridge(args: InsertV20V21BatchArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let mut rows = Vec::new();
    for packet in &args.packets {
        for ch in &packet.channels {
            let samples_json = serde_json::to_string(&ch.samples)
                .map_err(|e| GooseError::message(format!("samples serialization error: {e}")))?;
            rows.push(crate::store::OpticalSampleRow {
                device_id: args.device_id.clone(),
                ts: packet.ts,
                packet_k: packet.packet_k as i64,
                version: packet.version as i64,
                channel_index: ch.index as i64,
                samples_json,
            });
        }
    }
    let inserted = store.insert_optical_samples(&rows)?;
    Ok(serde_json::json!({"inserted": inserted}))
}

fn insert_v26_batch_bridge(args: InsertV26BatchArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let mut rows = Vec::new();
    for packet in &args.packets {
        let samples_json = serde_json::to_string(&packet.ppg)
            .map_err(|e| GooseError::message(format!("ppg serialization error: {e}")))?;
        rows.push(crate::store::OpticalSampleRow {
            device_id: args.device_id.clone(),
            ts: packet.ts,
            packet_k: packet.packet_k as i64,
            version: packet.version as i64,
            channel_index: 0,
            samples_json,
        });
    }
    let inserted = store.insert_optical_samples(&rows)?;
    Ok(serde_json::json!({"inserted": inserted}))
}

fn optical_between_bridge(args: OpticalBetweenArgs) -> GooseResult<serde_json::Value> {
    let store = acquire_bridge_conn(&args.database_path)?;
    let rows =
        store.query_optical_between(&args.device_id, args.packet_k, args.start_ts, args.end_ts)?;
    let result = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "ts": r.ts,
                "packet_k": r.packet_k,
                "version": r.version,
                "channel_index": r.channel_index,
                "samples_json": r.samples_json,
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!(result))
}

// ===========================================================================
// BIO-01: metrics.bio_age
// ===========================================================================
//
// Computes a Bio Age from already-resolved fitness markers passed as args. Swift
// resolves each marker's source (WHOOP-first, Apple Health fallback) via
// external.resolve_metric_for_day before calling this, so the engine here is a pure
// compute-plus-persist. Any absent marker is simply omitted and its factor drops out.

#[derive(Debug, Clone, Deserialize)]
struct BioAgeSleepArg {
    avg_duration_minutes: f64,
    efficiency: f64,
    consistency: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct BioAgeActivityArg {
    avg_daily_steps: f64,
    avg_active_kcal: f64,
    weekly_workouts: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct BioAgeArgs {
    database_path: String,
    /// Date this snapshot is for (YYYY-MM-DD); persisted when write_snapshot is true.
    date: String,
    profile_age_years: f64,
    #[serde(default)]
    profile_sex: Option<String>,
    #[serde(default)]
    vo2max: Option<f64>,
    #[serde(default)]
    hrv_value: Option<f64>,
    /// "rmssd" or "sdnn" — selects the norm table. Required if hrv_value is present.
    #[serde(default)]
    hrv_kind: Option<String>,
    #[serde(default)]
    resting_hr_bpm: Option<f64>,
    #[serde(default)]
    sleep: Option<BioAgeSleepArg>,
    #[serde(default)]
    activity: Option<BioAgeActivityArg>,
    #[serde(default)]
    days_of_history: u32,
    /// When true, persist the computed snapshot to bio_age_snapshots (idempotent per date).
    #[serde(default)]
    write_snapshot: bool,
}

fn confidence_str(tier: ConfidenceTier) -> &'static str {
    match tier {
        ConfidenceTier::Calibrating => "calibrating",
        ConfidenceTier::Building => "building",
        ConfidenceTier::Solid => "solid",
    }
}

fn bio_age_bridge(args: BioAgeArgs) -> GooseResult<serde_json::Value> {
    // HRV requires an explicit kind so SDNN is never scored on the RMSSD table
    // (healthkit_rmssd_must_not_be_written_as_sdnn). An unknown kind is a hard error
    // rather than a silent default.
    let hrv = match (args.hrv_value, args.hrv_kind.as_deref()) {
        (Some(value), Some("rmssd")) => Some((value, HrvKind::Rmssd)),
        (Some(value), Some("sdnn")) => Some((value, HrvKind::Sdnn)),
        (Some(_), other) => {
            return Err(GooseError::message(format!(
                "hrv_value requires hrv_kind of 'rmssd' or 'sdnn', got {other:?}"
            )));
        }
        (None, _) => None,
    };

    let inputs = BioAgeInputs {
        chrono_age_years: args.profile_age_years,
        sex: args.profile_sex.as_deref().and_then(Sex::parse),
        vo2max: args.vo2max,
        hrv,
        resting_hr_bpm: args.resting_hr_bpm,
        sleep: args.sleep.map(|s| SleepFactorInput {
            avg_duration_minutes: s.avg_duration_minutes,
            efficiency: s.efficiency,
            consistency: s.consistency,
        }),
        activity: args.activity.map(|a| ActivityFactorInput {
            avg_daily_steps: a.avg_daily_steps,
            avg_active_kcal: a.avg_active_kcal,
            weekly_workouts: a.weekly_workouts,
        }),
        days_of_history: args.days_of_history,
    };

    let result = compute_bio_age(&inputs);

    let factors_json = serde_json::to_string(
        &result
            .factors
            .iter()
            .map(|f| {
                serde_json::json!({
                    "name": f.name,
                    "value": f.value,
                    "percentile": f.percentile,
                    "offset_years": f.offset_years,
                    "weight": f.weight,
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|e| GooseError::message(e.to_string()))?;

    if args.write_snapshot {
        let store = acquire_bridge_conn(&args.database_path)?;
        store.upsert_bio_age_snapshot(
            &args.date,
            result.chrono_age_years,
            result.bio_age_years,
            result.delta_years,
            confidence_str(result.confidence),
            &factors_json,
        )?;
    }

    Ok(serde_json::json!({
        "date": args.date,
        "chrono_age_years": result.chrono_age_years,
        "bio_age_years": result.bio_age_years,
        "delta_years": result.delta_years,
        "confidence": confidence_str(result.confidence),
        "factors": serde_json::from_str::<serde_json::Value>(&factors_json)
            .unwrap_or(serde_json::Value::Array(vec![])),
    }))
}
