//! Bio Age engine (BIO-01 / BIO-02 / BIO-03).
//!
//! Computes a "biological age" as chronological age adjusted by fitness markers,
//! Bevel/WHOOP-style, with a per-factor breakdown and honest confidence states.
//!
//! ## Model
//!
//! For each present factor the user's value is mapped to a population percentile
//! `p ∈ [0,1]` (higher = healthier/"younger") within their age×sex norm, then to a
//! bounded year offset:
//!
//! ```text
//!   offset_years = -(p - 0.5) × 2 × MAX_FACTOR_OFFSET_YEARS
//! ```
//!
//! so a median value (p=0.5) contributes 0, a top value (p→1) subtracts up to
//! `MAX_FACTOR_OFFSET_YEARS`, and a bottom value (p→0) adds up to the same. The
//! factor offsets are combined by weight (renormalised over the factors actually
//! present), and the result is clamped:
//!
//! ```text
//!   BioAge = clamp(chrono_age + Σ wᵢ·offsetᵢ, chrono_age ± MAX_TOTAL_OFFSET_YEARS)
//! ```
//!
//! ## Honesty guarantees
//!
//! - **Missing factors degrade gracefully.** An absent factor (no VO2max, unknown
//!   sex where the factor is sex-specific, etc.) is dropped and the remaining
//!   weights are renormalised — never substituted with a fabricated value.
//! - **SDNN ≠ RMSSD.** HRV is scored against a *different* norm table depending on
//!   which kind the value is (`HrvKind`). Apple Health supplies SDNN; WHOOP/Goose
//!   compute RMSSD. They are never cross-scored
//!   (guard: `healthkit_rmssd_must_not_be_written_as_sdnn`).
//! - **Confidence is explicit.** Few factors or little history yields a
//!   `Calibrating` / `Building` tier rather than a falsely precise number.
//!
//! ## Norm anchors — sources
//!
//! Population anchors are approximate resting/consumer values drawn from published
//! normative data; they are documented per table below. This is a fitness-trend
//! lens, not a clinical measure. Anchors are intentionally coarse (per-decade p50
//! with a fixed relative spread) so the model stays explainable and easy to revise.

/// Maximum year offset a single factor may contribute before weighting.
pub const MAX_FACTOR_OFFSET_YEARS: f64 = 8.0;

/// Maximum total deviation of BioAge from chronological age (sanity clamp).
pub const MAX_TOTAL_OFFSET_YEARS: f64 = 20.0;

/// Percentile clamp so extreme values never map to exactly 0/1 (avoids maxed offsets).
const PERCENTILE_FLOOR: f64 = 0.02;
const PERCENTILE_CEIL: f64 = 0.98;

// Factor weights (sum to 1.0 when all present; renormalised over present factors).
pub const WEIGHT_VO2MAX: f64 = 0.30;
pub const WEIGHT_HRV: f64 = 0.25;
pub const WEIGHT_RHR: f64 = 0.20;
pub const WEIGHT_SLEEP: f64 = 0.15;
pub const WEIGHT_ACTIVITY: f64 = 0.10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sex {
    Male,
    Female,
}

impl Sex {
    /// Parse a profile sex string. Returns None for unknown/empty so the caller
    /// degrades sex-specific factors gracefully rather than guessing.
    pub fn parse(raw: &str) -> Option<Sex> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "male" | "m" | "man" => Some(Sex::Male),
            "female" | "f" | "woman" => Some(Sex::Female),
            _ => None,
        }
    }
}

/// Which HRV metric a value represents. Determines the norm table used; the two
/// are never interchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HrvKind {
    /// Root mean square of successive differences (WHOOP / Goose native).
    Rmssd,
    /// Standard deviation of NN intervals (Apple Health native).
    Sdnn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidenceTier {
    Calibrating,
    Building,
    Solid,
}

/// Inputs to a bio-age computation. Every fitness marker is optional; absent ones
/// are dropped from the weighted composite.
#[derive(Debug, Clone)]
pub struct BioAgeInputs {
    pub chrono_age_years: f64,
    pub sex: Option<Sex>,
    pub vo2max: Option<f64>,
    pub hrv: Option<(f64, HrvKind)>,
    pub resting_hr_bpm: Option<f64>,
    /// Composite sleep inputs (all three needed for the sleep factor to count).
    pub sleep: Option<SleepFactorInput>,
    /// Composite activity inputs (all needed for the activity factor to count).
    pub activity: Option<ActivityFactorInput>,
    /// Days of history backing these inputs; drives the confidence tier.
    pub days_of_history: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct SleepFactorInput {
    pub avg_duration_minutes: f64,
    /// Sleep efficiency in [0,1] (asleep / time-in-bed).
    pub efficiency: f64,
    /// Bedtime consistency in [0,1] (1 = perfectly regular). Caller-computed.
    pub consistency: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct ActivityFactorInput {
    pub avg_daily_steps: f64,
    pub avg_active_kcal: f64,
    pub weekly_workouts: f64,
}

/// One factor's contribution to the bio-age result.
#[derive(Debug, Clone, PartialEq)]
pub struct FactorBreakdown {
    pub name: String,
    pub value: f64,
    /// Percentile in [0,1], higher = healthier/younger.
    pub percentile: f64,
    /// Year offset before weighting (negative = younger).
    pub offset_years: f64,
    pub weight: f64,
}

#[derive(Debug, Clone)]
pub struct BioAgeResult {
    pub chrono_age_years: f64,
    pub bio_age_years: f64,
    /// bio_age - chrono_age (negative = biologically younger).
    pub delta_years: f64,
    pub confidence: ConfidenceTier,
    pub factors: Vec<FactorBreakdown>,
}

// ===========================================================================
// Norm tables
// ===========================================================================
//
// Each age-dependent table gives the population median (p50) at decade anchors.
// p10/p90 are derived from p50 with a fixed relative spread documented per table.
// Ages outside [20,70] clamp to the nearest anchor.

/// (age_anchor, p50) for VO2max (ml·kg⁻¹·min⁻¹), male.
/// Source: ACSM's Guidelines for Exercise Testing and Prescription; Cooper Institute
/// normative data — "fair-to-good" midpoints by age band.
const VO2MAX_P50_MALE: &[(f64, f64)] = &[
    (20.0, 45.0),
    (30.0, 43.0),
    (40.0, 40.0),
    (50.0, 36.0),
    (60.0, 32.0),
    (70.0, 28.0),
];

/// (age_anchor, p50) for VO2max, female. Same source as male.
const VO2MAX_P50_FEMALE: &[(f64, f64)] = &[
    (20.0, 38.0),
    (30.0, 36.0),
    (40.0, 33.0),
    (50.0, 30.0),
    (60.0, 27.0),
    (70.0, 24.0),
];

/// VO2max spread: p90 ≈ p50·1.25, p10 ≈ p50·0.75 (population CV ≈ 15–20%).
const VO2MAX_SPREAD_HI: f64 = 1.25;
const VO2MAX_SPREAD_LO: f64 = 0.75;

/// (age_anchor, p50) for RMSSD (ms). Source: Nunan D, Sandercock GRH, Brodie DA.
/// "A quantitative systematic review of normal values for short-term HRV in healthy
/// adults." Pacing Clin Electrophysiol 2010 (~42 ms baseline), with age decline per
/// Umetani et al. JACC 1998. Sex-independent (no consistent sex split applied).
const RMSSD_P50: &[(f64, f64)] = &[
    (20.0, 48.0),
    (30.0, 42.0),
    (40.0, 35.0),
    (50.0, 28.0),
    (60.0, 23.0),
    (70.0, 19.0),
];

/// (age_anchor, p50) for SDNN (ms), short-term. Source: Nunan 2010 (~50 ms baseline)
/// + Umetani 1998 age decline. Distinct from RMSSD; never interchanged.
const SDNN_P50: &[(f64, f64)] = &[
    (20.0, 55.0),
    (30.0, 48.0),
    (40.0, 42.0),
    (50.0, 36.0),
    (60.0, 31.0),
    (70.0, 27.0),
];

/// HRV spread (right-skewed, high CV): p90 ≈ p50·1.5, p10 ≈ p50·0.6.
const HRV_SPREAD_HI: f64 = 1.5;
const HRV_SPREAD_LO: f64 = 0.6;

/// Resting heart rate (bpm) anchors — lower is healthier. Weakly age-dependent, so a
/// single population triple is used. Source: Nanchen D. "Resting heart rate: what is
/// normal?" Heart 2018; population resting HR ≈ 60–80 bpm.
const RHR_P50: f64 = 66.0;
const RHR_BEST: f64 = 52.0; // maps to high percentile (younger)
const RHR_WORST: f64 = 84.0; // maps to low percentile (older)

/// Map an age to the interpolated p50 for a decade-anchored table.
fn interpolate_p50(table: &[(f64, f64)], age: f64) -> f64 {
    let first = table[0];
    let last = table[table.len() - 1];
    if age <= first.0 {
        return first.1;
    }
    if age >= last.0 {
        return last.1;
    }
    for window in table.windows(2) {
        let (a0, v0) = window[0];
        let (a1, v1) = window[1];
        if age >= a0 && age <= a1 {
            let t = (age - a0) / (a1 - a0);
            return v0 + t * (v1 - v0);
        }
    }
    last.1
}

/// Map a value to a percentile in [PERCENTILE_FLOOR, PERCENTILE_CEIL] given
/// (p10_value, p50_value, p90_value) anchors, where a *higher value* is healthier.
/// Piecewise-linear across the three anchors, clamped at the ends.
fn percentile_higher_is_better(value: f64, p10: f64, p50: f64, p90: f64) -> f64 {
    let raw = if value <= p10 {
        0.10 * (value / p10).clamp(0.0, 1.0)
    } else if value <= p50 {
        0.10 + 0.40 * (value - p10) / (p50 - p10)
    } else if value <= p90 {
        0.50 + 0.40 * (value - p50) / (p90 - p50)
    } else {
        0.90 + 0.09 * ((value - p90) / p90).clamp(0.0, 1.0)
    };
    raw.clamp(PERCENTILE_FLOOR, PERCENTILE_CEIL)
}

/// Percentile where a *lower value* is healthier (e.g. resting HR). `best` is the
/// low, healthy end (→ high percentile); `worst` is the high, unhealthy end.
fn percentile_lower_is_better(value: f64, best: f64, median: f64, worst: f64) -> f64 {
    // Reflect into the higher-is-better mapping by negating around the median.
    let reflected = 2.0 * median - value;
    let p10 = 2.0 * median - worst;
    let p90 = 2.0 * median - best;
    percentile_higher_is_better(reflected, p10, median, p90)
}

/// Convert a percentile (higher = younger) to a bounded year offset (negative = younger).
fn percentile_to_offset(percentile: f64) -> f64 {
    -(percentile - 0.5) * 2.0 * MAX_FACTOR_OFFSET_YEARS
}

// ===========================================================================
// Per-factor percentiles
// ===========================================================================

fn vo2max_percentile(value: f64, age: f64, sex: Sex) -> f64 {
    let table = match sex {
        Sex::Male => VO2MAX_P50_MALE,
        Sex::Female => VO2MAX_P50_FEMALE,
    };
    let p50 = interpolate_p50(table, age);
    percentile_higher_is_better(value, p50 * VO2MAX_SPREAD_LO, p50, p50 * VO2MAX_SPREAD_HI)
}

fn hrv_percentile(value: f64, kind: HrvKind, age: f64) -> f64 {
    let table = match kind {
        HrvKind::Rmssd => RMSSD_P50,
        HrvKind::Sdnn => SDNN_P50,
    };
    let p50 = interpolate_p50(table, age);
    percentile_higher_is_better(value, p50 * HRV_SPREAD_LO, p50, p50 * HRV_SPREAD_HI)
}

fn rhr_percentile(value: f64) -> f64 {
    percentile_lower_is_better(value, RHR_BEST, RHR_P50, RHR_WORST)
}

/// Sleep composite → percentile. Blends duration-vs-need, efficiency, and consistency.
/// Duration need per National Sleep Foundation (Hirshkowitz 2015): 7–9 h adults.
fn sleep_percentile(input: SleepFactorInput, age: f64) -> f64 {
    // Duration score: 1.0 at ≥ need, tapering below. Need is 8h (480m), 7.5h (450m) at 65+.
    let need_minutes = if age >= 65.0 { 450.0 } else { 480.0 };
    let duration_score = (input.avg_duration_minutes / need_minutes)
        .clamp(0.0, 1.1)
        .min(1.0);
    // Efficiency score: ≥ 0.85 is good; scale 0.65..0.95 → 0..1.
    let efficiency_score = ((input.efficiency - 0.65) / 0.30).clamp(0.0, 1.0);
    let consistency_score = input.consistency.clamp(0.0, 1.0);
    let blended = 0.5 * duration_score + 0.3 * efficiency_score + 0.2 * consistency_score;
    blended.clamp(PERCENTILE_FLOOR, PERCENTILE_CEIL)
}

/// Activity composite → percentile. Steps (Saint-Maurice JAMA 2020, ~8k+ associated
/// with lower mortality), active kcal, and weekly workouts (WHO 2020: 150 min MVPA ≈
/// 3–5 sessions).
fn activity_percentile(input: ActivityFactorInput) -> f64 {
    let steps_score = (input.avg_daily_steps / 10_000.0).clamp(0.0, 1.0);
    let kcal_score = (input.avg_active_kcal / 500.0).clamp(0.0, 1.0);
    let workout_score = (input.weekly_workouts / 5.0).clamp(0.0, 1.0);
    let blended = 0.5 * steps_score + 0.25 * kcal_score + 0.25 * workout_score;
    blended.clamp(PERCENTILE_FLOOR, PERCENTILE_CEIL)
}

fn confidence_from(factor_count: usize, days_of_history: u32) -> ConfidenceTier {
    // Tier is the more conservative of factor-coverage and history-depth.
    let by_factors = match factor_count {
        0 | 1 => ConfidenceTier::Calibrating,
        2 | 3 => ConfidenceTier::Building,
        _ => ConfidenceTier::Solid,
    };
    let by_days = if days_of_history < 7 {
        ConfidenceTier::Calibrating
    } else if days_of_history < 30 {
        ConfidenceTier::Building
    } else {
        ConfidenceTier::Solid
    };
    by_factors.min(by_days)
}

// Order Calibrating < Building < Solid so `.min()` picks the more conservative tier.
impl ConfidenceTier {
    fn rank(self) -> u8 {
        match self {
            ConfidenceTier::Calibrating => 0,
            ConfidenceTier::Building => 1,
            ConfidenceTier::Solid => 2,
        }
    }
}
impl PartialOrd for ConfidenceTier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ConfidenceTier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank().cmp(&other.rank())
    }
}

/// Compute the bio-age result from the given inputs. Pure — no I/O.
pub fn compute_bio_age(inputs: &BioAgeInputs) -> BioAgeResult {
    let age = inputs.chrono_age_years;
    let mut factors: Vec<FactorBreakdown> = Vec::new();

    // VO2max — requires sex (norms are sex-specific).
    if let (Some(vo2), Some(sex)) = (inputs.vo2max, inputs.sex) {
        let p = vo2max_percentile(vo2, age, sex);
        factors.push(FactorBreakdown {
            name: "vo2max".to_string(),
            value: vo2,
            percentile: p,
            offset_years: percentile_to_offset(p),
            weight: WEIGHT_VO2MAX,
        });
    }

    // HRV — norm depends on kind (SDNN vs RMSSD), sex-independent.
    if let Some((hrv_value, kind)) = inputs.hrv {
        let p = hrv_percentile(hrv_value, kind, age);
        factors.push(FactorBreakdown {
            name: match kind {
                HrvKind::Rmssd => "hrv_rmssd".to_string(),
                HrvKind::Sdnn => "hrv_sdnn".to_string(),
            },
            value: hrv_value,
            percentile: p,
            offset_years: percentile_to_offset(p),
            weight: WEIGHT_HRV,
        });
    }

    if let Some(rhr) = inputs.resting_hr_bpm {
        let p = rhr_percentile(rhr);
        factors.push(FactorBreakdown {
            name: "resting_hr".to_string(),
            value: rhr,
            percentile: p,
            offset_years: percentile_to_offset(p),
            weight: WEIGHT_RHR,
        });
    }

    if let Some(sleep) = inputs.sleep {
        let p = sleep_percentile(sleep, age);
        factors.push(FactorBreakdown {
            name: "sleep".to_string(),
            value: sleep.avg_duration_minutes,
            percentile: p,
            offset_years: percentile_to_offset(p),
            weight: WEIGHT_SLEEP,
        });
    }

    if let Some(activity) = inputs.activity {
        let p = activity_percentile(activity);
        factors.push(FactorBreakdown {
            name: "activity".to_string(),
            value: activity.avg_daily_steps,
            percentile: p,
            offset_years: percentile_to_offset(p),
            weight: WEIGHT_ACTIVITY,
        });
    }

    let confidence = confidence_from(factors.len(), inputs.days_of_history);

    // Renormalise weights over present factors; if none, bio age = chrono age.
    let total_weight: f64 = factors.iter().map(|f| f.weight).sum();
    let bio_age = if total_weight <= 0.0 {
        age
    } else {
        let weighted_offset: f64 = factors
            .iter()
            .map(|f| (f.weight / total_weight) * f.offset_years)
            .sum();
        (age + weighted_offset).clamp(age - MAX_TOTAL_OFFSET_YEARS, age + MAX_TOTAL_OFFSET_YEARS)
    };

    BioAgeResult {
        chrono_age_years: age,
        bio_age_years: bio_age,
        delta_years: bio_age - age,
        confidence,
        factors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_inputs(age: f64) -> BioAgeInputs {
        BioAgeInputs {
            chrono_age_years: age,
            sex: Some(Sex::Male),
            vo2max: None,
            hrv: None,
            resting_hr_bpm: None,
            sleep: None,
            activity: None,
            days_of_history: 60,
        }
    }

    #[test]
    fn median_values_yield_near_zero_offset() {
        // A 40yo male with exactly-median VO2max should sit ~ at chronological age.
        let mut inputs = base_inputs(40.0);
        inputs.vo2max = Some(40.0); // p50 for 40yo male
        let result = compute_bio_age(&inputs);
        assert!(
            result.delta_years.abs() < 1.0,
            "median VO2max should give near-zero delta, got {}",
            result.delta_years
        );
    }

    #[test]
    fn strong_vo2max_makes_younger() {
        let mut inputs = base_inputs(40.0);
        inputs.vo2max = Some(52.0); // well above p50 (40) for 40yo male
        let result = compute_bio_age(&inputs);
        assert!(
            result.delta_years < 0.0,
            "high VO2max must lower bio age, got delta {}",
            result.delta_years
        );
    }

    #[test]
    fn weak_vo2max_makes_older() {
        let mut inputs = base_inputs(40.0);
        inputs.vo2max = Some(28.0); // well below p50 for 40yo male
        let result = compute_bio_age(&inputs);
        assert!(
            result.delta_years > 0.0,
            "low VO2max must raise bio age, got delta {}",
            result.delta_years
        );
    }

    #[test]
    fn sdnn_and_rmssd_use_different_norm_tables() {
        // The same numeric HRV value must score differently as SDNN vs RMSSD, because
        // the two norm tables differ. Guard against conflation (SDNN≠RMSSD).
        // At age 40: RMSSD p50 = 35, SDNN p50 = 42. So value 35 is median-for-RMSSD
        // but below-median-for-SDNN.
        let age = 40.0;
        let value = 35.0;
        let p_rmssd = hrv_percentile(value, HrvKind::Rmssd, age);
        let p_sdnn = hrv_percentile(value, HrvKind::Sdnn, age);
        assert!(
            (p_rmssd - p_sdnn).abs() > 1e-6,
            "SDNN and RMSSD must not be scored identically: rmssd={p_rmssd}, sdnn={p_sdnn}"
        );
        assert!(
            (p_rmssd - 0.5).abs() < 0.05,
            "35 ms is the RMSSD median at age 40 → ~0.5, got {p_rmssd}"
        );
        assert!(
            p_sdnn < 0.5,
            "35 ms is below the SDNN median (42) at age 40 → <0.5, got {p_sdnn}"
        );
    }

    #[test]
    fn lower_resting_hr_is_younger() {
        let mut low = base_inputs(40.0);
        low.resting_hr_bpm = Some(50.0);
        let mut high = base_inputs(40.0);
        high.resting_hr_bpm = Some(82.0);
        let low_res = compute_bio_age(&low);
        let high_res = compute_bio_age(&high);
        assert!(
            low_res.delta_years < high_res.delta_years,
            "lower RHR must yield younger bio age: low={}, high={}",
            low_res.delta_years,
            high_res.delta_years
        );
    }

    #[test]
    fn missing_vo2max_drops_factor_and_renormalises() {
        // Two factors present, no VO2max. Weights (HRV .25 + RHR .20) must renormalise
        // to sum 1 so the result is a proper weighted blend, not diluted by absent VO2max.
        let mut inputs = base_inputs(40.0);
        inputs.hrv = Some((60.0, HrvKind::Rmssd)); // above median → younger
        inputs.resting_hr_bpm = Some(50.0); // low → younger
        let result = compute_bio_age(&inputs);
        assert_eq!(
            result.factors.len(),
            2,
            "only two factors should be present"
        );
        assert!(
            result.factors.iter().all(|f| f.name != "vo2max"),
            "vo2max must be absent"
        );
        assert!(
            result.delta_years < 0.0,
            "two favourable factors must make younger even without VO2max"
        );
    }

    #[test]
    fn no_factors_yields_chronological_age_and_calibrating() {
        let inputs = base_inputs(40.0);
        let result = compute_bio_age(&inputs);
        assert_eq!(
            result.bio_age_years, 40.0,
            "no factors → bio age = chrono age"
        );
        assert_eq!(result.confidence, ConfidenceTier::Calibrating);
    }

    #[test]
    fn vo2max_without_sex_is_dropped() {
        let mut inputs = base_inputs(40.0);
        inputs.sex = None;
        inputs.vo2max = Some(52.0);
        let result = compute_bio_age(&inputs);
        assert!(
            result.factors.iter().all(|f| f.name != "vo2max"),
            "VO2max must drop when sex is unknown (norms are sex-specific)"
        );
    }

    #[test]
    fn result_is_clamped_to_max_total_offset() {
        // Pile on maximally unfavourable factors; delta must not exceed the clamp.
        let mut inputs = base_inputs(40.0);
        inputs.vo2max = Some(10.0);
        inputs.hrv = Some((5.0, HrvKind::Rmssd));
        inputs.resting_hr_bpm = Some(120.0);
        inputs.sleep = Some(SleepFactorInput {
            avg_duration_minutes: 200.0,
            efficiency: 0.5,
            consistency: 0.0,
        });
        inputs.activity = Some(ActivityFactorInput {
            avg_daily_steps: 200.0,
            avg_active_kcal: 10.0,
            weekly_workouts: 0.0,
        });
        let result = compute_bio_age(&inputs);
        assert!(
            result.delta_years <= MAX_TOTAL_OFFSET_YEARS + 1e-9,
            "delta {} must be clamped to {}",
            result.delta_years,
            MAX_TOTAL_OFFSET_YEARS
        );
        assert!(result.bio_age_years <= 60.0 + 1e-9);
    }

    #[test]
    fn confidence_tiers_track_factor_count_and_history() {
        // Few factors → Calibrating even with long history.
        let mut one = base_inputs(40.0);
        one.days_of_history = 90;
        one.vo2max = Some(40.0);
        assert_eq!(
            compute_bio_age(&one).confidence,
            ConfidenceTier::Calibrating
        );

        // Enough factors but short history → capped at Calibrating by days.
        let mut short = base_inputs(40.0);
        short.days_of_history = 3;
        short.vo2max = Some(40.0);
        short.hrv = Some((42.0, HrvKind::Rmssd));
        short.resting_hr_bpm = Some(66.0);
        short.sleep = Some(SleepFactorInput {
            avg_duration_minutes: 470.0,
            efficiency: 0.9,
            consistency: 0.8,
        });
        assert_eq!(
            compute_bio_age(&short).confidence,
            ConfidenceTier::Calibrating,
            "short history must cap confidence regardless of factor count"
        );

        // Four+ factors and long history → Solid.
        let mut solid = base_inputs(40.0);
        solid.days_of_history = 60;
        solid.vo2max = Some(40.0);
        solid.hrv = Some((42.0, HrvKind::Rmssd));
        solid.resting_hr_bpm = Some(66.0);
        solid.sleep = Some(SleepFactorInput {
            avg_duration_minutes: 470.0,
            efficiency: 0.9,
            consistency: 0.8,
        });
        assert_eq!(compute_bio_age(&solid).confidence, ConfidenceTier::Solid);
    }

    #[test]
    fn factor_offsets_are_individually_bounded() {
        let mut inputs = base_inputs(40.0);
        inputs.vo2max = Some(100.0); // absurdly high
        let result = compute_bio_age(&inputs);
        let vo2 = result.factors.iter().find(|f| f.name == "vo2max").unwrap();
        assert!(
            vo2.offset_years.abs() <= MAX_FACTOR_OFFSET_YEARS + 1e-9,
            "single factor offset {} must be bounded to {}",
            vo2.offset_years,
            MAX_FACTOR_OFFSET_YEARS
        );
    }

    #[test]
    fn sex_parse_handles_unknowns() {
        assert_eq!(Sex::parse("male"), Some(Sex::Male));
        assert_eq!(Sex::parse("Female"), Some(Sex::Female));
        assert_eq!(Sex::parse("M"), Some(Sex::Male));
        assert_eq!(Sex::parse(""), None);
        assert_eq!(Sex::parse("other"), None);
    }
}
