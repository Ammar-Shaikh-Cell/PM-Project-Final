# Baseline Learning & AI Integration Analysis

## ✅ What's Correctly Implemented

### 1. Profile Creation ✅
- **Status**: ✅ **CORRECTLY IMPLEMENTED**
- Profiles are created for `(machine_id, material_id)` combinations
- Supports machine-specific profiles and material-default profiles
- Automatically starts baseline learning when profile is created
- Proper validation and uniqueness checks

### 2. Baseline Learning Process ✅
- **Status**: ✅ **CORRECTLY IMPLEMENTED**
- Samples collected **only during PRODUCTION state** (correct)
- Minimum 100 samples per metric required (good threshold)
- Statistics computed correctly:
  - `baseline_mean`: Average value
  - `baseline_std`: Standard deviation
  - `p05`, `p95`: Percentiles for bounds
- Samples stored in `ProfileBaselineSample` during learning
- Statistics stored in `ProfileBaselineStats` after finalization
- Proper lifecycle: `baseline_learning` → `baseline_ready`

### 3. Baseline Usage in Dashboard ✅
- **Status**: ✅ **CORRECTLY IMPLEMENTED**
- Baseline stats used for:
  - **Rule-based severity**: Comparing current value to `baseline_mean`
  - **Stability analysis**: Comparing `current_std` to `baseline_std`
  - **Traffic light evaluation**: GREEN/ORANGE/RED based on deviation
- Decision hierarchy correctly applied:
  1. Machine state gate (PRODUCTION only)
  2. Material rule-based thresholds (3-5% deviation rule)
  3. Stability indicators (std ratio)
  4. ML signal (informational only)

## ⚠️ What's Missing / Needs Improvement

### 1. AI Service Not Using Baseline Data ❌
- **Status**: ❌ **GAP IDENTIFIED**
- **Current Behavior**:
  - AI service uses **Isolation Forest** (unsupervised learning)
  - Trains models **per sensor_id** using buffered data
  - Does **NOT receive** profile or baseline information
  - Does **NOT use** `baseline_mean`, `baseline_std`, or material context
  
- **Impact**:
  - AI predictions are **generic** (not material-specific)
  - AI doesn't know what "normal" means for this material
  - AI can't leverage learned baseline statistics
  - Predictions may be less accurate for material-specific anomalies

- **What Should Happen**:
  - AI service should receive baseline stats in prediction requests
  - AI should use baseline as reference for anomaly detection
  - AI should adapt to material-specific normal ranges
  - AI should combine baseline knowledge with pattern detection

### 2. Profile Information Not Sent to AI Service ❌
- **Status**: ❌ **GAP IDENTIFIED**
- **Current Behavior**:
  - Backend calls AI service with only sensor readings
  - No profile_id, material_id, or baseline stats sent
  - AI service has no context about machine/material
  
- **What Should Happen**:
  - Backend should include profile_id in AI request
  - Backend should include baseline stats (mean, std) in request
  - AI service should use this context for better predictions

## 📊 Current Architecture Flow

```
1. Profile Creation
   ✅ Machine + Material → Profile created
   ✅ baseline_learning = true

2. Baseline Learning
   ✅ PRODUCTION state → Samples collected
   ✅ 100+ samples → Statistics computed
   ✅ baseline_ready = true

3. Dashboard Evaluation
   ✅ Uses ProfileBaselineStats for:
      - Rule-based severity (value vs baseline_mean)
      - Stability analysis (current_std vs baseline_std)
      - Traffic light colors

4. AI Prediction
   ❌ AI service receives: {sensor_id, readings}
   ❌ AI service does NOT receive: {profile_id, baseline_stats, material_id}
   ❌ AI service trains generic models (not material-aware)
```

## 🎯 Recommended Improvements

### Option 1: Enhance AI Service to Use Baseline (Recommended)

**Modify AI Service to Accept Baseline Context:**

```python
# In ai_service/main.py
class PredictPayload(BaseModel):
    sensor_id: str
    machine_id: Optional[str] = None
    profile_id: Optional[str] = None  # NEW
    material_id: Optional[str] = None  # NEW
    readings: Dict[str, float]
    baseline_stats: Optional[Dict[str, Dict[str, float]]] = None  # NEW
    # Format: {"ScrewSpeed_rpm": {"mean": 10.5, "std": 0.2}, ...}
```

**Modify Prediction Logic:**

```python
def predict(self, payload: PredictPayload) -> PredictResponse:
    # Use baseline stats if available
    if payload.baseline_stats:
        # Calculate z-scores for each metric
        z_scores = {}
        for metric, value in payload.readings.items():
            if metric in payload.baseline_stats:
                stats = payload.baseline_stats[metric]
                mean = stats.get("mean")
                std = stats.get("std")
                if mean is not None and std is not None and std > 0:
                    z_scores[metric] = abs((value - mean) / std)
        
        # Combine z-scores with Isolation Forest score
        baseline_anomaly_score = max(z_scores.values()) if z_scores else 0.0
        # Blend with ML score
        final_score = max(model_score, baseline_anomaly_score * 0.5)
    else:
        # Fallback to current behavior
        final_score = model_score
```

**Modify Backend to Send Baseline:**

```python
# In backend/app/services/mssql_extruder_poller.py
async def _score_with_ai_service(self, *, ts: datetime, readings: Dict[str, float]) -> Dict[str, Any]:
    # Get profile and baseline stats
    profile = await baseline_learning_service.get_active_profile(...)
    baseline_stats = {}
    if profile and profile.baseline_ready:
        # Load baseline stats
        stats_result = await session.execute(
            select(ProfileBaselineStats)
            .where(ProfileBaselineStats.profile_id == profile.id)
        )
        for stat in stats_result.scalars().all():
            baseline_stats[stat.metric_name] = {
                "mean": stat.baseline_mean,
                "std": stat.baseline_std,
                "p05": stat.p05,
                "p95": stat.p95,
            }
    
    # Send to AI service with baseline context
    payload = PredictPayload(
        sensor_id=str(self._sensor_id),
        machine_id=str(self._machine_id),
        profile_id=str(profile.id) if profile else None,
        material_id=profile.material_id if profile else None,
        readings=readings,
        baseline_stats=baseline_stats if baseline_stats else None,
    )
```

### Option 2: Hybrid Approach (Current + Baseline)

**Keep current AI service as-is, but enhance with baseline-aware post-processing:**

```python
# In backend/app/api/routers/dashboard.py
def enhance_ai_prediction_with_baseline(
    ai_result: Dict[str, Any],
    baseline_stats: Dict[str, ProfileBaselineStats],
    readings: Dict[str, float],
) -> Dict[str, Any]:
    """Enhance AI prediction using baseline context"""
    
    # Calculate baseline-based anomaly scores
    baseline_scores = {}
    for metric, value in readings.items():
        if metric in baseline_stats:
            stats = baseline_stats[metric]
            if stats.baseline_mean and stats.baseline_std:
                z_score = abs((value - stats.baseline_mean) / stats.baseline_std)
                baseline_scores[metric] = min(1.0, z_score / 3.0)  # Normalize
    
    # Blend AI score with baseline score
    baseline_score = max(baseline_scores.values()) if baseline_scores else 0.0
    ai_score = ai_result.get("score", 0.0)
    blended_score = max(ai_score, baseline_score * 0.7)
    
    return {
        **ai_result,
        "score": blended_score,
        "baseline_contribution": baseline_score,
        "contributing_features": {
            **ai_result.get("contributing_features", {}),
            "baseline_z_scores": baseline_scores,
        }
    }
```

## ✅ Summary

### What Works Well:
1. ✅ Profile creation and management
2. ✅ Baseline learning process (sample collection, statistics)
3. ✅ Baseline usage in dashboard evaluation
4. ✅ Material-specific rule-based thresholds

### What Needs Improvement:
1. ❌ AI service doesn't use baseline data
2. ❌ AI service doesn't receive profile/material context
3. ❌ AI predictions are generic (not material-aware)

### Recommendation:
**Implement Option 1** to make AI service baseline-aware. This will:
- Improve prediction accuracy for material-specific anomalies
- Leverage learned baseline statistics
- Make AI predictions context-aware (machine + material)
- Combine pattern detection (Isolation Forest) with statistical baselines

### Priority:
- **High**: AI service should use baseline data for better accuracy
- **Medium**: Profile/material context should be sent to AI
- **Low**: Current implementation works but could be more accurate
