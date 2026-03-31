# Standardized Baseline Structure Implementation

## Overview

The standardized baseline structure has been fully implemented to provide a consistent format for baseline data across all API endpoints. This ensures that the frontend receives baseline information in a standardized format with all required fields.

## Implementation Details

### 1. Utility Functions (`backend/app/utils/baseline_formatter.py`)

Created two helper functions to build standardized baseline structures:

#### `build_standardized_baseline()`
- **Purpose**: Builds standardized baseline from `ProfileBaselineStats` database objects
- **Input**: `ProfileBaselineStats` object, `Profile` object, optional fallback values
- **Output**: Standardized baseline dictionary

#### `build_standardized_baseline_from_dict()`
- **Purpose**: Builds standardized baseline from dictionary data (for rolling baselines)
- **Input**: Metric name, baseline dictionary, material ID, confidence
- **Output**: Standardized baseline dictionary

### 2. Standardized Baseline Structure

Each baseline object now includes all required fields:

```json
{
  "sensor_name": "Pressure_bar",
  "baseline_mean": 370.0,
  "baseline_min": 352.0,
  "baseline_max": 389.0,
  "baseline_material": "PP-H",
  "baseline_confidence": 0.92
}
```

#### Field Derivation Logic

1. **sensor_name**: From `ProfileBaselineStats.metric_name` or provided metric name
2. **baseline_mean**: From `ProfileBaselineStats.baseline_mean` or fallback
3. **baseline_min**: 
   - Priority 1: `ProfileBaselineStats.p05` (5th percentile)
   - Priority 2: Fallback min value
   - Priority 3: `baseline_mean - baseline_std` (mean - standard deviation)
   - Priority 4: `baseline_mean * 0.95` (mean - 5%)
4. **baseline_max**:
   - Priority 1: `ProfileBaselineStats.p95` (95th percentile)
   - Priority 2: Fallback max value
   - Priority 3: `baseline_mean + baseline_std` (mean + standard deviation)
   - Priority 4: `baseline_mean * 1.05` (mean + 5%)
5. **baseline_material**: From `Profile.material_id`
6. **baseline_confidence**: 
   - Based on sample count:
     - 100+ samples: 1.0
     - 50-99 samples: 0.9
     - 25-49 samples: 0.8
     - 10-24 samples: 0.7
     - <10 samples: 0.6
   - Default: 1.0 if not available

### 3. Updated API Endpoints

#### `/dashboard/current`
- **Location**: `backend/app/api/routers/dashboard.py`
- **Changes**: 
  - Added `baseline` field to each metric in `metrics_response`
  - Each metric now includes the standardized baseline structure
  - Example response structure:
    ```json
    {
      "metrics": {
        "Pressure_bar": {
          "current_value": 375.0,
          "baseline_mean": 370.0,
          "green_band": {...},
          "deviation": 5.0,
          "severity": 0,
          "baseline": {
            "sensor_name": "Pressure_bar",
            "baseline_mean": 370.0,
            "baseline_min": 352.0,
            "baseline_max": 389.0,
            "baseline_material": "Material 1",
            "baseline_confidence": 0.92
          }
        }
      }
    }
    ```

#### `/dashboard/extruder/derived`
- **Location**: `backend/app/api/routers/dashboard.py`
- **Changes**:
  - Added `baselines_standardized` field to response
  - Contains standardized baseline structures for all sensors
  - Example response structure:
    ```json
    {
      "baseline": {...},
      "baselines_standardized": {
        "Pressure_bar": {
          "sensor_name": "Pressure_bar",
          "baseline_mean": 370.0,
          "baseline_min": 352.0,
          "baseline_max": 389.0,
          "baseline_material": "Material 1",
          "baseline_confidence": 0.92
        },
        ...
      }
    }
    ```

### 4. Material Validation

The `baseline_material` field is automatically populated from the active profile's `material_id`. This ensures:
- Baseline material matches the active material for the profile
- Material information is always available in the baseline structure
- Frontend can validate baseline-material consistency

### 5. Confidence Calculation

Baseline confidence is calculated based on the number of samples used to compute the baseline:
- **High confidence (1.0)**: 100+ samples
- **Good confidence (0.9)**: 50-99 samples
- **Moderate confidence (0.8)**: 25-49 samples
- **Low confidence (0.7)**: 10-24 samples
- **Very low confidence (0.6)**: <10 samples
- **Default (1.0)**: If sample count is not available

### 6. Fallback Logic

The implementation includes robust fallback logic:
1. **Profile Baseline**: Uses `ProfileBaselineStats` if available (preferred)
2. **Rolling Baseline**: Falls back to rolling baseline computed from current window
3. **Statistical Derivation**: Derives min/max from mean ± std if percentiles unavailable
4. **Percentage Derivation**: Uses ±5% of mean as last resort

## Testing

### Test Cases

1. **Profile Baseline Available**:
   - Should use `ProfileBaselineStats` data
   - Should include material from profile
   - Should calculate confidence from sample count

2. **Rolling Baseline Only**:
   - Should use rolling baseline data
   - Should include material if profile available
   - Should use lower confidence (0.6-0.8)

3. **No Baseline Data**:
   - Should return `null` for all baseline fields
   - Should not cause errors

4. **Min/Max Derivation**:
   - Should prefer p05/p95 percentiles
   - Should fallback to mean ± std
   - Should use ±5% as last resort

## Usage Examples

### Frontend Usage

```typescript
// Access standardized baseline in /dashboard/current response
const response = await api.get('/dashboard/current');
const pressureBaseline = response.data.metrics.Pressure_bar.baseline;

console.log(pressureBaseline.sensor_name);        // "Pressure_bar"
console.log(pressureBaseline.baseline_mean);     // 370.0
console.log(pressureBaseline.baseline_min);      // 352.0
console.log(pressureBaseline.baseline_max);      // 389.0
console.log(pressureBaseline.baseline_material); // "Material 1"
console.log(pressureBaseline.baseline_confidence); // 0.92
```

### Backend Usage

```python
from app.utils.baseline_formatter import build_standardized_baseline

# Build standardized baseline from ProfileBaselineStats
baseline = build_standardized_baseline(
    baseline_stat=baseline_stat,
    profile=active_profile,
)
```

## Benefits

1. **Consistency**: All baseline data follows the same structure
2. **Completeness**: All required fields are always present
3. **Reliability**: Robust fallback logic ensures data is always available
4. **Validation**: Material information enables baseline-material validation
5. **Confidence**: Confidence scores help frontend make informed decisions

## Migration Notes

- **Backward Compatibility**: Existing `baseline_mean` and `green_band` fields are preserved
- **New Field**: `baseline` field added to metrics in `/dashboard/current`
- **New Field**: `baselines_standardized` field added to `/dashboard/extruder/derived`
- **No Breaking Changes**: Frontend can continue using existing fields while migrating to new structure

## Future Enhancements

1. Add baseline validation endpoint
2. Add baseline comparison utilities
3. Add baseline quality metrics
4. Add baseline versioning support
5. Add baseline export/import functionality
