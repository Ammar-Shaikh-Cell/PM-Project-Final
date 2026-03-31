# Baseline Learning Implementation

## Overview

The baseline learning lifecycle has been fully implemented to support profile-specific baseline collection and computation.

## Implementation Summary

### 1. Database Schema

**Profile Model (`backend/app/models/profile.py`)**:
- Added `baseline_learning` (Boolean, default=False) - Flag indicating active learning mode
- Added `baseline_ready` (Boolean, default=False) - Flag indicating baseline is computed and ready

**New Table: `profile_baseline_samples`**:
- Temporary storage for raw samples during learning
- Columns: `id`, `profile_id`, `metric_name`, `value`, `timestamp`
- Automatically cleaned up after baseline finalization

**Existing Table: `profile_baseline_stats`**:
- Stores computed baseline statistics (mean, std, p05, p95, sample_count)
- Updated during finalization

### 2. Baseline Learning Service

**File**: `backend/app/services/baseline_learning_service.py`

**Key Methods**:

1. **`start_baseline_learning(profile_id)`**
   - Sets `baseline_learning = true`
   - Sets `baseline_ready = false`
   - Clears existing baseline stats and samples
   - Resets sample counts

2. **`collect_sample(profile_id, metric_name, value, machine_state, timestamp)`**
   - Only collects when:
     - `baseline_learning = true`
     - `machine_state == PRODUCTION`
   - Stores sample in `ProfileBaselineSample` table
   - Increments sample count in `ProfileBaselineStats`

3. **`collect_samples_batch(profile_id, samples_dict, machine_state, timestamp)`**
   - Collects multiple samples at once
   - Returns count of successfully collected samples

4. **`finalize_baseline(profile_id)`**
   - Retrieves all samples from `ProfileBaselineSample`
   - Computes mean, std, p05, p95 for each metric
   - Stores statistics in `ProfileBaselineStats`
   - Sets `baseline_ready = true` and `baseline_learning = false`
   - Deletes samples (they're now in baseline_stats)
   - Requires minimum 100 samples per metric

5. **`reset_baseline(profile_id, archive=True)`**
   - Archives old baseline stats and samples
   - Sets `baseline_learning = false` and `baseline_ready = false`

6. **`get_active_profile(machine_id, material_id)`**
   - Fallback logic:
     1. Try Machine + Material profile
     2. Try Material Default profile (machine_id IS NULL)
     3. Return None if no profile found

7. **`is_learning_mode(profile_id)`**
   - Check if profile is in baseline learning mode

### 3. Integration Points

#### A. MSSQL Extruder Poller (`backend/app/services/mssql_extruder_poller.py`)

**Sample Collection**:
- When machine is in PRODUCTION state:
  - Gets active profile for machine + material
  - If `baseline_learning = true`:
    - Collects samples for: `ScrewSpeed_rpm`, `Pressure_bar`, `Temp_Zone1_C`, `Temp_Zone2_C`, `Temp_Zone3_C`, `Temp_Zone4_C`, `Temp_Avg`, `Temp_Spread`
    - Only collects non-None values
    - Non-blocking (errors don't break main flow)

#### B. Alarm Service (`backend/app/services/alarm_service.py`)

**Alarm Suppression**:
- `create_alarm()` now accepts `check_baseline_learning` parameter (default=True)
- When `check_baseline_learning = true`:
  - Gets active profile for machine
  - If `baseline_learning = true`: Suppresses alarm (returns None)
  - Logs suppression for debugging

**Updated Functions**:
- `create_alarm()` - Returns `Optional[Alarm]` (None if suppressed)
- `auto_alarm_from_sensor_value()` - Checks baseline learning before creating alarms

#### C. Extruder AI Service (`backend/app/services/extruder_ai_service.py`)

**Alarm Suppression**:
- All `alarm_service.create_alarm()` calls now pass `check_baseline_learning=True`
- Alarms are suppressed during baseline learning

### 4. Migration

**File**: `backend/alembic/versions/0006_add_profiles_and_evaluation_config.py`

**Changes**:
- Added `baseline_learning` and `baseline_ready` columns to `profiles` table
- Added `profile_baseline_samples` table with indexes

**To Apply**:
```bash
cd backend
alembic upgrade head
```

### 5. Baseline Learning Lifecycle

```
┌─────────────────┐
│ Start Learning  │
│ baseline_learning = true
│ baseline_ready = false
│ Clear samples
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Collect Samples │
│ Only in PRODUCTION state
│ Store in profile_baseline_samples
│ Increment sample_count
└────────┬────────┘
         │
         ▼ (when enough samples)
┌─────────────────┐
│ Finalize        │
│ Compute mean/std/p05/p95
│ Store in profile_baseline_stats
│ baseline_ready = true
│ baseline_learning = false
│ Delete samples
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Ready for Use   │
│ Baseline available
│ Alarms enabled
└─────────────────┘
```

### 6. Configuration

**Minimum Samples**: 100 samples per metric (configurable in `BaselineLearningService.MIN_SAMPLES_FOR_BASELINE`)

**Supported Metrics**:
- `ScrewSpeed_rpm`
- `Pressure_bar`
- `Temp_Zone1_C`
- `Temp_Zone2_C`
- `Temp_Zone3_C`
- `Temp_Zone4_C`
- `Temp_Avg` (computed)
- `Temp_Spread` (computed)

### 7. Usage Example

```python
from app.services.baseline_learning_service import baseline_learning_service
from uuid import UUID

# Start learning
profile_id = UUID("...")
await baseline_learning_service.start_baseline_learning(session, profile_id)

# Samples are automatically collected during PRODUCTION state
# (handled by MSSQL poller)

# When ready, finalize
await baseline_learning_service.finalize_baseline(session, profile_id)

# Reset if needed
await baseline_learning_service.reset_baseline(session, profile_id, archive=True)
```

### 8. Key Features

✅ **Profile-specific**: Each (machine, material) profile has its own baseline  
✅ **PRODUCTION-only**: Samples only collected when machine is in PRODUCTION state  
✅ **Alarm suppression**: Alarms are suppressed during baseline learning  
✅ **Automatic collection**: Integrated with MSSQL poller for seamless sample collection  
✅ **Statistics computation**: Mean, std, percentiles (p05, p95) computed automatically  
✅ **Sample storage**: Temporary samples table for efficient querying  
✅ **Fallback logic**: Material default profiles supported  

### 9. Next Steps (Optional)

- Add API endpoints for starting/finalizing/resetting baseline learning
- Add UI for baseline learning status and controls
- Add notifications when baseline learning completes
- Add baseline quality metrics (coefficient of variation, etc.)
