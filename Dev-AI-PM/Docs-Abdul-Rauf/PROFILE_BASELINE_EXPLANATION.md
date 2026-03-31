# Profile and Baseline Status Explanation

## Why These Statuses Appear in PRODUCTION State

When the machine is in **PRODUCTION** state, you may see:

### ✅ "Process active - Traffic light evaluation enabled"
**This is CORRECT** - The machine is in PRODUCTION, so process evaluation is enabled.

### ❌ "Profile: Not Available"
**This means:** No profile exists for the current machine/material combination.

**Why it happens:**
- A profile must be created for each (machine, material) combination
- The system looks for:
  1. Machine-specific profile: `machine_id` + `material_id`
  2. Material default profile: `machine_id = NULL` + `material_id`
- If neither exists, `profile_status = "not_available"`

**Solution:** Create a profile for the machine/material combination.

### ⏳ "Baseline: Not Ready"
**This means:** Baseline learning hasn't completed yet (or no profile exists).

**Why it happens:**
- If no profile exists → `baseline_status = "not_ready"` (default)
- If profile exists but `baseline_ready = False` → `baseline_status = "not_ready"`
- Baseline learning requires:
  1. Profile exists
  2. Machine in PRODUCTION state
  3. At least 100 samples collected (`MIN_SAMPLES_FOR_BASELINE = 100`)
  4. `baseline_learning = True` flag set

**Baseline Status Flow:**
1. **"not_available"** → No profile exists
2. **"not_ready"** → Profile exists, but baseline learning hasn't started or hasn't collected enough samples
3. **"learning"** → Profile exists, `baseline_learning = True`, collecting samples
4. **"ready"** → Profile exists, `baseline_ready = True`, baseline statistics computed

## How to Fix

### Step 1: Create a Profile
A profile creation endpoint has been added: `POST /api/profiles`

**Request:**
```json
{
  "machine_id": "46b3d77b-b993-4ac4-9fc6-1f582edd7921",  // Optional - if null, creates material default
  "material_id": "Material 1",  // Required
  "version": "1.0"  // Optional
}
```

**Response:**
- Profile is created with `baseline_learning = True` automatically
- Baseline learning starts immediately

### Step 2: Wait for Baseline Learning
- System collects samples when machine is in PRODUCTION
- Needs at least 100 samples (`MIN_SAMPLES_FOR_BASELINE`)
- Once enough samples collected, `baseline_ready = True`
- Status changes from "learning" → "ready"

## Current Status Logic

```python
# In dashboard.py get_current_dashboard_data()
active_profile = await baseline_learning_service.get_active_profile(
    session, machine.id, material_id
)

baseline_status = "not_ready"  # Default
profile_status = "not_available"  # Default

if active_profile:
    profile_status = "active"
    if active_profile.baseline_ready:
        baseline_status = "ready"
    elif active_profile.baseline_learning:
        baseline_status = "learning"
    else:
        baseline_status = "not_ready"
```

## Summary

**The machine being in PRODUCTION is correct**, but:
- **Profile must be created first** (one-time setup)
- **Baseline learning needs time** to collect 100+ samples
- **Once baseline is ready**, you'll see:
  - ✅ Profile: Active
  - ✅ Baseline: Ready
  - Traffic light evaluation with baseline comparison
