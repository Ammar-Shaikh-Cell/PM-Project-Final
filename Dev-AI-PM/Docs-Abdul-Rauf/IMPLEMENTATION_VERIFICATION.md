# Implementation Verification: System Flow

## ✅ **CONFIRMED: Implementation Matches Described Flow**

The system implementation **DOES MATCH** the described flow with one clarification noted below.

---

## 📊 **Actual Implementation Flow**

### **1. Live Sensor Data** ✅
- **Location**: `backend/app/services/mssql_extruder_poller.py:_run()`
- **Process**: 
  - Polls MSSQL database (`Tab_Actual` table) every N seconds
  - Fetches raw sensor readings: `ScrewSpeed_rpm`, `Pressure_bar`, `Temp_Zone1-4_C`
  - Maintains sliding window (last 10 minutes by default)
- **Status**: ✅ **IMPLEMENTED**

### **2. Metric Engine (Derived KPIs)** ✅
- **Location**: `backend/app/services/mssql_extruder_poller.py:_compute_features()`
- **Process**:
  - Calculates statistical features: mean, std, delta, delta_from_ma
  - Computes correlations: `corr_pressure_rpm`, `corr_tempavg_rpm`
  - Derives aggregated metrics: `Temp_Avg`, `Temp_Spread`
  - Calculates drift scores
- **Status**: ✅ **IMPLEMENTED** (runs regardless of state for AI predictions)

### **3. State Machine** ✅
- **Location**: `backend/app/services/mssql_extruder_poller.py:_persist_prediction()`
- **Process**:
  - After AI prediction, builds `SensorReading` from MSSQL data
  - Calls `state_service.process_sensor_reading()` 
  - State machine determines: **OFF / HEATING / IDLE / PRODUCTION / COOLING**
  - Uses hysteresis/debounce logic to prevent rapid state oscillation
- **Status**: ✅ **IMPLEMENTED**

### **4. IF state == PRODUCTION** ✅
- **Location**: 
  - `backend/app/services/mssql_extruder_poller.py:489-517` (AI decision logic)
  - `backend/app/api/routers/dashboard.py:389-430` (Dashboard evaluation)
- **Process**:
  - Checks `current_state.state.value == "PRODUCTION"`
  - Only proceeds with evaluation if in PRODUCTION
- **Status**: ✅ **IMPLEMENTED**

### **5. Baseline Comparison** ✅
- **Location**: `backend/app/api/routers/dashboard.py:440-468`
- **Process**:
  - Calculates per-sensor baseline: `mean`, `std`, `min_normal`, `max_normal`
  - Operating-point aware (buckets by ScrewSpeed_rpm)
  - Compares current values against baseline ranges
- **Status**: ✅ **IMPLEMENTED** (only in PRODUCTION)

### **6. Severity Scoring** ✅
- **Location**: `backend/app/api/routers/dashboard.py:520-541`
- **Process**:
  - `risk_level()` function calculates Z-score: `z = abs(value - mean) / std`
  - **Green**: `z <= 1` (within 1 std)
  - **Yellow**: `1 < z <= 2` (within 2 std)
  - **Red**: `z > 2` (beyond 2 std)
- **Status**: ✅ **IMPLEMENTED** (only in PRODUCTION)

### **7. Overall Risk** ✅
- **Location**: `backend/app/api/routers/dashboard.py:542-544`
- **Process**:
  - Takes worst sensor risk (red > yellow > green)
  - Returns overall risk level
- **Status**: ✅ **IMPLEMENTED** (only in PRODUCTION)

### **8. Text Explanation** ✅
- **Location**: `backend/app/api/routers/dashboard.py:546-562`
- **Process**:
  - Generates per-sensor explanations:
    - **Red**: "critically deviates from normal (mean±std)"
    - **Yellow**: "drifting from normal (mean±std)"
    - **Green**: "stable"
  - Stored in `derived["explanations"]`
- **Status**: ✅ **IMPLEMENTED** (only in PRODUCTION)

### **9. ELSE → Status only (no scoring)** ✅
- **Location**: `backend/app/api/routers/dashboard.py:417-430`
- **Process**:
  - When NOT in PRODUCTION (OFF/HEATING/IDLE/COOLING):
    - Returns empty `baseline: {}`
    - Returns minimal `derived` (no calculations)
    - Returns `risk: {"overall": "unknown", "sensors": {}}`
    - Includes `evaluation_enabled: false`
    - Includes message: "Process evaluation disabled - machine is in {state} state"
- **Status**: ✅ **IMPLEMENTED**

---

## 🔄 **Complete Data Flow Diagram**

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Live Sensor Data                                         │
│    MSSQL Database → MSSQLExtruderPoller._run()              │
│    • Polls Tab_Actual table                                 │
│    • Fetches: ScrewSpeed_rpm, Pressure_bar, Temp_Zone1-4   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Metric Engine (Derived KPIs)                             │
│    MSSQLExtruderPoller._compute_features()                   │
│    • Mean, std, delta, correlations                        │
│    • Temp_Avg, Temp_Spread                                  │
│    • Drift scores                                           │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. State Machine                                            │
│    MachineStateService.process_sensor_reading()              │
│    • Determines: OFF / HEATING / IDLE / PRODUCTION / COOLING │
│    • Uses hysteresis/debounce logic                         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
              ┌──────────────┐
              │ State Check  │
              └──────┬───────┘
                     │
        ┌────────────┴────────────┐
        │                         │
        ▼                         ▼
┌───────────────┐        ┌──────────────────┐
│ state ==      │        │ state !=          │
│ PRODUCTION    │        │ PRODUCTION        │
└───────┬───────┘        └────────┬─────────┘
        │                         │
        ▼                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Baseline Comparison                                      │
│    Dashboard API: /extruder/derived                        │
│    • Calculate mean, std per sensor                         │
│    • Operating-point aware (RPM buckets)                   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Severity Scoring                                         │
│    risk_level() function                                    │
│    • Z-score calculation: z = |value - mean| / std        │
│    • Green (z≤1) / Yellow (1<z≤2) / Red (z>2)             │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Overall Risk                                              │
│    • Worst sensor risk (red > yellow > green)               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. Text Explanation                                         │
│    • Per-sensor explanations                                │
│    • "stable" / "drifting" / "critically deviates"         │
└─────────────────────────────────────────────────────────────┘

        ┌──────────────────┐
        │ Status only      │
        │ (no scoring)     │
        │ • Empty baseline │
        │ • Unknown risk   │
        │ • Message        │
        └──────────────────┘
```

---

## 📝 **Clarification**

**Metric Engine runs in TWO contexts:**

1. **Always (for AI predictions)**: 
   - `MSSQLExtruderPoller._compute_features()` runs regardless of state
   - Used for AI anomaly detection predictions
   - Location: `backend/app/services/mssql_extruder_poller.py:117-218`

2. **Only in PRODUCTION (for dashboard display)**:
   - Dashboard `/extruder/derived` endpoint calculates baselines/risk
   - Only when `state == PRODUCTION`
   - Location: `backend/app/api/routers/dashboard.py:440-562`

This is **correct behavior** - AI predictions need features regardless of state, but process quality evaluation (baselines, risk scores) only runs in PRODUCTION.

---

## ✅ **Conclusion**

**YES, the implementation matches the described flow:**

```
Live Sensor Data
      ↓
Metric Engine (derived KPIs)
      ↓
State Machine (OFF / HEATING / IDLE / PRODUCTION / COOLING)
      ↓
IF state == PRODUCTION
    → Baseline Comparison ✅
    → Severity Scoring ✅
    → Overall Risk ✅
    → Text Explanation ✅
ELSE
    → Status only (no scoring) ✅
```

All components are implemented and working as described.
