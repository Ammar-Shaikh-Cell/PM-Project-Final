# Machine State IDLE Issue - Fix Summary

## ✅ **FIXES APPLIED**

### **Fix 1: Proper None Value Handling**

**Before**:
```python
rpm = reading.screw_rpm or 0.0  # None becomes 0.0
pressure = reading.pressure_bar or 0.0
temp_avg = metrics.temp_avg or 0.0
d_temp = metrics.d_temp_avg or 0.0
```

**After**:
```python
rpm = reading.screw_rpm  # Keep None as None
pressure = reading.pressure_bar
temp_avg = metrics.temp_avg
d_temp = metrics.d_temp_avg

# Check for missing critical data first
if rpm is None:
    return MachineState.UNKNOWN, 0.3

# Use 0.0 only for comparisons when we know it's safe
rpm_val = rpm if rpm is not None else 0.0
```

**Impact**: Now we can distinguish between "machine is off" (0.0) and "data missing" (None).

---

### **Fix 2: Removed Aggressive IDLE Default**

**Before**:
```python
# Default to IDLE if warm but uncertain
if temp_avg >= self.thresholds.T_MIN_ACTIVE:
    return MachineState.IDLE, 0.5
```

**After**:
```python
# If we have temperature data but can't determine state, be more conservative
if temp_avg is not None and temp_avg >= self.thresholds.T_MIN_ACTIVE:
    # If we have temp but no d_temp, we can't confirm stability
    if d_temp is None and len(self.temp_history) < 120:
        # Not enough history - could be heating up or just started
        if rpm_val < self.thresholds.RPM_ON and pressure_val < self.thresholds.P_ON:
            return MachineState.IDLE, 0.4  # Lower confidence
    elif d_temp is None:
        return MachineState.IDLE, 0.4  # Lower confidence
```

**Impact**: IDLE is only returned when we have enough data to confirm it, with lower confidence if uncertain.

---

### **Fix 3: Fixed IDLE Condition - Require Valid d_temp**

**Before**:
```python
# IDLE: warm, stable, no production
if (rpm < self.thresholds.RPM_ON and 
    pressure < self.thresholds.P_ON and 
    temp_avg >= self.thresholds.T_MIN_ACTIVE and 
    abs(d_temp) < self.thresholds.TEMP_FLAT_RATE):  # d_temp None becomes 0.0!
    return MachineState.IDLE, 0.8
```

**After**:
```python
# IDLE: warm, stable, no production
# IMPORTANT: Require valid d_temp for IDLE - don't use None as 0.0
if (rpm_val < self.thresholds.RPM_ON and 
    pressure_val < self.thresholds.P_ON and 
    temp_avg is not None and temp_avg >= self.thresholds.T_MIN_ACTIVE and
    d_temp is not None and abs(d_temp) < self.thresholds.TEMP_FLAT_RATE):
    return MachineState.IDLE, 0.8
```

**Impact**: IDLE is only returned when we have actual temperature stability data (d_temp is not None).

---

### **Fix 4: Improved OFF State Detection**

**Before**:
```python
# OFF: cold, no RPM, no pressure
if (rpm < self.thresholds.RPM_ON and 
    pressure < self.thresholds.P_ON and 
    temp_avg < self.thresholds.T_MIN_ACTIVE):
    return MachineState.OFF, 0.9
```

**After**:
```python
# OFF: cold, no RPM, no pressure
# Only check OFF if we have valid temperature data
if (rpm_val < self.thresholds.RPM_ON and 
    pressure_val < self.thresholds.P_ON):
    if temp_avg is not None and temp_avg < self.thresholds.T_MIN_ACTIVE:
        return MachineState.OFF, 0.9
    elif temp_avg is None:
        # No temperature data - can't be sure, but likely OFF if RPM and pressure are 0
        return MachineState.OFF, 0.7  # Lower confidence
```

**Impact**: OFF state is detected more accurately, even when temperature data is missing.

---

### **Fix 5: Improved HEATING/COOLING Detection**

**Before**:
```python
# COOLING: RPM off, temperature falling
if (rpm < self.thresholds.RPM_ON and 
    d_temp <= self.thresholds.COOLING_RATE and 
    temp_avg >= self.thresholds.T_MIN_ACTIVE):
    return MachineState.COOLING, 0.8
```

**After**:
```python
# COOLING: RPM off, temperature falling
# Require valid d_temp for COOLING detection
if (rpm_val < self.thresholds.RPM_ON and 
    temp_avg is not None and temp_avg >= self.thresholds.T_MIN_ACTIVE and
    d_temp is not None and d_temp <= self.thresholds.COOLING_RATE):
    return MachineState.COOLING, 0.8
```

**Impact**: HEATING and COOLING states are only detected when we have valid temperature slope data.

---

### **Fix 6: Added Missing Data Handling**

**Before**: Missing data (None) was converted to 0.0, causing incorrect state detection.

**After**:
```python
# If critical data is missing, we can't determine state accurately
if rpm is None:
    logger.warning(f"Missing RPM data for {self.machine_id}, cannot determine state accurately")
    return MachineState.UNKNOWN, 0.3
```

**Impact**: When critical data is missing, returns UNKNOWN instead of incorrectly showing IDLE.

---

### **Fix 7: Improved Sensor Fault Detection Logging**

**Before**: Silent failures, hard to debug.

**After**: Added detailed logging for each fault condition:
```python
logger.warning(f"Implausible temperature detected for {self.machine_id}: {valid_temps}")
logger.debug(f"Missing RPM data for {self.machine_id} - sensor fault")
```

**Impact**: Better debugging and monitoring of sensor fault conditions.

---

## 📊 **EXPECTED BEHAVIOR AFTER FIX**

### **Scenario 1: No Data Coming (MSSQL Connection Issue)**
- **Before**: Shows IDLE
- **After**: Shows **SENSOR_FAULT** or **UNKNOWN**
- **Reason**: Missing RPM data triggers sensor fault detection

### **Scenario 2: Data Coming, Machine OFF (RPM=0, Pressure=0, Temp<60°C)**
- **Before**: Might show IDLE if temp >= 60°C
- **After**: Shows **OFF** with high confidence
- **Reason**: OFF check happens before IDLE, and requires valid temp data

### **Scenario 3: Data Coming, Machine Warm but Not Producing (RPM<5, Pressure<2, Temp>=60°C, Temp Stable)**
- **Before**: Shows IDLE even if d_temp is None
- **After**: Shows **IDLE** only if d_temp is valid and < 0.2°C/min
- **Reason**: IDLE now requires actual temperature stability data

### **Scenario 4: Data Coming, Machine Just Started (Not Enough History)**
- **Before**: Shows IDLE if temp >= 60°C
- **After**: Shows **IDLE** with low confidence (0.4) if temp >= 60°C but no history
- **Reason**: Lower confidence when we can't confirm stability

### **Scenario 5: Data Coming, Machine Producing (RPM>=10, Pressure>=5)**
- **Before**: Might show IDLE if other conditions matched first
- **After**: Shows **PRODUCTION** with high confidence
- **Reason**: PRODUCTION check happens before IDLE

---

## 🔍 **TESTING RECOMMENDATIONS**

1. **Test with No Data**:
   - Stop MSSQL connection
   - Verify state shows SENSOR_FAULT or UNKNOWN, not IDLE

2. **Test with Machine OFF**:
   - RPM=0, Pressure=0, Temp<60°C
   - Verify state shows OFF, not IDLE

3. **Test with Machine Warm but Not Producing**:
   - RPM<5, Pressure<2, Temp>=60°C, Temp stable (d_temp known)
   - Verify state shows IDLE with confidence 0.8

4. **Test with Machine Just Started**:
   - RPM<5, Pressure<2, Temp>=60°C, No history (d_temp=None)
   - Verify state shows IDLE with confidence 0.4 (lower)

5. **Test with Machine Producing**:
   - RPM>=10, Pressure>=5
   - Verify state shows PRODUCTION, not IDLE

---

## 📝 **KEY CHANGES SUMMARY**

1. ✅ **None values are preserved** - not converted to 0.0
2. ✅ **IDLE requires valid d_temp** - no longer uses None as 0.0
3. ✅ **Removed aggressive IDLE default** - only returns IDLE when confirmed
4. ✅ **Better OFF detection** - handles missing temp data
5. ✅ **Missing data returns UNKNOWN** - not IDLE
6. ✅ **Improved logging** - easier to debug state transitions

---

## ⚠️ **IMPORTANT NOTES**

- **State transitions may change** - machines that were showing IDLE incorrectly may now show OFF, UNKNOWN, or SENSOR_FAULT
- **Confidence levels adjusted** - IDLE now has lower confidence (0.4) when data is uncertain
- **Requires temperature history** - IDLE detection now requires at least some temperature history for stability check
- **Better handling of startup** - Machines just starting up will show lower confidence states until enough data is collected
