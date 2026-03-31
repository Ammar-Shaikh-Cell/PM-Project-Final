# Machine State IDLE Issue - Analysis & Fix

## 🔴 **CRITICAL ISSUES FOUND**

### **Issue 1: Default Values Masking Missing Data**

**Location**: `_determine_state()` lines 323-326

**Problem**:
```python
rpm = reading.screw_rpm or 0.0
pressure = reading.pressure_bar or 0.0
temp_avg = metrics.temp_avg or 0.0
d_temp = metrics.d_temp_avg or 0.0
```

**Impact**:
- When sensor data is missing (None), it becomes 0.0
- This makes the code think RPM=0, pressure=0, temp=0, which triggers wrong states
- **Cannot distinguish between "machine is off" (0.0) and "data missing" (None)**

---

### **Issue 2: IDLE Default Fallback Too Aggressive**

**Location**: `_determine_state()` lines 384-386

**Problem**:
```python
# Default to IDLE if warm but uncertain
if temp_avg >= self.thresholds.T_MIN_ACTIVE:
    return MachineState.IDLE, 0.5
```

**Impact**:
- If temperature is >= 60°C but other checks fail, it defaults to IDLE
- This catches many scenarios that should be OFF, HEATING, or COOLING
- **Too broad - will show IDLE even when machine should be OFF**

---

### **Issue 3: IDLE Check Using None as 0.0**

**Location**: `_determine_state()` lines 378-382

**Problem**:
```python
# IDLE: warm, stable, no production
if (rpm < self.thresholds.RPM_ON and 
    pressure < self.thresholds.P_ON and 
    temp_avg >= self.thresholds.T_MIN_ACTIVE and 
    abs(d_temp) < self.thresholds.TEMP_FLAT_RATE):
    return MachineState.IDLE, 0.8
```

**Impact**:
- If `d_temp` is None (no history), it becomes 0.0
- `abs(0.0) < 0.2` is True
- **IDLE condition matches even when temperature slope is unknown!**

---

### **Issue 4: Sensor Fault Detection Too Strict**

**Location**: `_detect_sensor_fault()` lines 308-313

**Problem**:
```python
# Missing critical data
if reading.screw_rpm is None:
    return True

# Too many missing temperature zones
if len(valid_temps) < 2:  # At least 2 zones needed
    return True
```

**Impact**:
- If data is not coming (MSSQL connection issue), all readings are None
- This triggers SENSOR_FAULT, but then the state might still show IDLE
- **Should handle missing data differently than sensor faults**

---

### **Issue 5: State Logic Order - IDLE Before OFF**

**Location**: `_determine_state()` state checking order

**Problem**:
- IDLE check (line 378) happens before checking if machine is truly OFF
- If temp >= 60°C but RPM=0 and pressure=0, it might match IDLE instead of OFF
- **Should check OFF state more carefully before IDLE**

---

## 🔧 **ROOT CAUSE SUMMARY**

1. **Missing data (None) is converted to 0.0**, making it impossible to distinguish "machine off" from "no data"
2. **IDLE default fallback is too broad** - catches too many scenarios
3. **Temperature slope (d_temp) None becomes 0.0**, which matches IDLE condition incorrectly
4. **State logic order** - IDLE is checked too early, before proper OFF validation
5. **No handling for "no data" scenario** - should show UNKNOWN or SENSOR_FAULT, not IDLE

---

## ✅ **FIXES REQUIRED**

### **Fix 1: Handle None Values Properly**

Don't convert None to 0.0. Instead, check for None explicitly and handle missing data.

### **Fix 2: Remove Aggressive IDLE Default**

Remove or restrict the default IDLE fallback. Only use IDLE when we have enough data to confirm it.

### **Fix 3: Fix IDLE Condition**

Don't use `d_temp` in IDLE check if it's None. Require actual temperature stability data.

### **Fix 4: Improve State Logic Order**

Check OFF state more carefully, including checking if we have valid data.

### **Fix 5: Add "No Data" Handling**

If critical data is missing, return SENSOR_FAULT or UNKNOWN, not IDLE.

---

## 📊 **EXPECTED BEHAVIOR AFTER FIX**

### **When Data is NOT Coming**:
- Should show: **SENSOR_FAULT** or **UNKNOWN**
- Should NOT show: IDLE

### **When Data is Coming but Machine is OFF**:
- RPM = 0, Pressure = 0, Temp < 60°C
- Should show: **OFF**
- Should NOT show: IDLE

### **When Data is Coming and Machine is Warm but Not Producing**:
- RPM < 5, Pressure < 2, Temp >= 60°C, Temp stable (d_temp known and < 0.2)
- Should show: **IDLE**
- Should NOT show: IDLE if d_temp is unknown

### **When Data is Coming and Machine is Producing**:
- RPM >= 10, Pressure >= 5
- Should show: **PRODUCTION**
- Should NOT show: IDLE
