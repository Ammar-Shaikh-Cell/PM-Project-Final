# Machine and Sensor Edit/Delete Guide

## ✅ **FUNCTIONALITY IMPLEMENTED**

The system now supports full CRUD operations for machines and sensors with proper database cleanup.

---

## 🔧 **API ENDPOINTS**

### **Machines**

#### **1. List All Machines**
```
GET /api/machines
```
- Returns list of all machines
- No authentication required (viewer access)

#### **2. Get Single Machine**
```
GET /api/machines/{machine_id}
```
- Returns machine details
- No authentication required (viewer access)

#### **3. Create Machine**
```
POST /api/machines
Content-Type: application/json

{
  "name": "Machine Name",
  "location": "Location",
  "description": "Description",
  "status": "online",
  "criticality": "high",
  "metadata": {}
}
```
- Creates a new machine
- No authentication required

#### **4. Update Machine** ✅
```
PATCH /api/machines/{machine_id}
Content-Type: application/json

{
  "name": "Updated Name",
  "location": "New Location",
  "status": "offline",
  "criticality": "medium"
}
```
- Updates machine fields (only provided fields are updated)
- No authentication required
- Returns updated machine

#### **5. Delete Machine** ✅
```
DELETE /api/machines/{machine_id}
```
- **Requires**: Engineer/Admin role
- Deletes machine and ALL related data:
  - Sensors (and their sensor_data)
  - Predictions
  - Alarms
  - Tickets
  - Sensor data
  - Machine state records
  - Machine state thresholds
  - Machine state transitions
  - Machine state alerts
  - Machine process evaluations
- Returns: 204 No Content

---

### **Sensors**

#### **1. List All Sensors**
```
GET /api/sensors?machine_id={machine_id}
```
- Returns list of sensors (optionally filtered by machine_id)
- No authentication required (viewer access)

#### **2. Get Single Sensor**
```
GET /api/sensors/{sensor_id}
```
- Returns sensor details
- No authentication required (viewer access)

#### **3. Create Sensor**
```
POST /api/sensors
Content-Type: application/json

{
  "machine_id": "uuid",
  "name": "Sensor Name",
  "sensor_type": "temperature",
  "unit": "°C",
  "min_threshold": 0,
  "max_threshold": 100,
  "warning_threshold": 80,
  "critical_threshold": 90,
  "metadata": {}
}
```
- Creates a new sensor
- No authentication required

#### **4. Update Sensor** ✅
```
PATCH /api/sensors/{sensor_id}
Content-Type: application/json

{
  "name": "Updated Sensor Name",
  "sensor_type": "pressure",
  "unit": "bar",
  "warning_threshold": 150,
  "critical_threshold": 180
}
```
- Updates sensor fields (only provided fields are updated)
- No authentication required
- Returns updated sensor

#### **5. Delete Sensor** ✅
```
DELETE /api/sensors/{sensor_id}
```
- **Requires**: Engineer/Admin role
- Deletes sensor and ALL related data:
  - Sensor data (readings)
  - Predictions
  - Alarms
- Returns: 204 No Content

---

## 🔒 **AUTHENTICATION & AUTHORIZATION**

### **View/Edit Operations**:
- No special role required (viewer access)
- Any authenticated user can view and edit

### **Delete Operations**:
- **Requires**: Engineer or Admin role
- Protected by `require_engineer` dependency
- Prevents accidental deletions

---

## 🗑️ **CASCADE DELETE BEHAVIOR**

### **When Deleting a Machine**:

The following data is automatically deleted:

1. **Sensors** (CASCADE)
   - All sensors belonging to the machine
   
2. **Sensor Data** (Explicit + CASCADE)
   - All sensor readings for the machine
   
3. **Predictions** (Explicit)
   - All AI predictions for the machine
   
4. **Alarms** (Explicit)
   - All alarms associated with the machine
   
5. **Tickets** (Explicit)
   - All maintenance tickets for the machine
   
6. **Machine State Data** (Explicit)
   - Machine state records
   - Machine state thresholds
   - Machine state transitions
   - Machine state alerts
   - Machine process evaluations

### **When Deleting a Sensor**:

The following data is automatically deleted:

1. **Sensor Data** (Explicit + CASCADE)
   - All sensor readings for this sensor
   
2. **Predictions** (Explicit)
   - All AI predictions for this sensor
   
3. **Alarms** (Explicit)
   - All alarms associated with this sensor

---

## 📝 **USAGE EXAMPLES**

### **Example 1: Update Machine Name**
```bash
curl -X PATCH "http://localhost:8000/api/machines/{machine_id}" \
  -H "Authorization: Bearer {token}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Updated Machine Name"
  }'
```

### **Example 2: Update Sensor Thresholds**
```bash
curl -X PATCH "http://localhost:8000/api/sensors/{sensor_id}" \
  -H "Authorization: Bearer {token}" \
  -H "Content-Type: application/json" \
  -d '{
    "warning_threshold": 150,
    "critical_threshold": 180
  }'
```

### **Example 3: Delete Machine**
```bash
curl -X DELETE "http://localhost:8000/api/machines/{machine_id}" \
  -H "Authorization: Bearer {engineer_token}"
```

### **Example 4: Delete Sensor**
```bash
curl -X DELETE "http://localhost:8000/api/sensors/{sensor_id}" \
  -H "Authorization: Bearer {engineer_token}"
```

---

## ⚠️ **IMPORTANT NOTES**

### **1. Delete Operations are Permanent**
- Deleted machines and sensors cannot be recovered
- All related data is permanently removed
- Use with caution!

### **2. Transaction Safety**
- All delete operations are wrapped in transactions
- If any part fails, the entire operation is rolled back
- Database remains consistent

### **3. Logging**
- All delete operations are logged
- Check logs for confirmation: `Successfully deleted machine/sensor {id}`

### **4. Error Handling**
- 404: Machine/Sensor not found
- 403: Insufficient permissions (for delete operations)
- 500: Server error (transaction rolled back)

---

## 🔍 **VERIFICATION**

### **Check if Machine/Sensor was Deleted**:
```bash
# Should return 404
curl "http://localhost:8000/api/machines/{machine_id}" \
  -H "Authorization: Bearer {token}"
```

### **Check Related Data was Deleted**:
```bash
# Should return empty list
curl "http://localhost:8000/api/sensors?machine_id={machine_id}" \
  -H "Authorization: Bearer {token}"
```

---

## 📊 **DATABASE IMPACT**

### **Tables Affected by Machine Deletion**:
- `machine` - Machine record deleted
- `sensor` - All sensors for machine deleted
- `sensor_data` - All sensor data for machine deleted
- `prediction` - All predictions for machine deleted
- `alarm` - All alarms for machine deleted
- `ticket` - All tickets for machine deleted
- `machine_state` - All state records deleted
- `machine_state_thresholds` - All thresholds deleted
- `machine_state_transition` - All transitions deleted
- `machine_state_alert` - All alerts deleted
- `machine_process_evaluation` - All evaluations deleted

### **Tables Affected by Sensor Deletion**:
- `sensor` - Sensor record deleted
- `sensor_data` - All sensor data for sensor deleted
- `prediction` - All predictions for sensor deleted
- `alarm` - All alarms for sensor deleted

---

## ✅ **SUMMARY**

**Edit Operations**:
- ✅ PATCH endpoints available for both machines and sensors
- ✅ Partial updates supported (only provided fields updated)
- ✅ Changes immediately applied to database
- ✅ No special permissions required

**Delete Operations**:
- ✅ DELETE endpoints available for both machines and sensors
- ✅ Complete cascade deletion of all related data
- ✅ Transaction-safe (rollback on error)
- ✅ Requires engineer/admin role
- ✅ Proper logging and error handling

**All operations are ready to use!**
