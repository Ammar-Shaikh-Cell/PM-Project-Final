# Machine Delete 500 Error Fix

## 🔴 **ERROR IDENTIFIED**

### **Error Message**:
```
sqlalchemy.exc.ProgrammingError: column machine_state.updated_at does not exist
HINT:  Perhaps you meant to reference the column "machine_state.created_at".
```

### **Root Cause**:
- The `MachineState` model inherits from `Base` which defines `updated_at`
- But the actual database table `machine_state` does NOT have an `updated_at` column
- When using ORM `delete()`, SQLAlchemy tries to query records first, which includes selecting `updated_at`
- This causes the error because the column doesn't exist in the database

---

## ✅ **FIX APPLIED**

### **Solution**: Use Raw SQL Instead of ORM Delete

Changed from ORM `delete()` to raw SQL `text()` queries to avoid ORM trying to access non-existent columns.

### **Before (Causing Error)**:
```python
await session.execute(
    delete(MachineState).where(MachineState.machine_uuid == machine_id)
)
```

### **After (Fixed)**:
```python
await session.execute(
    text("DELETE FROM machine_state WHERE machine_uuid = :machine_id"),
    {"machine_id": machine_id}
)
```

---

## 🔧 **CHANGES MADE**

### **File**: `backend/app/services/machine_service.py`

**Updated Delete Function**:
1. **MachineState**: Uses raw SQL with `machine_uuid` (UUID foreign key)
2. **MachineStateThresholds**: Uses raw SQL with `machine_uuid`
3. **MachineStateTransition**: Uses raw SQL with both `machine_uuid` and `machine_id` (string)
4. **MachineStateAlert**: Uses raw SQL with both `machine_uuid` and `machine_id` (string)
5. **MachineProcessEvaluation**: Uses raw SQL with both `machine_uuid` and `machine_id` (string)

**Why Raw SQL?**:
- Avoids ORM trying to access `updated_at` column that doesn't exist
- Direct database operations are faster
- More control over the delete operation

---

## 📊 **DELETE ORDER**

The delete operations are performed in this order to avoid foreign key constraint violations:

1. **Sensor Data** - No dependencies
2. **Predictions** - References sensors and machines
3. **Alarms** - References machines, sensors, predictions
4. **Tickets** - References machines and alarms
5. **Machine State Data** - References machines
   - MachineState
   - MachineStateThresholds
   - MachineStateTransition
   - MachineStateAlert
   - MachineProcessEvaluation
6. **Sensors** - References machines (CASCADE will handle sensor_data)
7. **Machine** - Finally delete the machine itself

---

## 🧪 **TESTING**

### **Test Delete Operation**:

1. **Try to delete a machine**:
   - Go to Machines tab
   - Click Delete button
   - Confirm deletion

2. **Expected Result**:
   - ✅ No 500 error
   - ✅ Machine deleted successfully
   - ✅ All related data deleted
   - ✅ Success message displayed

3. **Check Backend Logs**:
   ```bash
   docker-compose logs backend | grep -i "delete\|machine"
   ```
   - Should see: "Successfully deleted machine {id} ({name}) and all related data"

4. **Verify Deletion**:
   - Machine should disappear from list
   - Machine should not appear after page refresh
   - Related sensors should also be deleted

---

## 🔍 **TROUBLESHOOTING**

### **If Still Getting 500 Error**:

1. **Check Backend Logs**:
   ```bash
   docker-compose logs backend --tail=100 | grep -i "error\|exception"
   ```

2. **Verify Database Schema**:
   - Check if `machine_state` table has `updated_at` column
   - If it does, the fix might need adjustment

3. **Check Foreign Key Constraints**:
   - Verify all foreign keys have proper CASCADE settings
   - Check if any constraints are preventing deletion

4. **Verify User Permissions**:
   - Delete requires Engineer/Admin role
   - Check if user has correct permissions

---

## 📝 **TECHNICAL DETAILS**

### **Why MachineState Doesn't Have updated_at**:

Looking at the model definition:
```python
class MachineState(Base):
    # ...
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    # NO updated_at column!
```

But `Base` class defines:
```python
class Base(DeclarativeBase):
    updated_at = Column(DateTime(timezone=True), ...)
```

This mismatch causes the issue when ORM tries to query the table.

### **Solution Benefits**:
- ✅ Avoids ORM column access issues
- ✅ Direct SQL is faster
- ✅ More explicit control
- ✅ Works regardless of ORM model definition

---

## ✅ **SUMMARY**

**Problem**: 500 error when deleting machine due to ORM trying to access non-existent `updated_at` column

**Solution**: Use raw SQL queries instead of ORM `delete()` for machine_state tables

**Result**: Machine deletion should now work without errors

---

## 🎯 **NEXT STEPS**

1. **Test the fix**:
   - Try deleting a machine
   - Verify no 500 error
   - Check that machine and related data are deleted

2. **Monitor logs**:
   - Check for any other errors
   - Verify deletion is working correctly

3. **If issues persist**:
   - Check backend logs for specific error messages
   - Verify database schema matches model definitions
