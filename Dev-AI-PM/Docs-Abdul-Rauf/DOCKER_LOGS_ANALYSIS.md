# Docker Logs Analysis Report

**Date**: 2026-02-09  
**Analysis Time**: 06:37 UTC

---

## 🔴 **CRITICAL ISSUES FOUND**

### **1. MSSQL Connection Timeout (CRITICAL)**

**Error**: `pymssql.exceptions.OperationalError: Unable to connect: Adaptive Server is unavailable or does not exist (10.1.61.252)`

**Details**:
- **Server IP**: `10.1.61.252`
- **Error Code**: `20009`
- **Error Type**: Connection timeout (110)
- **Frequency**: Continuous - happening every 10 seconds when dashboard tries to fetch data

**Affected Endpoints**:
- `/api/dashboard/extruder/latest` - Getting latest extruder rows
- `/api/dashboard/extruder/derived` - Getting derived KPIs

**Impact**:
- ❌ **Dashboard cannot display MSSQL data**
- ❌ **MSSQL poller cannot fetch data** (likely same issue)
- ❌ **No sensor data ingestion**
- ❌ **No AI predictions**
- ❌ **No machine state updates**

**Root Cause**:
- MSSQL server at `10.1.61.252` is either:
  1. Not accessible from Docker container network
  2. Firewall blocking connection
  3. Server is down/offline
  4. Network routing issue

---

### **2. Service Health Status**

**Current Status**:
- ✅ **PostgreSQL**: Running (Up 2 minutes)
- ⚠️ **Backend**: Running but **UNHEALTHY** (Up 2 minutes)
- ⚠️ **AI Service**: Running but **UNHEALTHY** (Up 2 minutes)
- ✅ **Frontend**: Running (Up 2 minutes)

**Health Check Issues**:
- Backend health check is likely failing due to MSSQL connection issues
- AI service health check might be failing for other reasons

---

### **3. MSSQL Poller Status**

**✅ CONFIRMED: Poller IS Starting**
- Startup log found: `"MSSQL extruder poller started"` (2026-02-09 06:34:34)
- Config reload log found: `"MSSQL extruder poller config reloaded from DB"` (2026-02-09 06:34:34)

**⚠️ OBSERVATION: No Poller Activity Logs**
- **NO logs** showing poller attempts to fetch data
- **NO logs** showing poller errors or retries
- **NO logs** showing "MSSQL extruder tick" (successful polling)

**Possible Reasons**:
1. **Poller is disabled via runtime config** (`connections.mssql.enabled = false` in DB)
2. **Poller is missing connection settings** (host/user/password empty)
3. **Poller is in backoff period** (waiting after connection failures)
4. **Poller is sleeping** (waiting for next poll interval - default 60 seconds)

**Code Analysis**:
- Poller checks `_effective_enabled` flag (from DB setting `connections.mssql.enabled`)
- If disabled, poller sleeps 2 seconds and continues (no logs)
- If missing host/user/password, poller logs error and sleeps 5 seconds
- If connection fails, poller logs error with backoff time
- If successful, poller logs "MSSQL extruder tick" with data

**Conclusion**: Poller is likely **DISABLED** or **MISSING CONNECTION SETTINGS** in the database configuration.

---

## ✅ **WORKING COMPONENTS**

### **1. PostgreSQL Database**
- ✅ Running and accessible
- ✅ SQLAlchemy queries executing successfully
- ✅ User authentication working
- ✅ Dashboard queries to PostgreSQL working

### **2. AI Service**
- ✅ Service started successfully
- ✅ Health checks responding (200 OK)
- ✅ Ready to receive prediction requests

### **3. Backend API**
- ✅ FastAPI application started
- ✅ Database connections working
- ✅ User authentication working
- ✅ API endpoints responding (except MSSQL-dependent ones)

### **4. Frontend**
- ✅ Running on port 3000
- ✅ Connected to backend

---

## 📊 **LOG PATTERNS OBSERVED**

### **Repeated Errors** (Every 10 seconds):
```
2026-02-09 06:36:XX.XXX | ERROR | app.api.routers.dashboard:get_extruder_latest_rows:237 - MSSQL extruder read failed
pymssql.exceptions.OperationalError: (20009, b'DB-Lib error message 20009, severity 9:\nUnable to connect: Adaptive Server is unavailable or does not exist (10.1.61.252)\nNet-Lib error during Connection timed out (110)\n')
```

### **Successful Operations**:
- SQLAlchemy queries to PostgreSQL
- User authentication
- Health checks (except MSSQL-dependent)

---

## 🔍 **MISSING INFORMATION**

### **What We Need to Check**:

1. **MSSQL Poller Startup**:
   - Is `mssql_extruder_poller.start(loop)` being called?
   - Are there any startup logs?
   - Is poller disabled via `MSSQL_ENABLED=false`?

2. **Environment Variables**:
   - What is `MSSQL_HOST` set to?
   - What is `MSSQL_PORT` set to?
   - What is `MSSQL_ENABLED` set to?

3. **Network Connectivity**:
   - Can Docker container reach `10.1.61.252:1433`?
   - Is there a firewall rule blocking it?
   - Is MSSQL server configured to accept remote connections?

---

## 🛠️ **RECOMMENDED ACTIONS**

### **Immediate Actions**:

1. **Verify MSSQL Server Accessibility**:
   ```bash
   # From Docker container
   docker-compose exec backend ping 10.1.61.252
   docker-compose exec backend telnet 10.1.61.252 1433
   ```

2. **Check Environment Variables**:
   ```bash
   docker-compose exec backend env | grep MSSQL
   ```

3. **Check MSSQL Poller Status**:
   ```bash
   docker-compose logs backend | grep -i "MSSQL\|extruder\|poller"
   ```

4. **Verify Poller is Starting**:
   - Check `backend/app/main.py` startup event
   - Verify `mssql_extruder_poller.start(loop)` is called
   - Check if `MSSQL_ENABLED` is set to false

### **Configuration Fixes**:

1. **If MSSQL Server is Unreachable**:
   - Check network configuration
   - Verify firewall rules
   - Consider using host network mode: `network_mode: "host"`

2. **If MSSQL Server is Down**:
   - Start/restart MSSQL server
   - Verify server is listening on port 1433
   - Check SQL Server configuration for remote connections

3. **If Poller is Not Starting**:
   - Check `MSSQL_ENABLED` environment variable
   - Verify startup code in `main.py`
   - Check for silent exceptions in poller initialization

---

## 📝 **SUMMARY**

### **Current State**:
- ✅ **Application Infrastructure**: Working
- ✅ **PostgreSQL Database**: Working
- ✅ **Backend API**: Working (except MSSQL endpoints)
- ✅ **AI Service**: Working
- ✅ **Frontend**: Working
- ❌ **MSSQL Connection**: **FAILING** (Critical)
- ❌ **MSSQL Poller**: **NOT VERIFIED** (No logs found)
- ❌ **Data Ingestion**: **BLOCKED** (Cannot fetch from MSSQL)
- ❌ **Machine State Detection**: **BLOCKED** (No data to process)
- ❌ **AI Predictions**: **BLOCKED** (No data to predict)

### **Primary Blocker**:
**MSSQL server at `10.1.61.252:1433` is not accessible from Docker container.**

### **Next Steps**:

1. **Check MSSQL Poller Configuration**:
   ```sql
   -- Check if poller is enabled in database
   SELECT key, value FROM setting WHERE key = 'connections.mssql';
   ```
   - If `enabled: false`, enable it via UI or database
   - If missing, create the setting

2. **Verify MSSQL Connection Settings**:
   ```bash
   # Check environment variables
   docker-compose exec backend env | grep MSSQL
   ```
   - Verify `MSSQL_HOST`, `MSSQL_USER`, `MSSQL_PASSWORD` are set
   - If empty, set them in `.env` file or environment

3. **Fix MSSQL Connectivity**:
   - Verify MSSQL server at `10.1.61.252:1433` is accessible
   - Check network/firewall rules
   - Test connection from Docker container

4. **Monitor Poller Activity**:
   ```bash
   # Watch for poller logs
   docker-compose logs -f backend | grep -i "MSSQL extruder"
   ```

5. **Once Connected**:
   - Verify data ingestion flow
   - Verify machine state detection is working
   - Check for "MSSQL extruder tick" logs

---

## 🔗 **RELATED FILES**

- `backend/app/services/mssql_extruder_poller.py` - MSSQL poller implementation
- `backend/app/main.py` - Application startup (poller initialization)
- `docker-compose.yml` - Environment variables configuration
- `backend/app/api/routers/dashboard.py` - Dashboard endpoints (MSSQL errors)
