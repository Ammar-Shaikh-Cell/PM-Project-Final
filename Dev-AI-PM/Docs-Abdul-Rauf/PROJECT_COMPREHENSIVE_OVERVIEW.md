# Predictive Maintenance Platform - Comprehensive Project Overview

**Date:** February 12, 2026  
**Status:** Production-Ready Industrial IoT Platform

---

## 📋 Executive Summary

The **Predictive Maintenance Platform** is a comprehensive Industrial IoT (IIoT) solution designed for real-time monitoring, anomaly detection, and predictive maintenance of industrial machinery. The platform combines sensor data ingestion, AI-powered analytics, baseline learning, and a modern web interface to enable proactive maintenance strategies.

### Key Highlights

- **Microservices Architecture**: FastAPI backend, React frontend, AI service, TimescaleDB database
- **Real-time Data Processing**: MSSQL-based sensor data ingestion with 3-second polling
- **AI-Powered Analytics**: Isolation Forest anomaly detection with rule-based fallback
- **Baseline Learning System**: Profile-based baseline collection and evaluation
- **Machine State Detection**: Automatic state detection (IDLE, PRODUCTION, SETUP, etc.)
- **Modern Dashboard**: Real-time visualization with baseline comparison and stability indicators
- **Enterprise Features**: RBAC, audit logging, webhooks, reporting, ticket system

---

## 🏗️ System Architecture

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Frontend** | React 18 + TypeScript + Vite + Tailwind CSS | Modern web dashboard |
| **Backend** | FastAPI + Python 3.11 + async SQLAlchemy | REST API and business logic |
| **Database** | TimescaleDB (PostgreSQL 15) | Time-series data storage |
| **AI Service** | FastAPI + scikit-learn (Isolation Forest) | Anomaly detection |
| **Data Source** | MSSQL Server | Real-time sensor data (extruder) |
| **Containerization** | Docker + Docker Compose | Service orchestration |

### Service Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Frontend   │────▶│   Backend   │────▶│  Database  │
│  (React)    │◀────│  (FastAPI)  │◀────│ (Timescale)│
└─────────────┘     └─────────────┘     └─────────────┘
                            │
                            ├────▶ AI Service (FastAPI)
                            │
                            ├────▶ MSSQL Server (Data Source)
                            │
                            └────▶ Edge Gateway (Optional)
```

### Docker Services

1. **postgres** - TimescaleDB database (Port 5432)
2. **backend** - FastAPI application (Port 8000)
3. **ai-service** - ML inference service (Port 8001)
4. **frontend** - React app with Nginx (Port 3000)
5. **mqtt** - Eclipse Mosquitto broker (Port 1883, optional)
6. **simulator** - MQTT data generator (optional)

---

## 📁 Project Structure

### Backend (`backend/`)

**Core Application**:
- `app/main.py` - FastAPI application entry point
- `app/core/` - Configuration and security (JWT, settings)
- `app/db/` - Database session and base models
- `app/api/routers/` - 27 API endpoint modules
- `app/models/` - 20 SQLAlchemy database models
- `app/schemas/` - Pydantic request/response schemas
- `app/services/` - 23 business logic services
- `app/utils/` - Utility functions (baseline formatter, etc.)

**Key Routers**:
- `dashboard.py` - Dashboard endpoints with baseline evaluation
- `machines.py` - Machine CRUD operations
- `sensors.py` - Sensor management
- `predictions.py` - AI predictions
- `alarms.py` - Alarm management
- `tickets.py` - Ticket system
- `profiles.py` - Profile and baseline management
- `machine_state.py` - Machine state detection
- `ai.py` - AI service integration
- `health.py` - Health checks

**Key Services**:
- `mssql_extruder_poller.py` - MSSQL data ingestion
- `baseline_learning_service.py` - Baseline collection and computation
- `machine_state_service.py` - State detection logic
- `extruder_ai_service.py` - AI integration for extruder
- `alarm_service.py` - Alarm generation and management
- `prediction_service.py` - Prediction orchestration

**Database Models**:
- `machine.py` - Machine definitions
- `sensor.py` - Sensor definitions
- `sensor_data.py` - Time-series sensor readings (hypertable)
- `prediction.py` - AI predictions
- `alarm.py` - Generated alarms
- `ticket.py` - Maintenance tickets
- `profile.py` - Material profiles with baseline stats
- `machine_state.py` - Machine state history
- `user.py` - User accounts with RBAC
- `audit_log.py` - Audit trail

### Frontend (`frontend/`)

**Core Application**:
- `src/main.tsx` - Application entry point
- `src/App.tsx` - Root component with routing
- `src/pages/` - Page components (Dashboard, Machines, Sensors, etc.)
- `src/components/` - 27 reusable components
- `src/api/` - API client modules (Axios-based)
- `src/hooks/` - Custom React hooks (useLiveData, useWebSocket, useSSE)
- `src/contexts/` - React contexts (AuthContext)
- `src/store/` - Zustand state management
- `src/i18n/` - Internationalization (German support)

**Key Components**:
- `SensorChart.tsx` - Reusable sensor chart with baseline visualization
- `Dashboard.tsx` - Main dashboard with real-time updates
- `Topbar.tsx` - Navigation bar
- `LoadingSkeleton.tsx` - Loading states
- `ErrorToast.tsx` - Error notifications

**Key Pages**:
- `Dashboard.tsx` - Main monitoring dashboard
- `Machines.tsx` - Machine management
- `Sensors.tsx` - Sensor management
- `Predictions.tsx` - Prediction history
- `Alarms.tsx` - Alarm management
- `Tickets.tsx` - Ticket system
- `AIService.tsx` - AI service status

### AI Service (`ai_service/`)

**Core Files**:
- `main.py` - FastAPI service with prediction engine
- `features.py` - Feature engineering (25 features)
- `model_manager.py` - Model loading and management
- `train_model.py` - Model training script
- `models/` - Trained models (isolation_forest.pkl, scaler.pkl)

**Features**:
- Isolation Forest anomaly detection
- Rule-based fallback system
- 25-feature engineering pipeline
- Hysteresis and dwell-time for stability
- Performance tracking and metrics

### Edge Components (Optional)

- `edge_gateway/` - OPC UA to MQTT gateway
- `edge_ai/` - Edge AI processing
- `simulator/` - MQTT data simulator
- `mqtt/` - MQTT broker configuration

---

## 🔄 Data Flow & Processing

### 1. Sensor Data Ingestion

```
MSSQL Server → MSSQL Extruder Poller → Backend → Database
```

**Process**:
1. MSSQL poller queries `Tab_Actual` table every 3 seconds
2. Extracts sensor values: `ScrewSpeed_rpm`, `Pressure_bar`, `Temp_Zone1_C..4_C`
3. Stores in `sensor_data` hypertable (TimescaleDB)
4. Triggers machine state detection
5. Collects baseline samples (if learning mode)
6. Triggers AI predictions
7. Generates alarms (if thresholds exceeded)

### 2. Machine State Detection

```
Sensor Data → State Service → State Detection → State History
```

**States**:
- **IDLE** - No production activity
- **PRODUCTION** - Active production
- **SETUP** - Machine setup/configuration
- **MAINTENANCE** - Maintenance mode
- **ERROR** - Error state

**Detection Logic**:
- Analyzes sensor patterns (screw speed, pressure, temperature)
- Uses thresholds and time windows
- Stores state transitions in `machine_state` table

### 3. Baseline Learning Lifecycle

```
Start Learning → Collect Samples → Finalize Baseline → Use Baseline
```

**Process**:
1. **Start Learning**: Set `baseline_learning = true` for profile
2. **Collect Samples**: Store samples in `profile_baseline_samples` (only during PRODUCTION)
3. **Finalize Baseline**: Compute statistics (mean, std, p05, p95) → `profile_baseline_stats`
4. **Use Baseline**: Set `baseline_ready = true`, use for evaluation

**Sample Collection**:
- Only during `PRODUCTION` state
- Minimum 100 samples per metric required
- Metrics: `ScrewSpeed_rpm`, `Pressure_bar`, `Temp_Zone1_C..4_C`, `Temp_Avg`, `Temp_Spread`

### 4. Baseline Evaluation

```
Current Value → Compare with Baseline → Calculate Severity → Apply Decision Hierarchy
```

**Evaluation Rules**:
- **GREEN (0)**: Value within baseline band (min-max)
- **ORANGE (1)**: Value 3-5% outside baseline
- **RED (2)**: Value >5% outside baseline

**Decision Hierarchy**:
1. Machine State Gate (must be PRODUCTION)
2. Material Rule-Based Thresholds (3-5% rule)
3. Stability Indicators (time spread analysis)
4. ML Signal (informational only, doesn't change status)

**Stability Analysis**:
- 10-minute sliding window for `current_std`
- Ratio = `current_std / baseline_std`
- Thresholds: ≤1.2 (green), 1.2-1.6 (orange), >1.6 (red)

### 5. AI Prediction Workflow

```
Sensor Data → Feature Extraction → AI Service → Prediction → Alarm (if needed)
```

**Process**:
1. Backend extracts 25 features from sensor window
2. Sends to AI service (`/predict` endpoint)
3. AI service processes with Isolation Forest
4. Returns anomaly score, confidence, prediction class
5. Backend stores prediction
6. Alarm created if prediction indicates anomaly
7. Ticket auto-created for critical alarms

### 6. Dashboard Updates

```
Frontend → API Polling (3s) → Backend → Database → Response → UI Update
```

**Real-time Features**:
- 3-second auto-refresh interval
- Parallel API requests for performance
- Cached responses (10s TTL)
- WebSocket/SSE support (optional)
- Offline mode with fallback data

---

## 🎯 Core Features

### 1. Real-Time Monitoring

- **Live Sensor Data**: 3-second polling from MSSQL
- **Dashboard**: Real-time charts with baseline comparison
- **Machine States**: Automatic state detection and visualization
- **Status Indicators**: Green/Orange/Red status for each sensor

### 2. Baseline Learning & Evaluation

- **Profile-Based Baselines**: Material-specific baseline learning
- **Automatic Collection**: Samples collected during PRODUCTION
- **Baseline Statistics**: Mean, std, p05, p95 percentiles
- **Evaluation**: 3-5% rule for ORANGE/RED status
- **Stability Analysis**: Time spread early-warning system

### 3. AI-Powered Anomaly Detection

- **Isolation Forest**: Unsupervised anomaly detection
- **25 Features**: Statistical and temporal feature extraction
- **Rule-Based Fallback**: Threshold-based detection
- **Confidence Scoring**: Prediction confidence levels
- **RUL Estimation**: Remaining Useful Life calculation

### 4. Machine State Management

- **Automatic Detection**: Pattern-based state detection
- **State History**: Complete state transition tracking
- **State-Based Logic**: Evaluation only during PRODUCTION
- **State Alerts**: Alerts for state transitions

### 5. Alarm & Ticket System

- **Automatic Alarms**: Threshold violations, AI anomalies
- **Severity Levels**: Info, Warning, Critical
- **Status Tracking**: Active, Resolved, Acknowledged
- **Auto-Tickets**: Critical alarms create tickets
- **Comments & Attachments**: Collaborative workflow

### 6. Dashboard Visualization

- **Sensor Charts**: Baseline bands, mean lines, live curves
- **Material Markers**: Vertical markers for material changes
- **Stability Indicators**: Dot indicators for stability status
- **Status Colors**: Green/Orange/Red based on severity
- **Deviation Display**: Percentage and absolute deviation

### 7. User Management & Security

- **JWT Authentication**: Access + refresh tokens
- **RBAC**: Admin, Engineer, Viewer roles
- **Password Security**: bcrypt hashing
- **Audit Logging**: Comprehensive activity tracking
- **Session Management**: Token refresh and logout

### 8. Reporting & Analytics

- **PDF Reports**: Generated reports with charts
- **CSV Exports**: Data export functionality
- **Date Range Filtering**: Custom time ranges
- **Machine-Specific Reports**: Per-machine analytics
- **Historical Trends**: Long-term analysis

### 9. System Integration

- **Webhooks**: Event-driven external integration
- **Email Notifications**: Gmail SMTP support
- **API Documentation**: Swagger/OpenAPI
- **Health Monitoring**: Comprehensive status endpoints
- **Metrics**: Prometheus-compatible metrics

---

## 📊 Database Schema

### Core Tables

**Machines & Sensors**:
- `machine` - Machine definitions
- `sensor` - Sensor definitions
- `sensor_data` - Time-series readings (hypertable)

**AI & Predictions**:
- `prediction` - AI predictions
- `model_registry` - Model versions

**Operations**:
- `alarm` - Generated alarms
- `ticket` - Maintenance tickets
- `comment` - Comments on alarms/tickets
- `attachment` - File attachments

**Baseline & Profiles**:
- `profile` - Material profiles
- `profile_baseline_stats` - Baseline statistics
- `profile_baseline_samples` - Learning samples (temporary)

**Machine State**:
- `machine_state` - State history
- `machine_state_thresholds` - State detection thresholds
- `machine_state_alert` - State alerts

**System**:
- `user` - User accounts
- `role` - RBAC roles
- `audit_log` - Audit trail
- `settings` - System configuration
- `webhook` - Webhook configurations
- `job` - Background jobs

---

## 🔧 Configuration

### Environment Variables

**Backend** (`backend/.env`):
```bash
# Database
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=pm_user
POSTGRES_PASSWORD=pm_pass
POSTGRES_DB=pm_db

# MSSQL (Data Source)
MSSQL_HOST=10.1.61.252
MSSQL_PORT=1433
MSSQL_USER=username
MSSQL_PASSWORD=password
MSSQL_DATABASE=HISTORISCH
MSSQL_TABLE=Tab_Actual
MSSQL_ENABLED=true

# JWT
JWT_SECRET=your-secret-key
JWT_ALGORITHM=HS256
JWT_EXP_MINUTES=60

# AI Service
AI_SERVICE_URL=http://ai-service:8000

# Email (Optional)
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=your-email@gmail.com
EMAIL_SMTP_PASS=app-password
```

### Docker Compose

**Services**:
- `postgres` - TimescaleDB
- `backend` - FastAPI backend
- `ai-service` - AI service
- `frontend` - React frontend

**Networks**:
- `pm-net` - Bridge network for all services

**Volumes**:
- `postgres-data` - Database persistence

---

## 🚀 Deployment

### Quick Start

```bash
# Build and start all services
docker-compose up --build -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f

# Access application
# Frontend: http://localhost:3000
# Backend API: http://localhost:8000
# API Docs: http://localhost:8000/docs
```

### Production Deployment

1. **Configure Environment**: Set production environment variables
2. **Security**: Change JWT_SECRET, use strong passwords
3. **HTTPS**: Configure reverse proxy with SSL/TLS
4. **Monitoring**: Set up health check monitoring
5. **Backups**: Configure database backups
6. **Scaling**: Use load balancer for multiple instances

---

## 📈 Key Metrics & Monitoring

### Dashboard Metrics

- **Machine Count**: Total and online machines
- **Sensor Count**: Total sensors
- **Active Alarms**: Open/acknowledged alarms
- **Recent Predictions**: Last 24 hours
- **Process Status**: Overall system status (green/orange/red)

### System Health

- **Backend Health**: `/health`, `/health/ready`, `/health/live`
- **AI Service Health**: `/ai/status`
- **Database Status**: Connection and query performance
- **MSSQL Status**: Connection and data freshness
- **System Metrics**: `/metrics` (Prometheus-compatible)

---

## 🔐 Security Features

- **JWT Authentication**: Secure token-based auth
- **Password Hashing**: bcrypt with salt
- **RBAC**: Role-based access control
- **SQL Injection Protection**: SQLAlchemy ORM
- **CORS Configuration**: Controlled cross-origin access
- **Input Validation**: Pydantic schemas
- **Audit Logging**: Complete activity tracking

---

## 📝 API Documentation

### Key Endpoints

**Dashboard**:
- `GET /dashboard/overview` - Overview statistics
- `GET /dashboard/current` - Current sensor status with baselines
- `GET /dashboard/extruder/latest` - Latest MSSQL data
- `GET /dashboard/extruder/derived` - Derived metrics

**Machines**:
- `GET /machines` - List machines
- `POST /machines` - Create machine
- `GET /machines/{id}` - Get machine details

**Sensors**:
- `GET /sensors` - List sensors
- `GET /sensors/{id}/trend` - Sensor trend data

**Predictions**:
- `GET /predictions` - List predictions
- `POST /predictions/trigger` - Trigger prediction

**Alarms**:
- `GET /alarms` - List alarms
- `POST /alarms/{id}/resolve` - Resolve alarm

**Profiles**:
- `GET /profiles` - List profiles
- `POST /profiles/{id}/start-learning` - Start baseline learning
- `POST /profiles/{id}/finalize` - Finalize baseline

**Machine State**:
- `GET /machine-state/states/current` - Current machine states
- `GET /machine-state/states/history` - State history

### Interactive Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

---

## 🧪 Testing

### Manual Testing

1. **Health Checks**: Verify all services are healthy
2. **Authentication**: Test login/logout with demo users
3. **Dashboard**: Verify real-time data updates
4. **Baseline Learning**: Start learning, collect samples, finalize
5. **Machine States**: Verify state detection
6. **Alarms**: Trigger alarms and verify generation
7. **Reports**: Generate and download reports

### Demo Users

- **Admin**: `admin@example.com` / `admin123`
- **Engineer**: `engineer@example.com` / `engineer123`
- **Viewer**: `viewer@example.com` / `viewer123`

---

## 🐛 Known Issues & Limitations

### Current Limitations

1. **MSSQL Connection**: Requires network access to MSSQL server
2. **Baseline Learning**: Requires minimum 100 samples per metric
3. **State Detection**: Requires sufficient sensor data for accuracy
4. **AI Model**: Requires trained model files in `ai_service/models/`

### Workarounds

- MSSQL connection timeouts handled gracefully
- Baseline learning can be manually triggered
- State detection uses fallback logic if data insufficient
- AI service falls back to rule-based if model unavailable

---

## 📚 Documentation Files

### Main Documentation

- `README.md` - Quick start and overview
- `PROJECT_STRUCTURE.md` - Detailed project structure
- `PRODUCT_SUMMARY.md` - Complete product documentation
- `WORK_STATUS_TODAY.md` - Latest implementation status
- `ALL_SERVICES_ENDPOINTS.md` - Complete API endpoint list

### Implementation Guides

- `BASELINE_LEARNING_IMPLEMENTATION.md` - Baseline learning details
- `BASELINE_STRUCTURE_IMPLEMENTATION.md` - Baseline structure
- `MACHINE_STATE_DETECTION.md` - State detection logic
- `CHART_RENDERING_ANALYSIS.md` - Chart implementation

### Deployment Guides

- `DEPLOYMENT_QUICK_START.md` - Quick deployment guide
- `NETCUP_DEPLOYMENT.md` - Production deployment
- `QUICK_START.md` - Development setup

---

## 🎯 Future Enhancements

### Potential Improvements

1. **Performance**:
   - Redis caching for frequently accessed data
   - Database query optimization
   - Frontend code splitting

2. **Features**:
   - Advanced ML models (LSTM, Transformer)
   - Multi-machine dashboard
   - Custom alert rules
   - Mobile app support

3. **Integration**:
   - OPC UA direct integration
   - MQTT broker re-enablement
   - Additional data sources
   - Third-party system integration

4. **Analytics**:
   - Advanced trend analysis
   - Predictive maintenance scheduling
   - Cost optimization recommendations
   - Performance benchmarking

---

## ✨ Summary

The Predictive Maintenance Platform is a **production-ready, enterprise-grade** Industrial IoT solution with:

✅ **Complete Feature Set**: Real-time monitoring, AI analytics, baseline learning, state detection  
✅ **Modern Architecture**: Microservices, Docker, async processing  
✅ **Scalable Design**: TimescaleDB, efficient queries, caching  
✅ **Security**: JWT auth, RBAC, audit logging  
✅ **User Experience**: Modern React dashboard, real-time updates  
✅ **Documentation**: Comprehensive guides and API docs  

**Status**: All core features implemented and working. System ready for production deployment.

---

**Version**: 1.0.0  
**Last Updated**: February 12, 2026  
**License**: Proprietary
