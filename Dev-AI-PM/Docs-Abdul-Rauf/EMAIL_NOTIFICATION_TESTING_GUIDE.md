# Email Notification Testing Guide

## 📧 **Email Notification System Status**

### **Current Configuration**

Based on your `.env` file (lines 32-33 equivalent):
- **SMTP Host**: `smtp.gmail.com`
- **SMTP Port**: `587`
- **SMTP User**: `abdul.rauf@zma-solutions.com`
- **SMTP Password**: ⚠️ **Needs to be updated with Gmail App Password**
- **Sender Email**: `abdul.rauf@zma-solutions.com`
- **Fallback Recipient**: `tanirajsingh574@gmail.com`

### **Email Notification Features**

The system sends automatic emails for:

1. ✅ **User Registration (Welcome Email)**
   - Triggered when a new user registers
   - Sent to the newly registered user's email
   - Location: `backend/app/api/routers/users.py:35`

2. ✅ **Critical/Warning AI Predictions**
   - Triggered when AI detects critical or warning predictions
   - Sent to all active email recipients from database
   - Location: `backend/app/services/notification_service.py:221`

3. ✅ **Alarm Triggers**
   - Triggered when alarms are created or updated
   - Sent to all active email recipients from database
   - Location: `backend/app/services/notification_service.py:enqueue_alarm_notification()`

4. ✅ **Machine State Changes**
   - Triggered automatically when machine state changes
   - Sent to all active email recipients from database
   - Location: `backend/app/services/machine_state_manager.py:_send_state_change_email()`

5. ✅ **Test Email Notifications**
   - Can be triggered manually from the frontend
   - Sends to all active recipients or a specific address
   - Location: `backend/app/api/routers/notifications.py:18`

---

## 🔧 **How Email Recipients Work**

### **Email Recipient Management**

The system uses a database table `emailrecipient` to manage recipients:

- **Add Recipients**: Via frontend Notifications page → "Email Recipients" section
- **Active Recipients**: Only emails with `is_active=True` receive notifications
- **Fallback**: If no active recipients, uses `NOTIFICATION_EMAIL_TO` from `.env`

### **Recipient Flow**

```
1. System needs to send email
   ↓
2. Check if to_override is provided (single recipient)
   ↓
3. If not, get all active recipients from database
   ↓
4. If no active recipients, use fallback email
   ↓
5. Send email to each recipient (continues even if one fails)
```

---

## 🧪 **Testing Email Notifications**

### **Step 1: Start the Services**

```powershell
cd "C:\Users\AbdulRauf(AIEngineer\OneDrive - Standardverzeichnis\Documents\AI_Predictive_Maintaince"
docker compose up -d
```

### **Step 2: Verify Email Configuration**

Check if email environment variables are loaded:

```powershell
docker exec ai_predictive_maintaince-backend-1 printenv | Select-String -Pattern "EMAIL|SMTP"
```

Expected output:
```
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=<your-app-password>
EMAIL_SENDER=abdul.rauf@zma-solutions.com
NOTIFICATION_EMAIL_TO=tanirajsingh574@gmail.com
```

### **Step 3: Add Email Recipients (Frontend)**

1. Open the application: http://localhost:3000
2. Navigate to **Notifications** page
3. In **Email Recipients** section, click **"+ Add Email"**
4. Add recipient email addresses
5. Ensure recipients are **Active** (green status)

### **Step 4: Test Email Sending**

#### **Option A: Test from Frontend**

1. Go to **Notifications** page
2. Scroll to **"Test Email Notifications"** section
3. Optionally enter a specific email address
4. Click **"Send Test Email"**
5. Check the response message

#### **Option B: Test via API**

```powershell
# Test email to all active recipients
curl -X POST http://localhost:8000/api/notifications/test-email

# Test email to specific address
curl -X POST http://localhost:8000/api/notifications/test-email `
  -H "Content-Type: application/json" `
  -d '{"to": "test@example.com"}'
```

### **Step 5: Check Logs**

```powershell
docker compose logs backend --tail=100 | Select-String -Pattern "email|Email|SMTP"
```

Look for:
- ✅ `Email notification sent successfully to <email>`
- ✅ `SMTP transport verified successfully`
- ❌ `SMTP transport verification failed` (authentication error)
- ❌ `Failed to send email to <email>` (delivery error)

---

## ⚠️ **Common Issues & Fixes**

### **Issue 1: "535 BadCredentials" Error**

**Symptom**: 
```
SMTP transport verification failed: (535, b'5.7.8 Username and Password not accepted...')
```

**Cause**: Using regular Gmail password instead of App Password

**Fix**:
1. Go to: https://myaccount.google.com/security
2. Enable 2-Step Verification (if not enabled)
3. Go to: https://myaccount.google.com/apppasswords
4. Generate App Password for "Mail"
5. Update `.env` file:
   ```
   EMAIL_SMTP_PASS=your-16-char-app-password
   ```
6. Restart backend:
   ```powershell
   docker compose restart backend
   ```

### **Issue 2: "No email recipients configured"**

**Symptom**: 
```
No email recipients configured, skipping email send
```

**Fix**:
1. Add email recipients via frontend Notifications page
2. Ensure recipients are marked as **Active**
3. Or set `NOTIFICATION_EMAIL_TO` in `.env` as fallback

### **Issue 3: Emails Not Sending Automatically**

**Check**:
1. Verify email recipients are active in database
2. Check backend logs for errors
3. Verify SMTP credentials are correct
4. Test email manually first

---

## 📋 **Email Notification Endpoints**

### **POST `/api/notifications/test-email`**

Test email sending functionality.

**Request Body** (optional):
```json
{
  "to": "test@example.com"  // Optional: specific email address
}
```

**Response**:
```json
{
  "ok": true,
  "message": "Test email sent successfully"
}
```

**Error Response**:
```json
{
  "ok": false,
  "error": "Error message here"
}
```

---

## 🔍 **Verification Checklist**

- [ ] Services are running (`docker compose ps`)
- [ ] Email environment variables are set in `.env`
- [ ] Gmail App Password is configured (not regular password)
- [ ] Email recipients are added and active in database
- [ ] Test email sends successfully
- [ ] Welcome emails work (register new user)
- [ ] State change emails work (machine state changes)
- [ ] Alarm emails work (alarm is triggered)
- [ ] Prediction alert emails work (AI detects anomaly)

---

## 📝 **Next Steps**

1. **Update `.env` file** with actual Gmail App Password
2. **Start services**: `docker compose up -d`
3. **Add email recipients** via frontend
4. **Test email** from Notifications page
5. **Monitor logs** for any errors

---

**Last Updated**: February 17, 2026
