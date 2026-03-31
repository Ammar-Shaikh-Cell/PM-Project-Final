# Outlook/Microsoft 365 SMTP Configuration

## 📧 **SMTP Settings for abdul.rauf@zma-solutions.com**

Since this is an Outlook email (Microsoft 365), use these settings:

### **Required `.env` Configuration:**

```bash
EMAIL_SMTP_HOST=smtp.office365.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=<your-outlook-password>
EMAIL_SENDER=abdul.rauf@zma-solutions.com
NOTIFICATION_EMAIL_TO=<optional-fallback-email>
```

---

## 🔐 **Password Options**

### **Option 1: Regular Password (if 2FA is NOT enabled)**
- Use your regular Outlook/Microsoft 365 password
- Set it directly in `EMAIL_SMTP_PASS`

### **Option 2: App Password (if 2FA IS enabled)**
If you have **Two-Factor Authentication (2FA)** enabled, you **must** use an App Password:

1. Go to: https://account.microsoft.com/security
2. Sign in with your Microsoft account
3. Go to **"Security"** → **"Advanced security options"**
4. Under **"App passwords"**, click **"Create a new app password"**
5. Select **"Mail"** and your device
6. Copy the generated password (it will look like: `abcd-efgh-ijkl-mnop`)
7. Use this App Password in `EMAIL_SMTP_PASS`

**Note:** If 2FA is enabled, your regular password will NOT work for SMTP.

---

## ⚙️ **Complete Configuration Steps**

### **Step 1: Update `.env` File**

Add or update these lines in your `.env` file:

```bash
# Email Notifications (Microsoft 365 / Outlook)
EMAIL_SMTP_HOST=smtp.office365.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=your-password-here
EMAIL_SENDER=abdul.rauf@zma-solutions.com
NOTIFICATION_EMAIL_TO=fallback@example.com
```

### **Step 2: Restart Backend Service**

```powershell
docker compose restart backend
```

### **Step 3: Test Email Sending**

1. Open: http://localhost:3000
2. Go to **Notifications** page
3. Click **"Send Test Email"**
4. Check for success/error messages

### **Step 4: Check Logs (if errors occur)**

```powershell
docker compose logs backend --tail=50 | Select-String -Pattern "email|SMTP|outlook"
```

---

## ⚠️ **Common Issues & Solutions**

### **Issue 1: "535 Authentication Failed"**

**Cause:** 
- Wrong password
- 2FA is enabled but using regular password instead of App Password

**Solution:**
- If 2FA is enabled, generate an App Password and use that
- Verify the password is correct (no extra spaces)

### **Issue 2: "Connection Timeout"**

**Cause:** 
- Firewall blocking port 587
- Network restrictions

**Solution:**
- Ensure port 587 is open
- Check if your network allows SMTP connections
- Try from a different network if possible

### **Issue 3: "Relay Access Denied"**

**Cause:** 
- Microsoft 365 security policies
- Account restrictions

**Solution:**
- Contact your IT administrator
- Verify SMTP AUTH is enabled for your account (admin may need to enable it)

---

## 🔍 **Verify SMTP AUTH is Enabled**

Some Microsoft 365 accounts have SMTP AUTH disabled by default. To check:

1. Contact your IT administrator
2. Ask them to verify SMTP AUTH is enabled for your account
3. Or check in Microsoft 365 admin center:
   - Go to **Users** → **Active users**
   - Select your account
   - Check **"Mail"** settings
   - Ensure **"SMTP AUTH"** is enabled

---

## 📋 **Quick Reference**

| Setting | Value |
|---------|-------|
| SMTP Host | `smtp.office365.com` |
| Port | `587` |
| Encryption | `STARTTLS` (automatic) |
| Username | `abdul.rauf@zma-solutions.com` |
| Password | Your password or App Password |
| Sender | `abdul.rauf@zma-solutions.com` |

---

## ✅ **Testing Checklist**

- [ ] Updated `.env` file with Outlook SMTP settings
- [ ] Used App Password if 2FA is enabled
- [ ] Restarted backend service
- [ ] Tested email from frontend
- [ ] Checked logs for errors
- [ ] Verified SMTP AUTH is enabled (if needed)

---

## 🚀 **Next Steps**

1. **Update your `.env` file** with the settings above
2. **Get your password** (regular or App Password if 2FA enabled)
3. **Restart the backend**
4. **Test email sending** from the Notifications page
5. **Add email recipients** via the frontend if needed

---

**Last Updated:** February 17, 2026
