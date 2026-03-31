# SMTP Settings Guide for abdul.rauf@zma-solutions.com

## 🔍 **Finding Your SMTP Settings**

Since `abdul.rauf@zma-solutions.com` is a custom domain email, the SMTP settings depend on your email hosting provider. Here are the most common scenarios:

---

## 📧 **Option 1: Google Workspace (G Suite)**

If your email is hosted by Google Workspace:

### **SMTP Settings:**
```bash
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=<your-app-password>  # Must use App Password, not regular password
EMAIL_SENDER=abdul.rauf@zma-solutions.com
```

### **How to Get App Password:**
1. Go to: https://myaccount.google.com/security
2. Enable **2-Step Verification** (if not already enabled)
3. Go to: https://myaccount.google.com/apppasswords
4. Select **"Mail"** and your device
5. Copy the **16-character password** (no spaces)
6. Use this password in `EMAIL_SMTP_PASS`

---

## 📧 **Option 2: Microsoft 365 / Outlook**

If your email is hosted by Microsoft 365:

### **SMTP Settings:**
```bash
EMAIL_SMTP_HOST=smtp.office365.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=<your-password>  # Your regular password or App Password
EMAIL_SENDER=abdul.rauf@zma-solutions.com
```

### **How to Get App Password (if 2FA enabled):**
1. Go to: https://account.microsoft.com/security
2. Enable **Two-step verification** (if not already enabled)
3. Go to: https://account.microsoft.com/security/app-passwords
4. Create a new app password for "Mail"
5. Use this password in `EMAIL_SMTP_PASS`

---

## 📧 **Option 3: Custom Mail Server**

If you have a custom mail server, you need to contact your IT administrator or hosting provider.

### **Common SMTP Settings:**
```bash
EMAIL_SMTP_HOST=mail.zma-solutions.com  # or smtp.zma-solutions.com
EMAIL_SMTP_PORT=587  # or 465 for SSL, 25 for non-encrypted
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=<your-password>
EMAIL_SENDER=abdul.rauf@zma-solutions.com
```

### **How to Find:**
1. Check your email client settings (Outlook, Thunderbird, etc.)
2. Contact your hosting provider
3. Check your hosting control panel (cPanel, Plesk, etc.)
4. Look in your email provider's documentation

---

## 📧 **Option 4: Other Common Providers**

### **Zoho Mail:**
```bash
EMAIL_SMTP_HOST=smtp.zoho.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=<your-password>
```

### **SendGrid:**
```bash
EMAIL_SMTP_HOST=smtp.sendgrid.net
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=apikey
EMAIL_SMTP_PASS=<your-sendgrid-api-key>
```

### **Mailgun:**
```bash
EMAIL_SMTP_HOST=smtp.mailgun.org
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=<your-mailgun-username>
EMAIL_SMTP_PASS=<your-mailgun-password>
```

---

## 🔍 **How to Find Your SMTP Settings**

### **Method 1: Check Your Email Client**

If you use Outlook, Thunderbird, or Apple Mail:

1. Open your email client
2. Go to **Account Settings** or **Preferences**
3. Find your email account
4. Look for **Outgoing Mail Server (SMTP)** settings
5. Note down:
   - Server name/host
   - Port number
   - Username
   - Authentication method

### **Method 2: Check Your Email Provider's Documentation**

1. Google: https://support.google.com/mail/answer/7126229
2. Microsoft 365: https://support.microsoft.com/en-us/office/pop-imap-and-smtp-settings-8361e398-8af4-4e97-b147-6c6c4ac95353
3. Zoho: https://www.zoho.com/mail/help/zoho-mail-smtp-configuration.html

### **Method 3: Contact Your IT Administrator**

If you're in an organization, contact your IT department for:
- SMTP server address
- Port number
- Authentication requirements
- Any special settings

### **Method 4: Check Your Hosting Control Panel**

If you manage your own domain:
1. Log into your hosting control panel (cPanel, Plesk, etc.)
2. Look for **Email Accounts** or **Mail Settings**
3. Find SMTP configuration details

---

## 🧪 **Testing Your SMTP Settings**

Once you have the settings, test them:

### **1. Update `.env` file:**
```bash
EMAIL_SMTP_HOST=<your-smtp-host>
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=abdul.rauf@zma-solutions.com
EMAIL_SMTP_PASS=<your-password>
EMAIL_SENDER=abdul.rauf@zma-solutions.com
```

### **2. Restart backend:**
```powershell
docker compose restart backend
```

### **3. Test from frontend:**
- Go to http://localhost:3000
- Navigate to **Notifications** page
- Click **"Send Test Email"**
- Check for success/error messages

### **4. Check logs:**
```powershell
docker compose logs backend --tail=50 | Select-String -Pattern "email|SMTP"
```

---

## ⚠️ **Common Issues**

### **Issue 1: Authentication Failed (535 Error)**
- **Cause:** Wrong password or need App Password
- **Fix:** Use App Password instead of regular password (for Google/Microsoft)

### **Issue 2: Connection Timeout**
- **Cause:** Wrong SMTP host or port
- **Fix:** Verify SMTP host and port with your email provider

### **Issue 3: SSL/TLS Error**
- **Cause:** Wrong port or encryption settings
- **Fix:** Use port 587 for STARTTLS or 465 for SSL

### **Issue 4: "Relay Access Denied"**
- **Cause:** SMTP server requires authentication from specific IPs
- **Fix:** Contact your email provider to whitelist your server IP

---

## 📋 **Quick Reference Table**

| Provider | SMTP Host | Port | Username | Password |
|---------|-----------|------|----------|----------|
| Google Workspace | smtp.gmail.com | 587 | Full email | App Password |
| Microsoft 365 | smtp.office365.com | 587 | Full email | Password/App Password |
| Zoho | smtp.zoho.com | 587 | Full email | Password |
| Custom Server | mail.domain.com | 587/465/25 | Full email | Password |

---

## 🎯 **Next Steps**

1. **Identify your email provider** (Google, Microsoft, custom, etc.)
2. **Get SMTP settings** using one of the methods above
3. **Update `.env` file** with the correct settings
4. **Test email sending** from the frontend
5. **Check logs** if there are any errors

---

**Need Help?** If you're unsure which provider you're using, check:
- Your email login page URL
- Your email client settings
- Your organization's IT documentation
- Contact your IT administrator
