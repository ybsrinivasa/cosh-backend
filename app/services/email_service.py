import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.config import settings

logger = logging.getLogger(__name__)


def send_otp_email(to_email: str, otp_code: str, user_name: str = None) -> bool:
    """Send OTP email. Returns True on success, False on failure."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{otp_code} — Your Cosh 2.0 sign-in code"
        msg["From"] = settings.email_from or settings.email_smtp_user
        msg["To"] = to_email
        if settings.email_reply_to:
            msg["Reply-To"] = settings.email_reply_to

        greeting = f"Hi {user_name}," if user_name else "Hi,"

        html = f"""
<!DOCTYPE html>
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f8fafc; margin: 0; padding: 40px 0;">
  <div style="max-width: 440px; margin: 0 auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 8px rgba(0,0,0,0.08);">
    <div style="background: linear-gradient(135deg, #071e12 0%, #0d3320 100%); padding: 32px 40px 28px;">
      <p style="color: #4ade80; font-size: 13px; letter-spacing: 2px; text-transform: uppercase; margin: 0 0 8px;">Neytiri Eywafarm Agritech</p>
      <h1 style="color: #ffffff; font-size: 22px; font-weight: 700; margin: 0;">Cosh 2.0</h1>
    </div>
    <div style="padding: 36px 40px;">
      <p style="color: #334155; font-size: 15px; margin: 0 0 8px;">{greeting}</p>
      <p style="color: #334155; font-size: 15px; margin: 0 0 28px;">Your sign-in code is:</p>
      <div style="background: #f1f5f9; border-radius: 10px; padding: 20px; text-align: center; margin-bottom: 28px;">
        <span style="font-size: 36px; font-weight: 700; letter-spacing: 10px; color: #065f46; font-family: monospace;">{otp_code}</span>
      </div>
      <p style="color: #64748b; font-size: 13px; margin: 0;">This code expires in <strong>10 minutes</strong>. Do not share it with anyone.</p>
    </div>
    <div style="padding: 16px 40px 24px; border-top: 1px solid #e2e8f0;">
      <p style="color: #94a3b8; font-size: 12px; margin: 0;">If you did not request this code, you can safely ignore this email.</p>
    </div>
  </div>
</body>
</html>
"""
        plain = f"{greeting}\n\nYour Cosh 2.0 sign-in code is: {otp_code}\n\nThis code expires in 10 minutes.\n\nIf you did not request this, ignore this email."

        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(settings.email_smtp_host, settings.email_smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(settings.email_smtp_user, settings.email_smtp_pass)
            server.sendmail(msg["From"], to_email, msg.as_string())

        return True

    except Exception as e:
        logger.error(f"Failed to send OTP email to {to_email}: {e}")
        return False
