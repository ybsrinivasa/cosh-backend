import random
import string
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from app.database import get_db
from app.dependencies import get_current_user
from app.services.auth_service import authenticate_user, create_access_token, get_user_by_email
from app.services.email_service import send_otp_email
from app.schemas.auth import LoginRequest, OtpRequestSchema, OtpVerifySchema, TokenResponse, UserOut
from app.models.models import User, LoginOTP

router = APIRouter(prefix="/auth", tags=["Authentication"])

OTP_EXPIRE_MINUTES = 10


def _build_token(user: User) -> str:
    active_roles = [r.role.value for r in user.roles if r.status.value == "ACTIVE"]
    return create_access_token({"sub": user.id, "email": user.email, "roles": active_roles})


@router.post("/request-otp", status_code=200)
async def request_otp(request: OtpRequestSchema, db: AsyncSession = Depends(get_db)):
    """Step 1 of OTP login: send a 6-digit code to the user's email."""
    user = await get_user_by_email(db, request.email)
    if not user:
        # Return 200 regardless — don't reveal whether the email exists
        return {"detail": "If that email is registered, a code has been sent."}

    # Delete any unexpired OTPs for this user
    await db.execute(delete(LoginOTP).where(LoginOTP.user_id == user.id))

    otp_code = "".join(random.choices(string.digits, k=6))
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)

    db.add(LoginOTP(user_id=user.id, otp_code=otp_code, expires_at=expires_at))
    await db.commit()

    sent = send_otp_email(user.email, otp_code, user.name)
    if not sent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to send email. Please try again in a moment.",
        )

    return {"detail": "If that email is registered, a code has been sent."}


@router.post("/verify-otp", response_model=TokenResponse)
async def verify_otp(request: OtpVerifySchema, db: AsyncSession = Depends(get_db)):
    """Step 2 of OTP login: verify the code and return a JWT."""
    user = await get_user_by_email(db, request.email)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid code")

    result = await db.execute(
        select(LoginOTP).where(
            LoginOTP.user_id == user.id,
            LoginOTP.otp_code == request.otp_code,
            LoginOTP.used == False,
        )
    )
    otp = result.scalar_one_or_none()

    if not otp or otp.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired code")

    otp.used = True
    await db.commit()

    return TokenResponse(access_token=_build_token(user))


@router.post("/login", response_model=TokenResponse, include_in_schema=False)
async def login(request: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Password-based login kept as emergency fallback (hidden from docs)."""
    user = await authenticate_user(db, request.email, request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )
    return TokenResponse(access_token=_build_token(user))


@router.get("/me", response_model=UserOut)
async def get_me(current_user: User = Depends(get_current_user)):
    return current_user
