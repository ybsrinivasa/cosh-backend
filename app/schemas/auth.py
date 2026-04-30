from pydantic import BaseModel, EmailStr
from typing import List, Optional
from app.models.models import UserRole, StatusEnum


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class OtpRequestSchema(BaseModel):
    email: EmailStr


class OtpVerifySchema(BaseModel):
    email: EmailStr
    otp_code: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class RoleOut(BaseModel):
    role: UserRole
    status: StatusEnum

    class Config:
        from_attributes = True


class UserOut(BaseModel):
    id: str
    email: str
    name: Optional[str]
    status: StatusEnum
    roles: List[RoleOut]

    class Config:
        from_attributes = True


class CreateUserRequest(BaseModel):
    email: EmailStr
    name: str
    roles: List[UserRole]


class UpdateUserStatusRequest(BaseModel):
    status: StatusEnum


class UpdateUserRolesRequest(BaseModel):
    roles: List[UserRole]
