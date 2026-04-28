from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from app.database import get_db
from app.dependencies import require_role
from app.services.auth_service import hash_password, get_user_by_email
from app.schemas.auth import CreateUserRequest, UpdateUserStatusRequest, UpdateUserRolesRequest, UserOut
from app.models.models import User, UserRoleModel, UserRole, StatusEnum

router = APIRouter(prefix="/admin/users", tags=["Admin — Users"])

require_admin = require_role(UserRole.ADMIN)


@router.get("", response_model=list[UserOut])
async def list_users(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(
        select(User).options(selectinload(User.roles)).order_by(User.created_at)
    )
    return result.scalars().all()


@router.post("", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def create_user(
    request: CreateUserRequest,
    db: AsyncSession = Depends(get_db),
    current_admin=Depends(require_admin),
):
    existing = await get_user_by_email(db, request.email)
    if existing:
        raise HTTPException(status_code=409, detail="A user with this email already exists")

    user = User(
        email=request.email,
        name=request.name,
        password_hash=hash_password(request.password),
        status=StatusEnum.ACTIVE,
    )
    db.add(user)
    await db.flush()

    for role in request.roles:
        db.add(UserRoleModel(user_id=user.id, role=role, status=StatusEnum.ACTIVE))

    await db.commit()
    await db.refresh(user)

    result = await db.execute(
        select(User).options(selectinload(User.roles)).where(User.id == user.id)
    )
    return result.scalar_one()


@router.put("/{user_id}/status", response_model=UserOut)
async def update_user_status(
    user_id: str,
    request: UpdateUserStatusRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(
        select(User).options(selectinload(User.roles)).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.status = request.status
    await db.commit()
    await db.refresh(user)
    return user


@router.put("/{user_id}/roles", response_model=UserOut)
async def update_user_roles(
    user_id: str,
    request: UpdateUserRolesRequest,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(
        select(User).options(selectinload(User.roles)).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    existing_roles = await db.execute(
        select(UserRoleModel).where(UserRoleModel.user_id == user_id)
    )
    for role_row in existing_roles.scalars().all():
        await db.delete(role_row)

    for role in request.roles:
        db.add(UserRoleModel(user_id=user.id, role=role, status=StatusEnum.ACTIVE))

    await db.commit()

    result = await db.execute(
        select(User).options(selectinload(User.roles)).where(User.id == user_id)
    )
    return result.scalar_one()
