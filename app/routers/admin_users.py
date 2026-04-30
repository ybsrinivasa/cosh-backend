from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from app.database import get_db
from app.dependencies import require_role
from app.services.auth_service import get_user_by_email
from app.schemas.auth import CreateUserRequest, UpdateUserStatusRequest, UpdateUserRolesRequest, UserOut
from app.models.models import User, UserRoleModel, UserRole, StatusEnum, Core, Connect

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


@router.get("/by-role/{role}", tags=["Admin — Users"])
async def list_users_by_role(
    role: UserRole,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_role(UserRole.DESIGNER, UserRole.ADMIN)),
):
    """
    Returns active users with a specific role.
    Accessible to Designers so they can assign Stockers to Cores/Connects.
    """
    result = await db.execute(
        select(User)
        .join(UserRoleModel, UserRoleModel.user_id == User.id)
        .where(
            UserRoleModel.role == role,
            UserRoleModel.status == StatusEnum.ACTIVE,
            User.status == StatusEnum.ACTIVE,
        )
        .order_by(User.name)
    )
    users = result.scalars().all()
    return [{"id": u.id, "name": u.name or u.email, "email": u.email} for u in users]


@router.get("/workload", tags=["Admin — Users"])
async def team_workload(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    """Returns each Designer/Stocker with their assigned cores and connects."""
    # Fetch all designer + stocker users
    team_result = await db.execute(
        select(User)
        .join(UserRoleModel, UserRoleModel.user_id == User.id)
        .where(
            UserRoleModel.role.in_([UserRole.DESIGNER, UserRole.STOCKER]),
            UserRoleModel.status == StatusEnum.ACTIVE,
            User.status == StatusEnum.ACTIVE,
        )
        .options(selectinload(User.roles))
        .order_by(User.name)
        .distinct()
    )
    team = team_result.scalars().all()

    # Cores designed by each user
    cores_designed_result = await db.execute(
        select(Core.created_by, Core.id, Core.name, Core.status)
        .where(Core.created_by.in_([u.id for u in team]))
    )
    cores_designed = cores_designed_result.fetchall()

    # Cores assigned to each stocker
    cores_stocked_result = await db.execute(
        select(Core.assigned_stocker_id, Core.id, Core.name, Core.status)
        .where(Core.assigned_stocker_id.in_([u.id for u in team]))
    )
    cores_stocked = cores_stocked_result.fetchall()

    # Connects designed by each user
    connects_designed_result = await db.execute(
        select(Connect.created_by, Connect.id, Connect.name, Connect.status)
        .where(Connect.created_by.in_([u.id for u in team]))
    )
    connects_designed = connects_designed_result.fetchall()

    # Connects assigned to each stocker
    connects_stocked_result = await db.execute(
        select(Connect.assigned_stocker_id, Connect.id, Connect.name, Connect.status)
        .where(Connect.assigned_stocker_id.in_([u.id for u in team]))
    )
    connects_stocked = connects_stocked_result.fetchall()

    def _item(row):
        return {"id": row[1], "name": row[2], "status": row[3].value}

    workload = []
    for user in team:
        active_roles = [r.role.value for r in user.roles if r.status.value == "ACTIVE"]
        workload.append({
            "user_id": user.id,
            "name": user.name or user.email,
            "email": user.email,
            "roles": active_roles,
            "cores_designed": [_item(r) for r in cores_designed if r[0] == user.id],
            "cores_stocked": [_item(r) for r in cores_stocked if r[0] == user.id],
            "connects_designed": [_item(r) for r in connects_designed if r[0] == user.id],
            "connects_stocked": [_item(r) for r in connects_stocked if r[0] == user.id],
        })

    return workload
