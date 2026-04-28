from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.services.auth_service import decode_token, get_user_by_id
from app.models.models import User, UserRole, StatusEnum

bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    token = credentials.credentials
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


def require_role(*roles: UserRole):
    async def role_checker(current_user: User = Depends(get_current_user)) -> User:
        active_roles = {r.role for r in current_user.roles if r.status == StatusEnum.ACTIVE}
        if not any(role in active_roles for role in roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required role: {[r.value for r in roles]}",
            )
        return current_user
    return role_checker


def check_stocker_exclusive_write(entity_assigned_stocker_id, current_user) -> None:
    """
    Enforce data-entry ownership: if an entity is assigned to a Stocker,
    ONLY that Stocker may add or edit its data. Designers and Admins are
    view-only for assigned entities. Unassigned entities are editable by Designers.
    """
    if entity_assigned_stocker_id and current_user.id != entity_assigned_stocker_id:
        raise HTTPException(
            status_code=403,
            detail="This entity is assigned to a Stocker for data entry. Only the assigned Stocker can add or edit data.",
        )


def is_stocker_only(user: User) -> bool:
    """True if the user has STOCKER role but not DESIGNER or ADMIN.
    Used to enforce assignment-based access: Stockers only see/edit
    Cores and Connects explicitly assigned to them."""
    active_roles = {r.role for r in user.roles if r.status == StatusEnum.ACTIVE}
    return (
        UserRole.STOCKER in active_roles
        and UserRole.DESIGNER not in active_roles
        and UserRole.ADMIN not in active_roles
    )
