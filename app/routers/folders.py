from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.dependencies import require_role, is_stocker_only
from app.models.models import Folder, Core, UserRole, StatusEnum
from app.schemas.folders import FolderCreate, FolderUpdate, FolderOut
from app.services.folder_service import name_is_unique, get_folder, folder_is_empty

router = APIRouter(prefix="/folders", tags=["Folders"])

require_designer = require_role(UserRole.DESIGNER, UserRole.ADMIN)
require_any = require_role(UserRole.DESIGNER, UserRole.STOCKER, UserRole.REVIEWER, UserRole.ADMIN)


@router.get("", response_model=list[FolderOut])
async def list_folders(db: AsyncSession = Depends(get_db), current_user=Depends(require_any)):
    if is_stocker_only(current_user):
        # Stocker sees only folders containing at least one Core assigned to them
        result = await db.execute(
            select(Folder)
            .join(Core, Core.folder_id == Folder.id)
            .where(Core.assigned_stocker_id == current_user.id, Core.status == StatusEnum.ACTIVE)
            .distinct()
            .order_by(Folder.name)
        )
    else:
        result = await db.execute(select(Folder).order_by(Folder.name))
    return result.scalars().all()


@router.post("", response_model=FolderOut, status_code=status.HTTP_201_CREATED)
async def create_folder(
    request: FolderCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_designer),
):
    if not await name_is_unique(db, request.name):
        raise HTTPException(status_code=409, detail=f"'{request.name}' is already used by a Folder or Core")

    folder = Folder(name=request.name, created_by=current_user.id)
    db.add(folder)
    await db.commit()
    await db.refresh(folder)
    return folder


@router.put("/{folder_id}", response_model=FolderOut)
async def rename_folder(
    folder_id: str,
    request: FolderUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    folder = await get_folder(db, folder_id)

    if folder.name != request.name:
        if not await name_is_unique(db, request.name, exclude_folder_id=folder_id):
            raise HTTPException(status_code=409, detail=f"'{request.name}' is already used by a Folder or Core")

    folder.name = request.name
    await db.commit()
    await db.refresh(folder)
    return folder


@router.delete("/{folder_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_folder(
    folder_id: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_designer),
):
    folder = await get_folder(db, folder_id)
    if not await folder_is_empty(db, folder_id):
        raise HTTPException(status_code=409, detail="Cannot delete a Folder that contains Cores")
    await db.delete(folder)
    await db.commit()
