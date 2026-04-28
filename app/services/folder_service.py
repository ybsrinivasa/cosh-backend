from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from fastapi import HTTPException
from app.models.models import Folder, Core


async def name_is_unique(db: AsyncSession, name: str, exclude_folder_id: str = None) -> bool:
    """Folder and Core names share one global namespace."""
    folder_q = select(Folder).where(Folder.name == name)
    if exclude_folder_id:
        folder_q = folder_q.where(Folder.id != exclude_folder_id)
    folder_exists = (await db.execute(folder_q)).scalar_one_or_none()
    if folder_exists:
        return False

    core_exists = (await db.execute(select(Core).where(Core.name == name))).scalar_one_or_none()
    return core_exists is None


async def get_folder(db: AsyncSession, folder_id: str) -> Folder:
    result = await db.execute(select(Folder).where(Folder.id == folder_id))
    folder = result.scalar_one_or_none()
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")
    return folder


async def folder_is_empty(db: AsyncSession, folder_id: str) -> bool:
    result = await db.execute(select(func.count()).select_from(Core).where(Core.folder_id == folder_id))
    count = result.scalar()
    return count == 0
