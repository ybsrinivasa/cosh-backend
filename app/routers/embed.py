"""Public embed router — exposes the visualization API without auth so the
Eywa marketing site (eywa.farm) can embed the Cosh knowledge graph in an
iframe at cosh.html → cosh2.eywa.farm/embed/explore.

Why a separate router instead of modifying visualization.py:
  The visualization module is FROZEN (see feedback_visualization_frozen_*
  memory note). We don't change its behaviour, defaults, or props without
  explicit user request. To support the embed use case, we wrap the
  existing handlers with no-auth thin wrappers — auth bypass + same logic.

Route mirror at /embed/viz/*:
  GET /embed/viz/filter-options    ← /viz/filter-options
  GET /embed/viz/search            ← /viz/search
  GET /embed/viz/slice             ← /viz/slice
  GET /embed/viz/connects-list     ← /viz/connects-list
  GET /embed/viz/connect-slice     ← /viz/connect-slice

The trick: the existing handlers take `_user=Depends(require_viz_user)`
but never use _user inside the function body — it's purely the FastAPI
dependency-injection auth gate. When we call them directly from Python
with `_user=None` as a kwarg, FastAPI's DI doesn't fire and the auth
check is bypassed.
"""
from typing import Optional, Literal
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.visualization import (
    FilterOptionsOut, ConnectListOut, SearchOut, SliceOut,
)
from app.routers.visualization import (
    filter_options as _admin_filter_options,
    search_items as _admin_search_items,
    slice_ as _admin_slice,
    connects_list as _admin_connects_list,
    connect_slice as _admin_connect_slice,
)

router = APIRouter(prefix="/embed/viz", tags=["embed-visualization"])


# Same FilterType alias used by the admin route — kept here to avoid
# importing private aliases.
FilterType = Literal["core", "item"]


@router.get("/filter-options", response_model=FilterOptionsOut)
async def filter_options(
    connected_to_core: Optional[str] = Query(None),
    connected_to_item: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Public mirror of /viz/filter-options."""
    return await _admin_filter_options(
        connected_to_core=connected_to_core,
        connected_to_item=connected_to_item,
        db=db,
        _user=None,
    )


@router.get("/search", response_model=SearchOut)
async def search_items(
    q: str = Query(..., min_length=1, max_length=200),
    core_id: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Public mirror of /viz/search."""
    return await _admin_search_items(
        q=q,
        core_id=core_id,
        limit=limit,
        db=db,
        _user=None,
    )


@router.get("/slice", response_model=SliceOut)
async def slice_(
    filter1_type: FilterType = Query(...),
    filter1_id: str = Query(..., min_length=36, max_length=36),
    filter2_type: Optional[FilterType] = Query(None),
    filter2_id: Optional[str] = Query(None, min_length=36, max_length=36),
    db: AsyncSession = Depends(get_db),
):
    """Public mirror of /viz/slice."""
    return await _admin_slice(
        filter1_type=filter1_type,
        filter1_id=filter1_id,
        filter2_type=filter2_type,
        filter2_id=filter2_id,
        db=db,
        _user=None,
    )


@router.get("/connects-list", response_model=ConnectListOut)
async def connects_list(
    db: AsyncSession = Depends(get_db),
):
    """Public mirror of /viz/connects-list."""
    return await _admin_connects_list(
        db=db,
        _user=None,
    )


@router.get("/connect-slice", response_model=SliceOut)
async def connect_slice(
    connect_id: str = Query(..., min_length=36, max_length=36),
    anchor_id: Optional[str] = Query(None, min_length=36, max_length=36),
    db: AsyncSession = Depends(get_db),
):
    """Public mirror of /viz/connect-slice."""
    return await _admin_connect_slice(
        connect_id=connect_id,
        anchor_id=anchor_id,
        db=db,
        _user=None,
    )
