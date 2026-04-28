from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.dependencies import require_role
from app.models.models import UserRole, SimilarityStatus
from app.schemas.similarity import (
    SimilarityPairOut, ReviewActionRequest, SimilarityQueueResponse,
)
from app.services.similarity_service import (
    get_pending_queue, get_pair_for_review,
    action_keep_both, action_remove_one, action_merge, action_ignore,
)

router = APIRouter(prefix="/similarity", tags=["Similarity Review"])

require_reviewer = require_role(UserRole.REVIEWER, UserRole.DESIGNER, UserRole.ADMIN)
require_admin = require_role(UserRole.ADMIN)

_VALID_REVIEW_ACTIONS = {
    SimilarityStatus.KEEP_BOTH,
    SimilarityStatus.REMOVE_ONE,
    SimilarityStatus.MERGED,
    SimilarityStatus.IGNORED,
}


@router.get("/queue", response_model=SimilarityQueueResponse)
async def get_similarity_queue(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_reviewer),
):
    """
    P5-02: Return up to 100 PENDING pairs ordered by similarity score DESC.
    Each pair includes item English values and Core name for context.
    """
    total, pairs, items_map, cores_map = await get_pending_queue(db)

    enriched = []
    for pair in pairs:
        item_a = items_map.get(pair.item_id_a)
        item_b = items_map.get(pair.item_id_b)
        core = cores_map.get(item_a.core_id) if item_a else None

        enriched.append(SimilarityPairOut(
            id=pair.id,
            item_id_a=pair.item_id_a,
            item_id_b=pair.item_id_b,
            english_value_a=item_a.english_value if item_a else None,
            english_value_b=item_b.english_value if item_b else None,
            core_name=core.name if core else None,
            similarity_score=float(pair.similarity_score),
            similarity_reason=pair.similarity_reason,
            status=pair.status,
            detected_at=pair.detected_at,
        ))

    return SimilarityQueueResponse(total_pending=total, pairs=enriched)


@router.post("/{pair_id}/review", status_code=status.HTTP_200_OK)
async def review_pair(
    pair_id: str,
    request: ReviewActionRequest,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(require_reviewer),
):
    """
    P5-02: Take one of four actions on a PENDING similarity pair.
    - KEEP_BOTH: both items remain active
    - REMOVE_ONE: inactivate one item (provide remove_item_id)
    - MERGED: merge into canonical value (provide canonical_value)
    - IGNORED: dismiss permanently
    """
    if request.action not in _VALID_REVIEW_ACTIONS:
        raise HTTPException(status_code=422, detail=f"Invalid action '{request.action}'. Use KEEP_BOTH, REMOVE_ONE, MERGED, or IGNORED")

    pair = await get_pair_for_review(db, pair_id)

    if request.action == SimilarityStatus.KEEP_BOTH:
        await action_keep_both(db, pair, current_user.id)

    elif request.action == SimilarityStatus.REMOVE_ONE:
        if not request.remove_item_id:
            raise HTTPException(status_code=422, detail="remove_item_id is required for REMOVE_ONE")
        await action_remove_one(db, pair, request.remove_item_id, current_user.id)

    elif request.action == SimilarityStatus.MERGED:
        if not request.canonical_value:
            raise HTTPException(status_code=422, detail="canonical_value is required for MERGED")
        await action_merge(db, pair, request.canonical_value, current_user.id)

    elif request.action == SimilarityStatus.IGNORED:
        await action_ignore(db, pair, current_user.id)

    return {"message": f"Pair {pair_id} actioned as {request.action.value}"}


@router.post("/first-pass", status_code=status.HTTP_202_ACCEPTED)
async def trigger_first_pass(
    _=Depends(require_admin),
):
    """
    P5-03: Admin triggers the First Pass similarity scan across all TEXT Cores.
    Runs as a background Celery task — returns immediately with task ID.
    """
    from app.tasks.similarity import detect_similarity_all_cores
    task = detect_similarity_all_cores.delay()
    return {
        "message": "First Pass similarity scan dispatched",
        "task_id": task.id,
    }
