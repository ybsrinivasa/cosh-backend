from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from app.models.models import SimilarityStatus, SimilarityReason


class SimilarityPairOut(BaseModel):
    id: str
    item_id_a: str
    item_id_b: str
    english_value_a: Optional[str] = None
    english_value_b: Optional[str] = None
    core_name: Optional[str] = None
    similarity_score: float
    similarity_reason: Optional[SimilarityReason]
    status: SimilarityStatus
    detected_at: datetime

    class Config:
        from_attributes = True


class ReviewActionRequest(BaseModel):
    action: SimilarityStatus
    remove_item_id: Optional[str] = None
    canonical_value: Optional[str] = None


class SimilarityQueueResponse(BaseModel):
    total_pending: int
    pairs: List[SimilarityPairOut]
