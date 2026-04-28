"""
Celery similarity detection tasks — BL-C-05.
Synchronous SQLAlchemy (Celery workers are not async).
"""
import json
import logging
import os
from pathlib import Path
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.celery_app import celery_app

logger = logging.getLogger(__name__)

_ABBREV_PATH = Path(__file__).parent.parent / "config" / "abbreviations.json"
_ABBREVIATIONS: dict = {}
if _ABBREV_PATH.exists():
    with open(_ABBREV_PATH) as _f:
        _ABBREVIATIONS = json.load(_f)


def _get_sync_engine():
    from dotenv import load_dotenv
    load_dotenv()
    url = os.getenv("DATABASE_URL_SYNC")
    return create_engine(url)


def _levenshtein(s1: str, s2: str) -> int:
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if not s2:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for c1 in s1:
        curr = [prev[0] + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
        prev = curr
    return prev[-1]


def _expand_abbreviations(text: str) -> str:
    tokens = text.lower().split()
    return " ".join(_ABBREVIATIONS.get(t, t) for t in tokens)


def _run_similarity_tests(val_a: str, val_b: str):
    """
    BL-C-05: five tests in sequence.
    Returns (score, SimilarityReason) if any test passes, else (None, None).
    """
    from app.models.models import SimilarityReason

    # Test 1 — Exact (after strip + lowercase)
    if val_a.strip().lower() == val_b.strip().lower():
        return 1.0, SimilarityReason.EXACT_DUPLICATE

    # Test 2 — Normalised: collapse whitespace + expand abbreviations
    norm_a = " ".join(_expand_abbreviations(val_a).split())
    norm_b = " ".join(_expand_abbreviations(val_b).split())
    if norm_a == norm_b:
        return 0.95, SimilarityReason.FORMAT_DIFFERENCE

    # Test 3 — Levenshtein (≤10% character change)
    clean_a = val_a.strip().lower()
    clean_b = val_b.strip().lower()
    max_len = max(len(clean_a), len(clean_b))
    if max_len > 0:
        dist = _levenshtein(clean_a, clean_b)
        ratio = dist / max_len
        if ratio <= 0.1:
            return round(0.90 - ratio, 4), SimilarityReason.SPELLING_ERROR

    # Test 4 — Sorted tokens
    tokens_a = sorted(clean_a.split())
    tokens_b = sorted(clean_b.split())
    if tokens_a and tokens_b and tokens_a == tokens_b:
        return 0.85, SimilarityReason.REARRANGED_WORDS

    # Test 5 — Token containment (one is a subset of the other)
    set_a = set(clean_a.split())
    set_b = set(clean_b.split())
    if set_a and set_b and (set_a.issubset(set_b) or set_b.issubset(set_a)):
        return 0.80, SimilarityReason.MISSING_WORDS

    return None, None


def _upsert_pair(session: Session, id_a: str, id_b: str, score: float, reason) -> bool:
    """
    Insert a new PENDING pair if it doesn't already exist.
    Canonical order: smaller id first to prevent (a,b) and (b,a) duplicates.
    Skips pairs that are already actioned (any status other than PENDING).
    Returns True if a new pair was inserted.
    """
    from app.models.models import SimilarityPair, SimilarityStatus

    if id_a > id_b:
        id_a, id_b = id_b, id_a

    existing = session.execute(
        select(SimilarityPair).where(
            SimilarityPair.item_id_a == id_a,
            SimilarityPair.item_id_b == id_b,
        )
    ).scalar_one_or_none()

    if existing:
        if existing.status != SimilarityStatus.PENDING:
            return False
        # Refresh score for already-pending pair
        existing.similarity_score = score
        existing.similarity_reason = reason
        return False

    session.add(SimilarityPair(
        item_id_a=id_a,
        item_id_b=id_b,
        similarity_score=score,
        similarity_reason=reason,
        status=SimilarityStatus.PENDING,
    ))
    return True


@celery_app.task(name="app.tasks.similarity.check_item_similarity", bind=True)
def check_item_similarity(self, item_id: str):
    """
    BL-C-05 targeted: check one new item against all existing active items
    in the same Core. Triggered on item creation (TEXT Cores only).
    """
    from app.models.models import CoreDataItem, Core, CoreType, StatusEnum

    engine = _get_sync_engine()
    with Session(engine) as session:
        new_item = session.execute(
            select(CoreDataItem).where(CoreDataItem.id == item_id)
        ).scalar_one_or_none()
        if not new_item:
            logger.warning(f"check_item_similarity: item {item_id} not found")
            return

        core = session.execute(
            select(Core).where(Core.id == new_item.core_id)
        ).scalar_one_or_none()
        if not core or core.core_type != CoreType.TEXT:
            return

        peers = session.execute(
            select(CoreDataItem).where(
                CoreDataItem.core_id == new_item.core_id,
                CoreDataItem.status == StatusEnum.ACTIVE,
                CoreDataItem.id != item_id,
            )
        ).scalars().all()

        new_pairs = 0
        for peer in peers:
            score, reason = _run_similarity_tests(new_item.english_value, peer.english_value)
            if score is not None:
                if _upsert_pair(session, new_item.id, peer.id, score, reason):
                    new_pairs += 1

        session.commit()
    logger.info(f"Targeted similarity check for item {item_id}: {new_pairs} new pair(s) found")


@celery_app.task(name="app.tasks.similarity.detect_similarity_all_cores", bind=True)
def detect_similarity_all_cores(self):
    """
    BL-C-05 full scan: all ACTIVE items across all TEXT Cores.
    Triggered by Celery Beat daily at 02:00 UTC, or manually by Admin (First Pass).
    Processes one Core at a time to keep memory usage bounded.
    """
    from app.models.models import CoreDataItem, Core, CoreType, StatusEnum

    engine = _get_sync_engine()
    total_pairs = 0

    with Session(engine) as session:
        cores = session.execute(
            select(Core).where(
                Core.core_type == CoreType.TEXT,
                Core.status == StatusEnum.ACTIVE,
            )
        ).scalars().all()

        for core in cores:
            items = session.execute(
                select(CoreDataItem).where(
                    CoreDataItem.core_id == core.id,
                    CoreDataItem.status == StatusEnum.ACTIVE,
                )
            ).scalars().all()

            core_pairs = 0
            for i in range(len(items)):
                for j in range(i + 1, len(items)):
                    score, reason = _run_similarity_tests(
                        items[i].english_value, items[j].english_value
                    )
                    if score is not None:
                        if _upsert_pair(session, items[i].id, items[j].id, score, reason):
                            core_pairs += 1

            if core_pairs:
                logger.info(f"Core '{core.name}': {core_pairs} new pair(s) detected")
            total_pairs += core_pairs

        session.commit()

    logger.info(f"Full similarity scan complete. Total new pairs: {total_pairs}")
    return {"pairs_detected": total_pairs}
