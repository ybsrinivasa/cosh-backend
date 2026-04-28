"""
Celery sync dispatch task — posts the payload to RootsTalk and records the result.
BL-C-07 steps 5-7.
"""
import json
import logging
import os
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session
from app.celery_app import celery_app

logger = logging.getLogger(__name__)


def _get_sync_engine():
    from dotenv import load_dotenv
    load_dotenv()
    url = os.getenv("DATABASE_URL_SYNC")
    return create_engine(url)


@celery_app.task(name="app.tasks.sync.dispatch_to_product", bind=True, max_retries=2)
def dispatch_to_product(self, sync_history_id: str, payload: dict, endpoint_url: str, api_key: str):
    """
    BL-C-07 steps 5-7.
    Posts the Cosh sync payload to the product's endpoint.
    Updates sync_history and sync_change_log on completion.
    """
    import requests as req
    from app.models.models import SyncHistory, SyncChangeLog, ProductSyncState, SyncStatus, SyncMode

    engine = _get_sync_engine()

    with Session(engine) as session:
        history = session.execute(
            select(SyncHistory).where(SyncHistory.id == sync_history_id)
        ).scalar_one_or_none()
        if not history:
            logger.error(f"SyncHistory {sync_history_id} not found")
            return

        try:
            response = req.post(
                endpoint_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Cosh-Api-Key": api_key or "",
                },
                timeout=120,
            )

            response_body = {}
            try:
                response_body = response.json()
            except Exception:
                response_body = {"raw": response.text[:2000]}

            if response.status_code == 200:
                summary = response_body.get("summary", {})
                history.status = SyncStatus.COMPLETED
                history.total_items = summary.get("total_items")
                history.items_inserted = summary.get("inserted")
                history.items_updated = summary.get("updated")
                history.items_failed = summary.get("failed", 0)

                if history.items_failed and history.items_failed > 0:
                    history.status = SyncStatus.PARTIAL
            else:
                history.status = SyncStatus.FAILED
                logger.warning(f"Sync {sync_history_id}: HTTP {response.status_code} from {endpoint_url}")

        except req.exceptions.Timeout:
            history.status = SyncStatus.FAILED
            response_body = {"error": "Request timed out after 120 seconds"}
            logger.error(f"Sync {sync_history_id}: timeout")
        except Exception as e:
            history.status = SyncStatus.FAILED
            response_body = {"error": str(e)}
            logger.error(f"Sync {sync_history_id}: {e}")

        from datetime import datetime, timezone
        history.product_response = response_body
        history.completed_at = datetime.now(timezone.utc)
        session.flush()

        # BL-C-07 step 6: on success, mark all dispatched items in sync_change_log
        if history.status in (SyncStatus.COMPLETED, SyncStatus.PARTIAL):
            session.execute(
                update(SyncChangeLog)
                .where(
                    SyncChangeLog.product_id == history.product_id,
                    SyncChangeLog.included_in_sync_id.is_(None),
                )
                .values(included_in_sync_id=sync_history_id)
            )

            # Update product_sync_state
            state = session.execute(
                select(ProductSyncState).where(ProductSyncState.product_id == history.product_id)
            ).scalar_one_or_none()
            if state:
                state.last_successful_sync_at = history.completed_at
                state.last_sync_mode = history.sync_mode
                state.last_sync_id = sync_history_id
            else:
                from app.models.models import ProductSyncState as PSS
                session.add(PSS(
                    product_id=history.product_id,
                    last_successful_sync_at=history.completed_at,
                    last_sync_mode=history.sync_mode,
                    last_sync_id=sync_history_id,
                ))

        session.commit()

    logger.info(f"Sync {sync_history_id}: {history.status.value}")
