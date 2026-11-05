"""
Backfill: propagate every existing expert correction to the other languages.

The automatic hook (routers/cores.py -> tasks/propagate_expert.py) only fires
on corrections made from now on. This covers the ones already in the database.

Dry run by default — prints what it would change and writes nothing.

    # see what it would do, whole database
    python scripts/backfill_expert_propagation.py

    # one Core, first 5 items
    python scripts/backfill_expert_propagation.py --core "Damage Symptoms" --limit 5

    # actually write
    python scripts/backfill_expert_propagation.py --apply

Resumable: with --apply, an item whose other languages are already
DERIVED_FROM_EXPERT is skipped, so a re-run after an interruption only does
the remainder. Pass --redo to reconsider those too.
"""
import argparse
import os
import sys

# This script runs for ~45 minutes and its progress report IS the audit trail.
# Redirected to a file (nohup ... > log), Python block-buffers stdout, so the
# log stays empty until the process exits — during the 2026-11-05 run it looked
# dead for minutes while it was working fine. Force line buffering here rather
# than relying on whoever invokes it to remember `python -u`.
sys.stdout.reconfigure(line_buffering=True)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv  # noqa: E402
from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

load_dotenv()

from app.models.models import (  # noqa: E402
    ChangeType,
    Core,
    CoreDataItem,
    CoreDataTranslation,
    CoreLanguageConfig,
    CoreProductTag,
    EntityType,
    StatusEnum,
    SyncChangeLog,
    ValidationStatus,
)
from app.services.expert_propagation import (  # noqa: E402
    PropagationUnavailableError,
    propagate_from_expert,
)

# Rough, from the 2026-11-05 dry run on Opus 5: ~1.6k in / 2.1k out per item.
COST_PER_ITEM_USD = 0.06


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--core", help="limit to one Core by exact name")
    ap.add_argument("--lang", default=None, help="only propagate from this source language")
    ap.add_argument("--limit", type=int, default=0, help="stop after N items")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--redo", action="store_true",
                    help="also reconsider items already carrying DERIVED_FROM_EXPERT")
    args = ap.parse_args()

    engine = create_engine(os.getenv("DATABASE_URL_SYNC"))
    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== Expert-correction propagation backfill [{mode}] ===\n")

    with Session(engine) as session:
        q = (
            select(CoreDataTranslation, CoreDataItem, Core)
            .join(CoreDataItem, CoreDataItem.id == CoreDataTranslation.item_id)
            .join(Core, Core.id == CoreDataItem.core_id)
            .where(
                CoreDataTranslation.validation_status == ValidationStatus.EXPERT_VALIDATED,
                CoreDataItem.status == StatusEnum.ACTIVE,
            )
            .order_by(Core.name, CoreDataItem.english_value)
        )
        if args.core:
            q = q.where(Core.name == args.core)
        if args.lang:
            q = q.where(CoreDataTranslation.language_code == args.lang)

        anchors = session.execute(q).all()
        print(f"{len(anchors)} expert-corrected rows to consider\n")

        configured_cache: dict[str, set[str]] = {}
        products_cache: dict[str, list[str]] = {}

        considered = rewritten_items = skipped_internal = skipped_consistent = 0
        rewritten_langs = 0

        for source, item, core in anchors:
            if args.limit and considered >= args.limit:
                print(f"\n-- stopping at --limit {args.limit} --")
                break

            if core.id not in configured_cache:
                configured_cache[core.id] = {
                    c.language_code
                    for c in session.execute(
                        select(CoreLanguageConfig).where(
                            CoreLanguageConfig.core_id == core.id
                        )
                    ).scalars()
                }
            configured = configured_cache[core.id]

            siblings = [
                r
                for r in session.execute(
                    select(CoreDataTranslation).where(
                        CoreDataTranslation.item_id == item.id
                    )
                ).scalars()
                if r.language_code not in (source.language_code, "en")
                and r.language_code in configured
                and r.validation_status != ValidationStatus.EXPERT_VALIDATED
            ]
            if not siblings:
                continue
            # Resume check: ANY derived sibling means this item has already been
            # through the propagation — the rest were judged 'ok' and left
            # alone deliberately. Testing `all` instead would re-run (and
            # re-charge for) every item where only some languages were wrong,
            # which is most of them.
            if not args.redo and any(
                r.validation_status == ValidationStatus.DERIVED_FROM_EXPERT
                for r in siblings
            ):
                continue

            considered += 1
            targets = [(r.language_code, r.translated_value) for r in siblings]
            rows_by_lang = {r.language_code: r for r in siblings}

            try:
                result = propagate_from_expert(
                    core.name, core.description, item.english_value,
                    source.language_code, source.translated_value, targets,
                )
            except PropagationUnavailableError as e:
                print(f"\nABORTING: {e}")
                if args.apply:
                    session.commit()
                    print("(partial progress committed)")
                return

            print(f"[{core.name}] {item.english_value}")
            print(f"    {source.language_code} (expert): {source.translated_value}")
            if not result:
                print("    -- call failed, left untouched --\n")
                continue
            print(f"    meaning: {result['english_meaning']}")

            if not result.get("correction_is_semantic"):
                print(f"    -> {source.language_code}-internal fix only; nothing to do\n")
                skipped_internal += 1
                continue

            changed_here = []
            for entry in result.get("languages", []):
                row = rows_by_lang.get(entry.get("lang"))
                if row is None or entry.get("verdict") != "wrong":
                    continue
                new_value = (entry.get("corrected") or "").strip()
                if not new_value or new_value == row.translated_value:
                    continue
                print(f"    [CHANGE] {row.language_code}: {row.translated_value}")
                print(f"                  -> {new_value}")
                print(f"                  ({entry.get('why')})")
                if args.apply:
                    row.translated_value = new_value
                    row.validation_status = ValidationStatus.DERIVED_FROM_EXPERT
                changed_here.append(row.language_code)

            if not changed_here:
                print("    -> other languages already consistent\n")
                skipped_consistent += 1
                continue

            rewritten_items += 1
            rewritten_langs += len(changed_here)

            if args.apply:
                if core.id not in products_cache:
                    products_cache[core.id] = session.execute(
                        select(CoreProductTag.product_id).where(
                            CoreProductTag.core_id == core.id
                        )
                    ).scalars().all()
                for pid in products_cache[core.id]:
                    session.add(
                        SyncChangeLog(
                            product_id=pid,
                            entity_type=EntityType.TRANSLATION,
                            entity_id=item.id,
                            change_type=ChangeType.TRANSLATION_UPDATED,
                        )
                    )
                # Commit per item so an interruption loses at most one item's
                # work and a re-run resumes cleanly.
                session.commit()
            print()

        print("=" * 70)
        print(f"considered          : {considered}")
        print(f"items rewritten     : {rewritten_items}  ({rewritten_langs} language rows)")
        print(f"source-internal only: {skipped_internal}")
        print(f"already consistent  : {skipped_consistent}")
        print(f"approx API cost     : ${considered * COST_PER_ITEM_USD:.2f}")
        if not args.apply:
            print("\nDRY RUN — nothing was written. Re-run with --apply to commit.")
        else:
            print("\nApplied. Dispatch a sync to carry these to RootsTalk.")


if __name__ == "__main__":
    main()
