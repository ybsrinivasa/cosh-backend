"""
Propagate an expert's translation correction to the other languages.

The idea, and why it is NOT pivot translation:

  The English label never changes. When an expert corrects, say, the Kannada,
  they are telling us the machine MISREAD THE ENGLISH — it took the wrong
  sense of an ambiguous word, or missed the agricultural meaning. Every other
  language was translated from that same English by the same machine, so they
  are likely to carry the SAME error.

  So the correction is not a translation to copy sideways. It is a key that
  pins down what the English actually denotes. We hand Claude the English, the
  expert's rendering, and the current machine output for the other languages,
  and ask it to re-translate FROM ENGLISH using the meaning the expert
  revealed. Translating Kannada -> Hindi directly would compound the Kannada
  reading and drag Kannada phrasing into other scripts.

  Anchor 2026-11-05, "Head Formation Stage": the expert's Kannada showed that
  "Head" meant the cabbage/cauliflower head. All five other languages had read
  it as an anatomical head or the plant's apex — Odia (ମୁଣ୍ଡ) and Telugu (తల)
  were literally saying a person's head. One correction fixed five languages.

Deliberately conservative. Two gates:

  * `correction_is_semantic` — false when the expert only fixed spelling, a
    conjunct, or register internal to their own language. Such a correction
    says nothing about the English and must not churn the others.
  * per-language `verdict` — "wrong" only when the current value would
    mislead, not merely when Claude would have phrased it differently.

Never overwrites EXPERT_VALIDATED in any language: a curator's work is final
in the language they curated. See `app/tasks/propagate_expert.py` for the
write side.
"""
import json
import logging
import os
import time
from typing import Optional

import httpx

from app.services.claude_translation_service import LANG_NAMES, TERM_HINTS

logger = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"

# Opus is the right tier here. This is a reasoning task — infer a term's
# domain sense from a cross-language signal — not the short-label rendering
# that claude_translation_service.py does. Measured ~1.6k in / 2.1k out per
# item, about $0.06. Don't lower the tier without re-running the dry run.
DEFAULT_MODEL = os.getenv("CLAUDE_PROPAGATION_MODEL", "claude-opus-5")

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "english_meaning": {
            "type": "string",
            "description": "What the English term denotes in this agricultural context, as pinned down by the expert's rendering.",
        },
        "correction_reveals": {
            "type": "string",
            "description": "One line: what the expert's choice tells us that a naive reading of the English would miss.",
        },
        "correction_is_semantic": {
            "type": "boolean",
            "description": "True if the correction carries meaning information useful to other languages. False if it is internal to the source language (spelling, conjunct, register).",
        },
        "languages": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "lang": {"type": "string"},
                    "verdict": {"type": "string", "enum": ["ok", "wrong"]},
                    "corrected": {"type": "string"},
                    "why": {"type": "string"},
                },
                "required": ["lang", "verdict", "corrected", "why"],
                "additionalProperties": False,
            },
        },
    },
    "required": [
        "english_meaning",
        "correction_reveals",
        "correction_is_semantic",
        "languages",
    ],
    "additionalProperties": False,
}


class PropagationUnavailableError(RuntimeError):
    """Raised for persistent, operator-fixable failures (no API key, credit
    exhausted). Propagated so the task fails visibly rather than silently
    leaving the other languages stale — the same reasoning as
    ClaudeCreditExhaustedError in claude_translation_service."""


def _build_prompt(
    core_name: Optional[str],
    core_description: Optional[str],
    english: str,
    source_lang: str,
    expert_value: str,
    others: list[tuple[str, str]],
) -> str:
    source_name = LANG_NAMES.get(source_lang, source_lang)
    parts = [
        "You are maintaining a multi-language agricultural reference database "
        "used by Indian farmers, extension officers and dealers.",
        "",
    ]
    if core_name:
        parts.append(f'Data category: "{core_name}"')
    if core_description:
        parts.append(f"Category description: {core_description}")
    parts += [
        "",
        f"English label: {english}",
        f"{source_name} ({source_lang}) — CORRECTED BY A DOMAIN EXPERT. This is "
        f"authoritative; do not question or re-translate it: {expert_value}",
        "",
        "Current machine-generated translations in the other languages:",
    ]
    for lang, value in others:
        parts.append(f"  - {LANG_NAMES.get(lang, lang)} ({lang}): {value}")

    parts += [
        "",
        "YOUR TASK",
        "",
        f"The English label never changed. The expert corrected the "
        f"{source_name} because the machine had MISREAD THE ENGLISH — wrong "
        f"sense of an ambiguous word, or the agricultural meaning missed. The "
        f"other languages came from that same English via the same machine, so "
        f"they are likely to carry THE SAME error.",
        "",
        "1. Work out what the English term actually denotes here, using the "
        "expert's rendering as the key.",
        "2. Decide whether the correction carries cross-language information at "
        f"all. If the expert only fixed {source_name} spelling, a conjunct, or "
        f"{source_name}-internal register — the meaning never in doubt — set "
        "correction_is_semantic false and mark every language 'ok'.",
        "3. For each other language, judge whether its current value conveys "
        "that same meaning. If it does: verdict 'ok', and return it unchanged "
        "in 'corrected'. If it conveys the wrong sense: verdict 'wrong', and "
        "give the correct rendering.",
        "",
        "CRITICAL RULES",
        "",
        f"- Do NOT translate from the {source_name}. Translate from the "
        f"ENGLISH, using the meaning the {source_name} revealed. Never carry "
        f"{source_name} words, morphology or phrasing into another language.",
        "- Write each language the way ITS OWN agricultural books and extension "
        "bulletins write it (ICAR / TNAU / KAU / state department register). "
        "Prefer the settled native term; transliterate only where that "
        "language's own literature transliterates — chemical and pesticide "
        "names, brands, virus codes.",
        "- Leave Hindu-Arabic digits (0-9) exactly as they appear. Do NOT "
        "convert to native digit forms. Preserve %, /, -, ., :, decimal "
        "points, ratios and units as-is.",
        "- Be conservative. 'wrong' means the current value would mislead a "
        "farmer or names the wrong thing — not merely that you would have "
        "phrased it differently. A needless rewrite is a cost, not a win.",
    ]

    # Reuse the calibrated per-language gold-standard terms so propagated
    # output matches the house style the rest of the pipeline produces.
    hinted = [lang for lang, _ in others if TERM_HINTS.get(lang)]
    if hinted:
        parts += ["", "Verified gold-standard mappings — use these EXACTLY where the English term appears:"]
        for lang in hinted:
            parts.append(f"  {LANG_NAMES.get(lang, lang)} ({lang}):")
            for hint in TERM_HINTS[lang]:
                parts.append(f"    • {hint}")

    parts += [
        "",
        "Return one entry in `languages` for every language listed above, "
        "using the same language codes.",
    ]
    return "\n".join(parts)


def propagate_from_expert(
    core_name: Optional[str],
    core_description: Optional[str],
    english: str,
    source_lang: str,
    expert_value: str,
    others: list[tuple[str, str]],
) -> Optional[dict]:
    """Ask Claude what the expert's correction implies for the other languages.

    `others` is [(language_code, current_value), ...] — only rows it is safe to
    overwrite (never EXPERT_VALIDATED).

    Returns the parsed response dict, or None on a transient failure (the
    caller leaves the rows untouched). Raises PropagationUnavailableError for
    persistent operator-fixable failures.
    """
    if not others:
        return None

    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise PropagationUnavailableError(
            "ANTHROPIC_API_KEY is not set — cannot propagate expert corrections."
        )

    prompt = _build_prompt(
        core_name, core_description, english, source_lang, expert_value, others
    )

    # Same 429 handling as claude_translation_service: worker concurrency 1
    # plus a low-tier key still trips the limit on a backfill run.
    MAX_RETRIES = 5
    BACKOFF = [3, 6, 12, 24, 40]
    response = None
    for attempt in range(MAX_RETRIES):
        try:
            response = httpx.post(
                ANTHROPIC_API_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": ANTHROPIC_VERSION,
                    "content-type": "application/json",
                },
                json={
                    "model": DEFAULT_MODEL,
                    "max_tokens": 8192,
                    "thinking": {"type": "adaptive"},
                    "output_config": {
                        "format": {"type": "json_schema", "schema": RESPONSE_SCHEMA}
                    },
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=180.0,
            )
        except Exception as e:  # network-level
            logger.warning(f"[propagate] request failed for item: {e}")
            return None
        if response.status_code != 429:
            break
        retry_after = response.headers.get("retry-after")
        try:
            wait = int(retry_after) if retry_after else BACKOFF[attempt]
        except ValueError:
            wait = BACKOFF[attempt]
        logger.warning(
            f"[propagate] 429 rate limit; backing off {wait}s "
            f"[attempt {attempt + 1}/{MAX_RETRIES}]"
        )
        time.sleep(wait)

    if response is None or response.status_code != 200:
        body = (response.text or "") if response is not None else ""
        if response is not None and response.status_code == 400:
            lowered = body.lower()
            if "credit balance" in lowered or "too low" in lowered:
                raise PropagationUnavailableError(
                    "Anthropic credit balance exhausted. Top up at "
                    "https://console.anthropic.com/settings/billing and re-run."
                )
        logger.warning(
            f"[propagate] HTTP {response.status_code if response else '?'}: {body[:300]}"
        )
        return None

    data = response.json()
    # A safety decline arrives as HTTP 200 — check before reading content.
    if data.get("stop_reason") == "refusal":
        logger.warning("[propagate] model declined the request; leaving rows untouched")
        return None

    text = next(
        (b.get("text") for b in data.get("content", []) if b.get("type") == "text"),
        None,
    )
    if not text:
        logger.warning(f"[propagate] no text block: {json.dumps(data)[:300]}")
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"[propagate] unparseable response ({e}): {text[:300]}")
        return None

    usage = data.get("usage", {})
    logger.info(
        f"[propagate] {english!r} from {source_lang}: "
        f"semantic={parsed.get('correction_is_semantic')} "
        f"tokens={usage.get('input_tokens')}in/{usage.get('output_tokens')}out"
    )
    return parsed
