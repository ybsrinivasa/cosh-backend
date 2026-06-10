"""
Claude translation service — domain-aware translation for Cosh Cores.

Why Claude instead of IndicTrans2 for TRANSLATION-mode Cores:
  IndicTrans2-distill-200M (and even the 1B variant) is a general MT model
  trained on sentence-level text. Cosh inputs are short noun phrases like
  "Ash Gourd - Beetle" — no sentence context, domain-specific vocabulary,
  multi-word compound terms. The distilled MT model collapses these to
  nearby common words (observed live: "Ash Gourd → Shunti" which means
  ginger; "Bitter Gourd" never coming out right).

  Claude can read the parent Core's name + description, understand the
  agricultural domain, and produce the term that Indian farmers actually
  use. Cost is ~$3 per 2237-item × 4-language Core run on Haiku 4.5.

  TRANSLITERATION-mode Cores still go to IndicXlit — different problem,
  IndicXlit does it well.
"""
import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


# Map BCP-47 / Cosh language codes to readable names for the prompt.
# The names mirror RootsTalk's seeded language registry.
LANG_NAMES = {
    "hi": "Hindi",
    "bn": "Bengali",
    "ta": "Tamil",
    "te": "Telugu",
    "kn": "Kannada",
    "ml": "Malayalam",
    "mr": "Marathi",
    "gu": "Gujarati",
    "pa": "Punjabi (Gurmukhi)",
    "or": "Odia",
    "ur": "Urdu",
    "as": "Assamese",
}


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
# Haiku 4.5 is fast (~1 s per call), cheap (~$0.0003/call), and good enough
# for short structured agricultural labels. Override with CLAUDE_MODEL if
# you want to A/B against Sonnet on a harder Core.
DEFAULT_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")


def _build_prompt(text: str, target_lang: str, core_name: Optional[str], core_description: Optional[str]) -> str:
    """Construct the user message for a single short-label translation."""
    target_name = LANG_NAMES.get(target_lang, target_lang)
    parts = [
        f"You are translating one short data label from English to {target_name} "
        f"for an Indian agricultural knowledge graph (Cosh).",
        "",
        "Context for this label:",
    ]
    if core_name:
        parts.append(f'- Data category: "{core_name}"')
    if core_description:
        parts.append(f"- Category description: {core_description}")
    parts.extend([
        "- Audience: rural farmers and agricultural extension experts in India",
        f"- Use the standard {target_name} term that local farmers and field experts "
        f"actually use, NOT a literal word-for-word rendering",
        "- If the source is a compound (e.g. 'Ash Gourd - Beetle'), translate each part "
        "with the correct domain term (the crop name, the pest name) and keep the same "
        "separator structure",
        "- Preserve hyphens, slashes, and other separators exactly as in the source",
        "- Do NOT transliterate proper-noun chemical or brand names; translate by meaning "
        "where a domain term exists, otherwise transliterate naturally",
        "",
        f"Source: {text}",
        "",
        "Output rules — strict:",
        f"- Reply with ONE single line: the {target_name} translation only.",
        "- Do NOT include quotes, backticks, explanations, alternatives, parentheticals, "
        "or English. Do NOT 'reconsider'. Do NOT add a second attempt.",
        f"- If you are unsure, give your single best {target_name} answer in one line and stop.",
    ])
    return "\n".join(parts)


def claude_translate(
    text: str,
    source_lang: str,
    target_lang: str,
    core_name: Optional[str] = None,
    core_description: Optional[str] = None,
) -> Optional[str]:
    """
    Translate a single short label using Claude with Core context.
    Returns the translation or None on failure (caller can fall back).
    """
    if source_lang == target_lang or target_lang == "en":
        return None

    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None

    try:
        import httpx
        prompt = _build_prompt(text, target_lang, core_name, core_description)
        response = httpx.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": ANTHROPIC_VERSION,
                "content-type": "application/json",
            },
            json={
                "model": DEFAULT_MODEL,
                # Short labels — 80 tokens covers even longer Indic compounds with
                # script overhead. Capping low also blocks the "let me reconsider…"
                # rambling pattern observed on uncertain agricultural terms.
                # (The Anthropic API rejects whitespace-only stop_sequences,
                # so we enforce single-line at parse time below instead.)
                "max_tokens": 80,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60.0,
        )
        if response.status_code != 200:
            logger.warning(
                f"Claude API HTTP {response.status_code} for lang={target_lang}: "
                f"{response.text[:200]}"
            )
            return None
        data = response.json()
        content = data.get("content") or []
        if not content or content[0].get("type") != "text":
            logger.warning(f"Claude API returned unexpected shape: {json.dumps(data)[:200]}")
            return None
        raw = (content[0].get("text") or "").strip()
        # Take ONLY the first non-empty line. Claude occasionally rambles past
        # the "no alternatives" instruction with a "Let me reconsider…" second
        # line on uncertain agricultural terms; the first line is always the
        # primary answer.
        first_line = ""
        for line in raw.splitlines():
            line = line.strip()
            if line:
                first_line = line
                break
        # Strip surrounding quotes if Claude added them despite instructions.
        if len(first_line) >= 2 and first_line[0] in '"' "'" and first_line[-1] == first_line[0]:
            first_line = first_line[1:-1].strip()
        if not first_line:
            return None
        return first_line
    except Exception as e:
        logger.warning(f"Claude API call failed for lang={target_lang}: {e}")
        return None
