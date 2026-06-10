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
# Sonnet 4.6 was confirmed by a native-Kannada-speaking domain expert as
# producing the right register — terms that appear in agricultural books
# and university extension publications, not literary/Sanskrit synonyms,
# and English transliterations where books actually transliterate
# ("Early Blight" → "ಅರ್ಲಿ ಬ್ಲೈಟ್"). Haiku failed this test (e.g. translated
# Rice as ಅಕ್ಕಿ — cooked rice — instead of ಭತ್ತ paddy). Do not lower the
# model tier without re-running the comparison.
DEFAULT_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")


def _build_prompt(text: str, target_lang: str, core_name: Optional[str], core_description: Optional[str]) -> str:
    """Construct the user message for a single short-label translation.

    The prompt is deliberately oriented around the *register* of Indian
    agricultural literature (university extension publications, state
    department of agriculture bulletins, ICAR / UAS / TNAU / KAU
    handbooks) rather than abstract "translation". A native Kannada
    domain expert verified that this framing produces the terminology
    farmers and field experts actually read and use — and that includes
    transliteration of English technical terms when books transliterate
    them ("Early Blight" → "ಅರ್ಲಿ ಬ್ಲೈಟ್", not invented synonyms).
    """
    target_name = LANG_NAMES.get(target_lang, target_lang)
    parts = [
        f"You are writing one cell in a {target_name} agricultural reference "
        f"publication — the kind produced by Indian agricultural universities "
        f"(UAS, TNAU, KAU, ICAR) and state agriculture departments for use by "
        f"farmers, extension officers, and dealers.",
        "",
        "Context for this cell:",
    ]
    if core_name:
        parts.append(f'- Data category: "{core_name}"')
    if core_description:
        parts.append(f"- Category description: {core_description}")
    parts.extend([
        "",
        "Register rules (very important — this is NOT general translation):",
        f"- Write the term EXACTLY as a {target_name} agricultural book, extension "
        "bulletin, or field-officer article would write it.",
        f"- For crop names, use the regional {target_name} name that farmers actually "
        "use (e.g. for paddy, write the field/grain term — ಭತ್ತ in Kannada — not the "
        "kitchen term ಅಕ್ಕಿ).",
        "- For pests, diseases, deficiencies, and nutrients, use whichever form books "
        "and bulletins actually use. That may be a native term, or a transliteration "
        "of the English technical term — books transliterate freely for terms like "
        "'Early Blight', 'Powdery Mildew', 'Hopper', 'Beetle' — follow that convention "
        "instead of inventing literary or Sanskrit synonyms nobody reads.",
        "- Do NOT produce poetic, literary, or Sanskrit-leaning renderings.",
        "- Compound inputs ('Crop - Pest/Disease'): translate each part by the rules "
        "above and keep the same separator (hyphen, slash, etc.) in the same place.",
        "- Be CONSISTENT: the same crop must always be rendered the same way across "
        "rows, whether it appears alone or in a 'Crop - X' compound.",
        "",
        f"English label to render in {target_name}: {text}",
        "",
        "Output rules — strict:",
        f"- Reply with ONE single line: the {target_name} rendering only.",
        "- No quotes, no backticks, no explanations, no alternatives, no parentheticals, "
        "no English notes. Do NOT 'reconsider' or add a second attempt.",
        f"- If you are unsure, give your single best book/bulletin {target_name} answer "
        "in one line and stop.",
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
