"""
Transliteration service — IndicXlit wrapper over HTTP.

Used for Cores whose language_mode is TRANSLITERATION. There's no
sensible fallback: machine-translating a brand name produces wrong
output, so if IndicXlit is unreachable we return None and let the
caller skip writing a translation row. The "Translate" button can
be re-tried once the service is back.
"""
import logging
import os

logger = logging.getLogger(__name__)


def call_indicxlit(text: str, source_lang: str, target_lang: str) -> str | None:
    api_url = os.getenv("INDICXLIT_API_URL", "").strip()
    if not api_url:
        return None

    try:
        import httpx
        # Same reasoning as translation_service: IndicXlit serializes on CPU
        # and a longer phrase under concurrent load can take 30-60 s. 120 s
        # leaves headroom for the worst case.
        response = httpx.post(
            f"{api_url}/transliterate",
            json={
                "source_language": source_lang,
                "target_language": target_lang,
                "sentences": [text],
            },
            timeout=120.0,
        )
        if response.status_code == 200:
            data = response.json()
            out = data.get("transliterations") or []
            if out:
                return out[0]
        else:
            logger.warning(f"IndicXlit returned HTTP {response.status_code} "
                           f"for lang={target_lang}: {response.text[:200]}")
    except Exception as e:
        logger.warning(f"IndicXlit call failed for lang={target_lang}: {e}")
    return None


def transliterate_text(text: str, source_lang: str, target_lang: str) -> str | None:
    """
    Transliterate text using IndicXlit. Returns the transliterated string,
    or None if the service is unreachable / source already matches target /
    target is English (we never transliterate Indic → English).
    """
    if source_lang == target_lang or target_lang == "en":
        return None
    return call_indicxlit(text, source_lang, target_lang)
