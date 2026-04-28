"""
Translation service — IndicTrans2 primary, Google Translate fallback.
All calls are made via HTTP to keep the translation engine pluggable.
"""
import logging
import os

logger = logging.getLogger(__name__)


def call_indictrans2(text: str, source_lang: str, target_lang: str) -> str | None:
    """
    Call the self-hosted IndicTrans2 service.
    Returns translated text or None if unavailable/failed.
    Expected wrapper API: POST /translate
    Body: {"source_language": "en", "target_language": "hi", "sentences": ["..."]}
    Response: {"translations": ["..."]}
    """
    api_url = os.getenv("INDICTRANS2_API_URL", "").strip()
    if not api_url:
        return None

    try:
        import httpx
        response = httpx.post(
            f"{api_url}/translate",
            json={
                "source_language": source_lang,
                "target_language": target_lang,
                "sentences": [text],
            },
            timeout=30.0,
        )
        if response.status_code == 200:
            data = response.json()
            translations = data.get("translations") or data.get("output") or []
            if translations:
                return translations[0]
    except Exception as e:
        logger.warning(f"IndicTrans2 call failed for lang={target_lang}: {e}")
    return None


def call_google_translate(text: str, source_lang: str, target_lang: str) -> str | None:
    """
    Call Google Cloud Translation API as fallback.
    Returns translated text or None if key not configured/call failed.
    """
    api_key = os.getenv("GOOGLE_TRANSLATE_API_KEY", "").strip()
    if not api_key or api_key == "placeholder":
        return None

    try:
        import httpx
        response = httpx.post(
            f"https://translation.googleapis.com/language/translate/v2?key={api_key}",
            json={
                "q": text,
                "source": source_lang,
                "target": target_lang,
                "format": "text",
            },
            timeout=15.0,
        )
        if response.status_code == 200:
            data = response.json()
            translations = data.get("data", {}).get("translations", [])
            if translations:
                return translations[0].get("translatedText")
    except Exception as e:
        logger.warning(f"Google Translate call failed for lang={target_lang}: {e}")
    return None


def translate_text(text: str, source_lang: str, target_lang: str) -> str | None:
    """
    Translate text: IndicTrans2 first, Google Translate as fallback.
    Never translate English to English.
    """
    if source_lang == target_lang or target_lang == "en":
        return None

    result = call_indictrans2(text, source_lang, target_lang)
    if result:
        logger.info(f"IndicTrans2 translated [{target_lang}]: {text[:30]}...")
        return result

    result = call_google_translate(text, source_lang, target_lang)
    if result:
        logger.info(f"Google Translate (fallback) translated [{target_lang}]: {text[:30]}...")
        return result

    logger.warning(f"No translation available for [{target_lang}]: {text[:30]}...")
    return None
