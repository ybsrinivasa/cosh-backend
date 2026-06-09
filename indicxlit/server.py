"""
IndicXlit HTTP wrapper — exposes the AI4Bharat English→Indic
transliteration engine as a tiny FastAPI service so Cosh's Celery
worker can call it over HTTP.

Transliteration ≠ translation: "Mancozeb" → "मॅन्कोझेब" (the *sound*
of the English word written in Devanagari), not the *meaning*. Used
for Cores whose language_mode == TRANSLITERATION — typically brand
names, chemical names, and other proper nouns where the meaning
doesn't carry across scripts.

Engine: ai4bharat-transliteration's XlitEngine, which wraps the
underlying transformer model and handles per-language model loading
+ a beam-width search to pick the most likely transliteration.

Lazy-loading: each language's transformer is ~300 MB in RAM once
loaded. Pre-loading all 12 RootsTalk languages on this prod box
(11 GB RAM, shared with api/postgres/neo4j/celery) caused repeated
OOM-style crashes during startup. Instead we initialise an XlitEngine
per language on first request and cache it. The first request for a
given language pays ~10 s of model load; every subsequent request
to the same language is fast. RAM grows only with the languages
actually used — most production batches hit 1–3 languages.
"""
import logging
import os
import threading
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("indicxlit")

# 12 Indic languages (English is the source; not transliterated to itself).
# Matches the seeded set in scripts/seed_db.py exactly.
SUPPORTED_LANGS = {"hi", "bn", "ta", "te", "kn", "ml", "mr", "gu", "pa", "or", "ur", "as"}

BEAM_WIDTH = int(os.getenv("INDICXLIT_BEAM_WIDTH", "4"))

# One XlitEngine per language, cached lazily. The lock serialises the
# load step (two concurrent first-requests for the same language would
# otherwise both try to initialise the engine).
_engines: Dict[str, object] = {}
_engine_lock = threading.Lock()


def _get_engine(lang: str):
    """Return an XlitEngine for `lang`, loading on first use."""
    eng = _engines.get(lang)
    if eng is not None:
        return eng
    with _engine_lock:
        # Re-check inside the lock in case another request raced us.
        eng = _engines.get(lang)
        if eng is not None:
            return eng
        # Lazy import — keeps torch out of module-import scope.
        from ai4bharat.transliteration import XlitEngine
        logger.info(f"Loading XlitEngine for {lang!r} (beam_width={BEAM_WIDTH})…")
        eng = XlitEngine(lang, beam_width=BEAM_WIDTH, src_script_type="en")
        _engines[lang] = eng
        logger.info(f"XlitEngine[{lang}] ready ({len(_engines)} language(s) cached).")
        return eng


app = FastAPI(title="IndicXlit Wrapper")


class TransliterateRequest(BaseModel):
    source_language: str  # always "en" today, kept for forward compat
    target_language: str
    sentences: List[str]


class TransliterateResponse(BaseModel):
    transliterations: List[str]


@app.get("/health")
def health():
    # The service is "ready" the moment the process is alive — actual
    # engine loading is per-language and on-demand. Listing the cached
    # set helps observability.
    return {
        "status": "ok",
        "supported_languages": sorted(SUPPORTED_LANGS),
        "loaded_languages": sorted(_engines.keys()),
        "beam_width": BEAM_WIDTH,
    }


@app.post("/transliterate", response_model=TransliterateResponse)
def transliterate(req: TransliterateRequest):
    if not req.sentences:
        return TransliterateResponse(transliterations=[])
    if req.target_language not in SUPPORTED_LANGS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported target language: {req.target_language!r}. "
                   f"Supported: {sorted(SUPPORTED_LANGS)}",
        )
    if req.source_language != "en":
        raise HTTPException(
            status_code=400,
            detail="Only English source is supported at present.",
        )

    try:
        engine = _get_engine(req.target_language)
    except Exception as e:
        logger.exception(f"Failed to load engine for {req.target_language}: {e}")
        raise HTTPException(status_code=503, detail=f"Engine load failed: {e}")

    out: List[str] = []
    for sentence in req.sentences:
        # translit_sentence returns the top-beam result for a full
        # sentence — handles whitespace, punctuation, and per-word
        # transliteration internally. Falls back to input on any error
        # so a bad sentence doesn't kill the whole batch.
        try:
            result = engine.translit_sentence(sentence, lang_code=req.target_language)
        except Exception as e:
            logger.warning(f"translit_sentence failed for "
                           f"{req.target_language}/{sentence[:40]}: {e}")
            result = sentence
        out.append(result or sentence)

    return TransliterateResponse(transliterations=out)
