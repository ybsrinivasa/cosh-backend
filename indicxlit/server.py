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

The first call to a language downloads its model (~50–80 MB each)
and warms it in memory. We pre-load all 14 RootsTalk languages at
startup so the first real request isn't a multi-minute wait.
"""
import logging
import os
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("indicxlit")

# 13 Indic languages (English is the source; not transliterated to itself).
# Matches the seeded set in scripts/seed_db.py exactly.
DEFAULT_LANGS = ["hi", "bn", "ta", "te", "kn", "ml", "mr", "gu", "pa", "or", "ur", "as"]
SUPPORTED_LANGS = set(DEFAULT_LANGS)

BEAM_WIDTH = int(os.getenv("INDICXLIT_BEAM_WIDTH", "4"))

state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Imported lazily so module import doesn't bring torch into scope before
    # uvicorn has logging configured.
    from ai4bharat.transliteration import XlitEngine

    logger.info(f"Loading XlitEngine for {len(DEFAULT_LANGS)} languages "
                f"(beam_width={BEAM_WIDTH})…")
    # `src_script_type='en'` lets the engine accept ASCII input and emit
    # the target Indic script. Passing all languages up-front means
    # subsequent requests don't pay the cold-start cost.
    state["engine"] = XlitEngine(
        DEFAULT_LANGS,
        beam_width=BEAM_WIDTH,
        src_script_type="en",
    )
    logger.info("XlitEngine ready.")
    yield


app = FastAPI(lifespan=lifespan, title="IndicXlit Wrapper")


class TransliterateRequest(BaseModel):
    source_language: str  # always "en" today, kept for forward compat
    target_language: str
    sentences: List[str]


class TransliterateResponse(BaseModel):
    transliterations: List[str]


@app.get("/health")
def health():
    return {
        "status": "ok",
        "engine_loaded": "engine" in state,
        "languages": sorted(SUPPORTED_LANGS),
        "beam_width": BEAM_WIDTH,
    }


@app.post("/transliterate", response_model=TransliterateResponse)
def transliterate(req: TransliterateRequest):
    if "engine" not in state:
        raise HTTPException(status_code=503, detail="Engine still loading")
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

    engine = state["engine"]
    out: List[str] = []
    for sentence in req.sentences:
        # XlitEngine.translit_sentence returns the top-beam result for a
        # full sentence — handles whitespace, punctuation, and per-word
        # transliteration internally. Falls back to the input as-is if
        # nothing can be transliterated, which is the safe default.
        try:
            result = engine.translit_sentence(sentence, lang_code=req.target_language)
        except Exception as e:
            logger.warning(f"translit_sentence failed for "
                           f"{req.target_language}/{sentence[:40]}: {e}")
            result = sentence
        out.append(result or sentence)

    return TransliterateResponse(transliterations=out)
