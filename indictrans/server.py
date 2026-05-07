"""
IndicTrans2 HTTP wrapper — exposes the AI4Bharat English→Indic translation
model as a tiny FastAPI service so Cosh's Celery worker can call it over
HTTP. Loads the model once at startup and keeps it in memory.
"""
import logging
import os
from contextlib import asynccontextmanager
from typing import List

import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("indictrans")

MODEL_NAME = os.getenv("INDICTRANS_MODEL", "ai4bharat/indictrans2-en-indic-dist-200M")
MAX_LENGTH = int(os.getenv("INDICTRANS_MAX_LENGTH", "256"))
NUM_BEAMS = int(os.getenv("INDICTRANS_NUM_BEAMS", "5"))
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# ISO 2-letter → IndicTrans2 flores tag.
LANG_MAP = {
    "en": "eng_Latn",
    "hi": "hin_Deva",
    "bn": "ben_Beng",
    "ta": "tam_Taml",
    "te": "tel_Telu",
    "kn": "kan_Knda",
    "ml": "mal_Mlym",
    "mr": "mar_Deva",
    "gu": "guj_Gujr",
    "pa": "pan_Guru",
    "or": "ory_Orya",
    "ur": "urd_Arab",
    "as": "asm_Beng",
    "ne": "npi_Deva",
    "sa": "san_Deva",
}

state: dict = {}


def _flores(code: str) -> str:
    code = (code or "").lower().strip()
    if code in LANG_MAP:
        return LANG_MAP[code]
    if "_" in code:
        return code  # caller already passed a flores tag
    raise HTTPException(status_code=400, detail=f"Unsupported language code: {code!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
    from IndicTransToolkit import IndicProcessor

    logger.info(f"Loading {MODEL_NAME} on {DEVICE}…")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME, trust_remote_code=True).to(DEVICE)
    model.eval()

    state["tokenizer"] = tokenizer
    state["model"] = model
    state["ip"] = IndicProcessor(inference=True)
    logger.info("Model ready.")
    yield


app = FastAPI(lifespan=lifespan, title="IndicTrans2 Wrapper")


class TranslateRequest(BaseModel):
    source_language: str
    target_language: str
    sentences: List[str]


class TranslateResponse(BaseModel):
    translations: List[str]


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_loaded": "model" in state,
        "device": DEVICE,
        "model": MODEL_NAME,
    }


@app.post("/translate", response_model=TranslateResponse)
def translate(req: TranslateRequest):
    if "model" not in state:
        raise HTTPException(status_code=503, detail="Model still loading")
    if not req.sentences:
        return TranslateResponse(translations=[])

    src = _flores(req.source_language)
    tgt = _flores(req.target_language)

    tokenizer = state["tokenizer"]
    model = state["model"]
    ip = state["ip"]

    batch = ip.preprocess_batch(req.sentences, src_lang=src, tgt_lang=tgt)
    inputs = tokenizer(
        batch,
        truncation=True,
        padding="longest",
        return_tensors="pt",
        max_length=MAX_LENGTH,
    ).to(DEVICE)

    with torch.no_grad():
        generated = model.generate(
            **inputs,
            max_length=MAX_LENGTH,
            num_beams=NUM_BEAMS,
            num_return_sequences=1,
        )

    decoded = tokenizer.batch_decode(
        generated,
        skip_special_tokens=True,
        clean_up_tokenization_spaces=True,
    )
    translations = ip.postprocess_batch(decoded, lang=tgt)
    return TranslateResponse(translations=translations)
