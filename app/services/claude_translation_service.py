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


# Language-specific gold-standard term mappings. These are inserted into the
# prompt only for the matching target_lang so we don't leak Kannada examples
# into a Hindi call. Calibrated by native-speaking domain experts as feedback
# arrives. When other languages are calibrated, add a sibling dict for them.
# (V2 follow-up: move these to a DB-backed per-Core glossary so non-engineers
# can maintain them.)
TERM_HINTS = {
    "kn": [
        # crop names (kitchen vs field/grain distinction matters)
        "Rice / Paddy → ಭತ್ತ  (NOT ಅಕ್ಕಿ — that's the cooked-rice term)",
        "Mango → ಮಾವು  (and ಮಾವಿನ in the genitive when it modifies a following noun)",
        "Bitter Gourd → ಹಾಗಲಕಾಯಿ",
        "Bottle Gourd → ಸೋರೆಕಾಯಿ",
        "Ash Gourd → ಬೂದು ಕುಂಬಳ",
        "Brinjal → ಬದನೆ",
        # pests
        "Hopper → ಜಿಗಿ ಹುಳು  (NOT ಹಾಪರ್)",
        "Brown Plant Hopper → ಕಂದು ಜಿಗಿ ಹುಳು",
        "Beetle → ಜೀರುಂಡೆ  (NOT ಬಿಟಲ್)",
        "Hadda Beetle → ಹಡ್ಡ ಜೀರುಂಡೆ  (keep the 'Hadda' qualifier — it identifies the species)",
        "Fruit Fly → ಹಣ್ಣಿನ ನೊಣ",
        "Aphid → ಸಸ್ಯ ಹೇನು  (write the ಸ್ಯ conjunct exactly — ಸ + ್ + ಯ. This is the UAS Bangalore Padakosh standard, Pub-255 p26, used across the book: 'Aphis gossypii' → ಹತ್ತಿ ಸಸ್ಯಹೇನು; 'Aphidicide' → ಸಸ್ಯ ಹೇನು ನಾಶಕ. Use ಸಸ್ಯ ಹೇನು for EVERY 'Aphid' in EVERY crop/compound.)",
        "Leaf Miner → ರಂಗೋಲಿ ಹುಳು  (named after rangoli — the curving trail it leaves on the leaf; NOT 'leaf-eating worm')",
        "Mite → ನುಸಿ  (NOT the transliteration ಮಿಟೆ)",
        "Nematode → ಜಂತು ಹುಳು  (NOT the transliteration ನೆಮಟೋಡ್)",
        "Shoot and Fruit Borer → ಚಿಗುರು ಮತ್ತು ಕಾಯಿ ಕೊರಕ",
        # diseases
        "Blast → ಬೆಂಕಿ ರೋಗ  (the 'fire disease' term used in books; NOT a literal translation)",
        "Powdery Mildew → ಬೂದಿ ರೋಗ  (NOT ಪೌಡರಿ ಮಿಲ್ಡ್ಯೂ — and 'ಬೂದಿ' = ash, not 'ಬೂದು' = grey)",
        "Early Blight → ಅಗ್ರ ಅಂಗಮಾರಿ  (blights are 'Angamari' in Kannada)",
        "Mosaic Disease → ಮೊಸೇಕ್ ನಂಜು ರೋಗ  ('Nanju' = virus/poison; transliterate Mosaic, native term for the rest)",
    ],
    # Hindi: sourced primarily from CSTT (Commission for Scientific and
    # Technical Terminology, Ministry of Education) — "Fundamental Glossary
    # of Agriculture, English-Hindi-Gujarati", 2017. These are the official
    # government-standardised terms used in books and extension publications.
    "hi": [
        # crops — note the field/grain term distinction
        "Rice / Paddy → धान  (field/grain term; NOT चावल — that's the kitchen term)",
        "Wheat → गेहूं",
        "Maize → मक्का",
        "Cotton → कपास",
        "Sugarcane → गन्ना",
        "Sorghum / Jowar → ज्वार",
        "Pearl Millet / Bajra → बाजरा",
        "Groundnut → मूंगफली",
        "Mustard → सरसों",
        "Sunflower → सूरजमुखी",
        "Sesame → तिल",
        "Castor → अरंडी",
        "Linseed → अलसी",
        "Tobacco → तम्बाकू",
        "Mango → आम",
        "Tomato → टमाटर",
        "Brinjal → बैंगन",
        "Onion → प्याज",
        "Potato → आलू",
        "Cabbage → बंदगोभी",
        "Cauliflower → फूलगोभी",
        "Cucumber → खीरा",
        "Pumpkin → कद्दू",
        "Bitter Gourd → करेला",
        "Bottle Gourd → लौकी",
        "Ash Gourd → पेठा",
        "Capsicum → शिमलामिर्च",
        "Coconut → नारियल",
        "Guava → अमरूद",
        "Apple → सेब",
        "Banana → केला",
        "Papaya → पपीता",
        "Pomegranate → अनार",
        "Ginger → अदरक",
        "Turmeric → हल्दी",
        "Cardamom → इलाइची",
        "Spinach → पालक",
        "Garlic → लहसुन",
        "Fenugreek → मेथी",
        "Coriander → धनिया",
        "Cumin → जीरा",
        "Black Gram → उड़द",
        "Green Gram → मूंग",
        "Cluster Bean → ग्वारबीन",
        "Cashew → काजू",
        "Barley → जौ",
        # pests
        "Aphid → माहू",
        "Hopper → खुरलिका",
        "Jassid → जैसिड",
        "Stem Borer → तना बेधक",
        "Caterpillar → इल्ली",
        "Leaf Miner → पर्ण सुरंगक",
        "Locust → टिड्डी",
        "Moth → पतंगा",
        "Nematode → सूत्रकृमि",
        "Scale Insect → शल्ककीट",
        "Thrips → थ्रिप्स",
        "Gall Midge → पीटिकाकीट",
        # diseases
        "Blast → प्रध्वंस रोग  (also called झोंका रोग in some areas)",
        "Blight → अंगमारी रोग",
        "Canker → कैंकर",
        "Virus → विषाणु",
        "Wilting → म्लानि",
        "Chlorosis → हरिमाहीनता",
        "Necrosis → ऊतकक्षय",
        # deficiencies / nutrients
        "Deficiency → न्यूनता",
        "Nitrogen → नाइट्रोजन",
        "Nitrogen Deficiency → नाइट्रोजन न्यूनता",
        "Nutrient Deficiency → पोषक न्यूनता",
        "Zinc → जिंक",
        "Zinc Deficiency → जिंक अल्पता",
    ],
    # Gujarati: same CSTT source as Hindi.
    "gu": [
        # crops
        "Rice / Paddy → ડાંગર  (field/grain term; NOT ચોખા — that's the kitchen term)",
        "Wheat → ઘઉં",
        "Maize → મકાઈ",
        "Cotton → કપાસ",
        "Sugarcane → શેરડી",
        "Sorghum / Jowar → જુવાર",
        "Pearl Millet / Bajra → બાજરો",
        "Groundnut → મગફળી",
        "Mustard → રાઈ",
        "Sunflower → સૂર્યમુખી",
        "Sesame → તલ",
        "Castor → એરંડો",
        "Linseed → અળસી",
        "Tobacco → તમાકુ",
        "Mango → આંબો",
        "Tomato → ટામેટા",
        "Brinjal → રીંગણ",
        "Onion → ડુંગળી",
        "Potato → બટાકા",
        "Cabbage → કોબીજ",
        "Cauliflower → ફુલાવર",
        "Cucumber → કાકડી",
        "Pumpkin → કોળું",
        "Bitter Gourd → કારેલાં",
        "Bottle Gourd → દૂધી",
        "Ash Gourd → તુંબડું",
        "Capsicum → શિમલા મરચું",
        "Coconut → નાળિયેર",
        "Guava → જામફળ",
        "Apple → સફરજન",
        "Banana → કેળાં",
        "Papaya → પપૈયું",
        "Pomegranate → દાડમ",
        "Ginger → આદું",
        "Turmeric → હળદર",
        "Cardamom → એલચી",
        "Spinach → પાલક",
        "Garlic → લસણ",
        "Fenugreek → મેથી",
        "Coriander → ધાણા",
        "Cumin → જીરું",
        "Black Gram → અડદ",
        "Cluster Bean → ગુવાર",
        "Cashew → કાજુ",
        "Barley → જવ",
        # pests
        "Aphid → મોલો",
        "Hopper → ઓરણી",
        "Jassid → લીલા તડતડિયા",
        "Stem Borer → થડ વેધક",
        "Caterpillar → ઇયળ",
        "Leaf Miner → પાનકોરિયું",
        "Locust → તીડ",
        "Moth → ફૂદું",
        "Nematode → કૃમિ",
        "Scale Insect → સ્કેલ કીટક",
        "Thrips → થ્રિપ્સ",
        "Gall Midge → ગાંઠિયા ઈયળ",
        # diseases
        "Blast → કરમોડી રોગ",
        "Blight → ચરમી રોગ",
        "Canker → ઉપસેલું ચાઠું",
        "Virus → વિષાણુ",
        "Wilting → સુકારો",
        "Chlorosis → હરિતદ્રવ્યહીનતા",
        "Necrosis → પેશીક્ષય",
        # deficiencies / nutrients
        "Deficiency → ઊણપ",
        "Nitrogen → નાઇટ્રોજન",
        "Nitrogen Deficiency → નાઇટ્રોજન ઉણપ",
        "Nutrient Deficiency → પોષકતત્વની ઊણપ",
        "Zinc → જસત",
        "Zinc Deficiency → જસતની ઊણપ",
    ],
    # Tamil / Telugu / Bengali / Malayalam / Marathi / Punjabi / Odia / Urdu /
    # Assamese — calibrate similarly with help from a native-speaking field
    # expert before running full batches.
}


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"


class ClaudeCreditExhaustedError(RuntimeError):
    """Raised when Anthropic returns 'credit balance too low'.

    Deliberately propagated up out of `claude_translate()` (NOT swallowed
    like generic API failures) so that bulk translation tasks abort
    immediately rather than silently falling through to the
    IndicTrans2/Google fallback, which produces poor-quality
    transliterations for short agricultural labels. The previous behaviour
    looked like 'success' in the status badge but quietly destroyed
    quality — see the 2026-06-11 Hindi incident.
    """
    pass



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
        "",
        f"- **Default to the native {target_name} agricultural term** for crops, common "
        "pests, common diseases, deficiencies, and nutrient names. Books and "
        "extension publications almost always have a settled term for these.",
    ])
    hints = TERM_HINTS.get(target_lang)
    if hints:
        parts.extend([
            "",
            f"  Verified gold-standard {target_name} mappings — use these EXACTLY "
            f"when these English terms appear (calibrated by a native-speaking "
            f"agricultural domain expert):",
        ])
        for h in hints:
            parts.append(f"    • {h}")
    parts.extend([
        "",
        "- **Transliterate ONLY when there is genuinely no settled native term** in "
        "agricultural literature. Use transliteration for things like:",
        "    • Specific chemical/pesticide names (Mancozeb, Carbendazim, Imidacloprid)",
        "    • Specific brand names",
        "    • Modern technical jargon, virus codes (TYLCV, etc.)",
        "    • A handful of recently-borrowed terms where books themselves transliterate",
        "",
        f"- If unsure whether a settled native {target_name} term exists, choose the "
        "native term. The downside of a slightly less-common native word is small; "
        "the downside of an unnecessary transliteration is large (it makes the data "
        "feel foreign).",
        "",
        "- Do NOT produce poetic, literary, or Sanskrit-leaning renderings.",
        "- Compound inputs ('Crop - Pest/Disease'): render each part by the rules "
        "above and keep the same separator (hyphen, slash, etc.) in the same place.",
        f"  When a crop modifies a pest/disease in a compound, use the grammatically "
        f"correct form for the language (e.g. in Kannada, ಮಾವು is the noun but ಮಾವಿನ "
        f"is the form that modifies a following noun like ಜಿಗಿ ಹುಳು).",
        "- Be CONSISTENT: the same crop name must always be rendered the same way "
        "across rows, whether it appears alone or as the modifier in a 'Crop - X' "
        "compound (allowing for the grammatical case shift just described).",
        "",
        "- **Numerals: leave Hindu-Arabic digits (0-9) EXACTLY as they appear.** Do "
        "NOT convert them to the native script's digit forms (no ೦೧೨ for Kannada, "
        "no ०१२ for Hindi/Marathi, no ০১২ for Bengali, no ௦௧௨ for Tamil, no "
        "౦౧౨ for Telugu, no ٠١٢ for Urdu, etc.). This rule also applies to digits "
        "embedded inside compound text — translate the surrounding words, keep the "
        "numerals untouched. Preserve symbols around numerals as-is too: %, /, -, ., :, "
        "decimal points, ratios, units (e.g. '19-19-19', '0.5 g/L', '10%', '2 weeks').",
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

    import time
    try:
        import httpx
        prompt = _build_prompt(text, target_lang, core_name, core_description)
        # Retry on 429 (rate limit). Worker concurrency=1 plus a tier-1
        # Anthropic key (30k input tokens/min on Sonnet) is enough to
        # exceed the limit when batching short labels. When we hit 429 we
        # honour the `retry-after` header from Anthropic if present,
        # otherwise back off exponentially. 5 attempts × max ~40 s ≈
        # ~2 min worst case per item — acceptable; the worker is async
        # from the user's perspective anyway.
        MAX_RETRIES = 5
        BACKOFF = [3, 6, 12, 24, 40]
        response = None
        for attempt in range(MAX_RETRIES):
            response = httpx.post(
                ANTHROPIC_API_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": ANTHROPIC_VERSION,
                    "content-type": "application/json",
                },
                json={
                    "model": DEFAULT_MODEL,
                    # Short labels — 80 tokens covers even longer Indic compounds
                    # with script overhead. Capping low also blocks the "let me
                    # reconsider…" rambling pattern observed on uncertain
                    # agricultural terms. (The Anthropic API rejects
                    # whitespace-only stop_sequences, so we enforce single-line
                    # at parse time below instead.)
                    "max_tokens": 80,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=60.0,
            )
            if response.status_code != 429:
                break
            # Rate-limited. Sleep then retry.
            retry_after = response.headers.get("retry-after")
            try:
                wait = int(retry_after) if retry_after else BACKOFF[attempt]
            except ValueError:
                wait = BACKOFF[attempt]
            logger.warning(
                f"Claude 429 (rate limit) for lang={target_lang}; "
                f"backing off {wait}s [attempt {attempt + 1}/{MAX_RETRIES}]"
            )
            time.sleep(wait)
        if response is None or response.status_code != 200:
            # Credit exhaustion is a deliberate, persistent failure — every
            # subsequent call will fail the same way until the user tops up.
            # Raise loudly so the bulk task aborts and the user sees it, rather
            # than letting 2,000+ rows silently fall through to the
            # IndicTrans2/Google fallback (which produces garbage for these
            # short agricultural labels).
            if response is not None and response.status_code == 400:
                body_lower = (response.text or "").lower()
                if "credit balance" in body_lower or "too low" in body_lower:
                    raise ClaudeCreditExhaustedError(
                        f"Anthropic credit balance exhausted (lang={target_lang}). "
                        "Top up at https://console.anthropic.com/settings/billing "
                        "and re-fire the translation."
                    )
            logger.warning(
                f"Claude API HTTP {response.status_code if response else '?'} for "
                f"lang={target_lang}: {response.text[:200] if response else '(no response)'}"
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
    except ClaudeCreditExhaustedError:
        # Propagate — task layer aborts cleanly. Never silently downgrade.
        raise
    except Exception as e:
        logger.warning(f"Claude API call failed for lang={target_lang}: {e}")
        return None
