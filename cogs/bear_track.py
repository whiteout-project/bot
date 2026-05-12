"""
Bear damage tracking. Records, views, and charts bear hunt damage per alliance.
"""
import discord
from discord.ext import commands
from discord import app_commands
import asyncio
from contextlib import asynccontextmanager
import io
import re
import os
import sqlite3
import unicodedata
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from cogs.attendance import MATPLOTLIB_AVAILABLE
from .pimp_my_bot import theme, safe_edit_message, check_interaction_user
from .permission_handler import PermissionManager
import numpy as np

logger = logging.getLogger('bot')

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    Image = None
    PIL_AVAILABLE = False

try:
    from rapidfuzz import process as _rf_process, fuzz as _rf_fuzz, utils as _rf_utils
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _rf_process = None
    _rf_fuzz = None
    _rf_utils = None
    RAPIDFUZZ_AVAILABLE = False
    logger.warning("rapidfuzz not installed — player-name resolution will fall back to manual entry only.")

from . import onnx_lifecycle

# RapidOCR setup. Engines are lazy-loaded per language via onnx_lifecycle and
# unloaded ~2 min after the last bear session finalises.
OCR_AVAILABLE = False

# Above ~1800px ONNXRuntime hits 'bad allocation' on the 2nd-3rd image.
MAX_OCR_DIM = 1600

DEFAULT_OCR_LANG = "en"

if PIL_AVAILABLE:
    try:
        from rapidocr import RapidOCR, LangRec
        try:
            from rapidocr.utils.download_file import DownloadFile
            DownloadFile.check_is_atty = staticmethod(lambda: False)
        except Exception:
            pass
        OCR_AVAILABLE = True
        logger.info("Bear track OCR ready (engines load on demand per language).")
    except ImportError:
        logger.warning("rapidocr not installed. OCR will be disabled.")
        print("[WARNING] rapidocr not installed. Bear track OCR disabled.")
    except Exception as e:
        logger.error(f"Failed to initialize RapidOCR: {e}")
        print(f"[ERROR] Failed to initialize RapidOCR: {e}")

os.makedirs("db", exist_ok=True)


def init_bear_database():
    """Initialize bear_hunts + bear_player_damage tables."""
    db_path = "db/bear_data.sqlite"
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bear_hunts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alliance_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            hunting_trap INTEGER NOT NULL,
            rallies INTEGER,
            total_damage INTEGER,
            UNIQUE (alliance_id, date, hunting_trap)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bear_player_damage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hunt_id INTEGER NOT NULL REFERENCES bear_hunts(id) ON DELETE CASCADE,
            fid INTEGER,
            raw_name TEXT,
            resolved_nickname TEXT,
            damage INTEGER NOT NULL,
            rank INTEGER,
            match_score INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bpd_fid ON bear_player_damage(fid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bpd_hunt ON bear_player_damage(hunt_id)")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bear_ocr_lang_stats (
            alliance_id INTEGER NOT NULL,
            lang        TEXT    NOT NULL,
            role        TEXT    NOT NULL,
            runs        INTEGER NOT NULL DEFAULT 0,
            rows_filled INTEGER NOT NULL DEFAULT 0,
            last_run_at TEXT,
            PRIMARY KEY (alliance_id, lang, role)
        )
    """)
    conn.commit()
    conn.close()


init_bear_database()


_OCR_DIGIT_MAP = {
    'O': '0', 'o': '0', 'Q': '0',
    'I': '1', 'l': '1', '|': '1', '!': '1',
    'Z': '2',
    'S': '5',
    'B': '8',
    'g': '9',
}

_OCR_DIGIT_RUN = re.compile(
    rf"[\d,\.{re.escape(''.join(_OCR_DIGIT_MAP.keys()))}]{{3,}}"
)

# Chinese OCR sometimes emits full-width punctuation inside number runs.
_FULLWIDTH_PUNCT = str.maketrans({
    '，': ',', '．': '.', '：': ':', '；': ';',
})

# Korean OCR omits thousand separators; reinsert them on 9-12 digit runs
# (damage range; sidesteps 8-digit FIDs and date/time fragments).
_BARE_DAMAGE_RUN_RE = re.compile(r'(?<!\d)(\d{9,12})(?!\d)')


def repair_ocr_digits(text: str) -> str:
    text = text.translate(_FULLWIDTH_PUNCT)
    text = _BARE_DAMAGE_RUN_RE.sub(lambda m: f'{int(m.group(1)):,}', text)
    def _fix(match):
        run = match.group(0)
        digits = sum(c.isdigit() for c in run)
        if digits == 0 or (len(run) >= 6 and digits <= 1):
            return run
        return ''.join(_OCR_DIGIT_MAP.get(c, c) for c in run)
    return _OCR_DIGIT_RUN.sub(_fix, text)


_FORMATTED_NUMBER_RE = re.compile(r'\d{1,3}(?:[,\.]\d{3})+')
# Two capture groups: digit-at-end (`[Hunting Trap 1]`) or digit-at-start
# (Arabic visual-order: `[1 فخ الصيد]` → OCR `[1 ...]`).
_BRACKETED_TRAP_RE = re.compile(r'\[(?:[^\]\d][^\]]*?(\d+)|(\d+)[^\]]*?)\]')
# Headerless fallback for engines that drop `[`/`]` (japan, chinese_cht).
# Single digit only — keeps merged-rank junk like `狩獵陷阱21` from matching.
_HEADERLESS_TRAP_RE = re.compile(
    r'(?:Hunting\s*Trap|狩獵陷阱|狩猎陷阱|同盟罠|사냥\s*함정|사냥함정|Охота)\s*(\d)',
    re.IGNORECASE,
)
# Backup summary marker for when OCR ate the brackets (no `]` to rfind on).
_PERSONAL_REWARDS_RE = re.compile(
    r'Personal\s+Damage\s+Rewards|個人ダメージ報酬|个人伤害奖励|個人傷害獎勵|개인\s*피해\s*보상',
    re.IGNORECASE,
)
# Localized "Rallies: N" markers — preferred extraction when present.
_RALLIES_MARKER_RE = re.compile(
    r'(?:Rallies|Rally|集結回数|集結次數|集结次数|집결\s*횟수|الحشود|Сборы)'
    r'\s*[:：]?\s*(\d+)',
    re.IGNORECASE,
)
_BARE_SMALL_INT_RE = re.compile(r'(?<![\d,\.])\b\d{1,3}\b(?![\d,\.])')
_HUNT_DATE_RE = re.compile(r'\b(\d{4})-(\d{2})-(\d{2})')
_EXPIRES_MARKER_RE = re.compile(
    r'Expire[sd]?|期限|有効期限|만료|تنتهي|Истекает',
    re.IGNORECASE,
)


def extract_hunt_date(text: str) -> str | None:
    """Earliest valid YYYY-MM-DD that appears before any expiry marker.
    Returns None when only expiry-side dates exist."""
    expiry = _EXPIRES_MARKER_RE.search(text)
    scan_text = text[:expiry.start()] if expiry else text
    dates = []
    for y, m, d in _HUNT_DATE_RE.findall(scan_text):
        try:
            yi, mi, di = int(y), int(m), int(d)
        except ValueError:
            continue
        if 2020 <= yi <= 2099 and 1 <= mi <= 12 and 1 <= di <= 31:
            dates.append((yi, mi, di))
    if not dates:
        return None
    yi, mi, di = min(dates)
    return f"{yi:04d}-{mi:02d}-{di:02d}"


def extract_bear_hunt_stats(text: str):
    """Returns (hunting_trap_str, rallies_str, total_damage_int)."""
    trap_match = _BRACKETED_TRAP_RE.search(text)
    if trap_match:
        hunting_trap = trap_match.group(1) or trap_match.group(2)
    else:
        m = _HEADERLESS_TRAP_RE.search(text)
        hunting_trap = m.group(1) if m else ""

    # Total damage dwarfs any per-player damage, so max() works.
    number_runs = list(_FORMATTED_NUMBER_RE.finditer(text))
    if number_runs:
        total_damage = max(int(re.sub(r'[^\d]', '', m.group(0))) for m in number_runs)
    else:
        total_damage = 0

    marker = _RALLIES_MARKER_RE.search(text)
    if marker and marker.group(1) != "0":
        rallies = marker.group(1)
    else:
        pre_damage = text[:number_runs[0].start()] if number_runs else text
        candidates = [
            c for c in _BARE_SMALL_INT_RE.findall(pre_damage)
            if len(c) >= 2 and int(c) > 0
        ]
        rallies = candidates[-1] if candidates else ""

    return hunting_trap, rallies, total_damage


def find_ranking_section_start(text: str):
    """Index where the rank list starts, or None when the image is a
    summary (no rank list to parse — caller must skip row extraction)."""
    last_bracket = text.rfind(']')
    if last_bracket != -1:
        tail = text[last_bracket + 1:]
        if len(_FORMATTED_NUMBER_RE.findall(tail)) >= 3:
            return last_bracket + 1
        return None
    if _PERSONAL_REWARDS_RE.search(text):
        return None
    m = re.search(r'(?i)\b(?:damage\s+ranking|ranking)\b', text)
    if m:
        return m.end()
    return 0


def parse_player_rows(text: str, after_pos: int = None):
    """Parse the damage-ranking section into [(name, damage, rank), ...].
    Names keep OCR noise — fuzzy resolution happens in the UI layer."""
    if after_pos is None:
        after_pos = find_ranking_section_start(text)
    if after_pos is None:
        return []
    tail = text[after_pos:]
    matches = list(_FORMATTED_NUMBER_RE.finditer(tail))
    if not matches:
        return []

    chunks, damages = [], []
    prev_end = 0
    for m in matches:
        chunks.append(tail[prev_end:m.start()])
        damages.append(int(re.sub(r'[^\d]', '', m.group(0))))
        prev_end = m.end()

    # Drop the "[Hunting Trap N]" section header from the first chunk.
    chunks[0] = re.sub(r'^.*\][^A-Za-z]*', '', chunks[0], count=1)
    # Strip the English "Damage Points" suffix from any chunk.
    _label_suffix_re = re.compile(r'(?i)\s*(?:damage\s+points|damage|points)\s*:?\s*$')
    for i, c in enumerate(chunks):
        chunks[i] = _label_suffix_re.sub('', c).rstrip()

    valid = [i for i, c in enumerate(chunks) if not re.search(r'[.?\[\]]', c)]
    if 0 in valid and len(chunks[0].split()) > 8:
        valid.remove(0)

    for _ in range(4):
        stripped = _strip_common_trailing_token([chunks[i] for i in valid])
        if stripped == [chunks[i] for i in valid]:
            break
        for idx, new_c in zip(valid, stripped):
            chunks[idx] = new_c

    def _name_token_count(c):
        return sum(1 for t in c.split() if not re.fullmatch(r'\d{1,2}', t))

    if 0 in valid and len(valid) > 1:
        other_counts = [_name_token_count(chunks[i]) for i in valid if i != 0]
        target = max(min(c for c in other_counts if c) if any(other_counts) else 1, 1)
        first_tokens = chunks[0].split()
        while _name_token_count(' '.join(first_tokens)) > target:
            first_tokens.pop(0)
        chunks[0] = ' '.join(first_tokens)

    rows = []
    for i in valid:
        chunk = chunks[i]
        rank = None
        rank_m = re.search(r'(?<!\S)(\d{1,2})(?!\S)', chunk)
        if rank_m:
            rank = int(rank_m.group(1))
            chunk = (chunk[:rank_m.start()] + ' ' + chunk[rank_m.end():])
        name = re.sub(r'\s+', ' ', chunk).strip()
        # Strip leading ≤3-char tokens (label leak from previous row's
        # "Damage Points:") when followed by a real 4+ char name.
        name = re.sub(r'^(?:\S{1,3}\s+)+(?=\S{4,})', '', name)
        # Blank when chunk is mostly non-letters (status-bar leak).
        if sum(c.isalpha() for c in name) < 3:
            name = ''
        rows.append({'name': name, 'damage': damages[i], 'rank': rank})
    return rows


def _better_row(existing, candidate, roster=None) -> bool:
    """Cross-image merge tiebreaker for rows sharing a damage value.
    Order: name presence > roster score > rank info > name length."""
    e_name = existing.get('name') or ''
    c_name = candidate.get('name') or ''
    if not e_name and c_name:
        return True
    if e_name and not c_name:
        return False

    if roster and e_name and c_name:
        e_cands = match_roster(e_name, roster)
        c_cands = match_roster(c_name, roster)
        e_score = e_cands[0][2] if e_cands else 0
        c_score = c_cands[0][2] if c_cands else 0
        if c_score != e_score:
            return c_score > e_score

    if existing.get('rank') is None and candidate.get('rank') is not None:
        return True
    if existing.get('rank') is not None and candidate.get('rank') is None:
        return False

    if e_name and c_name and len(c_name) > len(e_name) + 2:
        return True
    return False


MATCH_AUTO_CONFIRM = 90
MATCH_LIKELY_MIN = 80
MATCH_AMBIGUOUS_DELTA = 5


# Default initialised above (DEFAULT_OCR_LANG) before RapidOCR import.
OCR_LANGUAGES = [
    ("en",          "Latin only (default — English/French/German/etc, fastest)"),
    ("ch",          "Multilingual (Latin + Simplified Chinese)"),
    ("japan",       "Japanese"),
    ("korean",      "Korean"),
    ("chinese_cht", "Traditional Chinese"),
    ("latin",       "Latin Extended"),
    ("arabic",      "Arabic"),
    ("cyrillic",    "Cyrillic (Russian, etc.)"),
    ("devanagari",  "Devanagari (Hindi, etc.)"),
]
OCR_LANG_CODES = {code for code, _label in OCR_LANGUAGES}
OCR_LANG_LABEL = dict(OCR_LANGUAGES)

# Useful fallback runs per image — skipped/rejected ones don't count.
MAX_FALLBACK_ATTEMPTS = 4
MAX_CONCURRENT_OCR = 2

_ocr_semaphore: asyncio.Semaphore | None = None


def _get_ocr_semaphore() -> asyncio.Semaphore:
    # Lazy: no asyncio loop at import time.
    global _ocr_semaphore
    if _ocr_semaphore is None:
        _ocr_semaphore = asyncio.Semaphore(MAX_CONCURRENT_OCR)
    return _ocr_semaphore

# Min runs (with 0 fills) before auto-prune strips a fallback.
AUTOPRUNE_MIN_RUNS = 10

# Latin-only recognition models.
_LATIN_ONLY_LANGS = {"en", "latin"}

# Unicode ranges each language's recognition model is supposed to produce.
_LANG_UNICODE_RANGES = {
    "arabic":      [(0x0600, 0x06FF), (0x0750, 0x077F)],
    "cyrillic":    [(0x0400, 0x04FF), (0x0500, 0x052F)],
    "devanagari":  [(0x0900, 0x097F)],
    "japan":       [(0x3040, 0x309F), (0x30A0, 0x30FF), (0x4E00, 0x9FFF)],
    "korean":      [(0xAC00, 0xD7A3), (0x1100, 0x11FF)],
    "chinese_cht": [(0x4E00, 0x9FFF), (0x3400, 0x4DBF)],
}


def _output_matches_lang_script(text: str, lang: str) -> bool:
    """True when `text` has any char in `lang`'s expected Unicode range.
    Latin/`ch` engines aren't filtered (no range to check against)."""
    ranges = _LANG_UNICODE_RANGES.get(lang)
    if not ranges:
        return True
    return any(any(lo <= ord(c) <= hi for lo, hi in ranges) for c in text)


_RTL_RANGES = [(0x0590, 0x08FF), (0xFB1D, 0xFDFF), (0xFE70, 0xFEFF)]
_RTL_LANGS = {"arabic"}


def _has_rtl(text: str) -> bool:
    return any(any(lo <= ord(c) <= hi for lo, hi in _RTL_RANGES) for c in text)


def _extract_script_substrings_with_pos(text: str, lang: str, *, min_script_chars: int = 2) -> list:
    """Like `_extract_script_substrings` but returns `(substring, start_pos)`
    tuples so callers can position-align substrings to row anchors.
    """
    ranges = _LANG_UNICODE_RANGES.get(lang)
    if not ranges:
        return []

    def _in_script(c):
        return any(lo <= ord(c) <= hi for lo, hi in ranges)

    char_class = ''.join(f'\\u{lo:04X}-\\u{hi:04X}' for lo, hi in ranges)
    pattern = re.compile(f'[{char_class}\\s]+', re.UNICODE)
    out, seen = [], set()
    for m in pattern.finditer(text):
        s = m.group(0).strip()
        if not s:
            continue
        if sum(1 for c in s if _in_script(c)) < min_script_chars:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append((s, m.start()))
    return out


def _reverse_for_rtl(text: str, lang: str) -> str:
    """Arabic OCR emits chars in visual order, reversing the logical order
    relative to API-stored roster nicknames. Reverse the full string to
    recover logical (and word) order so fuzzy match against the roster
    works. Non-RTL languages pass through unchanged.
    """
    if lang in _RTL_LANGS and text:
        return text[::-1]
    return text


try:
    import arabic_reshaper as _arabic_reshaper
    from bidi.algorithm import get_display as _bidi_get_display
    _RESHAPE_AVAILABLE = True
except ImportError:
    _arabic_reshaper = None
    _bidi_get_display = None
    _RESHAPE_AVAILABLE = False
    logger.warning("arabic-reshaper / python-bidi not installed — RTL names will render in logical order.")


def _ltr_line(text) -> str:
    """Prepend LRM so Discord left-aligns lines containing RTL chars."""
    if not text:
        return text or ""
    return "‎" + text if _has_rtl(text) else text


def _isolate_rtl(text: str) -> str:
    """Wrap RTL chars in FSI…PDI so they can't reorder surrounding tokens."""
    if not text or not _has_rtl(text):
        return text or ""
    return f"⁨{text}⁩"


def _reshape_for_chart(text) -> str:
    """Shape Arabic for matplotlib (which doesn't run bidi itself).
    Discord uses `_ltr_line` instead — it shapes Arabic natively."""
    if not text or not _RESHAPE_AVAILABLE:
        return text or ""
    if not _has_rtl(text):
        return text
    try:
        return _bidi_get_display(_arabic_reshaper.reshape(text))
    except Exception:
        return text


# Per-language LazyOnnxModel registry. Engines load on demand and unload after
# the configured grace period (set in onnx_lifecycle).
_OCR_MODELS: dict[str, "onnx_lifecycle.LazyOnnxModel"] = {}

# Short labels shown in the Bot Health dashboard. Kept compact so they fit
# in the narrow column-layout fields without wrapping.
_OCR_LANG_DISPLAY = {
    "en": "EN",
    "ch": "CH",
    "japan": "JA",
    "korean": "KO",
    "chinese_cht": "ZH-Hant",
    "latin": "Latin",
    "arabic": "AR",
    "cyrillic": "CY",
    "devanagari": "DV",
}

# Per-language footprint, measured empirically on Windows with onnxruntime.
# `ram` is resident memory growth when the engine is loaded; `disk` is the
# on-disk model size. Both are approximate.
_OCR_LANG_FOOTPRINT_MB = {
    "en":          {"ram": 35, "disk": 7},
    "ch":          {"ram": 28, "disk": 15},
    "japan":       {"ram": 26, "disk": 9},
    "korean":      {"ram": 47, "disk": 23},
    "chinese_cht": {"ram": 33, "disk": 11},
    "latin":       {"ram": 27, "disk": 9},
    "arabic":      {"ram": 31, "disk": 7},
    "cyrillic":    {"ram": 20, "disk": 9},
    "devanagari": {"ram": 30, "disk": 7},
}


def get_ocr_model(lang: str):
    """Return the LazyOnnxModel for `lang`, falling back to the default if the
    code is unknown. Returns None when RapidOCR isn't available at all."""
    if not OCR_AVAILABLE:
        return None
    if lang not in OCR_LANG_CODES:
        lang = DEFAULT_OCR_LANG
    cached = _OCR_MODELS.get(lang)
    if cached is not None:
        return cached

    label = _OCR_LANG_DISPLAY.get(lang, lang)

    def _factory(lang_code=lang):
        return RapidOCR(params={"Rec.lang_type": LangRec(lang_code)})

    model = onnx_lifecycle.get_or_create(
        name=f'bear_track:{lang}',
        display_name=f'Bear Track ({label})',
        factory=_factory,
    )
    _OCR_MODELS[lang] = model
    return model


def _ocr_image_with_engine(image_bytes: bytes, engine) -> str:
    """Sync OCR call against an already-loaded engine. Returns space-joined
    text or "". Pulled out so async callers can dispatch via to_thread."""
    if engine is None or not image_bytes:
        return ""
    image = Image.open(io.BytesIO(image_bytes)).convert('RGB')
    if max(image.size) > MAX_OCR_DIM:
        image.thumbnail((MAX_OCR_DIM, MAX_OCR_DIM), Image.LANCZOS)
    result = engine(np.array(image))
    if not result:
        return ""
    if hasattr(result, 'txts') and result.txts:
        return " ".join(result.txts)
    if hasattr(result, '__iter__'):
        texts = [str(item[1]) for item in result
                 if isinstance(item, (list, tuple)) and len(item) >= 2]
        return " ".join(texts) if texts else str(result)
    return str(result)


async def ocr_bytes(image_bytes: bytes, lang: str = DEFAULT_OCR_LANG, *, session=None) -> str:
    """OCR `image_bytes` → space-joined text, or "" on failure.

    If `session` is given, the engine is acquired via the session (warm reuse
    across multiple OCR calls in one bear session). Otherwise this falls back
    to a one-shot acquire/release covered by the lifecycle grace period."""
    if not OCR_AVAILABLE or not image_bytes:
        return ""
    model = get_ocr_model(lang)
    if model is None:
        return ""
    if session is not None:
        engine = await session._ensure_engine(lang)
        if engine is None:
            return ""
        return await asyncio.to_thread(_ocr_image_with_engine, image_bytes, engine)
    async with model.use() as engine:
        return await asyncio.to_thread(_ocr_image_with_engine, image_bytes, engine)


_SCRIPT_TAGS = (
    ('LATIN',       'latin'),
    ('ARABIC',      'arabic'),
    ('HEBREW',      'arabic'),
    ('CJK',         'cjk'),
    ('HIRAGANA',    'cjk'),
    ('KATAKANA',    'cjk'),
    ('HANGUL',      'cjk'),
    ('CYRILLIC',    'cyrillic'),
    ('GREEK',       'greek'),
    ('DEVANAGARI',  'devanagari'),
    ('THAI',        'thai'),
)


def _script_of(c: str) -> str | None:
    if not c.isalpha():
        return None
    if ord(c) < 0x80:
        return 'latin'
    name = unicodedata.name(c, '')
    for marker, tag in _SCRIPT_TAGS:
        if marker in name:
            return tag
    return 'other'


def _strip_minority_script(name: str, threshold: float = 0.85) -> str:
    """Drop alpha chars from non-dominant scripts when one script holds
    ≥threshold of the alpha mass. Preserves names that are pure or
    legitimately mixed."""
    if not name:
        return name
    counts: dict[str, int] = {}
    for c in name:
        s = _script_of(c)
        if s and s != 'other':
            counts[s] = counts.get(s, 0) + 1
    if len(counts) < 2:
        return name
    total = sum(counts.values())
    dom = max(counts, key=counts.get)
    if counts[dom] / total < threshold:
        return name
    return ''.join(c for c in name if _script_of(c) in (None, dom, 'other'))


def match_roster(detected_name: str, roster):
    """Top 5 fuzzy matches as [(fid, nickname, score_0_100), ...].
    Names < 3 alpha chars require case-insensitive exact equality
    (WRatio's partial-ratio would otherwise let "G" hit 100% on anything)."""
    if not detected_name or not roster or not RAPIDFUZZ_AVAILABLE:
        return []
    if sum(c.isalpha() for c in detected_name) < 3:
        normalized = detected_name.strip().lower()
        for fid, nick in roster:
            if (nick or '').strip().lower() == normalized:
                return [(fid, nick, 100)]
        return []
    cleaned = _strip_minority_script(detected_name)
    names = [nick or "" for (_fid, nick) in roster]
    results = _rf_process.extract(
        cleaned, names,
        scorer=_rf_fuzz.WRatio,
        processor=_rf_utils.default_process,
        limit=5, score_cutoff=MATCH_LIKELY_MIN,
    )
    out = []
    for _match_str, score, idx in results:
        fid, nick = roster[idx]
        out.append((fid, nick, int(score)))
    return out


def classify_match(candidates):
    """Given the output of match_roster, return a status tag for the row.

    'auto'      — best score ≥ AUTO_CONFIRM and not ambiguous
    'likely'    — best score ≥ LIKELY_MIN but below AUTO_CONFIRM
    'ambiguous' — two or more candidates within AMBIGUOUS_DELTA points
    'none'      — no candidates above LIKELY_MIN
    """
    if not candidates:
        return 'none'
    best = candidates[0][2]
    if len(candidates) > 1 and best - candidates[1][2] < MATCH_AMBIGUOUS_DELTA:
        return 'ambiguous'
    return 'auto' if best >= MATCH_AUTO_CONFIRM else 'likely'


def name_match_score(name: str, roster) -> int:
    """Best fuzzy-match score 0-100. Empty name → 0; empty roster → 100."""
    if not name:
        return 0
    if not roster:
        return 100
    cands = match_roster(name, roster)
    return cands[0][2] if cands else 0


def is_row_unfilled(row, roster) -> bool:
    """True when no roster fuzzy-match >= MATCH_LIKELY_MIN."""
    return name_match_score(row.get('name') or '', roster) < MATCH_LIKELY_MIN


def record_ocr_lang_run(alliance_id: int, lang: str, role: str,
                        rows_filled: int) -> None:
    """Bump the (alliance, lang, role) effectiveness counter."""
    if not alliance_id or not lang:
        return
    try:
        with sqlite3.connect("db/bear_data.sqlite", timeout=30.0) as conn:
            conn.execute("""
                INSERT INTO bear_ocr_lang_stats
                    (alliance_id, lang, role, runs, rows_filled, last_run_at)
                VALUES (?, ?, ?, 1, ?, datetime('now'))
                ON CONFLICT(alliance_id, lang, role) DO UPDATE SET
                    runs        = runs + 1,
                    rows_filled = rows_filled + excluded.rows_filled,
                    last_run_at = excluded.last_run_at
            """, (alliance_id, lang, role, rows_filled))
    except Exception as e:
        logger.warning(f"Bear OCR: could not record lang stats ({lang}/{role}): {e}")


def get_ocr_lang_stats(alliance_id: int) -> list[dict]:
    """Per-language effectiveness rows, most-used first."""
    try:
        with sqlite3.connect("db/bear_data.sqlite", timeout=30.0) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT lang, role, runs, rows_filled, last_run_at
                FROM bear_ocr_lang_stats
                WHERE alliance_id = ?
                ORDER BY runs DESC, lang
            """, (alliance_id,)).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"Bear OCR: could not read lang stats: {e}")
        return []


def _format_last_used(last_run_at: str | None) -> str:
    """UTC `YYYY-MM-DD HH:MM:SS` → 'today' / 'yesterday' / 'N days ago'."""
    if not last_run_at:
        return "never"
    try:
        last_d = datetime.strptime(last_run_at[:19], "%Y-%m-%d %H:%M:%S").date()
    except (ValueError, TypeError):
        return last_run_at[:10] or "never"
    today = datetime.now(timezone.utc).date()
    delta = (today - last_d).days
    if delta <= 0:
        return "today"
    if delta == 1:
        return "yesterday"
    return f"{delta} days ago"


def _collect_claimed_fids(img_rows: dict, roster: list | None) -> set:
    """Roster fids already auto-confirmed by a row in img_rows."""
    claimed: set = set()
    if roster:
        for r in img_rows.values():
            cands = match_roster(r.get('name') or '', roster)
            if cands and cands[0][2] >= MATCH_AUTO_CONFIRM:
                claimed.add(cands[0][0])
    return claimed


def merge_fallback_rows_by_damage(img_rows: dict, fb_rows: list,
                                  roster: list | None,
                                  fb_lang: str = "") -> bool:
    """Fill img_rows from fb_rows by matching damage. Skips writes that
    would map a fid already claimed by a different row. Returns True if
    any row was filled."""
    filled = False
    claimed_fids = _collect_claimed_fids(img_rows, roster)
    for fr in fb_rows:
        existing = img_rows.get(fr['damage'])
        if not existing or not fr.get('name'):
            continue
        cands = match_roster(fr['name'], roster) if roster else []
        if cands and cands[0][2] >= MATCH_AUTO_CONFIRM and cands[0][0] in claimed_fids:
            existing_cands = match_roster(existing.get('name') or '', roster) if roster else []
            if not existing_cands or existing_cands[0][0] != cands[0][0]:
                continue
        if name_match_score(fr['name'], roster) > name_match_score(existing.get('name') or '', roster):
            existing['name'] = fr['name']
            filled = True
            if fb_lang:
                logger.info(
                    f"Bear OCR fallback [{fb_lang}] filled "
                    f"{fr['name']!r} for damage {fr['damage']}"
                )
    return filled


def fill_unfilled_by_position(img_rows: dict, fb_text: str,
                              fb_lang: str, filename: str,
                              roster: list | None) -> None:
    """Anchor non-Latin (script) substrings to unfilled rows by their
    position in fb_text relative to the primary's Latin row names. Used
    when damage-keyed merge leaves nothing because the script-only OCR
    doesn't share clean damage values with the primary."""
    sorted_rows = sorted(img_rows.values(), key=lambda r: -r['damage'])

    # Walk in row order so repeated names get distinct anchor positions.
    anchors = []  # [(text_pos, sorted_row_idx)]
    fb_lower = fb_text.lower()
    search_start = 0
    for idx, row in enumerate(sorted_rows):
        name = (row.get('name') or '').strip()
        if not name:
            continue
        ascii_letters = ''.join(c for c in name if c.isalpha() and c.isascii())
        if len(ascii_letters) < 4:
            continue
        anchor_key = ascii_letters[:4].lower()
        pos = fb_lower.find(anchor_key, search_start)
        if pos != -1:
            anchors.append((pos, idx))
            search_start = pos + len(anchor_key)
    if not anchors:
        return
    anchors.sort()

    subs_with_pos = _extract_script_substrings_with_pos(fb_text, fb_lang)
    if not subs_with_pos:
        return

    # Per-substring noise filter (token appears 3+ times → drop it).
    from collections import Counter
    token_counts: Counter = Counter()
    for s, _pos in subs_with_pos:
        for tok in set(s.split()):
            if len(tok) >= 2:
                token_counts[tok] += 1
    noise = sorted(
        (tok for tok, count in token_counts.items() if count >= 3),
        key=len, reverse=True,
    )

    def _strip_noise(s: str) -> str:
        for nt in noise:
            s = s.replace(nt, ' ')
        return ' '.join(s.split()).strip()

    # Build (pos, cleaned_substring) keeping only non-empty post-strip.
    clean_with_pos = []
    for s, p in subs_with_pos:
        cs = _strip_noise(s)
        if cs:
            clean_with_pos.append((p, cs))
    if not clean_with_pos:
        return

    claimed_fids = _collect_claimed_fids(img_rows, roster)

    for row_idx, row in enumerate(sorted_rows):
        if not is_row_unfilled(row, roster):
            continue
        prev_pos = -1
        next_pos = len(fb_text)
        for a_pos, a_idx in anchors:
            if a_idx < row_idx and a_pos > prev_pos:
                prev_pos = a_pos
            elif a_idx > row_idx and a_pos < next_pos:
                next_pos = a_pos
        in_range = [cs for p, cs in clean_with_pos if prev_pos < p < next_pos]
        if not in_range:
            continue
        in_range.sort(key=len, reverse=True)
        best = None
        for cs in in_range:
            display_cs = _reverse_for_rtl(cs, fb_lang) if fb_lang in _RTL_LANGS else cs
            cands = match_roster(display_cs, roster) if roster else []
            if cands and cands[0][2] >= MATCH_AUTO_CONFIRM and cands[0][0] in claimed_fids:
                continue
            best = display_cs
            break
        if best is None:
            continue
        existing_score = name_match_score(row.get('name') or '', roster)
        new_score = name_match_score(best, roster)
        if new_score <= existing_score:
            continue
        row['name'] = best
        new_cands = match_roster(best, roster) if roster else []
        if new_cands and new_cands[0][2] >= MATCH_AUTO_CONFIRM:
            claimed_fids.add(new_cands[0][0])
        logger.info(
            f"Bear OCR fallback [{fb_lang}] filled (by-position) "
            f"{best!r} for damage {row['damage']} ({filename})"
        )


def _strip_common_trailing_token(chunks):
    """If the same whitespace-separated token appears at the end of at
    least two chunks, strip that token from every chunk that ends with it.
    Used to peel off per-row labels without a hardcoded list.
    """
    if len(chunks) < 2:
        return chunks
    last_tokens = [c.split()[-1] for c in chunks if c.split()]
    counts = {}
    for t in last_tokens:
        counts[t] = counts.get(t, 0) + 1
    recurring = {t for t, n in counts.items() if n >= 2}
    if not recurring:
        return chunks
    out = []
    for c in chunks:
        parts = c.split()
        if parts and parts[-1] in recurring:
            parts = parts[:-1]
        out.append(' '.join(parts))
    return out


def bear_damage(raw) -> int:
    """Clean bear score from different language formats."""
    if not raw:
        return 0
    clean = re.sub(r"[^\d]", "", str(raw))
    return int(clean) if clean else 0


def format_damage_for_embed(value) -> str:
    """Formats integer with commas for embed display."""
    try:
        cleaned = re.sub(r"[^\d]", "", str(value))
        if not cleaned:
            return "0"
        return f"{int(cleaned):,}"
    except Exception:
        return "0"


def validate_bear_submission(date_str, hunting_trap, rallies, total_damage):
    """Validate bear submission fields and return list of errors."""
    errors = []

    try:
        datetime.strptime(str(date_str), "%Y-%m-%d")
    except Exception:
        errors.append("Date must be in YYYY-MM-DD format.")

    try:
        hunting_trap = int(hunting_trap)
        if hunting_trap not in (1, 2):
            errors.append("Hunting trap must be 1 or 2.")
    except Exception:
        errors.append("Hunting trap must be a number (1 or 2).")

    try:
        rallies = int(rallies)
        if rallies <= 0:
            errors.append("Rallies must be a number greater than 0.")
    except Exception:
        errors.append("Rallies must be a whole number.")

    try:
        total_damage = int(total_damage)
        if total_damage <= 0:
            errors.append("Total damage must be a number greater than 0.")
    except Exception:
        errors.append("Total damage must be a whole number.")

    return errors


# ---------------------------------------------------------------------------
# Session model — multi-screenshot bear hunt collection
# ---------------------------------------------------------------------------


@dataclass
class ImageResult:
    """Output of OCR'ing a single screenshot, before clustering into events."""
    ok: bool = False
    trap: str = ""
    rallies: str = ""
    total_damage: int = 0
    date: str = ""
    rows: dict = field(default_factory=dict)


@dataclass
class EventGroup:
    """One bear event accumulated from one or more compatible screenshots."""
    trap_value: str = ""
    rallies_value: str = ""
    damage_int: int = 0
    date_value: str = ""
    merged_rows: dict = field(default_factory=dict)
    image_count: int = 0

    def merge(self, result: ImageResult, roster: list | None = None):
        if not self.trap_value and result.trap:
            self.trap_value = result.trap
        if not self.rallies_value and result.rallies:
            self.rallies_value = result.rallies
        if not self.date_value and result.date:
            self.date_value = result.date
        if result.total_damage > self.damage_int:
            self.damage_int = result.total_damage
        for row in result.rows.values():
            key = row['damage']
            existing = self.merged_rows.get(key)
            if existing is None or _better_row(existing, row, roster=roster):
                self.merged_rows[key] = row
        self.image_count += 1

    def is_compatible(self, result: ImageResult, roster: list) -> bool:
        if self.trap_value and result.trap and self.trap_value != result.trap:
            logger.info(
                f"Bear cluster: split — trap conflict "
                f"(event={self.trap_value!r}, new={result.trap!r})"
            )
            return False
        has_agreement = has_conflict = False
        details = []
        for new_dmg, new_row in result.rows.items():
            existing_row = self.merged_rows.get(new_dmg)
            if not existing_row:
                continue
            status = _row_pair_status(existing_row, new_row, roster)
            details.append(
                f"  dmg={new_dmg} existing={existing_row.get('name')!r} "
                f"new={new_row.get('name')!r} → {status}"
            )
            if status == 'same':
                has_agreement = True
            elif status == 'different':
                has_conflict = True
        if has_agreement:
            decision = True
        else:
            decision = not has_conflict
        if details:
            logger.info(
                f"Bear cluster: is_compatible={decision} "
                f"(agree={has_agreement} conflict={has_conflict})\n"
                + "\n".join(details)
            )
        else:
            logger.info(
                f"Bear cluster: is_compatible={decision} (no shared damages)"
            )
        return decision


def _row_pair_status(row_a: dict, row_b: dict, roster: list) -> str:
    """Compare two rows sharing a damage value: 'same' / 'different' /
    'unknown' (when either name lacks a confident roster match)."""
    name_a = (row_a.get('name') or '').strip()
    name_b = (row_b.get('name') or '').strip()
    if not name_a or not name_b or not roster:
        return 'unknown'
    cand_a = match_roster(name_a, roster)
    cand_b = match_roster(name_b, roster)
    if not cand_a or not cand_b:
        return 'unknown'
    fid_a, _, score_a = cand_a[0]
    fid_b, _, score_b = cand_b[0]
    if score_a < MATCH_AUTO_CONFIRM or score_b < MATCH_AUTO_CONFIRM:
        return 'unknown'
    return 'same' if fid_a == fid_b else 'different'


class BearAutoDeleteTracker:
    """Deletes source screenshots after every review is actioned, iff at
    least one was submitted."""
    def __init__(self, source_messages, enabled: bool):
        self.source_messages = list(source_messages)
        self.enabled = enabled
        self.pending = 0
        self.any_submitted = False
        logger.info(
            f"Bear auto-delete tracker: enabled={enabled}, "
            f"source_messages={len(self.source_messages)}"
        )

    def register(self):
        self.pending += 1

    async def on_submit(self):
        self.any_submitted = True
        self.pending -= 1
        await self._maybe_delete()

    async def on_cancel(self):
        self.pending -= 1
        await self._maybe_delete()

    async def _maybe_delete(self):
        if not self.enabled:
            logger.info("Bear auto-delete: skipped (disabled in alliance settings)")
            return
        if self.pending != 0:
            return
        if not self.any_submitted:
            logger.info("Bear auto-delete: skipped (no submissions)")
            return
        deleted = not_found = forbidden = other_failed = 0
        for msg in self.source_messages:
            try:
                await msg.delete()
                deleted += 1
            except discord.NotFound:
                not_found += 1
            except discord.Forbidden:
                forbidden += 1
            except Exception as e:
                other_failed += 1
                logger.warning(
                    f"Bear auto-delete: unexpected failure deleting "
                    f"message {msg.id}: {e}"
                )
        if forbidden:
            logger.warning(
                f"Bear auto-delete: {forbidden} message(s) blocked — "
                f"bot needs **Manage Messages** on the bear channel."
            )
        logger.info(
            f"Bear auto-delete: total={len(self.source_messages)}, "
            f"deleted={deleted}, already_gone={not_found}, "
            f"forbidden={forbidden}, errors={other_failed}"
        )


class BearSession:
    """Per-(channel, user) session: accumulates screenshots within a
    sliding timeout, finalises into one review."""

    def __init__(self, *, cog, channel_id: int, user_id: int, alliance_id: int,
                 alliance_name: str, roster, primary_lang: str, fallback_langs: list,
                 timeout_min: int, auto_delete: bool):
        self.cog = cog
        self.channel_id = channel_id
        self.user_id = user_id
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.roster = roster
        self.primary_lang = primary_lang
        self.fallback_langs = fallback_langs
        self.timeout_min = timeout_min
        self.auto_delete = auto_delete

        self.events: list[EventGroup] = []
        self.source_messages: list[discord.Message] = []
        self.lock = asyncio.Lock()
        self.timer_task: asyncio.Task | None = None
        self.progress_msg: discord.Message | None = None
        self.session_view: discord.ui.View | None = None
        self.processed_images = 0
        self.known_total_images = 0  # All images uploaded so far, even ones queued behind the lock
        self.any_ocr_success = False
        self.finalized = False
        # In-flight OCR state (None when idle).
        self.current_image_idx: int | None = None
        self.current_image_total: int | None = None
        self.current_phase: str | None = None  # 'ocr' or 'fallback'
        self.current_lang: str | None = None
        # OCR engines acquired by this session (released at finalize/cancel).
        self._engine_handles: dict[str, "onnx_lifecycle.LazyOnnxModel"] = {}
        self._engine_cache: dict[str, object] = {}

    async def _ensure_engine(self, lang: str):
        """Lazy per-session engine acquire. First call for `lang` loads the
        model and pins it for the rest of the session; subsequent calls
        return the cached engine."""
        cached = self._engine_cache.get(lang)
        if cached is not None:
            return cached
        model = get_ocr_model(lang)
        if model is None:
            return None
        engine = await model.acquire()
        self._engine_handles[lang] = model
        self._engine_cache[lang] = engine
        return engine

    async def _release_all_engines(self):
        for handle in self._engine_handles.values():
            try:
                await handle.release()
            except Exception as e:
                logger.warning(f"Bear OCR: engine release error: {e}")
        self._engine_handles.clear()
        self._engine_cache.clear()

    def cluster(self, result: ImageResult) -> EventGroup:
        for event in self.events:
            if event.is_compatible(result, self.roster):
                event.merge(result, roster=self.roster)
                return event
        new_event = EventGroup()
        new_event.merge(result, roster=self.roster)
        self.events.append(new_event)
        return new_event

    def restart_timer(self):
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        self.timer_task = asyncio.create_task(self._timer_run())

    async def _timer_run(self):
        try:
            await asyncio.sleep(self.timeout_min * 60)
        except asyncio.CancelledError:
            return
        if not self.finalized:
            await self.finalize(timed_out=True)

    def stop_timer(self):
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        self.timer_task = None

    def build_progress_embed(self) -> discord.Embed:
        title = f"{theme.searchIcon} Bear Hunt — collecting"

        if self.processed_images == 0 and self.current_image_idx is None:
            summary = f"{theme.processingIcon} Processing first screenshot…"
        elif not self.events and self.current_image_idx is None:
            summary = (
                f"{theme.warnIcon} **{self.processed_images}** screenshot(s) processed, "
                f"no readable data extracted yet."
            )
        elif len(self.events) <= 1:
            event = self.events[0] if self.events else None
            if event:
                trap = f"Trap {event.trap_value}" if event.trap_value else "Trap unknown"
                total = format_damage_for_embed(event.damage_int) if event.damage_int else "—"
                n_players = len(event.merged_rows)
                summary = (
                    f"**{self.processed_images}** screenshot{'s' if self.processed_images != 1 else ''} "
                    f"processed · **{n_players}** player{'s' if n_players != 1 else ''} found\n"
                    f"{trap} · {total} total"
                )
            else:
                summary = f"**{self.processed_images}** screenshots processed"
        else:
            lines = [
                f"**{self.processed_images}** screenshots → "
                f"{theme.warnIcon} **{len(self.events)}** events detected:"
            ]
            for i, ev in enumerate(self.events, start=1):
                trap = f"Trap {ev.trap_value}" if ev.trap_value else "Trap ?"
                total = format_damage_for_embed(ev.damage_int) if ev.damage_int else "—"
                lines.append(
                    f"• Event {i}: {trap} · {len(ev.merged_rows)} players · {total}"
                )
            summary = "\n".join(lines)

        if self.current_image_idx is not None:
            bar = self._progress_bar(self.current_image_idx, self.current_image_total or 1)
            phase_label = "fallback OCR" if self.current_phase == 'fallback' else "running OCR"
            lang_label = self._short_lang_label(self.current_lang)
            lang_part = f" ({lang_label})" if lang_label else ""
            status_line = (
                f"\n\n{bar} **{self.current_image_idx}/{self.current_image_total}** · "
                f"{theme.processingIcon} {phase_label}{lang_part}…"
            )
            footer_line = (
                f"\n\n{theme.hourglassIcon} You can click **Done Uploading** anytime — "
                f"it will wait for current screenshots to finish before opening the review."
            )
        else:
            status_line = ""
            footer_line = (
                f"\n\n{theme.hourglassIcon} Waiting up to **{self.timeout_min} min** "
                f"for more screenshots…"
            )

        description = (
            f"{theme.upperDivider}\n"
            f"{summary}"
            f"{status_line}"
            f"{footer_line}\n"
            f"{theme.lowerDivider}"
        )
        return discord.Embed(title=title, description=description, color=theme.emColor1)

    @staticmethod
    def _progress_bar(current: int, total: int) -> str:
        width = max(min(total, 12), 6)
        filled = max(0, min(width, round(current / total * width))) if total else 0
        return "▰" * filled + "▱" * (width - filled)

    @staticmethod
    def _short_lang_label(lang: str | None) -> str:
        if not lang:
            return ""
        label = OCR_LANG_LABEL.get(lang, lang)
        return label.split("(")[0].strip().split(" only")[0]

    async def render_progress(self):
        if not self.progress_msg:
            return
        try:
            await self.progress_msg.edit(
                embed=self.build_progress_embed(),
                view=self.session_view,
            )
        except Exception:
            pass

    async def add_message(self, message: discord.Message, image_attachments: list):
        # Bump total before the lock so the progress shows new uploads
        # immediately instead of after the current batch.
        self.known_total_images += len(image_attachments)
        if self.current_image_idx is not None:
            self.current_image_total = self.known_total_images
            await self.render_progress()

        async with self.lock:
            if self.finalized:
                return
            self.source_messages.append(message)

            async def _phase_callback(phase: str, lang: str):
                self.current_phase = phase
                self.current_lang = lang
                await self.render_progress()

            for attachment in image_attachments:
                self.current_image_idx = self.processed_images + 1
                self.current_image_total = self.known_total_images
                self.current_phase = 'ocr'
                self.current_lang = self.primary_lang
                await self.render_progress()
                try:
                    image_bytes = await attachment.read()
                except Exception as e:
                    logger.error(f"Bear OCR read error on {attachment.filename}: {e}")
                    continue
                result = await self.cog._ocr_attachment_to_result(
                    image_bytes,
                    self.primary_lang,
                    self.fallback_langs,
                    filename=attachment.filename,
                    roster=self.roster,
                    alliance_id=self.alliance_id,
                    progress_callback=_phase_callback,
                    session=self,
                )
                self.processed_images += 1
                if result.ok:
                    self.any_ocr_success = True
                    self.cluster(result)
                await self.render_progress()

            self.current_image_idx = None
            self.current_image_total = None
            self.current_phase = None
            self.current_lang = None
            await self.render_progress()
            self.restart_timer()

    async def finalize(self, *, timed_out: bool = False):
        async with self.lock:
            if self.finalized:
                return
            self.finalized = True
            self.stop_timer()
            _active_sessions.pop((self.channel_id, self.user_id), None)
        try:
            await self.cog._finalize_session(self, timed_out=timed_out)
        finally:
            await self._release_all_engines()

    async def cancel(self):
        async with self.lock:
            if self.finalized:
                return
            self.finalized = True
            self.stop_timer()
            _active_sessions.pop((self.channel_id, self.user_id), None)
        await self._release_all_engines()
        if self.progress_msg:
            embed = discord.Embed(
                description=f"{theme.deniedIcon} Bear hunt collection cancelled.",
                color=theme.emColor2,
            )
            try:
                await self.progress_msg.edit(embed=embed, view=None)
            except Exception:
                pass


_active_sessions: dict = {}


class BearSessionView(discord.ui.View):
    """Done Uploading / Cancel buttons attached to the collecting message."""

    def __init__(self, session: BearSession):
        super().__init__(timeout=None)
        self.session = session

        done_btn = discord.ui.Button(
            label="Done Uploading",
            emoji=f"{theme.verifiedIcon}",
            style=discord.ButtonStyle.success,
        )
        done_btn.callback = self._on_done

        cancel_btn = discord.ui.Button(
            label="Cancel",
            emoji=f"{theme.deniedIcon}",
            style=discord.ButtonStyle.secondary,
        )
        cancel_btn.callback = self._on_cancel

        self.add_item(done_btn)
        self.add_item(cancel_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who started this session can finalize it.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_done(self, interaction: discord.Interaction):
        if not await self._ack(interaction):
            return
        asyncio.create_task(self.session.finalize(timed_out=False))

    async def _on_cancel(self, interaction: discord.Interaction):
        if not await self._ack(interaction):
            return
        asyncio.create_task(self.session.cancel())

    @staticmethod
    async def _ack(interaction: discord.Interaction) -> bool:
        if interaction.response.is_done():
            return True
        try:
            await interaction.response.defer()
            return True
        except discord.NotFound:
            logger.warning("Bear session button: interaction expired before defer")
            return False
        except Exception as e:
            logger.warning(f"Bear session button: defer failed ({e!r})")
            return False


# ---------------------------------------------------------------------------
# bear_data_embed — chart generation
# ---------------------------------------------------------------------------

def bear_data_embed(
    *,
    alliance_id: int,
    alliance_name: str,
    hunting_trap: int,
    dates: list[datetime],
    rallies_list: list[int],
    total_damages: list[int],
    title_suffix: str | None = None,
    damage_range_days: int | None = None
):
    first_date = min(dates)
    last_date = max(dates)

    last_rallies = rallies_list[-1]
    last_damage = total_damages[-1]

    avg_rallies = int(sum(rallies_list) / len(rallies_list))
    avg_damage = int(sum(total_damages) / len(total_damages))

    rallies_diff = last_rallies - avg_rallies
    damage_diff = last_damage - avg_damage

    title = f"{alliance_name} Trap {hunting_trap}"
    if title_suffix:
        title += f" - {title_suffix}"

    embed = discord.Embed(title=title, color=theme.emColor1)

    embed.add_field(
        name="Date Range",
        value=f"{first_date:%Y-%m-%d} → {last_date:%Y-%m-%d}",
        inline=False
    )
    embed.add_field(name="Average Rallies", value=str(avg_rallies), inline=False)
    embed.add_field(name="Average Total Damage", value=f"{avg_damage:,}", inline=False)
    embed.add_field(name="Last Bear Rallies", value=str(last_rallies), inline=True)
    embed.add_field(name="Last Bear Damage", value=f"{last_damage:,}", inline=True)
    embed.add_field(name="Difference in Rallies", value=f"{rallies_diff:+d}", inline=True)
    embed.add_field(name="Difference in Damage", value=f"{damage_diff:+,}", inline=True)

    if damage_range_days and damage_range_days > 0:
        embed.set_footer(text=f"Showing last {damage_range_days} days of damage")
    else:
        embed.set_footer(text="Showing all historical damage records")

    image_file = _render_damage_chart(
        dates, total_damages,
        title=f"{alliance_name} Total Damage Over Time - Trap {hunting_trap}",
        ylabel="Total Damage",
    )
    if image_file is not None:
        embed.set_image(url="attachment://plot.png")
    return embed, image_file


def bear_data_embed_combined(
    *,
    alliance_id: int,
    alliance_name: str,
    trap_series: list[tuple[int, list[datetime], list[int], list[int]]],
    title_suffix: str | None = None,
    damage_range_days: int | None = None,
):
    """Combined-trap embed: `trap_series` is a list of
    `(trap_number, dates, rallies_list, total_damages)` for each trap that
    had data in the range. Renders a 2-line chart with a legend."""
    title = f"{alliance_name} Both Traps"
    if title_suffix:
        title += f" - {title_suffix}"

    embed = discord.Embed(title=title, color=theme.emColor1)

    all_dates = [d for _, dates, _, _ in trap_series for d in dates]
    embed.add_field(
        name="Date Range",
        value=f"{min(all_dates):%Y-%m-%d} → {max(all_dates):%Y-%m-%d}",
        inline=False,
    )

    for trap, dates, rallies_list, total_damages in trap_series:
        avg_rallies = int(sum(rallies_list) / len(rallies_list))
        avg_damage = int(sum(total_damages) / len(total_damages))
        embed.add_field(
            name=f"Trap {trap} — {len(dates)} hunt{'s' if len(dates) != 1 else ''}",
            value=(
                f"Avg rallies: **{avg_rallies}**\n"
                f"Avg damage: **{avg_damage:,}**\n"
                f"Last damage: **{total_damages[-1]:,}**"
            ),
            inline=True,
        )

    if damage_range_days and damage_range_days > 0:
        embed.set_footer(text=f"Showing last {damage_range_days} days of damage")
    else:
        embed.set_footer(text="Showing all historical damage records")

    series = [
        (f"Trap {trap}", dates, total_damages)
        for trap, dates, _, total_damages in trap_series
    ]
    image_file = _render_damage_chart(
        None, None,
        title=f"{alliance_name} Total Damage Over Time — Both Traps",
        ylabel="Total Damage",
        series=series,
    )
    if image_file is not None:
        embed.set_image(url="attachment://plot.png")
    return embed, image_file


def _format_damage_axis(x, _pos):
    """matplotlib FuncFormatter for damage values: 12,300,000,000 -> '12.3B'."""
    try:
        x = float(x)
    except Exception:
        return str(x)
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if x >= divisor:
            val = x / divisor
            return f"{int(val)}{suffix}" if val.is_integer() else f"{val:.1f}{suffix}"
    return f"{int(x)}"


def _render_damage_chart(dates, values, *, title, ylabel="Damage", series=None):
    """discord.File('plot.png') with the styled line chart, or None on
    failure. Pass `series=[(label, dates, values), ...]` for multi-line;
    legacy single-line callers keep the `dates, values` positional form."""
    if not MATPLOTLIB_AVAILABLE:
        return None
    if series is None:
        if not dates:
            return None
        series = [(None, dates, values)]
    if not any(s[1] for s in series):
        return None
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import MaxNLocator, FuncFormatter
    try:
        plt.style.use("fivethirtyeight")
        plt.figure(figsize=(10, 7), facecolor="#1a1a2d")
        for label, s_dates, s_values in series:
            if not s_dates:
                continue
            plt.plot(s_dates, s_values, marker='o', linewidth=3, label=label)
        ax = plt.gca()
        ax.set_facecolor("#1a1a2d")
        for spine in ax.spines.values():
            spine.set_visible(False)
        plt.title(
            title, color="#99c2ff", fontfamily="sans-serif",
            fontweight="bold", fontsize=16, loc="left", pad=30,
        )
        plt.ylabel(ylabel, color="white", fontsize=12, fontweight="bold", labelpad=15)
        plt.yticks(color="white")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=15))
        plt.xticks(rotation=45, color="white")
        ax.yaxis.set_major_formatter(FuncFormatter(_format_damage_axis))
        if len(series) > 1 and any(label for label, _, _ in series):
            legend = plt.legend(loc="upper left", facecolor="#1a1a2d", edgecolor="none")
            for text in legend.get_texts():
                text.set_color("white")
        plt.tight_layout()
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", dpi=200, transparent=True)
        plt.close()
        buffer.seek(0)
        return discord.File(buffer, filename="plot.png")
    except Exception as e:
        logger.error(f"Failed to generate chart: {e}")
        print(f"[ERROR] Failed to generate chart: {e}")
        return None


def bear_player_history_embed(*, alliance_name: str, fid: int, nickname: str,
                               hunting_trap: int | None,
                               rows: list):
    """Build an embed + chart for a single player's bear hunt history.

    `rows` is an iterable of tuples `(date_str, damage, rank)` ordered by
    date ascending. Returns (embed, discord.File|None).
    """
    discord_nick = _ltr_line(nickname)
    chart_nick = _reshape_for_chart(nickname)
    if not rows:
        embed = discord.Embed(
            title=f"{discord_nick} · Bear Hunt History",
            description=f"No damage records for ID `{fid}` in {alliance_name}"
                        + (f" for Trap {hunting_trap}" if hunting_trap else "") + ".",
            color=theme.emColor2,
        )
        return embed, None

    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _, _ in rows]
    damages = [int(dmg) for _, dmg, _ in rows]
    ranks = [int(r) if r is not None else None for _, _, r in rows]

    total_hunts = len(rows)
    total_dmg = sum(damages)
    avg_dmg = int(total_dmg / total_hunts)
    max_dmg = max(damages)
    best_rank = min((r for r in ranks if r is not None), default=None)
    last_date, last_dmg, last_rank = dates[-1], damages[-1], ranks[-1]

    title = f"{discord_nick} · Bear Hunt History"
    if hunting_trap:
        title += f" · Trap {hunting_trap}"

    embed = discord.Embed(title=title, color=theme.emColor1)
    embed.add_field(name="Alliance", value=alliance_name, inline=True)
    embed.add_field(name="ID", value=str(fid), inline=True)
    embed.add_field(name="Hunts Attended", value=str(total_hunts), inline=True)
    embed.add_field(name="Average Damage", value=f"{avg_dmg:,}", inline=True)
    embed.add_field(name="Best Damage", value=f"{max_dmg:,}", inline=True)
    embed.add_field(
        name="Best Rank",
        value=f"#{best_rank}" if best_rank is not None else "—",
        inline=True,
    )
    embed.add_field(
        name="Most Recent",
        value=f"{last_date:%Y-%m-%d} — `{last_dmg:,}`"
              + (f" (#{last_rank})" if last_rank else ""),
        inline=False,
    )

    title = f"{chart_nick} — Damage over time" + (f" · Trap {hunting_trap}" if hunting_trap else "")
    image_file = _render_damage_chart(dates, damages, title=title)
    if image_file is not None:
        embed.set_image(url="attachment://plot.png")
    return embed, image_file


# ---------------------------------------------------------------------------
# BearTrack cog
# ---------------------------------------------------------------------------

class BearTrack(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Pre-register the default English engine so it's visible in the
        # Health dashboard from boot, even before any bear screenshot runs.
        # Other languages still register lazily on first use.
        if OCR_AVAILABLE:
            get_ocr_model(DEFAULT_OCR_LANG)

        # Persistent DB connections with WAL mode
        self.alliance_conn, self.alliance_cursor = self._open_db("db/alliance.sqlite")
        self.bear_conn, self.bear_cursor = self._open_db("db/bear_data.sqlite")
        self.users_conn, self.users_cursor = self._open_db("db/users.sqlite")

        # Ensure required columns exist on alliancesettings
        self.alliance_cursor.execute("PRAGMA table_info(alliancesettings)")
        columns = [col[1] for col in self.alliance_cursor.fetchall()]

        new_columns = {
            "bear_score_channel": "INTEGER",
            "bear_keywords": "TEXT",
            "bear_damage_range": "INTEGER DEFAULT 0",
            "bear_admin_only_view": "INTEGER DEFAULT 0",
            "bear_admin_only_add": "INTEGER DEFAULT 0",
            "bear_ocr_lang": "TEXT DEFAULT 'en'",
            "bear_ocr_fallback_langs": "TEXT DEFAULT ''",
            "bear_ocr_autoprune": "INTEGER DEFAULT 0",
            "bear_session_timeout_min": "INTEGER DEFAULT 15",
            "bear_auto_delete_screenshots": "INTEGER DEFAULT 1",
        }
        for col_name, col_type in new_columns.items():
            if col_name not in columns:
                self.alliance_cursor.execute(
                    f"ALTER TABLE alliancesettings ADD COLUMN {col_name} {col_type}"
                )

        self.alliance_conn.commit()

        # Pre-register engines for every language any alliance has configured
        # (primary or fallback) so they all show up in the Bot Health dashboard
        # from boot, not just after a screenshot has actually triggered them.
        if OCR_AVAILABLE:
            try:
                self.alliance_cursor.execute(
                    "SELECT bear_ocr_lang, bear_ocr_fallback_langs FROM alliancesettings"
                )
                configured = set()
                for primary, fallbacks_raw in self.alliance_cursor.fetchall():
                    if primary and primary in OCR_LANG_CODES:
                        configured.add(primary)
                    if fallbacks_raw:
                        for f in fallbacks_raw.split(','):
                            f = f.strip()
                            if f in OCR_LANG_CODES:
                                configured.add(f)
                for lang in configured:
                    get_ocr_model(lang)
            except Exception as e:
                logger.warning(f"Could not pre-register configured OCR languages: {e}")

        # DataSubmit helper with shared connections
        self.data_submit = DataSubmit(self.alliance_conn, self.bear_conn)

    @staticmethod
    def _open_db(path):
        """Open a SQLite connection with the cog's standard WAL settings.
        Returns (connection, cursor)."""
        conn = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.commit()
        return conn, conn.cursor()

    def cog_unload(self):
        for session in list(_active_sessions.values()):
            session.stop_timer()
            session.finalized = True
        _active_sessions.clear()
        for attr in ('alliance_conn', 'bear_conn', 'users_conn'):
            conn = getattr(self, attr, None)
            if conn is not None:
                conn.close()

    def get_alliance_roster(self, alliance_id):
        """Return [(fid, nickname), ...] for members of the given alliance."""
        self.users_cursor.execute(
            "SELECT fid, nickname FROM users WHERE alliance = ?",
            (str(alliance_id),),
        )
        return [(int(fid), nick or "") for (fid, nick) in self.users_cursor.fetchall()]

    def get_ocr_language_settings(self, alliance_id):
        """Return (primary_lang, [fallback_langs]) for the alliance."""
        self.alliance_cursor.execute(
            "SELECT bear_ocr_lang, bear_ocr_fallback_langs FROM alliancesettings "
            "WHERE alliance_id = ?",
            (alliance_id,),
        )
        row = self.alliance_cursor.fetchone()
        if not row:
            return DEFAULT_OCR_LANG, []
        primary = (row[0] or DEFAULT_OCR_LANG).strip()
        if primary not in OCR_LANG_CODES:
            primary = DEFAULT_OCR_LANG
        fb_raw = (row[1] or '').strip()
        fallbacks = [c.strip() for c in fb_raw.split(',') if c.strip() in OCR_LANG_CODES and c.strip() != primary]
        return primary, fallbacks

    def set_ocr_language_settings(self, alliance_id, primary=None, fallbacks=None):
        """Persist new OCR-language settings. None values keep existing."""
        updates = []
        params = []
        if primary is not None:
            if primary not in OCR_LANG_CODES:
                raise ValueError(f"Unknown OCR language code: {primary}")
            updates.append("bear_ocr_lang = ?")
            params.append(primary)
        if fallbacks is not None:
            cleaned = [c for c in fallbacks if c in OCR_LANG_CODES]
            updates.append("bear_ocr_fallback_langs = ?")
            params.append(",".join(cleaned))
        if not updates:
            return
        params.append(alliance_id)
        self.alliance_cursor.execute(
            f"UPDATE alliancesettings SET {', '.join(updates)} WHERE alliance_id = ?",
            params,
        )
        self.alliance_conn.commit()

    def get_ocr_autoprune(self, alliance_id) -> bool:
        self.alliance_cursor.execute(
            "SELECT bear_ocr_autoprune FROM alliancesettings WHERE alliance_id = ?",
            (alliance_id,),
        )
        row = self.alliance_cursor.fetchone()
        return bool(row and row[0])

    def set_ocr_autoprune(self, alliance_id, enabled: bool) -> None:
        self.alliance_cursor.execute(
            "UPDATE alliancesettings SET bear_ocr_autoprune = ? WHERE alliance_id = ?",
            (1 if enabled else 0, alliance_id),
        )
        self.alliance_conn.commit()

    def autoprune_dead_fallbacks(self, alliance_id) -> list[str]:
        """Strip fallbacks with >= AUTOPRUNE_MIN_RUNS and 0 fills.
        Returns the removed lang codes. Caller checks `get_ocr_autoprune`."""
        fallbacks = self.get_ocr_language_settings(alliance_id)[1]
        if not fallbacks:
            return []
        stats = {(s['lang'], s['role']): s for s in get_ocr_lang_stats(alliance_id)}
        to_remove = [
            lang for lang in fallbacks
            if (s := stats.get((lang, 'fallback'))) is not None
            and s['runs'] >= AUTOPRUNE_MIN_RUNS
            and s['rows_filled'] == 0
        ]
        if to_remove:
            kept = [lang for lang in fallbacks if lang not in to_remove]
            self.set_ocr_language_settings(alliance_id, fallbacks=kept)
            logger.info(
                f"Bear OCR autoprune (alliance {alliance_id}): removed "
                f"{to_remove} (>= {AUTOPRUNE_MIN_RUNS} runs, 0 rows filled)"
            )
        return to_remove

    # -------------------------------------------------------------------
    # Settings helpers (column-based, not JSON)
    # -------------------------------------------------------------------

    def get_bear_settings(self, alliance_id: int) -> dict:
        """Return bear settings dict from individual columns."""
        self.alliance_cursor.execute(
            "SELECT bear_score_channel, bear_keywords, bear_damage_range, "
            "bear_admin_only_view, bear_admin_only_add, "
            "bear_session_timeout_min, bear_auto_delete_screenshots "
            "FROM alliancesettings WHERE alliance_id = ?",
            (alliance_id,)
        )
        row = self.alliance_cursor.fetchone()
        if not row:
            return {
                "channel_id": None,
                "keywords": [],
                "damage_range": 0,
                "admin_only_view": 0,
                "admin_only_add": 0,
                "session_timeout_min": 15,
                "auto_delete_screenshots": 1,
            }
        return {
            "channel_id": row[0],
            "keywords": [kw.strip() for kw in row[1].split(",") if kw.strip()] if row[1] else [],
            "damage_range": row[2] or 0,
            "admin_only_view": row[3] or 0,
            "admin_only_add": row[4] or 0,
            "session_timeout_min": row[5] if row[5] is not None else 15,
            "auto_delete_screenshots": row[6] if row[6] is not None else 1,
        }

    def update_bear_setting(self, alliance_id: int, column: str, value):
        """Update a single bear setting column."""
        allowed = {"bear_score_channel", "bear_keywords", "bear_damage_range",
                    "bear_admin_only_view", "bear_admin_only_add",
                    "bear_session_timeout_min", "bear_auto_delete_screenshots"}
        if column not in allowed:
            return
        self.alliance_cursor.execute(
            f"UPDATE alliancesettings SET {column} = ? WHERE alliance_id = ?",
            (value, alliance_id)
        )
        self.alliance_conn.commit()

    async def get_keywords_for_channel(self, channel_id: int) -> list:
        """Return keywords list for the alliance that has this bear channel."""
        self.alliance_cursor.execute(
            "SELECT bear_keywords FROM alliancesettings WHERE bear_score_channel = ?",
            (channel_id,)
        )
        result = self.alliance_cursor.fetchone()
        if result and result[0]:
            return [kw.strip() for kw in result[0].split(",") if kw.strip()]
        return []

    # -------------------------------------------------------------------
    # Permission check
    # -------------------------------------------------------------------

    async def check_bear_permission(self, interaction: discord.Interaction, alliance_id: int, action: str) -> bool:
        """
        Check if user has permission for an action on bear data.
        Actions: "view", "add", "manage"
        """
        is_admin, is_global = PermissionManager.is_admin(interaction.user.id)

        if action == "manage":
            if is_global:
                return True
            if is_admin:
                alliance_ids, _ = PermissionManager.get_admin_alliance_ids(
                    interaction.user.id, interaction.guild_id if interaction.guild else 0
                )
                if alliance_id in alliance_ids:
                    return True
            await interaction.response.send_message(
                f"{theme.deniedIcon} You don't have permission to manage settings for this alliance.",
                ephemeral=True
            )
            return False

        settings = self.get_bear_settings(alliance_id)
        key = "admin_only_add" if action == "add" else "admin_only_view"
        only_admin = settings.get(key, 0)

        if not only_admin:
            return True

        if is_global:
            return True
        if is_admin:
            alliance_ids, _ = PermissionManager.get_admin_alliance_ids(
                interaction.user.id, interaction.guild_id if interaction.guild else 0
            )
            if alliance_id in alliance_ids:
                return True

        await interaction.response.send_message(
            f"{theme.deniedIcon} You don't have permission to {action} bear damage for this alliance.",
            ephemeral=True
        )
        return False

    # -------------------------------------------------------------------
    # Autocomplete helpers
    # -------------------------------------------------------------------

    async def alliance_autocomplete(self, interaction: discord.Interaction, current: str):
        self.alliance_cursor.execute(
            "SELECT alliance_id, name FROM alliance_list WHERE name LIKE ? ORDER BY name LIMIT 20",
            (f"%{current}%",)
        )
        rows = self.alliance_cursor.fetchall()
        return [
            discord.app_commands.Choice(name=row[1], value=str(row[0]))
            for row in rows
        ]

    async def hunting_trap_autocomplete(self, interaction: discord.Interaction, current: str):
        return [
            discord.app_commands.Choice(name="1", value=1),
            discord.app_commands.Choice(name="2", value=2),
        ]

    async def player_autocomplete(self, interaction: discord.Interaction, current: str):
        """Suggest player FIDs from the alliance already picked in the
        `alliance` parameter. Returns a choice value of the FID as a string.
        """
        alliance_val = getattr(interaction.namespace, 'alliance', None)
        if not alliance_val:
            return []
        try:
            alliance_id = int(alliance_val)
        except (TypeError, ValueError):
            return []
        self.users_cursor.execute(
            "SELECT fid, nickname FROM users WHERE alliance = ? AND nickname LIKE ? "
            "ORDER BY nickname LIMIT 25",
            (str(alliance_id), f"%{current}%"),
        )
        return [
            discord.app_commands.Choice(name=f"{nick} ({fid})", value=str(fid))
            for fid, nick in self.users_cursor.fetchall()
        ]

    # -------------------------------------------------------------------
    # on_message — OCR processing
    # -------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return
        if not message.content.strip() and not message.attachments:
            return

        # Check if channel is a bear_score_channel and get keywords + alliance_id
        self.alliance_cursor.execute(
            "SELECT alliance_id, bear_keywords FROM alliancesettings WHERE bear_score_channel = ?",
            (message.channel.id,)
        )
        row = self.alliance_cursor.fetchone()
        if not row:
            return
        alliance_id, keywords_raw = row

        keywords = [kw.strip() for kw in keywords_raw.split(",") if kw.strip()] if keywords_raw else []
        if keywords and not any(kw.lower() in message.content.lower() for kw in keywords):
            return

        await self.process_bear_hunt_data(message, alliance_id=int(alliance_id))

    async def process_bear_hunt_data(self, message, *, alliance_id=None):
        """Route a screenshot upload into the per-(channel, user) session."""
        image_attachments = [
            a for a in message.attachments
            if any(a.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg'])
        ]
        if not image_attachments:
            return

        key = (message.channel.id, message.author.id)
        session = _active_sessions.get(key)

        if session is None:
            if alliance_id is None:
                self.alliance_cursor.execute(
                    "SELECT alliance_id FROM alliancesettings WHERE bear_score_channel = ?",
                    (message.channel.id,),
                )
                row = self.alliance_cursor.fetchone()
                if not row:
                    return
                alliance_id = int(row[0])

            self.alliance_cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,)
            )
            anrow = self.alliance_cursor.fetchone()
            alliance_name = anrow[0] if anrow else f"Alliance {alliance_id}"
            roster = self.get_alliance_roster(alliance_id)
            primary_lang, fallback_langs = self.get_ocr_language_settings(alliance_id)
            fallback_langs = sorted(fallback_langs, key=lambda l: l in _LATIN_ONLY_LANGS)
            timeout_min, auto_delete = self.get_session_settings(alliance_id)

            session = BearSession(
                cog=self,
                channel_id=message.channel.id,
                user_id=message.author.id,
                alliance_id=alliance_id,
                alliance_name=alliance_name,
                roster=roster,
                primary_lang=primary_lang,
                fallback_langs=fallback_langs,
                timeout_min=timeout_min,
                auto_delete=auto_delete,
            )
            _active_sessions[key] = session

            try:
                session.session_view = BearSessionView(session)
                session.progress_msg = await message.channel.send(
                    embed=session.build_progress_embed(),
                    view=session.session_view,
                )
            except Exception as e:
                logger.warning(f"Bear hunt: could not post collecting message: {e}")

        await session.add_message(message, image_attachments)

    def get_session_settings(self, alliance_id: int) -> tuple[int, bool]:
        """Return (timeout_min, auto_delete_screenshots) for the alliance."""
        self.alliance_cursor.execute(
            "SELECT bear_session_timeout_min, bear_auto_delete_screenshots "
            "FROM alliancesettings WHERE alliance_id = ?",
            (alliance_id,),
        )
        row = self.alliance_cursor.fetchone()
        if not row:
            return 15, True
        timeout = row[0] if row[0] is not None else 15
        auto_delete = bool(row[1]) if row[1] is not None else True
        return int(timeout), auto_delete

    @asynccontextmanager
    async def _acquire_ocr_slot(self):
        """Yield to gift redemption first, then acquire an OCR slot."""
        pq = self.bot.get_cog('ProcessQueue')
        while pq and pq.has_queued_or_active('gift_redeem'):
            await asyncio.sleep(2)
        async with _get_ocr_semaphore():
            yield

    async def _ocr_attachment_to_result(self, image_bytes: bytes, primary_lang: str,
                                        fallback_langs: list, *, filename: str = "",
                                        roster: list | None = None,
                                        alliance_id: int | None = None,
                                        progress_callback=None,
                                        session=None) -> ImageResult:
        """OCR one screenshot (primary + fallbacks) → ImageResult.
        `progress_callback(phase, lang)` is awaited per OCR phase. When called
        with `session`, OCR engines are reused across all calls for that
        session; otherwise each call uses its own short acquire/release."""
        result = ImageResult()
        if progress_callback:
            await progress_callback('ocr', primary_lang)
        try:
            async with self._acquire_ocr_slot():
                extracted_text = await ocr_bytes(image_bytes, primary_lang, session=session)
        except Exception as e:
            logger.error(f"Bear OCR error ({primary_lang}) on {filename}: {e}")
            return result
        if not extracted_text.strip():
            return result

        result.ok = True
        repaired = repair_ocr_digits(extracted_text)
        logger.info(
            f"Bear OCR [{primary_lang}] ({filename}): {extracted_text!r} → {repaired!r}"
        )

        trap, rallies, damage = extract_bear_hunt_stats(repaired)
        result.trap = trap
        result.rallies = rallies
        result.total_damage = damage
        result.date = extract_hunt_date(repaired) or ""

        img_rows = {row['damage']: row for row in parse_player_rows(repaired)}

        def _score_snapshot():
            return {dmg: name_match_score(r.get('name') or '', roster)
                    for dmg, r in img_rows.items()}
        primary_filled = sum(1 for r in img_rows.values()
                             if not is_row_unfilled(r, roster))
        record_ocr_lang_run(alliance_id, primary_lang, 'primary', primary_filled)

        # Stale DB rows can list the current primary as a fallback.
        fallback_langs = [lang for lang in fallback_langs if lang != primary_lang]
        if fallback_langs and any(is_row_unfilled(r, roster) for r in img_rows.values()):
            seen_repaired_texts = {repaired}
            attempts = 0
            for fb_lang in fallback_langs:
                if attempts >= MAX_FALLBACK_ATTEMPTS:
                    logger.info(
                        f"Bear OCR: fallback budget hit ({MAX_FALLBACK_ATTEMPTS} "
                        f"useful runs), stopping early on {filename}"
                    )
                    break
                if not any(is_row_unfilled(r, roster) for r in img_rows.values()):
                    break
                if progress_callback:
                    await progress_callback('fallback', fb_lang)
                try:
                    async with self._acquire_ocr_slot():
                        fb_text = await ocr_bytes(image_bytes, fb_lang, session=session)
                except Exception as e:
                    logger.warning(f"Bear OCR fallback {fb_lang} failed: {e}")
                    continue
                if not fb_text.strip():
                    continue
                fb_repaired = repair_ocr_digits(fb_text)
                if fb_repaired in seen_repaired_texts:
                    logger.info(
                        f"Bear OCR fallback [{fb_lang}] skipped: identical "
                        f"output to a previous pass"
                    )
                    record_ocr_lang_run(alliance_id, fb_lang, 'fallback', 0)
                    continue
                seen_repaired_texts.add(fb_repaired)
                logger.info(
                    f"Bear OCR fallback [{fb_lang}] ({filename}): {fb_repaired!r}"
                )
                if not _output_matches_lang_script(fb_repaired, fb_lang):
                    logger.info(
                        f"Bear OCR fallback [{fb_lang}] rejected: "
                        f"output contains no {fb_lang}-script characters"
                    )
                    record_ocr_lang_run(alliance_id, fb_lang, 'fallback', 0)
                    continue
                attempts += 1
                pre_scores = _score_snapshot()
                filled_via_damage = False
                fb_rows = parse_player_rows(fb_repaired)
                if fb_lang in _RTL_LANGS:
                    for fr in fb_rows:
                        if fr.get('name'):
                            fr['name'] = _reverse_for_rtl(fr['name'], fb_lang)
                filled_via_damage = merge_fallback_rows_by_damage(
                    img_rows, fb_rows, roster, fb_lang
                )
                if not filled_via_damage and fb_lang not in _LATIN_ONLY_LANGS:
                    fill_unfilled_by_position(
                        img_rows, fb_repaired, fb_lang, filename, roster
                    )
                rows_improved = sum(
                    1 for dmg, r in img_rows.items()
                    if name_match_score(r.get('name') or '', roster) > pre_scores.get(dmg, 0)
                )
                record_ocr_lang_run(alliance_id, fb_lang, 'fallback', rows_improved)

        if alliance_id and self.get_ocr_autoprune(alliance_id):
            try:
                self.autoprune_dead_fallbacks(alliance_id)
            except Exception as e:
                logger.warning(f"Bear OCR autoprune failed (alliance {alliance_id}): {e}")

        result.rows = img_rows
        return result

    async def _finalize_session(self, session: BearSession, *, timed_out: bool):
        """Build the review for the largest detected event in the session."""
        if not session.events:
            if session.progress_msg:
                embed = discord.Embed(
                    title=f"{theme.warnIcon} OCR could not read any screenshot",
                    description="No data was extracted. Please upload clearer screenshots.",
                    color=theme.emColor2,
                )
                try:
                    await session.progress_msg.edit(embed=embed, view=None)
                except Exception:
                    pass
            return

        tracker = BearAutoDeleteTracker(session.source_messages, session.auto_delete)
        today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        primary_event = max(
            session.events,
            key=lambda ev: (ev.image_count, len(ev.merged_rows)),
        )
        dropped_events = [ev for ev in session.events if ev is not primary_event]
        dropped_screenshots = sum(ev.image_count for ev in dropped_events)

        merged_rows = dict(primary_event.merged_rows)
        merged_rows.pop(primary_event.damage_int, None)
        rows_sum = sum(r['damage'] for r in merged_rows.values())
        damage_int = primary_event.damage_int
        if rows_sum > damage_int:
            damage_int = rows_sum
        for i, row in enumerate(sorted(merged_rows.values(), key=lambda r: -r['damage'])):
            row['rank'] = i + 1

        hunt_meta = {
            'date': primary_event.date_value or today_date,
            'hunting_trap': int(primary_event.trap_value)
                if primary_event.trap_value and primary_event.trap_value.isdigit() else None,
            'rallies': int(primary_event.rallies_value)
                if primary_event.rallies_value and primary_event.rallies_value.isdigit() else None,
            'total_damage': damage_int or 0,
        }
        sorted_rows = sorted(
            merged_rows.values(),
            key=lambda r: (r['rank'] if r['rank'] is not None else 999, -r['damage']),
        )
        review = BearHuntReviewView(
            cog=self,
            data_submit=self.data_submit,
            hunt_meta=hunt_meta,
            rows=sorted_rows,
            roster=session.roster,
            alliance_id=session.alliance_id,
            alliance_name=session.alliance_name,
            original_user_id=session.user_id,
            auto_delete_tracker=tracker,
            source_messages=session.source_messages,
        )
        tracker.register()

        embed = review.build_embed()
        prefixes = []
        if timed_out:
            prefixes.append(
                f"{theme.hourglassIcon} **Session timed out after "
                f"{session.timeout_min} min** — review and Submit when ready."
            )
        if dropped_screenshots:
            prefixes.append(
                f"{theme.warnIcon} **{dropped_screenshots} screenshot"
                f"{'s' if dropped_screenshots != 1 else ''} didn't fit this "
                f"event and were ignored.** If they were from a different "
                f"hunt (different trap, rallies, or alliance total), upload "
                f"them as a separate batch."
            )
        if prefixes:
            embed.description = "\n".join(prefixes) + "\n\n" + (embed.description or "")
        if not session.any_ocr_success:
            embed.title = f"{theme.warnIcon} OCR could not read the image(s) — add rows manually"

        channel = self.bot.get_channel(session.channel_id)
        if session.progress_msg is not None:
            try:
                await session.progress_msg.edit(embed=embed, view=review)
                review.message = session.progress_msg
                return
            except Exception as e:
                logger.warning(f"Bear hunt: could not edit progress into review: {e}")
        if channel:
            try:
                review.message = await channel.send(embed=embed, view=review)
            except Exception as e:
                logger.warning(f"Bear hunt: could not send review: {e}")

    # -------------------------------------------------------------------
    # Slash commands
    # -------------------------------------------------------------------

    @app_commands.command(name="bear_damage_add", description="Manually add bear hunt damage data")
    @app_commands.autocomplete(alliance=alliance_autocomplete, hunting_trap=hunting_trap_autocomplete)
    @app_commands.describe(
        alliance="Alliance name",
        hunting_trap="Hunting trap number",
        rallies="Number of rallies",
        total_damage="Total alliance damage",
        date="UTC date (YYYY-MM-DD). Defaults to today."
    )
    async def bear_damage_add(self, interaction: discord.Interaction, alliance: str, hunting_trap: int,
                              rallies: int, total_damage: int, date: str | None = None):
        try:
            alliance_id = int(alliance)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid alliance selected.", ephemeral=True
            )
            return

        allowed = await self.check_bear_permission(interaction, alliance_id, "add")
        if not allowed:
            return

        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        self.alliance_cursor.execute(
            "SELECT name FROM alliance_list WHERE alliance_id = ?",
            (alliance_id,)
        )
        row = self.alliance_cursor.fetchone()
        alliance_name = row[0] if row else f"Alliance ID: {alliance_id}"

        errors = validate_bear_submission(date, hunting_trap, rallies, total_damage)
        if errors:
            msg = f"{theme.deniedIcon} Submission failed:\n" + "\n".join(f"- {e}" for e in errors)
            await interaction.response.send_message(msg, ephemeral=True)
            return

        await self.data_submit.process_submission(
            interaction,
            date=date,
            hunting_trap=hunting_trap,
            rallies=rallies,
            total_damage=total_damage,
            alliance_id=alliance_id,
            alliance_name=alliance_name
        )

    @app_commands.command(name="bear_damage_view", description="View bear damage for an alliance")
    @app_commands.autocomplete(alliance=alliance_autocomplete, hunting_trap=hunting_trap_autocomplete)
    @app_commands.describe(
        alliance="Select an alliance",
        hunting_trap="Hunting trap number",
        from_date="Start date (YYYY-MM-DD)",
        to_date="End date (YYYY-MM-DD)"
    )
    async def bear_damage_view(self, interaction: discord.Interaction, alliance: str, hunting_trap: int,
                               from_date: str | None = None, to_date: str | None = None):
        try:
            alliance_id = int(alliance)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid alliance selected.", ephemeral=True
            )
            return

        allowed = await self.check_bear_permission(interaction, alliance_id, "view")
        if not allowed:
            return

        await interaction.response.defer()

        view = BearDamageView(
            data_submit=self.data_submit,
            cog=self,
            original_user_id=interaction.user.id,
            alliance_id=alliance_id,
            hunting_trap=hunting_trap,
            from_date=datetime.strptime(from_date, "%Y-%m-%d").date() if from_date else None,
            to_date=datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else None
        )

        embed, file = await self.data_submit.process_view(
            alliance_id=alliance_id,
            hunting_trap=hunting_trap,
            from_date=from_date,
            to_date=to_date,
        )

        if not embed:
            await interaction.followup.send(
                f"{theme.deniedIcon} No data found for the selected parameters.", ephemeral=True
            )
            return

        await interaction.followup.send(embed=embed, file=file if file else None, view=view)

    @app_commands.command(name="bear_player_history", description="Show a player's bear hunt damage history")
    @app_commands.autocomplete(alliance=alliance_autocomplete,
                               player=player_autocomplete,
                               hunting_trap=hunting_trap_autocomplete)
    @app_commands.describe(
        alliance="Alliance the player belongs to",
        player="Player (pick from the suggestions or enter an ID)",
        hunting_trap="Filter by trap (optional; both traps combined if omitted)",
    )
    async def bear_player_history(self, interaction: discord.Interaction,
                                  alliance: str, player: str,
                                  hunting_trap: int | None = None):
        try:
            alliance_id = int(alliance)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid alliance.", ephemeral=True
            )
            return

        allowed = await self.check_bear_permission(interaction, alliance_id, "view")
        if not allowed:
            return

        try:
            fid = int(player)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid player — must be an ID.", ephemeral=True
            )
            return

        await interaction.response.defer()

        self.users_cursor.execute(
            "SELECT nickname FROM users WHERE fid = ?", (fid,)
        )
        nick_row = self.users_cursor.fetchone()
        nickname = nick_row[0] if nick_row else f"ID {fid}"

        self.alliance_cursor.execute(
            "SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,)
        )
        an_row = self.alliance_cursor.fetchone()
        alliance_name = an_row[0] if an_row else f"Alliance {alliance_id}"

        query = (
            "SELECT h.date, bpd.damage, bpd.rank "
            "FROM bear_player_damage bpd "
            "JOIN bear_hunts h ON h.id = bpd.hunt_id "
            "WHERE h.alliance_id = ? AND bpd.fid = ?"
        )
        params = [alliance_id, fid]
        if hunting_trap:
            query += " AND h.hunting_trap = ?"
            params.append(hunting_trap)
        query += " ORDER BY h.date ASC"
        rows = self.bear_cursor.execute(query, params).fetchall()

        embed, image_file = bear_player_history_embed(
            alliance_name=alliance_name, fid=fid, nickname=nickname,
            hunting_trap=hunting_trap, rows=rows,
        )
        await interaction.followup.send(
            embed=embed, file=image_file if image_file else None
        )

    # -------------------------------------------------------------------
    # Main menu entry point
    # -------------------------------------------------------------------

    def _needs_bear_setup_hint(self, user_id: int, guild_id: int) -> bool:
        """True iff none of the user's accessible alliances has a bear channel
        configured yet. Used to hide the 'new here?' hint once any in-scope
        alliance is set up — see show_bear_track_menu."""
        is_admin, is_global = PermissionManager.is_admin(user_id)
        if not is_admin:
            return False
        try:
            if is_global:
                row = self.alliance_cursor.execute(
                    "SELECT 1 FROM alliancesettings "
                    "WHERE bear_score_channel IS NOT NULL LIMIT 1"
                ).fetchone()
                return row is None

            alliance_ids, _ = PermissionManager.get_admin_alliance_ids(user_id, guild_id)
            if not alliance_ids:
                return True
            placeholders = ','.join('?' * len(alliance_ids))
            row = self.alliance_cursor.execute(
                f"SELECT 1 FROM alliancesettings "
                f"WHERE alliance_id IN ({placeholders}) "
                f"AND bear_score_channel IS NOT NULL LIMIT 1",
                alliance_ids,
            ).fetchone()
            return row is None
        except Exception as e:
            logger.warning(f"Bear setup hint check failed: {e}")
            return False

    async def show_bear_track_menu(self, interaction: discord.Interaction):
        """Display the bear damage tracking main menu."""
        try:
            view = BearMenuView(cog=self, original_user_id=interaction.user.id)

            setup_hint = ""
            if self._needs_bear_setup_hint(
                interaction.user.id,
                interaction.guild_id if interaction.guild else 0,
            ):
                setup_hint = (
                    f"{theme.warnIcon} **New here?** Click **Bear Channel "
                    f"Setup** below to pick a channel for an ally — until "
                    f"that's done the bot won't process any screenshots.\n\n"
                )

            embed = discord.Embed(
                title=f"{theme.chartIcon} Bear Damage Tracking",
                description=(
                    f"Track your alliance's bear hunt damage by uploading "
                    f"in-game screenshots — no manual data entry.\n\n"
                    f"{setup_hint}"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.chartIcon} **View Bear Damage**\n"
                    f"└ Damage trend charts per alliance and trap\n\n"
                    f"{theme.editListIcon} **Bear Channel Setup**\n"
                    f"└ Pick the channel, keywords, OCR engines and chart "
                    f"range — everything per-alliance\n\n"
                    f"{theme.documentIcon} **Edit Bear Damage**\n"
                    f"└ Edit or delete saved records, re-match unmatched rows\n\n"
                    f"{theme.settingsIcon} **Settings**\n"
                    f"└ Session timeout, auto-delete, and permissions\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_bear_track_menu: {e}")
            print(f"[ERROR] Error in show_bear_track_menu: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Failed to load Bear Tracking menu.", ephemeral=True
                    )
            except Exception:
                pass


# ---------------------------------------------------------------------------
# BearHuntReviewView — paginated per-row review with roster matching
# ---------------------------------------------------------------------------

_STATUS_ICONS = {
    'auto': '✅',
    'likely': '🟡',
    'ambiguous': '⚠',
    'none': '❌',
    'manual': '✏',
}

_STATUS_LABELS = {
    'auto': 'auto match',
    'likely': 'likely match',
    'ambiguous': 'ambiguous match',
    'none': 'no match',
    'manual': 'manual entry',
}


class RetryOcrLanguagePicker(discord.ui.View):
    """Ephemeral one-shot language selector shown when an admin clicks
    'Retry OCR' on a review. Picking a language drives the parent
    review's `_run_retry_ocr`."""

    def __init__(self, parent_review, current_primary):
        super().__init__(timeout=7200)
        self.parent_review = parent_review

        # Offer every configured engine except the one already in use as
        # primary — there's no point retrying with the same model.
        opts = [
            discord.SelectOption(label=label, value=code)
            for code, label in OCR_LANGUAGES if code != current_primary
        ]
        select = discord.ui.Select(placeholder="Pick an OCR engine…", options=opts)
        select.callback = self._on_pick
        self.add_item(select)

    async def _on_pick(self, interaction: discord.Interaction):
        new_primary = interaction.data['values'][0]
        await interaction.response.edit_message(
            content=(
                f"{theme.hourglassIcon} Re-running OCR with `{new_primary}`… "
                f"this can take a few seconds per screenshot."
            ),
            view=None,
        )
        await self.parent_review._run_retry_ocr(interaction, new_primary)


class BearHuntReviewView(discord.ui.View):
    """Review/edit OCR-extracted hunt data; submit persists to DB."""

    ROWS_PER_PAGE = 25

    def __init__(self, cog, data_submit, *, hunt_meta, rows, roster,
                 alliance_id, alliance_name, original_user_id,
                 auto_delete_tracker=None, source_messages=None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.data_submit = data_submit
        self.hunt_meta = hunt_meta
        self.roster = roster
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.original_user_id = original_user_id
        self.auto_delete_tracker = auto_delete_tracker
        # Used by Retry OCR to re-download attachments.
        self.source_messages = source_messages or []
        self.message = None
        self.page = 0
        self._tracker_resolved = False

        self.rows = [self._enrich_row(r) for r in rows]
        self._resolve_unique_assignments()
        self._sort_rows()
        self._build_components()

    async def _notify_tracker_submit(self):
        # Cleanup must never break submit UX.
        if self.auto_delete_tracker and not self._tracker_resolved:
            self._tracker_resolved = True
            try:
                await self.auto_delete_tracker.on_submit()
            except Exception as e:
                logger.warning(f"Bear auto-delete tracker (on_submit) raised: {e}")

    async def _notify_tracker_cancel(self):
        if self.auto_delete_tracker and not self._tracker_resolved:
            self._tracker_resolved = True
            try:
                await self.auto_delete_tracker.on_cancel()
            except Exception as e:
                logger.warning(f"Bear auto-delete tracker (on_cancel) raised: {e}")

    def _enrich_row(self, raw_row):
        candidates = match_roster(raw_row.get('name') or '', self.roster)
        status = classify_match(candidates)
        fid = nickname = None
        if status == 'auto':
            fid, nickname, _ = candidates[0]
        return {
            'name': raw_row.get('name') or '',
            'damage': int(raw_row.get('damage') or 0),
            'rank': raw_row.get('rank'),
            'fid': fid,
            'nickname': nickname,
            'candidates': candidates,
            'status': status,
        }

    def _resolve_unique_assignments(self):
        """Greedy unique-fid assignment across rows. Manual entries
        reserve their fid first, then highest-score wins."""
        assigned_fids: set = set()
        for row in self.rows:
            if row.get('status') == 'manual' and row.get('fid'):
                assigned_fids.add(row['fid'])
            else:
                row['fid'] = None
                row['nickname'] = None
                row['status'] = 'none'

        candidates = []
        for row_idx, row in enumerate(self.rows):
            if row.get('status') == 'manual':
                continue
            for fid, nick, score in row.get('candidates') or []:
                candidates.append((score, row_idx, fid, nick))
        candidates.sort(key=lambda c: (-c[0], c[1]))

        for score, row_idx, fid, nick in candidates:
            row = self.rows[row_idx]
            if row.get('fid') is not None or fid in assigned_fids:
                continue
            row['fid'] = fid
            row['nickname'] = nick
            row['status'] = 'auto' if score >= MATCH_AUTO_CONFIRM else 'likely'
            assigned_fids.add(fid)

    def _sort_rows(self):
        self.rows.sort(
            key=lambda r: (r['rank'] if r['rank'] is not None else 999, -r['damage'])
        )

    def _lookup_nickname(self, fid):
        for f, nick in self.roster:
            if f == fid:
                return nick
        return None

    def _total_pages(self):
        return max(1, -(-len(self.rows) // self.ROWS_PER_PAGE))

    def build_embed(self):
        embed = discord.Embed(
            title=f"{theme.chartIcon} Review Bear Hunt",
            description=(
                f"Review the detected rows below. Use **Edit a row** to fix "
                f"matches, **Add Row** for missed players, **Edit Hunt Info** "
                f"for date/trap/rallies/total, then **Submit**.\n"
                f"To **delete a row**, pick it from **Edit a row** and clear "
                f"the **Player** field before submitting the modal.\n"
                f"Use **Save Totals Only** to record just the summary and "
                f"skip per-player tracking for this hunt."
            ),
            color=theme.emColor1,
        )
        # Hint only on truly unreadable rows; `none`-with-name = roster
        # gap and `likely` = OCR captured well enough.
        unreadable_rows = sum(
            1 for r in self.rows
            if sum(c.isalpha() for c in (r.get('name') or '')) < 3
        )
        if self.rows and unreadable_rows / len(self.rows) >= 0.25:
            embed.description += (
                f"\n\n{theme.warnIcon} *Many rows didn't match cleanly. "
                f"If this happens often, your OCR language may not fit your "
                f"alliance's player names — adjust under "
                f"**Settings → Bear Tracking → OCR Languages**.*"
            )
        embed.add_field(name="Alliance", value=self.alliance_name or f"ID {self.alliance_id}", inline=False)
        embed.add_field(name="Date", value=self.hunt_meta['date'], inline=True)
        embed.add_field(
            name="Hunting Trap",
            value=str(self.hunt_meta['hunting_trap']) if self.hunt_meta['hunting_trap'] is not None else "-",
            inline=True,
        )
        embed.add_field(
            name="Rallies",
            value=str(self.hunt_meta['rallies']) if self.hunt_meta['rallies'] is not None else "-",
            inline=True,
        )
        embed.add_field(
            name="Total Alliance Damage",
            value=format_damage_for_embed(self.hunt_meta['total_damage']) if self.hunt_meta['total_damage'] is not None else "-",
            inline=False,
        )

        if not self.rows:
            embed.add_field(name="Players", value="*No player rows detected. Use + Add Row to add manually.*", inline=False)
            return embed

        start = self.page * self.ROWS_PER_PAGE
        end = min(start + self.ROWS_PER_PAGE, len(self.rows))
        lines = []
        for i, r in enumerate(self.rows[start:end], start=start):
            rank_str = f"**#{r['rank']}**" if r['rank'] is not None else "**?**"
            icon = _STATUS_ICONS.get(r['status'], '')
            status = r['status']
            if status == 'auto':
                player = f"`{_isolate_rtl(r['nickname'])}` · `{r['fid']}`"
            elif status == 'likely':
                top_fid, top_nick, score = r['candidates'][0]
                player = f"`{_isolate_rtl(top_nick)}` ({score}%) · `{top_fid}`"
            elif status == 'ambiguous':
                tops = " / ".join(
                    f"`{_isolate_rtl(c[1])}` (`{c[0]}`, {c[2]}%)"
                    for c in r['candidates'][:2]
                )
                player = f"{tops}"
            elif status == 'manual':
                player = f"`{_isolate_rtl(r['nickname'])}` · `{r['fid']}`"
            else:
                name = r['name'] or "unreadable"
                player = f"`{_isolate_rtl(name)}` — no match"
            lines.append(_ltr_line(f"{rank_str} {icon} {player} — `{format_damage_for_embed(r['damage'])}`"))

        total_pages = self._total_pages()
        header = (
            f"Players {start + 1}-{end} of {len(self.rows)}"
            if total_pages > 1 else f"Players ({len(self.rows)})"
        )
        # Discord field value limit 1024 chars; truncate and paginate if needed.
        value = "\n".join(lines)
        if len(value) > 1024:
            value = value[:1010] + "\n…(truncated)"
        embed.add_field(name=header, value=value, inline=False)

        unresolved = sum(1 for r in self.rows if r['status'] in ('none', 'ambiguous'))
        if unresolved:
            embed.set_footer(
                text=f"{unresolved} row(s) without a confirmed player will be saved "
                     f"as unmatched — resolve them now or later from Bear Damage Records."
            )
        return embed

    def _build_components(self):
        self.clear_items()

        if self.rows:
            start = self.page * self.ROWS_PER_PAGE
            end = min(start + self.ROWS_PER_PAGE, len(self.rows))
            options = []
            for i, r in enumerate(self.rows[start:end], start=start):
                rank_part = f"#{r['rank']}" if r['rank'] is not None else "?"
                name_part = r['nickname'] or r['name'] or "(unreadable)"
                fid_part = f" · {r['fid']}" if r.get('fid') else ""
                label = _ltr_line(f"{rank_part} {name_part}{fid_part}")[:100]
                status_label = _STATUS_LABELS.get(r['status'], r['status'])
                desc = f"{format_damage_for_embed(r['damage'])} · {status_label}"[:100]
                options.append(discord.SelectOption(label=label, value=str(i), description=desc))
            select = discord.ui.Select(
                placeholder="Edit a row…",
                options=options,
                row=0,
            )
            select.callback = self._on_row_selected
            self.add_item(select)

        # Retry OCR needs source_messages to re-download attachments.
        row1 = [
            ("Add Row", theme.addIcon, discord.ButtonStyle.secondary, self._on_add_row, False),
            ("Edit Hunt Info", theme.editListIcon, discord.ButtonStyle.secondary, self._on_edit_header, False),
            ("Retry OCR", theme.globeIcon, discord.ButtonStyle.secondary, self._on_retry_ocr,
             not self.source_messages),
        ]
        for label, emoji, style, cb, disabled in row1:
            btn = discord.ui.Button(label=label, emoji=emoji, style=style, row=1, disabled=disabled)
            btn.callback = cb
            self.add_item(btn)

        row2 = [
            ("Submit", theme.verifiedIcon, discord.ButtonStyle.success, self._on_submit),
            ("Save Totals Only", theme.totalIcon, discord.ButtonStyle.primary, self._on_submit_totals_only),
            ("Cancel", theme.deniedIcon, discord.ButtonStyle.secondary, self._on_cancel),
        ]
        for label, emoji, style, cb in row2:
            btn = discord.ui.Button(label=label, emoji=emoji, style=style, row=2)
            btn.callback = cb
            self.add_item(btn)

        total_pages = self._total_pages()
        if total_pages > 1:
            prev_btn = discord.ui.Button(
                label="Prev", emoji=theme.prevIcon,
                style=discord.ButtonStyle.secondary,
                row=3, disabled=(self.page == 0),
            )
            prev_btn.callback = self._on_prev
            self.add_item(prev_btn)
            page_label = discord.ui.Button(
                label=f"Page {self.page + 1}/{total_pages}",
                style=discord.ButtonStyle.secondary,
                row=3, disabled=True,
            )
            self.add_item(page_label)
            next_btn = discord.ui.Button(
                label="Next", emoji=theme.nextIcon,
                style=discord.ButtonStyle.secondary,
                row=3, disabled=(self.page >= total_pages - 1),
            )
            next_btn.callback = self._on_next
            self.add_item(next_btn)

    async def refresh(self, interaction):
        self._resolve_unique_assignments()
        self._sort_rows()
        # Clamp page after deletions
        total_pages = self._total_pages()
        if self.page >= total_pages:
            self.page = total_pages - 1
        self._build_components()
        embed = self.build_embed()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    # ---------- button callbacks ----------

    async def _on_row_selected(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        idx = int(interaction.data['values'][0])
        if idx >= len(self.rows):
            await interaction.response.send_message(
                f"{theme.deniedIcon} That row no longer exists. Please try again.",
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(EditRowModal(self, idx))

    async def _on_edit_header(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await interaction.response.send_modal(EditHeaderModal(self))

    async def _on_add_row(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await interaction.response.send_modal(AddRowModal(self))

    async def _on_submit(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        errors = validate_bear_submission(
            self.hunt_meta['date'], self.hunt_meta['hunting_trap'],
            self.hunt_meta['rallies'], self.hunt_meta['total_damage'],
        )
        if errors:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Cannot submit: " + "; ".join(errors),
                ephemeral=True,
            )
            return
        seen_fids = set()
        for r in self.rows:
            if r['fid'] is None:
                continue
            if r['fid'] in seen_fids:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} ID `{r['fid']}` is assigned to more than one row. "
                    f"Resolve the duplicate before submitting.",
                    ephemeral=True,
                )
                return
            seen_fids.add(r['fid'])

        try:
            await self.data_submit.process_full_submission(
                interaction,
                hunt_meta=self.hunt_meta,
                player_rows=self.rows,
                alliance_id=self.alliance_id,
                alliance_name=self.alliance_name,
            )
            await self._notify_tracker_submit()
        except Exception as e:
            logger.error(f"Error in bear review submit: {e}")
            print(f"[ERROR] Error in bear review submit: {e}")
            try:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Error during submission: {e}", ephemeral=True
                )
            except Exception:
                pass

    async def _on_submit_totals_only(self, interaction):
        """Save the hunt summary (date / trap / rallies / total damage) and
        ignore every detected player row. For alliances that don't track
        per-player damage, or when the OCR row data is too noisy to
        bother curating.
        """
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        errors = validate_bear_submission(
            self.hunt_meta['date'], self.hunt_meta['hunting_trap'],
            self.hunt_meta['rallies'], self.hunt_meta['total_damage'],
        )
        if errors:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Cannot submit: " + "; ".join(errors),
                ephemeral=True,
            )
            return
        try:
            await self.data_submit.process_full_submission(
                interaction,
                hunt_meta=self.hunt_meta,
                player_rows=None,
                alliance_id=self.alliance_id,
                alliance_name=self.alliance_name,
            )
            await self._notify_tracker_submit()
        except Exception as e:
            logger.error(f"Error in bear review summary-only submit: {e}")
            print(f"[ERROR] Error in bear review summary-only submit: {e}")
            try:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Error during submission: {e}", ephemeral=True
                )
            except Exception:
                pass

    async def _on_cancel(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        embed = discord.Embed(
            description=f"{theme.deniedIcon} Bear hunt review canceled.",
            color=theme.emColor2,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=None)
        await self._notify_tracker_cancel()

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message is not None:
            embed = discord.Embed(
                title=f"{theme.hourglassIcon} Bear Hunt Review Expired",
                description=(
                    f"This review timed out after 2 hours of inactivity and "
                    f"can no longer be submitted. Upload the screenshots again "
                    f"to start a new review."
                ),
                color=theme.emColor2,
            )
            try:
                await self.message.edit(content=None, embed=embed, view=None)
            except Exception as e:
                logger.warning(f"Bear hunt: could not edit timed-out review message: {e}")
        await self._notify_tracker_cancel()

    async def _on_retry_ocr(self, interaction):
        """Send a single ephemeral with a language picker. The picked
        language drives `_run_retry_ocr`, which merges the new engine's
        results into the existing review (additive — never destructive)."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.source_messages:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Original screenshots aren't available for "
                f"this review (auto-deleted or external upload). Upload them "
                f"again to retry.",
                ephemeral=True,
            )
            return
        current_primary, _ = self.cog.get_ocr_language_settings(self.alliance_id)
        picker = RetryOcrLanguagePicker(self, current_primary)
        await interaction.response.send_message(
            content=(
                f"Pick an OCR engine to retry with. New rows the chosen "
                f"engine finds are **added or used to upgrade weak matches** "
                f"in the review below — your existing confirmed rows stay "
                f"intact.\n"
                f"Your alliance's permanent OCR setting is not changed; "
                f"adjust it under **Settings → Bear Tracking → OCR Languages** "
                f"if a different engine works better."
            ),
            view=picker,
            ephemeral=True,
        )

    async def _run_retry_ocr(self, interaction, new_primary_lang: str):
        """Re-OCR all sources with `new_primary_lang` and merge results
        additively. `auto`/`manual` rows untouched; weaker rows upgraded
        only if new score > existing AND >= MATCH_LIKELY_MIN."""
        attachments = []
        for msg in self.source_messages:
            try:
                refreshed = await msg.channel.fetch_message(msg.id)
            except Exception as e:
                logger.warning(f"Bear retry OCR: source message {msg.id} unavailable: {e}")
                continue
            for att in refreshed.attachments:
                if att.filename and any(att.filename.lower().endswith(ext)
                                        for ext in ('.png', '.jpg', '.jpeg', '.webp')):
                    attachments.append(att)
        if not attachments:
            await interaction.edit_original_response(
                content=(
                    f"{theme.deniedIcon} No screenshots could be re-fetched "
                    f"from the original messages (likely deleted). Upload "
                    f"them again to retry."
                ),
                view=None,
            )
            return

        _, fallbacks = self.cog.get_ocr_language_settings(self.alliance_id)
        new_rows_by_damage: dict[int, dict] = {}
        new_trap = ""
        new_rallies = ""
        new_total = 0
        ok_count = 0
        for att in attachments:
            try:
                image_bytes = await att.read()
            except Exception as e:
                logger.warning(f"Bear retry OCR: read failed on {att.filename}: {e}")
                continue
            result = await self.cog._ocr_attachment_to_result(
                image_bytes,
                new_primary_lang,
                fallbacks,
                filename=att.filename,
                roster=self.roster,
                alliance_id=self.alliance_id,
            )
            if not result.ok:
                continue
            ok_count += 1
            if result.trap and not new_trap:
                new_trap = result.trap
            if result.rallies and not new_rallies:
                new_rallies = result.rallies
            if result.total_damage > new_total:
                new_total = result.total_damage
            for dmg, row in result.rows.items():
                existing = new_rows_by_damage.get(dmg)
                if existing is None or _better_row(existing, row, roster=self.roster):
                    new_rows_by_damage[dmg] = row

        if ok_count == 0:
            await interaction.edit_original_response(
                content=(
                    f"{theme.deniedIcon} The `{new_primary_lang}` engine "
                    f"couldn't read any of the {len(attachments)} screenshot"
                    f"{'s' if len(attachments) != 1 else ''}. Existing "
                    f"review left unchanged. Try a different engine."
                ),
                view=None,
            )
            return

        # Strip the alliance-total damage so it doesn't merge as a row.
        new_rows_by_damage.pop(new_total, None)

        strong_statuses = {'auto', 'manual'}
        existing_by_damage = {r['damage']: r for r in self.rows}
        rows_added = 0
        rows_upgraded = 0
        for dmg, new_row in new_rows_by_damage.items():
            existing = existing_by_damage.get(dmg)
            if existing is None:
                enriched = self._enrich_row({**new_row, 'rank': None})
                self.rows.append(enriched)
                rows_added += 1
                continue
            if existing['status'] in strong_statuses:
                continue
            existing_score = name_match_score(existing.get('name') or '', self.roster)
            new_score = name_match_score(new_row.get('name') or '', self.roster)
            if new_score > existing_score and new_score >= MATCH_LIKELY_MIN:
                idx = self.rows.index(existing)
                self.rows[idx] = self._enrich_row({
                    **new_row,
                    'rank': existing.get('rank'),
                })
                rows_upgraded += 1

        # Hunt header is additive — preserve admin Edit Hunt Info edits.
        if not self.hunt_meta.get('hunting_trap') and new_trap and new_trap.isdigit():
            self.hunt_meta['hunting_trap'] = int(new_trap)
        if not self.hunt_meta.get('rallies') and new_rallies and new_rallies.isdigit():
            self.hunt_meta['rallies'] = int(new_rallies)
        if not self.hunt_meta.get('total_damage') and new_total:
            self.hunt_meta['total_damage'] = new_total

        for i, row in enumerate(sorted(self.rows, key=lambda r: -r['damage']), start=1):
            if row.get('rank') is None:
                row['rank'] = i

        self.page = 0
        self._resolve_unique_assignments()
        self._sort_rows()
        self._build_components()

        if self.message is not None:
            try:
                await self.message.edit(embed=self.build_embed(), view=self)
            except Exception as e:
                logger.warning(f"Bear retry OCR: could not refresh review message: {e}")

        if rows_added or rows_upgraded:
            parts = []
            if rows_added:
                parts.append(f"**{rows_added}** new row{'s' if rows_added != 1 else ''}")
            if rows_upgraded:
                parts.append(f"**{rows_upgraded}** match{'es' if rows_upgraded != 1 else ''} upgraded")
            summary = " · ".join(parts)
            await interaction.edit_original_response(
                content=(
                    f"{theme.verifiedIcon} Re-OCR with `{new_primary_lang}` "
                    f"complete: {summary}. Existing confirmed rows left "
                    f"untouched."
                ),
                view=None,
            )
        else:
            await interaction.edit_original_response(
                content=(
                    f"{theme.warnIcon} Re-OCR with `{new_primary_lang}` "
                    f"didn't add or improve any rows. Try a different engine "
                    f"or edit unmatched rows manually."
                ),
                view=None,
            )

    async def _on_prev(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if self.page > 0:
            self.page -= 1
        await self.refresh(interaction)

    async def _on_next(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if self.page < self._total_pages() - 1:
            self.page += 1
        await self.refresh(interaction)


class EditHeaderModal(discord.ui.Modal):
    """Edit the hunt-level header fields (date / trap / rallies / total)."""

    def __init__(self, review_view: BearHuntReviewView):
        super().__init__(title="Edit Bear Hunt Info")
        self.review_view = review_view
        meta = review_view.hunt_meta
        self.date_input = discord.ui.TextInput(
            label="Date (YYYY-MM-DD)", default=meta['date'] or "", max_length=10,
        )
        self.trap_input = discord.ui.TextInput(
            label="Hunting Trap (1 or 2)",
            default=str(meta['hunting_trap']) if meta['hunting_trap'] else "",
            max_length=2,
        )
        self.rallies_input = discord.ui.TextInput(
            label="Rallies",
            default=str(meta['rallies']) if meta['rallies'] else "",
            required=False, max_length=4,
        )
        self.total_input = discord.ui.TextInput(
            label="Total Damage",
            default=format_damage_for_embed(meta['total_damage']) if meta['total_damage'] else "",
            max_length=30,
        )
        for item in (self.date_input, self.trap_input, self.rallies_input, self.total_input):
            self.add_item(item)

    async def on_submit(self, interaction):
        try:
            dt = datetime.strptime(self.date_input.value.strip(), "%Y-%m-%d")
            date_norm = dt.strftime("%Y-%m-%d")
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Date must be YYYY-MM-DD.", ephemeral=True,
            )
            return
        try:
            trap = int(self.trap_input.value)
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Hunting Trap must be a whole number.", ephemeral=True,
            )
            return
        rallies = None
        if self.rallies_input.value.strip():
            try:
                rallies = int(self.rallies_input.value)
            except Exception:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Rallies must be a whole number.", ephemeral=True,
                )
                return
        self.review_view.hunt_meta = {
            'date': date_norm,
            'hunting_trap': trap,
            'rallies': rallies,
            'total_damage': bear_damage(self.total_input.value),
        }
        await self.review_view.refresh(interaction)


class EditRowModal(discord.ui.Modal):
    """Edit a single player row. Blank player field deletes the row."""

    def __init__(self, review_view: BearHuntReviewView, row_idx: int):
        row = review_view.rows[row_idx]
        title = f"Edit Row {row_idx + 1}"
        if row.get('fid'):
            title += f" · ID {row['fid']}"
        super().__init__(title=title[:45])
        self.review_view = review_view
        self.row_idx = row_idx
        current_player = row['nickname'] or row['name'] or ''
        self.player_input = discord.ui.TextInput(
            label="Player (ID or name — blank to delete)",
            default=current_player, required=False, max_length=80,
        )
        self.damage_input = discord.ui.TextInput(
            label="Damage", default=format_damage_for_embed(row['damage']),
            required=True, max_length=30,
        )
        self.rank_input = discord.ui.TextInput(
            label="Rank (optional)",
            default=str(row['rank']) if row['rank'] is not None else "",
            required=False, max_length=3,
        )
        self.add_item(self.player_input)
        self.add_item(self.damage_input)
        self.add_item(self.rank_input)

    async def on_submit(self, interaction):
        if self.row_idx >= len(self.review_view.rows):
            await interaction.response.send_message(
                f"{theme.deniedIcon} Row no longer exists.", ephemeral=True,
            )
            return
        # Edit-only behaviour: blank player field deletes the row.
        if not self.player_input.value.strip():
            del self.review_view.rows[self.row_idx]
            await self.review_view.refresh(interaction)
            return
        parsed, err = _parse_row_inputs(
            self.player_input.value, self.damage_input.value,
            self.rank_input.value, self.review_view.roster,
        )
        if err:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {err}", ephemeral=True,
            )
            return
        self.review_view.rows[self.row_idx].update(parsed)
        await self.review_view.refresh(interaction)


class AddRowModal(discord.ui.Modal):
    """Add a new player row manually (e.g. for rows OCR missed entirely)."""

    def __init__(self, review_view: BearHuntReviewView):
        super().__init__(title="Add Player Row")
        self.review_view = review_view
        self.player_input = discord.ui.TextInput(
            label="Player (ID or name)", required=True, max_length=80,
        )
        self.damage_input = discord.ui.TextInput(
            label="Damage", required=True, max_length=30,
        )
        self.rank_input = discord.ui.TextInput(
            label="Rank (optional)", required=False, max_length=3,
        )
        self.add_item(self.player_input)
        self.add_item(self.damage_input)
        self.add_item(self.rank_input)

    async def on_submit(self, interaction):
        parsed, err = _parse_row_inputs(
            self.player_input.value, self.damage_input.value,
            self.rank_input.value, self.review_view.roster,
        )
        if err:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {err}", ephemeral=True,
            )
            return
        self.review_view.rows.append(parsed)
        await self.review_view.refresh(interaction)


def _resolve_player(text, roster):
    """Resolve `text` (FID-digits or a name) to (fid, nickname, candidates)
    against the roster. Returns (None, None, []) if no match.
    """
    if text.isdigit():
        fid = int(text)
        for f, nick in roster:
            if f == fid:
                return fid, nick, [(fid, nick, 100)]
        return None, None, []
    matches = match_roster(text, roster)
    if not matches:
        return None, None, []
    fid, nick, _ = matches[0]
    return fid, nick, matches


def _parse_row_inputs(player_text, damage_text, rank_text, roster):
    """Returns (row_dict, error_message); exactly one is non-None."""
    text = (player_text or '').strip()
    if not text:
        return None, "Player is required."
    fid, nick, candidates = _resolve_player(text, roster)
    if fid is None:
        return None, f"No roster match for `{text}`. Try an ID or a closer spelling."
    try:
        damage = bear_damage(damage_text)
        if damage <= 0:
            raise ValueError()
    except Exception:
        return None, "Invalid damage value."
    rank = None
    rank_clean = (rank_text or '').strip()
    if rank_clean:
        try:
            rank = int(rank_clean)
        except ValueError:
            return None, "Rank must be a whole number."
    return {
        'fid': fid, 'nickname': nick, 'name': nick,
        'damage': damage, 'rank': rank,
        'candidates': candidates, 'status': 'manual',
    }, None


# ---------------------------------------------------------------------------
# BearMenuView — main navigation
# ---------------------------------------------------------------------------

class BearMenuView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id

    @discord.ui.button(label="View Bear Damage", style=discord.ButtonStyle.primary, emoji=theme.chartIcon, row=1)
    async def view_bear_damage(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        view = BearDamageView(
            data_submit=self.cog.data_submit,
            cog=self.cog,
            original_user_id=self.original_user_id,
        )

        embed = discord.Embed(
            title=f"{theme.chartIcon} Bear Damage Viewer",
            description=(
                f"Pick an alliance to load its damage chart.\n"
                f"{theme.upperDivider}\n"
                f"Defaults to **Trap 1** for the **last 3 months**. Use the "
                f"buttons below to switch trap or date range. **Edit Date "
                f"Range** opens a custom picker.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Bear Channel Setup", style=discord.ButtonStyle.success, emoji=theme.editListIcon, row=1)
    async def bear_channel_setup(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} You need admin permissions to set the bear channel.",
                ephemeral=True
            )
            return
        view = BearChannelSetupView(cog=self.cog, original_user_id=self.original_user_id)
        await safe_edit_message(
            interaction, embed=view._build_embed(), view=view, content=None,
        )

    @discord.ui.button(label="Edit Bear Damage", style=discord.ButtonStyle.secondary, emoji=theme.documentIcon, row=2)
    async def edit_bear_damage(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} You need admin permissions to edit bear damage records.",
                ephemeral=True
            )
            return

        view = BearDamageEditView(
            cog=self.cog,
            original_user_id=self.original_user_id,
        )

        embed = discord.Embed(
            title=f"{theme.documentIcon} Edit Bear Damage",
            description=(
                f"Select an alliance to view and manage its damage records.\n"
                f"{theme.upperDivider}\n"
                f"Pick an alliance from the dropdown, then select a record to edit or delete.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.secondary, emoji=theme.settingsIcon, row=2)
    async def settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} You need admin permissions to access bear settings.",
                ephemeral=True
            )
            return

        view = BearSettingsView(cog=self.cog, original_user_id=self.original_user_id)
        embed = view._build_embed()
        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Main Menu", style=discord.ButtonStyle.secondary, emoji=theme.homeIcon, row=2)
    async def main_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        main_menu_cog = self.cog.bot.get_cog("MainMenu")
        if main_menu_cog:
            await main_menu_cog.show_main_menu(interaction)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Main menu not available.", ephemeral=True
            )


# ---------------------------------------------------------------------------
# AllianceSelect — reusable alliance dropdown
# ---------------------------------------------------------------------------

class AllianceSelect(discord.ui.Select):
    def __init__(self, parent_view, options: list[discord.SelectOption], action: str):
        self.parent_view = parent_view
        self.action = action
        for opt in options:
            opt.default = (parent_view.alliance_id is not None and int(opt.value) == parent_view.alliance_id)

        super().__init__(
            placeholder="Select an alliance",
            min_values=1,
            max_values=1,
            options=options if options else [discord.SelectOption(label="No alliances", value="__none__")]
        )

    async def callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.parent_view.original_user_id):
            return
        try:
            new_alliance_id = int(self.values[0])

            allowed = await self.parent_view.cog.check_bear_permission(
                interaction, new_alliance_id, self.action
            )
            if not allowed:
                return

            self.parent_view.alliance_id = new_alliance_id

            for opt in self.options:
                opt.default = (int(opt.value) == self.parent_view.alliance_id)

            if hasattr(self.parent_view, "on_alliance_selected"):
                await self.parent_view.on_alliance_selected(interaction)
            elif hasattr(self.parent_view, "try_redraw"):
                await self.parent_view.try_redraw(interaction)

        except Exception as e:
            logger.error(f"Error in AllianceSelect callback: {e}")
            print(f"[ERROR] Error in AllianceSelect callback: {e}")
            try:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Error processing alliance selection.", ephemeral=True
                )
            except Exception:
                pass


def build_alliance_options(alliance_conn) -> list[discord.SelectOption]:
    """Build alliance select options from DB."""
    cursor = alliance_conn.cursor()
    cursor.execute("SELECT alliance_id, name FROM alliance_list ORDER BY name ASC LIMIT 25")
    rows = cursor.fetchall()
    return [discord.SelectOption(label=name, value=str(aid)) for aid, name in rows]


# ---------------------------------------------------------------------------
# BearDamageView — view damage graph with filters
# ---------------------------------------------------------------------------

class BearDamageView(discord.ui.View):
    PRESET_LABELS = {
        'this_month': 'This Month',
        'last_month': 'Last Month',
        '3m':         '3 Months',
        '1y':         '1 Year',
        'all':        'All Time',
    }

    def __init__(self, data_submit, *, cog, original_user_id,
                 alliance_id: int | None = None, hunting_trap: int | None = None,
                 from_date: date | None = None, to_date: date | None = None):
        super().__init__(timeout=7200)
        self.data_submit = data_submit
        self.cog = cog
        self.original_user_id = original_user_id
        self.alliance_id = alliance_id
        self.hunting_trap = hunting_trap or 1
        self.from_date = from_date
        self.to_date = to_date
        # `preset = None` means custom range from the modal.
        if from_date is None and to_date is None:
            self.preset: str | None = '3m'
            self._apply_preset(self.preset)
        else:
            self.preset = None
        self._build_components()

    def _apply_preset(self, preset_name: str):
        today = datetime.now(timezone.utc).date()
        if preset_name == 'this_month':
            self.from_date = today.replace(day=1)
            self.to_date = today
        elif preset_name == 'last_month':
            first_of_this = today.replace(day=1)
            last_month_end = first_of_this - timedelta(days=1)
            self.from_date = last_month_end.replace(day=1)
            self.to_date = last_month_end
        elif preset_name == '3m':
            self.from_date = today - timedelta(days=90)
            self.to_date = today
        elif preset_name == '1y':
            self.from_date = today - timedelta(days=365)
            self.to_date = today
        elif preset_name == 'all':
            # process_view treats None as 'first/last record for this
            # alliance+trap'.
            self.from_date = None
            self.to_date = None
        self.preset = preset_name

    def _build_components(self):
        self.clear_items()
        options = build_alliance_options(self.cog.alliance_conn)
        for opt in options:
            opt.default = (int(opt.value) == (self.alliance_id or 0))
        self.add_item(AllianceSelect(self, options, action="view"))

        trap_buttons = [
            (1, f"Trap 1", theme.bearTrapIcon),
            (2, f"Trap 2", theme.bearTrapIcon),
            ('both', "Both", theme.chartIcon),
        ]
        for trap, label, emoji in trap_buttons:
            btn = discord.ui.Button(
                label=label,
                emoji=emoji,
                style=(discord.ButtonStyle.success if self.hunting_trap == trap
                       else discord.ButtonStyle.secondary),
                row=1,
            )
            btn.callback = self._make_trap_cb(trap)
            self.add_item(btn)

        # Row 2: rolling-window presets. Active preset styled .success.
        for preset in ('this_month', 'last_month', '3m', '1y'):
            btn = discord.ui.Button(
                label=self.PRESET_LABELS[preset],
                style=(discord.ButtonStyle.success if self.preset == preset
                       else discord.ButtonStyle.secondary),
                row=2,
            )
            btn.callback = self._make_preset_cb(preset)
            self.add_item(btn)

        all_time_btn = discord.ui.Button(
            label=self.PRESET_LABELS['all'],
            style=(discord.ButtonStyle.success if self.preset == 'all'
                   else discord.ButtonStyle.secondary),
            row=3,
        )
        all_time_btn.callback = self._make_preset_cb('all')
        self.add_item(all_time_btn)

        edit_btn = discord.ui.Button(
            label="Edit Date Range",
            emoji=theme.editListIcon,
            style=discord.ButtonStyle.secondary,
            row=3,
        )
        edit_btn.callback = self._on_edit_range
        self.add_item(edit_btn)

        back_btn = discord.ui.Button(
            label="Back",
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            row=3,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    def _make_trap_cb(self, trap: int):
        async def _cb(interaction: discord.Interaction):
            if not await check_interaction_user(interaction, self.original_user_id):
                return
            if self.hunting_trap == trap:
                await interaction.response.defer()
                return
            self.hunting_trap = trap
            await self.try_redraw(interaction)
        return _cb

    def _make_preset_cb(self, preset: str):
        async def _cb(interaction: discord.Interaction):
            if not await check_interaction_user(interaction, self.original_user_id):
                return
            if self.preset == preset:
                await interaction.response.defer()
                return
            self._apply_preset(preset)
            await self.try_redraw(interaction)
        return _cb

    async def _on_edit_range(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await interaction.response.send_modal(DateRangeModal(self))

    async def _on_back(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)

    async def try_redraw(self, interaction: discord.Interaction):
        self._build_components()
        if not self.alliance_id:
            placeholder = discord.Embed(
                title=f"{theme.chartIcon} Bear Damage Viewer",
                description=(
                    f"Pick an alliance to load its damage chart.\n"
                    f"{theme.upperDivider}\n"
                    f"Trap and date range can be set first; the chart "
                    f"renders as soon as you pick an alliance.\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1,
            )
            await interaction.response.edit_message(
                embed=placeholder, attachments=[], view=self,
            )
            return
        from_str = self.from_date.strftime("%Y-%m-%d") if self.from_date else None
        to_str = self.to_date.strftime("%Y-%m-%d") if self.to_date else None
        embed, file = await self.data_submit.process_view(
            alliance_id=self.alliance_id,
            hunting_trap=self.hunting_trap,
            from_date=from_str,
            to_date=to_str,
        )
        if not embed:
            trap_label = "Both traps" if self.hunting_trap == 'both' else f"Trap {self.hunting_trap}"
            empty = discord.Embed(
                title=f"{theme.warnIcon} No data for this range",
                description=(
                    f"{trap_label} has no recorded hunts in the selected "
                    f"range. Try a different preset or **Edit Date Range**."
                ),
                color=theme.emColor2,
            )
            await interaction.response.edit_message(
                embed=empty, attachments=[], view=self,
            )
            return
        await interaction.response.edit_message(
            embed=embed, attachments=[file] if file else [], view=self,
        )


# ---------------------------------------------------------------------------
# BearDamageEditView — edit/delete records
# ---------------------------------------------------------------------------

class BearDamageEditView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id

        self.alliance_id: int | None = None
        self.selected_record_id: int | None = None
        self.date: str | None = None
        self.hunting_trap: int | None = None
        self.rallies: int | None = None
        self.total_damage: int | None = None

        options = build_alliance_options(cog.alliance_conn)
        self.add_item(AllianceSelect(self, options, action="manage"))

        self.date_trap_select = discord.ui.Select(
            placeholder="Select a record",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label="Select an alliance first", value="__placeholder__")],
            disabled=True
        )
        self.date_trap_select.callback = self.date_trap_selected
        self.add_item(self.date_trap_select)

    def _player_counts(self) -> tuple[int, int]:
        """Return (matched, unmatched) row counts for the selected record."""
        if not self.selected_record_id:
            return 0, 0
        try:
            row = self.cog.bear_cursor.execute(
                "SELECT "
                "SUM(CASE WHEN fid IS NOT NULL THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN fid IS NULL THEN 1 ELSE 0 END) "
                "FROM bear_player_damage WHERE hunt_id = ?",
                (self.selected_record_id,)
            ).fetchone()
        except Exception:
            return 0, 0
        return (row[0] or 0, row[1] or 0)

    def build_record_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{theme.editListIcon} Bear Damage Record",
            color=theme.emColor1
        )
        embed.add_field(name="Date", value=self.date or "-", inline=False)
        embed.add_field(name="Hunting Trap", value=self.hunting_trap or "-", inline=True)
        embed.add_field(name="Rallies", value=self.rallies or "-", inline=True)
        embed.add_field(
            name="Total Alliance Damage",
            value=format_damage_for_embed(self.total_damage) if self.total_damage else "-",
            inline=False
        )
        if self.selected_record_id:
            matched, unmatched = self._player_counts()
            value = f"{matched} matched"
            if unmatched:
                value += f" · {unmatched} unmatched"
            embed.add_field(name="Players", value=value, inline=False)
        return embed

    async def date_trap_selected(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        selected_value = self.date_trap_select.values[0]
        if selected_value in ("__placeholder__", "__none__"):
            return

        self.selected_record_id = int(selected_value)

        try:
            row = self.cog.bear_cursor.execute(
                "SELECT date, hunting_trap, rallies, total_damage FROM bear_hunts WHERE id = ?",
                (self.selected_record_id,)
            ).fetchone()
        except Exception as e:
            logger.error(f"Failed to fetch record: {e}")
            print(f"[ERROR] Failed to fetch record: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to fetch record.", ephemeral=True
            )
            return

        if not row:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Record not found.", ephemeral=True
            )
            return

        self.date, self.hunting_trap, self.rallies, self.total_damage = row

        # Update defaults on the select
        self._refresh_record_select_defaults()

        await interaction.response.edit_message(content=None, view=self, embed=self.build_record_embed())

    def _refresh_record_select_defaults(self):
        for opt in self.date_trap_select.options:
            if opt.value in ("__placeholder__", "__none__"):
                continue
            opt.default = (int(opt.value) == self.selected_record_id) if self.selected_record_id else False

    async def on_alliance_selected(self, interaction: discord.Interaction):
        """Called by AllianceSelect when an alliance is picked."""
        try:
            rows = self.cog.bear_cursor.execute(
                "SELECT id, hunting_trap, date FROM bear_hunts WHERE alliance_id = ? ORDER BY date DESC LIMIT 25",
                (self.alliance_id,)
            ).fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch records: {e}")
            print(f"[ERROR] Failed to fetch records: {e}")
            rows = []

        if not rows:
            self.date_trap_select.options = [discord.SelectOption(label="No records found", value="__none__")]
            self.date_trap_select.disabled = True
        else:
            self.date_trap_select.options = [
                discord.SelectOption(
                    label=f"{dt} - Trap {trap}",
                    value=str(row_id),
                    default=(self.selected_record_id == row_id) if self.selected_record_id else False
                )
                for row_id, trap, dt in rows
            ]
            self.date_trap_select.disabled = False

        self.selected_record_id = None
        self.date = self.hunting_trap = self.rallies = self.total_damage = None

        await interaction.response.edit_message(content=None, view=self, embed=self.build_record_embed())

    @discord.ui.button(label="Filter Records", style=discord.ButtonStyle.primary, emoji=theme.searchIcon, row=2)
    async def filter_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.alliance_id:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select an alliance first.", ephemeral=True
            )
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        await interaction.response.send_modal(RecordFilterModal(self))

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary, emoji=theme.editListIcon, row=2)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.alliance_id or not self.selected_record_id:
            await interaction.response.send_message(
                f"{theme.warnIcon} Select an alliance and record first.", ephemeral=True
            )
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        await interaction.response.send_modal(RecordEditModal(self))

    @discord.ui.button(label="Fix Unmatched", style=discord.ButtonStyle.primary, emoji=theme.warnIcon, row=2)
    async def fix_unmatched_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.alliance_id or not self.selected_record_id:
            await interaction.response.send_message(
                f"{theme.warnIcon} Select an alliance and record first.", ephemeral=True
            )
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        _, unmatched = self._player_counts()
        if not unmatched:
            await interaction.response.send_message(
                f"{theme.verifiedIcon} This record has no unmatched rows.", ephemeral=True
            )
            return
        view = FixUnmatchedView(
            cog=self.cog,
            alliance_id=self.alliance_id,
            hunt_id=self.selected_record_id,
            parent_view=self,
            original_user_id=self.original_user_id,
        )
        await interaction.response.edit_message(content=None, embed=view.build_embed(), view=view)

    @discord.ui.button(label="Delete", style=discord.ButtonStyle.danger, emoji=theme.trashIcon, row=2)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.alliance_id or not self.selected_record_id:
            await interaction.response.send_message(
                f"{theme.warnIcon} Select an alliance and record first.", ephemeral=True
            )
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        # Delete the record
        try:
            self.cog.bear_cursor.execute(
                "DELETE FROM bear_hunts WHERE id = ?", (self.selected_record_id,)
            )
            self.cog.bear_conn.commit()
        except Exception as e:
            logger.error(f"Failed to delete record: {e}")
            print(f"[ERROR] Failed to delete record: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to delete record.", ephemeral=True
            )
            return

        self.selected_record_id = None
        self.date = self.hunting_trap = self.rallies = self.total_damage = None

        # Refresh record list
        await self.on_alliance_selected(interaction)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=2)
    async def back_to_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)


class RecordFilterModal(discord.ui.Modal):
    """Filter damage records by trap, month, year."""

    def __init__(self, parent_view: BearDamageEditView):
        super().__init__(title="Filter Damage Records")
        self.parent_view = parent_view

        self.trap_input = discord.ui.TextInput(
            label="Trap Number (1 or 2)", required=False,
            placeholder="Leave empty for no trap filter"
        )
        self.month_input = discord.ui.TextInput(
            label="Month (1-12)", required=False,
            placeholder="Leave empty for no month filter"
        )
        self.year_input = discord.ui.TextInput(
            label="Year (YYYY)", required=False,
            placeholder="Leave empty for no year filter"
        )
        self.add_item(self.trap_input)
        self.add_item(self.month_input)
        self.add_item(self.year_input)

    async def on_submit(self, interaction: discord.Interaction):
        trap_val = self.trap_input.value.strip()
        month_val = self.month_input.value.strip()
        year_val = self.year_input.value.strip()

        filters = []
        params = [self.parent_view.alliance_id]

        if trap_val:
            filters.append("hunting_trap = ?")
            params.append(trap_val)
        if month_val:
            filters.append("strftime('%m', date) = ?")
            params.append(month_val.zfill(2))
        if year_val:
            filters.append("strftime('%Y', date) = ?")
            params.append(year_val)

        where_extra = (" AND " + " AND ".join(filters)) if filters else ""

        try:
            rows = self.parent_view.cog.bear_cursor.execute(
                f"SELECT id, hunting_trap, date FROM bear_hunts "
                f"WHERE alliance_id = ?{where_extra} ORDER BY date DESC LIMIT 25",
                params
            ).fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch filtered records: {e}")
            print(f"[ERROR] Failed to fetch filtered records: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to fetch filtered records.", ephemeral=True
            )
            return

        if not rows:
            self.parent_view.date_trap_select.options = [
                discord.SelectOption(label="No records found", value="__none__")
            ]
            self.parent_view.date_trap_select.disabled = True
        else:
            self.parent_view.date_trap_select.options = [
                discord.SelectOption(label=f"{dt} - Trap {trap}", value=str(row_id))
                for row_id, trap, dt in rows
            ]
            self.parent_view.date_trap_select.disabled = False

        self.parent_view.selected_record_id = None
        self.parent_view.date = self.parent_view.hunting_trap = None
        self.parent_view.rallies = self.parent_view.total_damage = None

        await interaction.response.edit_message(
            view=self.parent_view, embed=self.parent_view.build_record_embed()
        )


class RecordEditModal(discord.ui.Modal):
    """Modal for editing an existing bear damage record in the database."""

    def __init__(self, parent_view: BearDamageEditView):
        super().__init__(title="Edit Bear Record")
        self.parent_view = parent_view

        self.date_input = discord.ui.TextInput(
            label="Date", default=parent_view.date or ""
        )
        self.hunting_trap_input = discord.ui.TextInput(
            label="Hunting Trap", default=str(parent_view.hunting_trap or "")
        )
        self.rallies_input = discord.ui.TextInput(
            label="Rallies", default=str(parent_view.rallies or "")
        )
        self.total_damage_input = discord.ui.TextInput(
            label="Total Damage",
            default=format_damage_for_embed(parent_view.total_damage) if parent_view.total_damage else ""
        )
        self.add_item(self.date_input)
        self.add_item(self.hunting_trap_input)
        self.add_item(self.rallies_input)
        self.add_item(self.total_damage_input)

    async def on_submit(self, interaction: discord.Interaction):
        # Validate
        try:
            dt = datetime.strptime(self.date_input.value, "%Y-%m-%d")
            new_date = dt.strftime("%Y-%m-%d")
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Date must be in YYYY-MM-DD format.", ephemeral=True
            )
            return

        try:
            new_trap = int(self.hunting_trap_input.value)
            if new_trap not in (1, 2):
                raise ValueError
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Hunting Trap must be 1 or 2.", ephemeral=True
            )
            return

        try:
            new_rallies = int(self.rallies_input.value)
            if new_rallies <= 0:
                raise ValueError
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Rallies must be a whole number greater than 0.", ephemeral=True
            )
            return

        new_damage = bear_damage(self.total_damage_input.value)
        if new_damage <= 0:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Total Damage must be greater than 0.", ephemeral=True
            )
            return

        # Save to DB
        try:
            self.parent_view.cog.bear_cursor.execute(
                "UPDATE bear_hunts SET date = ?, hunting_trap = ?, rallies = ?, total_damage = ? WHERE id = ?",
                (new_date, new_trap, new_rallies, new_damage, self.parent_view.selected_record_id)
            )
            self.parent_view.cog.bear_conn.commit()
        except Exception as e:
            logger.error(f"Failed to update record: {e}")
            print(f"[ERROR] Failed to update record: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save record.", ephemeral=True
            )
            return

        self.parent_view.date = new_date
        self.parent_view.hunting_trap = new_trap
        self.parent_view.rallies = new_rallies
        self.parent_view.total_damage = new_damage

        embed = self.parent_view.build_record_embed()
        embed.description = f"{theme.verifiedIcon} Record updated successfully."

        await interaction.response.edit_message(embed=embed, view=self.parent_view)


# ---------------------------------------------------------------------------
# FixUnmatchedView — resolve NULL-fid rows of a saved hunt
# ---------------------------------------------------------------------------

class FixUnmatchedView(discord.ui.View):
    """Paginated picker for unmatched rows of a saved hunt."""

    PAGE_SIZE = 25

    def __init__(self, *, cog, alliance_id: int, hunt_id: int,
                 parent_view: 'BearDamageEditView', original_user_id: int):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id
        self.hunt_id = hunt_id
        self.parent_view = parent_view
        self.original_user_id = original_user_id
        self.roster = cog.get_alliance_roster(alliance_id)
        self.page = 0
        self.rows: list[dict] = []
        # One-shot rematch summary, cleared by build_embed.
        self._rematch_result: tuple[int, int] | None = None
        self._load_rows()
        self._build_components()

    def _load_rows(self):
        cur = self.cog.bear_cursor
        cur.execute(
            "SELECT id, raw_name, damage, rank FROM bear_player_damage "
            "WHERE hunt_id = ? AND fid IS NULL ORDER BY damage DESC",
            (self.hunt_id,),
        )
        self.rows = [
            {'id': r[0], 'raw_name': r[1] or '', 'damage': int(r[2]), 'rank': r[3]}
            for r in cur.fetchall()
        ]
        max_page = max(0, (len(self.rows) - 1) // self.PAGE_SIZE)
        self.page = min(self.page, max_page)

    def _total_pages(self) -> int:
        return max(1, -(-len(self.rows) // self.PAGE_SIZE))

    def build_embed(self) -> discord.Embed:
        description = (
            "Pick an unmatched row to assign it to a roster member, or "
            "leave the player field blank in the modal to delete it. "
            "**Re-match against roster** retries the auto-matcher against "
            "the current roster — useful after adding new alliance members."
        )
        if self._rematch_result is not None:
            resolved, remaining = self._rematch_result
            self._rematch_result = None
            if resolved:
                banner = (
                    f"{theme.verifiedIcon} Re-matched **{resolved}** row"
                    f"{'s' if resolved != 1 else ''} against the roster. "
                    f"**{remaining}** still unmatched."
                )
            else:
                banner = (
                    f"{theme.warnIcon} No additional rows could be matched "
                    f"against the current roster (need a confident ≥90% "
                    f"match). **{remaining}** still unmatched — pick rows "
                    f"individually below."
                )
            description = banner + "\n\n" + description
        embed = discord.Embed(
            title=f"{theme.warnIcon} Fix Unmatched Rows",
            description=description,
            color=theme.emColor1,
        )
        if not self.rows:
            embed.description += f"\n\n{theme.verifiedIcon} All rows are matched."
            return embed

        start = self.page * self.PAGE_SIZE
        end = min(start + self.PAGE_SIZE, len(self.rows))
        lines = []
        for r in self.rows[start:end]:
            rank_str = f"**#{r['rank']}**" if r['rank'] is not None else "**?**"
            raw = r['raw_name'] or "(unreadable)"
            lines.append(_ltr_line(
                f"{rank_str} `{raw}` — `{format_damage_for_embed(r['damage'])}`"
            ))
        total_pages = self._total_pages()
        header = (
            f"Unmatched {start + 1}-{end} of {len(self.rows)}"
            if total_pages > 1 else f"Unmatched ({len(self.rows)})"
        )
        value = "\n".join(lines)
        if len(value) > 1024:
            value = value[:1010] + "\n…(truncated)"
        embed.add_field(name=header, value=value, inline=False)
        return embed

    def _build_components(self):
        self.clear_items()
        if self.rows:
            start = self.page * self.PAGE_SIZE
            end = min(start + self.PAGE_SIZE, len(self.rows))
            options = []
            for i, r in enumerate(self.rows[start:end], start=start):
                rank_part = f"#{r['rank']}" if r['rank'] is not None else "?"
                raw = r['raw_name'] or "(unreadable)"
                label = _ltr_line(f"{rank_part} {raw}")[:100]
                desc = format_damage_for_embed(r['damage'])[:100]
                options.append(discord.SelectOption(label=label, value=str(i), description=desc))
            select = discord.ui.Select(
                placeholder="Pick a row to fix…",
                options=options,
                row=0,
            )
            select.callback = self._on_row_selected
            self.add_item(select)

        rematch_btn = discord.ui.Button(
            label="Re-match against roster",
            emoji=theme.refreshIcon,
            style=discord.ButtonStyle.primary, row=1,
        )
        rematch_btn.callback = self._on_rematch
        self.add_item(rematch_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

        total_pages = self._total_pages()
        if total_pages > 1:
            prev_btn = discord.ui.Button(
                label="Prev", emoji=theme.prevIcon,
                style=discord.ButtonStyle.secondary, row=2,
                disabled=(self.page == 0),
            )
            prev_btn.callback = self._on_prev
            next_btn = discord.ui.Button(
                label="Next", emoji=theme.nextIcon,
                style=discord.ButtonStyle.secondary, row=2,
                disabled=(self.page >= total_pages - 1),
            )
            next_btn.callback = self._on_next
            self.add_item(prev_btn)
            self.add_item(next_btn)

    async def _on_row_selected(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        idx = int(interaction.data['values'][0])
        if idx >= len(self.rows):
            await interaction.response.send_message(
                f"{theme.deniedIcon} That row no longer exists.", ephemeral=True
            )
            return
        await interaction.response.send_modal(FixUnmatchedModal(self, self.rows[idx]))

    async def _on_rematch(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        resolved = self._do_rematch()
        # Snapshot remaining BEFORE refresh reloads `self.rows`.
        self._rematch_result = (resolved, max(0, len(self.rows) - resolved))
        await self.refresh(interaction)

    def _do_rematch(self) -> int:
        """Re-run roster match on unmatched rows; persist auto-confirmed
        only. Returns the count resolved."""
        self.roster = self.cog.get_alliance_roster(self.alliance_id)

        cur = self.cog.bear_cursor
        cur.execute(
            "SELECT fid FROM bear_player_damage "
            "WHERE hunt_id = ? AND fid IS NOT NULL",
            (self.hunt_id,),
        )
        assigned_fids: set = {row[0] for row in cur.fetchall()}

        candidates = []
        for row_idx, row in enumerate(self.rows):
            for fid, nick, score in match_roster(row['raw_name'], self.roster):
                candidates.append((score, row_idx, fid, nick))
        candidates.sort(key=lambda c: (-c[0], c[1]))

        row_assignments: dict[int, tuple[int, str, int]] = {}
        for score, row_idx, fid, nick in candidates:
            if score < MATCH_AUTO_CONFIRM:
                continue
            if row_idx in row_assignments or fid in assigned_fids:
                continue
            row_assignments[row_idx] = (fid, nick, score)
            assigned_fids.add(fid)

        if not row_assignments:
            return 0

        for row_idx, (fid, nick, score) in row_assignments.items():
            cur.execute(
                "UPDATE bear_player_damage SET fid = ?, "
                "resolved_nickname = ?, match_score = ? WHERE id = ?",
                (fid, nick, score, self.rows[row_idx]['id']),
            )
        self.cog.bear_conn.commit()
        return len(row_assignments)

    async def _on_prev(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if self.page > 0:
            self.page -= 1
        await self.refresh(interaction)

    async def _on_next(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if self.page < self._total_pages() - 1:
            self.page += 1
        await self.refresh(interaction)

    async def _on_back(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await safe_edit_message(
            interaction,
            embed=self.parent_view.build_record_embed(),
            view=self.parent_view,
            content=None,
        )

    async def refresh(self, interaction: discord.Interaction):
        self._load_rows()
        self._build_components()
        await safe_edit_message(
            interaction, embed=self.build_embed(), view=self, content=None,
        )


class FixUnmatchedModal(discord.ui.Modal):
    """Resolve a single unmatched bear_player_damage row to a roster member,
    or delete it when the player field is left blank."""

    def __init__(self, parent_view: FixUnmatchedView, row: dict):
        title = f"Fix Row · {format_damage_for_embed(row['damage'])}"
        super().__init__(title=title[:45])
        self.parent_view = parent_view
        self.row = row
        self.player_input = discord.ui.TextInput(
            label="Player (ID or name — blank to delete)",
            default=row['raw_name'] or '',
            required=False,
            max_length=80,
        )
        self.add_item(self.player_input)

    async def on_submit(self, interaction: discord.Interaction):
        cur = self.parent_view.cog.bear_cursor
        text = self.player_input.value.strip()
        if not text:
            try:
                cur.execute(
                    "DELETE FROM bear_player_damage WHERE id = ?", (self.row['id'],)
                )
                self.parent_view.cog.bear_conn.commit()
            except Exception as e:
                logger.error(f"Failed to delete unmatched row {self.row['id']}: {e}")
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Failed to delete row.", ephemeral=True,
                )
                return
            await self.parent_view.refresh(interaction)
            return

        fid, nick, candidates = _resolve_player(text, self.parent_view.roster)
        if fid is None:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No roster match for `{text}`. "
                f"Try an ID or a closer spelling.",
                ephemeral=True,
            )
            return

        existing = cur.execute(
            "SELECT id FROM bear_player_damage WHERE hunt_id = ? AND fid = ? AND id != ?",
            (self.parent_view.hunt_id, fid, self.row['id']),
        ).fetchone()
        if existing:
            await interaction.response.send_message(
                f"{theme.deniedIcon} `{nick}` (ID {fid}) is already in this hunt. "
                f"Delete one of the two rows instead.",
                ephemeral=True,
            )
            return

        score = candidates[0][2] if candidates else None
        try:
            cur.execute(
                "UPDATE bear_player_damage "
                "SET fid = ?, resolved_nickname = ?, match_score = ? WHERE id = ?",
                (fid, nick, score, self.row['id']),
            )
            self.parent_view.cog.bear_conn.commit()
        except Exception as e:
            logger.error(f"Failed to resolve unmatched row {self.row['id']}: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to update row.", ephemeral=True,
            )
            return
        await self.parent_view.refresh(interaction)


# ---------------------------------------------------------------------------
# BearSettingsView — settings management
# ---------------------------------------------------------------------------

class BearSettingsView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id
        self.alliance_id: int | None = None
        self._build_components()

    def _build_components(self):
        self.clear_items()
        options = build_alliance_options(self.cog.alliance_conn)
        self.add_item(AllianceSelect(self, options, action="manage"))

        has_alliance = self.alliance_id is not None

        timeout_btn = discord.ui.Button(label="Session Timeout", style=discord.ButtonStyle.primary, emoji=theme.hourglassIcon, row=2, disabled=not has_alliance)
        timeout_btn.callback = self._session_timeout_callback
        self.add_item(timeout_btn)

        auto_delete_btn = discord.ui.Button(label="Toggle Auto-Delete", style=discord.ButtonStyle.primary, emoji=theme.trashIcon, row=2, disabled=not has_alliance)
        auto_delete_btn.callback = self._toggle_auto_delete_callback
        self.add_item(auto_delete_btn)

        add_perm_btn = discord.ui.Button(label="Toggle Add Permission", style=discord.ButtonStyle.primary, emoji=theme.lockIcon, row=2, disabled=not has_alliance)
        add_perm_btn.callback = self._toggle_add_callback
        self.add_item(add_perm_btn)

        view_perm_btn = discord.ui.Button(label="Toggle View Permission", style=discord.ButtonStyle.primary, emoji=theme.eyeIcon, row=2, disabled=not has_alliance)
        view_perm_btn.callback = self._toggle_view_callback
        self.add_item(view_perm_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=3)
        back_btn.callback = self._back_callback
        self.add_item(back_btn)

    def _build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{theme.settingsIcon} Bear Settings",
            description=(
                f"Operational settings — session pacing, cleanup, and who "
                f"can interact with the system. \n\n"
                f"**Available Settings**\n"
                f"{theme.upperDivider}\n"
                f"{theme.hourglassIcon} **Session Timeout**\n"
                f"└ Minutes to wait for more screenshots before finalising the event"
                f"(1-60)\n\n"
                f"{theme.trashIcon} **Toggle Auto-Delete**\n"
                f"└ Delete uploaded screenshots after submit (needs **Manage Messages**)\n\n"
                f"{theme.lockIcon} **Toggle Permissions**\n"
                f"└ Who can add hunts and view saved data\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        if self.alliance_id:
            settings = self.cog.get_bear_settings(self.alliance_id)
            view_text = "Admins only" if settings["admin_only_view"] else "Everyone"
            add_text = "Admins only" if settings["admin_only_add"] else "Everyone"
            timeout_min = settings["session_timeout_min"]
            auto_delete_text = "On" if settings["auto_delete_screenshots"] else "Off"

            current_settings = (
                f"{theme.upperDivider}\n"
                f"**Session Timeout:** {timeout_min} min\n"
                f"**Auto-Delete Screenshots:** {auto_delete_text}\n"
                f"**Add Permission:** {add_text}\n"
                f"**View Permission:** {view_text}\n"
                f"{theme.lowerDivider}"
            )
            embed.add_field(name="Current Settings", value=current_settings, inline=False)

        return embed

    async def on_alliance_selected(self, interaction: discord.Interaction):
        """Called by AllianceSelect when an alliance is picked."""
        if not self.alliance_id:
            return
        self._build_components()
        embed = self._build_embed()
        await interaction.response.edit_message(content=None, view=self, embed=embed)

    async def _session_timeout_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        settings = self.cog.get_bear_settings(self.alliance_id)
        await interaction.response.send_modal(
            SessionTimeoutModal(self.cog, self.alliance_id, settings["session_timeout_min"], self)
        )

    async def _toggle_auto_delete_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        settings = self.cog.get_bear_settings(self.alliance_id)
        new_value = 0 if settings["auto_delete_screenshots"] else 1
        self.cog.update_bear_setting(self.alliance_id, "bear_auto_delete_screenshots", new_value)
        embed = self._build_embed()
        on_off = "On" if new_value else "Off"
        embed.description += f"\n{theme.verifiedIcon} Auto-delete is now **{on_off}**."
        await safe_edit_message(interaction, embed=embed, view=self, content=None)

    async def _toggle_add_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        await self._toggle_permission(interaction, "add")

    async def _toggle_view_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        await self._toggle_permission(interaction, "view")

    async def _toggle_permission(self, interaction: discord.Interaction, mode: str):
        settings = self.cog.get_bear_settings(self.alliance_id)
        key = f"admin_only_{mode}"
        current = settings.get(key, 0)
        new_value = 0 if current else 1

        column = f"bear_admin_only_{mode}"
        self.cog.update_bear_setting(self.alliance_id, column, new_value)

        embed = self._build_embed()
        embed.description += f"\n{theme.verifiedIcon} {mode.capitalize()} permission updated."
        await safe_edit_message(interaction, embed=embed, view=self, content=None)

    async def _back_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)


# ---------------------------------------------------------------------------
# OCR language configuration
# ---------------------------------------------------------------------------

class BearOcrLanguagesView(discord.ui.View):
    """Per-alliance OCR language picker — a single primary engine plus
    any number of fallback engines to retry with when a row's name came
    back unreadable from the primary pass. Settings persist as soon as
    the admin changes a dropdown.
    """

    def __init__(self, cog, alliance_id, original_user_id, parent_view):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id
        self.original_user_id = original_user_id
        self.parent = parent_view
        self._build()

    def _build(self):
        self.clear_items()
        primary, fallbacks = self.cog.get_ocr_language_settings(self.alliance_id)
        autoprune = self.cog.get_ocr_autoprune(self.alliance_id)

        def _cost_description(code: str) -> str:
            fp = _OCR_LANG_FOOTPRINT_MB.get(code)
            if not fp:
                return None
            return f"~{fp['ram']} MB RAM · ~{fp['disk']} MB disk"

        primary_opts = [
            discord.SelectOption(
                label=label, value=code,
                description=_cost_description(code),
                default=(code == primary),
            )
            for code, label in OCR_LANGUAGES
        ]
        primary_select = discord.ui.Select(
            placeholder="Primary OCR language",
            options=primary_opts, min_values=1, max_values=1, row=0,
        )
        primary_select.callback = self._on_primary_change
        self.add_item(primary_select)

        fb_opts = [
            discord.SelectOption(
                label=label, value=code,
                description=_cost_description(code),
                default=(code in fallbacks),
            )
            for code, label in OCR_LANGUAGES if code != primary
        ]
        if fb_opts:
            fb_select = discord.ui.Select(
                placeholder="Fallback languages (optional, pick any number)",
                options=fb_opts,
                min_values=0, max_values=len(fb_opts), row=1,
            )
            fb_select.callback = self._on_fallbacks_change
            self.add_item(fb_select)

        autoprune_btn = discord.ui.Button(
            label=f"Auto-Prune: {'On' if autoprune else 'Off'}",
            emoji=theme.cleanIcon,
            style=discord.ButtonStyle.success if autoprune else discord.ButtonStyle.secondary,
            row=2,
        )
        autoprune_btn.callback = self._on_autoprune_toggle
        self.add_item(autoprune_btn)

        back = discord.ui.Button(
            label="Back", style=discord.ButtonStyle.secondary,
            emoji=theme.backIcon, row=2,
        )
        back.callback = self._on_back
        self.add_item(back)

    def _build_embed(self):
        primary, fallbacks = self.cog.get_ocr_language_settings(self.alliance_id)
        primary_label = OCR_LANG_LABEL.get(primary, primary)
        fb_labels = [OCR_LANG_LABEL.get(c, c) for c in fallbacks]

        description = (
            f"Pick which OCR models read player names off screenshots. Each "
            f"model handles a specific script. Memory and disk cost per "
            f"language are shown in the dropdowns.\n\n"
            f"{theme.upperDivider}\n"
            f"**Primary:** {primary_label}\n"
            f"└ Runs first on every screenshot.\n\n"
            f"**Fallbacks:** {', '.join(fb_labels) if fb_labels else '*(none)*'}\n"
            f"└ Re-OCR rows the primary couldn't read (mixed-script alliances).\n\n"
            f"{theme.cleanIcon} **Auto-Prune**\n"
            f"└ Drops fallbacks from this alliance's list if they run "
            f"{AUTOPRUNE_MIN_RUNS}+ times without filling any rows. Engine "
            f"files stay on disk; other alliances are unaffected.\n"
            f"{theme.lowerDivider}\n"
        )
        description += self._build_effectiveness_section(primary, fallbacks)
        return discord.Embed(
            title=f"{theme.globeIcon} Character Recognition",
            description=description,
            color=theme.emColor1,
        )

    def _build_effectiveness_section(self, primary: str, fallbacks: list) -> str:
        """Per-language stats grouped into Primary / Fallback sections."""
        stats = get_ocr_lang_stats(self.alliance_id)
        if not stats:
            return (
                f"\n{theme.upperDivider}\n"
                f"**Effectiveness** — *no OCR runs recorded yet for this "
                f"alliance. Stats appear here after a few hunts.*\n"
                f"{theme.lowerDivider}"
            )

        configured = {primary, *fallbacks}
        primary_stats = sorted(
            (s for s in stats if s['role'] == 'primary'),
            key=lambda s: -s['runs'],
        )
        fallback_stats = sorted(
            (s for s in stats if s['role'] == 'fallback'),
            key=lambda s: -s['runs'],
        )

        def _fmt_row(s, is_fallback: bool) -> str:
            runs = s['runs']
            filled = s['rows_filled']
            in_use = s['lang'] in configured
            if is_fallback and in_use and runs >= 5 and filled == 0:
                marker = theme.warnIcon
            elif filled > 0:
                marker = theme.verifiedIcon
            else:
                marker = "  "  # two-space pad keeps columns aligned
            last_used = _format_last_used(s.get('last_run_at'))
            return (
                f"{marker} `{s['lang']:<11}` "
                f"runs `{runs:>3}` · filled `{filled:>3}` · "
                f"last used {last_used}"
            )

        lines = [f"\n{theme.upperDivider}"]
        if primary_stats:
            lines.append("**Primary engines** *(run on every screenshot)*")
            for s in primary_stats:
                lines.append(_fmt_row(s, is_fallback=False))
        if fallback_stats:
            if primary_stats:
                lines.append("")  # visual separator
            lines.append("**Fallback engines**")
            for s in fallback_stats:
                lines.append(_fmt_row(s, is_fallback=True))
            stale = [s for s in fallback_stats
                     if s['lang'] in configured
                     and s['runs'] >= 5 and s['rows_filled'] == 0]
            if stale:
                names = ", ".join(f"`{s['lang']}`" for s in stale)
                lines.append(
                    f"\n{theme.warnIcon} *Consider removing: {names} — "
                    f"5+ runs without filling a row.*"
                )

        # Engines configured but not yet recorded (fresh install / new fallback).
        recorded_keys = {(s['lang'], s['role']) for s in stats}
        pending = []
        if (primary, 'primary') not in recorded_keys:
            pending.append(primary)
        for fb in fallbacks:
            if (fb, 'fallback') not in recorded_keys and fb != primary:
                pending.append(fb)
        if pending:
            labels = ", ".join(f"`{c}`" for c in pending)
            lines.append(f"\n*(configured but no runs yet: {labels})*")

        lines.append(theme.lowerDivider)
        return "\n".join(lines)

    async def _on_primary_change(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        new_primary = interaction.data['values'][0]
        try:
            self.cog.set_ocr_language_settings(self.alliance_id, primary=new_primary)
        except ValueError as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {e}", ephemeral=True
            )
            return
        self._build()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    async def _on_fallbacks_change(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        selected = list(interaction.data.get('values') or [])
        self.cog.set_ocr_language_settings(self.alliance_id, fallbacks=selected)
        self._build()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    async def _on_autoprune_toggle(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        new_value = not self.cog.get_ocr_autoprune(self.alliance_id)
        self.cog.set_ocr_autoprune(self.alliance_id, new_value)
        self._build()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    async def _on_back(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=self.parent._build_embed(), view=self.parent,
        )


# ---------------------------------------------------------------------------
# Supporting views and modals
# ---------------------------------------------------------------------------

class DateRangeModal(discord.ui.Modal, title="Select Date Range"):
    from_date = discord.ui.TextInput(
        label="From Date (YYYY-MM-DD)", required=False, placeholder="2026-01-01"
    )
    to_date = discord.ui.TextInput(
        label="To Date (YYYY-MM-DD)", required=False, placeholder="2026-01-31"
    )

    def __init__(self, parent_view: BearDamageView):
        super().__init__()
        self.parent_view = parent_view
        if self.parent_view.from_date:
            self.from_date.default = self.parent_view.from_date.strftime("%Y-%m-%d")
        if self.parent_view.to_date:
            self.to_date.default = self.parent_view.to_date.strftime("%Y-%m-%d")

    async def on_submit(self, interaction: discord.Interaction):
        try:
            if self.from_date.value:
                self.parent_view.from_date = datetime.strptime(self.from_date.value, "%Y-%m-%d").date()
            if self.to_date.value:
                self.parent_view.to_date = datetime.strptime(self.to_date.value, "%Y-%m-%d").date()
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid date format. Use YYYY-MM-DD.", ephemeral=True
            )
            return

        # User chose dates manually — drop the active preset so no button
        # is highlighted as 'current'.
        self.parent_view.preset = None
        await self.parent_view.try_redraw(interaction)


class BearChannelSetupView(discord.ui.View):
    """Per-alliance bear setup: where to listen, what to listen for, and
    how to read the screenshots. Operational settings live in
    `BearSettingsView`."""

    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id
        self.alliance_id: int | None = None
        self._build_components()

    def _build_components(self):
        self.clear_items()
        opts = build_alliance_options(self.cog.alliance_conn)
        for opt in opts:
            opt.default = (int(opt.value) == (self.alliance_id or 0))
        self.add_item(AllianceSelect(self, opts, action="manage"))

        has_alliance = self.alliance_id is not None

        channel_btn = discord.ui.Button(label="Change Channel", style=discord.ButtonStyle.primary, emoji=theme.announceIcon, row=2, disabled=not has_alliance)
        channel_btn.callback = self._change_channel_callback
        self.add_item(channel_btn)

        ocr_btn = discord.ui.Button(label="Character Recognition", style=discord.ButtonStyle.primary, emoji=theme.globeIcon, row=2, disabled=not has_alliance)
        ocr_btn.callback = self._ocr_languages_callback
        self.add_item(ocr_btn)

        keywords_btn = discord.ui.Button(label="Keywords", style=discord.ButtonStyle.primary, emoji=theme.editListIcon, row=2, disabled=not has_alliance)
        keywords_btn.callback = self._manage_keywords_callback
        self.add_item(keywords_btn)

        range_btn = discord.ui.Button(label="Damage Range", style=discord.ButtonStyle.primary, emoji=theme.chartIcon, row=2, disabled=not has_alliance)
        range_btn.callback = self._set_range_callback
        self.add_item(range_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=3)
        back_btn.callback = self._back_callback
        self.add_item(back_btn)

    def _build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{theme.editListIcon} Bear Channel Setup",
            description=(
                f"Per-alliance setup for screenshot collection: which "
                f"channel to watch, which messages to process, and how to "
                f"read them.\n\n"
                f"**Available Operations**\n"
                f"{theme.upperDivider}\n"
                f"{theme.announceIcon} **Change Channel**\n"
                f"└ Which channel the bot watches for Bear screenshot uploads "
                f"(**required**)\n\n"
                f"{theme.globeIcon} **Character Recognition**\n"
                f"└ Pick the characters the bot should recognize in player "
                f"names; adjust based on what your names use\n\n"
                f"{theme.editListIcon} **Keywords**\n"
                f"└ Words required in the message text to trigger processing; "
                f"use this if folks upload other images in the same channel; "
                f"blank = accept all (default)\n\n"
                f"{theme.chartIcon} **Damage Range**\n"
                f"└ Default lookback for the auto-posted summary chart "
                f"after each hunt; `0` = full history (default)\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1,
        )

        if self.alliance_id is not None:
            settings = self.cog.get_bear_settings(self.alliance_id)
            channel = (
                f"<#{settings['channel_id']}>" if settings.get('channel_id')
                else "**Not set** — required"
            )
            keywords = ", ".join(settings["keywords"]) if settings["keywords"] else "None"
            damage_range = settings["damage_range"]
            primary, fallbacks = self.cog.get_ocr_language_settings(self.alliance_id)
            primary_label = OCR_LANG_LABEL.get(primary, primary)
            fb_labels = [OCR_LANG_LABEL.get(c, c) for c in fallbacks]
            ocr_summary = primary_label + (
                f" · fallbacks: {', '.join(fb_labels)}" if fb_labels else ""
            )
            embed.add_field(
                name="Current Setup",
                value=(
                    f"{theme.upperDivider}\n"
                    f"**Channel:** {channel}\n"
                    f"**Keywords:** {keywords}\n"
                    f"**Damage Range:** {damage_range} day(s)"
                    f"{' (full history)' if damage_range == 0 else ''}\n"
                    f"**OCR:** {ocr_summary}\n"
                    f"{theme.lowerDivider}"
                ),
                inline=False,
            )
        return embed

    async def on_alliance_selected(self, interaction: discord.Interaction):
        self._build_components()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    async def _change_channel_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        view = BearChannelSelectView(
            cog=self.cog, alliance_id=self.alliance_id,
            parent_view=self, parent_message=interaction.message,
        )
        await interaction.response.send_message(
            "Select the bear score channel for this alliance:",
            view=view, ephemeral=True,
        )

    async def _manage_keywords_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        settings = self.cog.get_bear_settings(self.alliance_id)
        current_keywords = ", ".join(settings["keywords"])
        await interaction.response.send_modal(
            KeywordsModal(current_keywords, self.cog, self.alliance_id, self)
        )

    async def _set_range_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        settings = self.cog.get_bear_settings(self.alliance_id)
        await interaction.response.send_modal(
            DamageRangeModal(self.cog, self.alliance_id, settings["damage_range"], self)
        )

    async def _ocr_languages_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        view = BearOcrLanguagesView(self.cog, self.alliance_id, self.original_user_id, self)
        await interaction.response.edit_message(embed=view._build_embed(), view=view)

    async def _back_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)


class BearChannelSelectView(discord.ui.View):
    def __init__(self, cog, alliance_id: int, parent_view, parent_message: discord.Message = None):
        super().__init__(timeout=180)
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_view = parent_view
        self.parent_message = parent_message
        self.add_item(BearChannelSelect(self))


class BearChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, parent_view: BearChannelSelectView):
        super().__init__(
            placeholder="Select a channel...",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text, discord.ChannelType.news]
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        try:
            selected_channel = self.values[0]
            channel_id = selected_channel.id

            self.parent_view.cog.update_bear_setting(
                self.parent_view.alliance_id,
                "bear_score_channel",
                channel_id
            )

            await interaction.response.edit_message(
                content=f"{theme.verifiedIcon} Bear score channel set to {selected_channel.mention}",
                view=None
            )

            # Refresh parent settings embed on the original message
            try:
                parent_msg = self.parent_view.parent_message
                if parent_msg:
                    settings_view = self.parent_view.parent_view
                    embed = settings_view._build_embed()
                    await parent_msg.edit(embed=embed, view=settings_view)
            except Exception as e:
                logger.warning(f"Could not refresh parent settings embed: {e}")

        except Exception as e:
            logger.error(f"BearChannelSelect callback error: {e}")
            print(f"[ERROR] BearChannelSelect callback error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save channel.", ephemeral=True
            )


class KeywordsModal(discord.ui.Modal):
    def __init__(self, current_keywords: str, cog, alliance_id: int,
                 parent_view):
        super().__init__(title="Manage Bear Keywords")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_view = parent_view

        self.keywords_input = discord.ui.TextInput(
            label="Required words in the typed message text",
            style=discord.TextStyle.paragraph,
            default=current_keywords or "",
            placeholder="comma-separated, e.g. bear, damage. Blank = accept any upload.",
            required=False,
            max_length=400
        )
        self.add_item(self.keywords_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            keywords = self.keywords_input.value.strip()
            keyword_csv = ", ".join([kw.strip() for kw in keywords.split(",") if kw.strip()]) if keywords else None

            self.cog.update_bear_setting(self.alliance_id, "bear_keywords", keyword_csv)

            embed = self.parent_view._build_embed()
            embed.description += f"\n{theme.verifiedIcon} Keywords updated."
            await safe_edit_message(interaction, embed=embed, view=self.parent_view, content=None)

        except Exception as e:
            logger.error(f"KeywordsModal error: {e}")
            print(f"[ERROR] KeywordsModal error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to update keywords.", ephemeral=True
            )


class DamageRangeModal(discord.ui.Modal):
    def __init__(self, cog, alliance_id: int, current_range: int,
                 parent_view):
        super().__init__(title="Set Damage History Range")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_view = parent_view

        self.range_input = discord.ui.TextInput(
            label="Number of days (0 = full history)",
            placeholder="Enter number of days",
            default=str(current_range),
            required=True,
            max_length=5
        )
        self.add_item(self.range_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.range_input.value.strip())
            if days < 0:
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a non-negative whole number.", ephemeral=True
            )
            return

        try:
            self.cog.update_bear_setting(self.alliance_id, "bear_damage_range", days)
        except Exception as e:
            logger.error(f"Failed to update damage range: {e}")
            print(f"[ERROR] Failed to update damage range: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save damage range.", ephemeral=True
            )
            return

        embed = self.parent_view._build_embed()
        embed.description += f"\n{theme.verifiedIcon} Damage range set to {days} days."
        await safe_edit_message(interaction, embed=embed, view=self.parent_view, content=None)


class SessionTimeoutModal(discord.ui.Modal):
    def __init__(self, cog, alliance_id: int, current_timeout: int,
                 parent_view):
        super().__init__(title="Set Session Timeout")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_view = parent_view

        self.timeout_input = discord.ui.TextInput(
            label="Minutes to wait for more screenshots (1-60)",
            placeholder="e.g. 15",
            default=str(current_timeout),
            required=True,
            max_length=3,
        )
        self.add_item(self.timeout_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            minutes = int(self.timeout_input.value.strip())
            if not (1 <= minutes <= 60):
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a whole number between 1 and 60.",
                ephemeral=True,
            )
            return

        try:
            self.cog.update_bear_setting(self.alliance_id, "bear_session_timeout_min", minutes)
        except Exception as e:
            logger.error(f"Failed to update session timeout: {e}")
            print(f"[ERROR] Failed to update session timeout: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save session timeout.", ephemeral=True
            )
            return

        embed = self.parent_view._build_embed()
        embed.description += f"\n{theme.verifiedIcon} Session timeout set to {minutes} min."
        await safe_edit_message(interaction, embed=embed, view=self.parent_view, content=None)


# ---------------------------------------------------------------------------
# DataSubmit — handles data insertion and view generation
# ---------------------------------------------------------------------------

class DataSubmit:
    def __init__(self, alliance_conn, bear_conn):
        self.alliance_conn = alliance_conn
        self.alliance_cursor = alliance_conn.cursor()
        self.bear_conn = bear_conn
        self.bear_cursor = bear_conn.cursor()

    async def process_submission(self, interaction, date, hunting_trap, rallies, total_damage,
                                 *, alliance_id: int | None = None, alliance_name: str | None = None):
        await self._persist_hunt_and_render(
            interaction, date=date, hunting_trap=hunting_trap,
            rallies=rallies, total_damage=total_damage,
            alliance_id=alliance_id, alliance_name=alliance_name,
        )

    async def process_full_submission(self, interaction, *, hunt_meta, player_rows,
                                      alliance_id=None, alliance_name=None):
        """Persist a reviewed bear hunt: hunt summary + every player row.
        Rows with `fid is None` are saved with NULL fid so the raw OCR'd
        name and damage are preserved for later resolution from the
        records UI."""
        await self._persist_hunt_and_render(
            interaction,
            date=hunt_meta['date'],
            hunting_trap=hunt_meta['hunting_trap'],
            rallies=hunt_meta.get('rallies'),
            total_damage=hunt_meta.get('total_damage') or 0,
            player_rows=player_rows,
            alliance_id=alliance_id, alliance_name=alliance_name,
        )

    async def _persist_hunt_and_render(self, interaction, *, date, hunting_trap, rallies,
                                       total_damage, player_rows=None,
                                       alliance_id=None, alliance_name=None):
        """Insert one hunt row (and any provided player rows), then build
        and send the standard per-trap chart embed. Shared by both the
        manual `/bear_damage_add` slash command and the OCR review submit.
        """
        if not interaction.response.is_done():
            await interaction.response.defer()

        # Resolve alliance from channel when not passed in.
        if alliance_id is None:
            self.alliance_cursor.execute(
                "SELECT alliance_id FROM alliancesettings WHERE bear_score_channel = ?",
                (interaction.channel.id,),
            )
            row = self.alliance_cursor.fetchone()
            if not row:
                await interaction.followup.send(
                    f"{theme.deniedIcon} This channel is not configured as a bear score channel.",
                    ephemeral=True,
                )
                return
            alliance_id = int(row[0])
        if alliance_name is None:
            self.alliance_cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,)
            )
            arow = self.alliance_cursor.fetchone()
            alliance_name = arow[0] if arow else f"Alliance ID: {alliance_id}"

        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")
        hunting_trap = int(hunting_trap)
        rallies = int(rallies) if rallies is not None else None
        total_damage = int(total_damage)

        # Insert the hunt summary row.
        try:
            self.bear_cursor.execute(
                "INSERT INTO bear_hunts (alliance_id, date, hunting_trap, rallies, total_damage) "
                "VALUES (?, ?, ?, ?, ?)",
                (alliance_id, date, hunting_trap, rallies, total_damage),
            )
            hunt_id = self.bear_cursor.lastrowid
        except sqlite3.IntegrityError:
            await interaction.followup.send(
                f"{theme.warnIcon} This alliance already submitted this trap for that date.",
                ephemeral=True,
            )
            return

        matched = unmatched = 0
        if player_rows:
            for r in player_rows:
                fid = r.get('fid')
                score = r['candidates'][0][2] if r.get('candidates') else None
                self.bear_cursor.execute(
                    "INSERT INTO bear_player_damage "
                    "(hunt_id, fid, raw_name, resolved_nickname, damage, rank, match_score) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (hunt_id, fid, r.get('name'), r.get('nickname'),
                     int(r['damage']), r.get('rank'), score),
                )
                if fid:
                    matched += 1
                else:
                    unmatched += 1
        self.bear_conn.commit()

        title_suffix = "Latest Submission"
        if player_rows:
            title_suffix += f" · {matched} player(s)"
            if unmatched:
                title_suffix += f" · {unmatched} unmatched"

        # Pull damage-range setting + per-trap history for the chart.
        self.alliance_cursor.execute(
            "SELECT bear_damage_range FROM alliancesettings WHERE alliance_id = ?",
            (alliance_id,),
        )
        range_row = self.alliance_cursor.fetchone()
        damage_range_days = range_row[0] if range_row and range_row[0] else 0

        self.bear_cursor.execute(
            "SELECT date, rallies, total_damage FROM bear_hunts "
            "WHERE alliance_id = ? AND hunting_trap = ? ORDER BY date ASC",
            (alliance_id, hunting_trap),
        )
        rows = self.bear_cursor.fetchall()
        if damage_range_days > 0:
            today = datetime.now(timezone.utc).date()
            range_start = today - timedelta(days=damage_range_days)
            filtered_rows = [
                r for r in rows
                if datetime.strptime(r[0], "%Y-%m-%d").date() >= range_start
            ] or rows
        else:
            filtered_rows = rows

        dates = [datetime.strptime(r[0], "%Y-%m-%d") for r in filtered_rows]
        rallies_list = [int(r[1]) if r[1] else 0 for r in filtered_rows]
        total_damages = [int(r[2]) if r[2] else 0 for r in filtered_rows]

        try:
            embed, image_file = bear_data_embed(
                alliance_id=alliance_id, alliance_name=alliance_name,
                hunting_trap=hunting_trap, dates=dates,
                rallies_list=rallies_list, total_damages=total_damages,
                title_suffix=title_suffix,
                damage_range_days=damage_range_days,
            )
        except Exception as e:
            logger.error(f"Failed to generate submission embed: {e}")
            print(f"[ERROR] Failed to generate submission embed: {e}")
            saved_total = matched + unmatched
            msg = (f"{theme.deniedIcon} Data saved ({saved_total} player rows) but chart generation failed."
                   if saved_total else f"{theme.deniedIcon} Error generating graph.")
            await interaction.followup.send(msg, ephemeral=True)
            return

        if unmatched:
            embed.set_footer(
                text=f"{matched} matched · {unmatched} saved as unmatched — "
                     f"resolve from Bear Damage Records when ready."
            )

        try:
            await interaction.edit_original_response(
                embed=embed, attachments=[image_file] if image_file else [], view=None,
            )
        except discord.NotFound:
            # Original response unavailable (e.g. OCR flow used channel.send).
            try:
                await interaction.followup.send(
                    embed=embed, file=image_file if image_file else None,
                )
            except Exception as e:
                logger.error(f"Failed to send submission result: {e}")
                print(f"[ERROR] Failed to send submission result: {e}")
        except Exception as e:
            logger.error(f"Failed to edit submission message: {e}")
            print(f"[ERROR] Failed to edit submission message: {e}")

    async def process_view(self, *, alliance_id: int, hunting_trap,
                           from_date: str | None = None, to_date: str | None = None,
                           alliance_name: str | None = None):
        """Generate a view embed and chart. `hunting_trap` is 1, 2, or 'both'."""
        if alliance_name is None:
            self.alliance_cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?",
                (alliance_id,)
            )
            row = self.alliance_cursor.fetchone()
            alliance_name = row[0] if row else f"Alliance ID: {alliance_id}"

        if hunting_trap == 'both':
            return await self._process_view_combined(
                alliance_id=alliance_id, alliance_name=alliance_name,
                from_date=from_date, to_date=to_date,
            )

        self.bear_cursor.execute(
            "SELECT date, rallies, total_damage FROM bear_hunts "
            "WHERE alliance_id = ? AND hunting_trap = ? ORDER BY date ASC",
            (alliance_id, hunting_trap)
        )
        rows = self.bear_cursor.fetchall()

        if not rows:
            return None, None

        if not from_date:
            from_date = rows[0][0]
        if not to_date:
            to_date = rows[-1][0]

        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
        except ValueError:
            return None, None

        if from_dt > to_dt:
            return None, None

        filtered_rows = [
            r for r in rows
            if from_dt <= datetime.strptime(r[0], "%Y-%m-%d").date() <= to_dt
        ]
        if not filtered_rows:
            return None, None

        dates = [datetime.strptime(r[0], "%Y-%m-%d") for r in filtered_rows]
        rallies_list = [int(r[1]) if r[1] else 0 for r in filtered_rows]
        total_damages = [int(r[2]) if r[2] else 0 for r in filtered_rows]

        try:
            embed, file = bear_data_embed(
                alliance_id=alliance_id,
                alliance_name=alliance_name,
                hunting_trap=hunting_trap,
                dates=dates,
                rallies_list=rallies_list,
                total_damages=total_damages,
                title_suffix="View Damage",
                damage_range_days=None
            )
        except Exception as e:
            logger.error(f"bear_data_embed failed: {e}")
            print(f"[ERROR] bear_data_embed failed: {e}")
            return None, None

        return embed, file

    async def _process_view_combined(self, *, alliance_id, alliance_name,
                                     from_date, to_date):
        """Both-traps render: pull each trap's series, drop empty ones,
        defer to combined embed builder."""
        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d").date() if from_date else None
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else None
        except ValueError:
            return None, None
        if from_dt and to_dt and from_dt > to_dt:
            return None, None

        trap_series = []
        for trap in (1, 2):
            self.bear_cursor.execute(
                "SELECT date, rallies, total_damage FROM bear_hunts "
                "WHERE alliance_id = ? AND hunting_trap = ? ORDER BY date ASC",
                (alliance_id, trap),
            )
            rows = self.bear_cursor.fetchall()
            if not rows:
                continue
            filtered = [
                r for r in rows
                if (not from_dt or datetime.strptime(r[0], "%Y-%m-%d").date() >= from_dt)
                and (not to_dt or datetime.strptime(r[0], "%Y-%m-%d").date() <= to_dt)
            ]
            if not filtered:
                continue
            trap_series.append((
                trap,
                [datetime.strptime(r[0], "%Y-%m-%d") for r in filtered],
                [int(r[1]) if r[1] else 0 for r in filtered],
                [int(r[2]) if r[2] else 0 for r in filtered],
            ))

        if not trap_series:
            return None, None

        try:
            embed, file = bear_data_embed_combined(
                alliance_id=alliance_id,
                alliance_name=alliance_name,
                trap_series=trap_series,
                title_suffix="View Damage",
            )
        except Exception as e:
            logger.error(f"bear_data_embed_combined failed: {e}")
            print(f"[ERROR] bear_data_embed_combined failed: {e}")
            return None, None
        return embed, file


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

async def setup(bot):
    await bot.add_cog(BearTrack(bot))
