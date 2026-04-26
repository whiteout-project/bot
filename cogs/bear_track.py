"""
Bear damage tracking. Records, views, and charts bear hunt damage per alliance.
"""
import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import io
import re
import os
import sqlite3
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

# RapidOCR setup
OCR_AVAILABLE = False
rapid_ocr = None

if PIL_AVAILABLE:
    try:
        from rapidocr import RapidOCR
        # rapidocr ignores TQDM_DISABLE — its DownloadFile passes
        # `disable=not check_is_atty()` directly to tqdm. Override the
        # tty check so the model-download progress bars never render to
        # the terminal (logged to log/rapidocr.txt is fine).
        try:
            from rapidocr.utils.download_file import DownloadFile
            DownloadFile.check_is_atty = staticmethod(lambda: False)
        except Exception:
            pass
        rapid_ocr = RapidOCR()
        OCR_AVAILABLE = True
        logger.info("RapidOCR initialized successfully")
        print("[INFO] RapidOCR initialized for bear track OCR")
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


def repair_ocr_digits(text: str) -> str:
    # Fix confusable letters (O↔0, I/l↔1, S↔5, etc.) only inside 3+ char
    # runs that contain at least one real digit. Pure-letter sequences
    # like "ISBN", "Bill", "OSLO" are left alone.
    def _fix(match):
        run = match.group(0)
        if not any(c.isdigit() for c in run):
            return run
        return ''.join(_OCR_DIGIT_MAP.get(c, c) for c in run)
    return _OCR_DIGIT_RUN.sub(_fix, text)


_FORMATTED_NUMBER_RE = re.compile(r'\d{1,3}(?:[,\.]\d{3})+')
_BRACKETED_TRAP_RE = re.compile(r'\[[^\]\d][^\]]*?(\d+)\]')
_BARE_SMALL_INT_RE = re.compile(r'(?<=[^\s\d,\.\]\[])\s*(\d{1,3})(?![\d,\.\]])')


def extract_bear_hunt_stats(text: str):
    """Language-agnostic extraction of hunting trap #, rallies, total damage.

    Returns (hunting_trap_str, rallies_str, total_damage_int).
    """
    trap_match = _BRACKETED_TRAP_RE.search(text)
    hunting_trap = trap_match.group(1) if trap_match else ""

    # Total damage = the numerically largest thousands-separated number
    # anywhere in the text. Damage totals always dwarf per-player damages.
    number_runs = list(_FORMATTED_NUMBER_RE.finditer(text))
    if number_runs:
        total_damage = max(int(re.sub(r'[^\d]', '', m.group(0))) for m in number_runs)
    else:
        total_damage = 0

    pre_damage = text[:number_runs[0].start()] if number_runs else text
    candidates = _BARE_SMALL_INT_RE.findall(pre_damage)
    rallies = candidates[-1] if candidates else ""

    return hunting_trap, rallies, total_damage


def find_ranking_section_start(text: str) -> int:
    """Return the index where the player-ranking section starts.

    The ranking section is always preceded by a bracketed tag like
    '[Hunting Trap N] Damage Ranking' in every locale, so the last `]`
    reliably marks where player rows begin. When OCR misses the brackets,
    fall back to common English UI keywords. Returns 0 when nothing is
    found (ranking-only mid-list captures).
    """
    last_bracket = text.rfind(']')
    if last_bracket != -1:
        return last_bracket + 1
    m = re.search(r'(?i)\b(?:damage\s+ranking|ranking)\b', text)
    if m:
        return m.end()
    return 0


def parse_player_rows(text: str, after_pos: int = None):
    """Parse the damage-ranking section into rows of (name, damage, rank).

    `after_pos` defaults to `find_ranking_section_start(text)` which trims
    overview / personal-rewards prose that may precede the ranking table.
    Names retain OCR noise — fuzzy resolution to real alliance members
    happens later in the UI layer.
    """
    if after_pos is None:
        after_pos = find_ranking_section_start(text)
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
    # Strip common English UI suffix labels even from single-occurrence chunks
    # (so _strip_common_trailing_token doesn't need >=2 hits to fire).
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
        rows.append({'name': name, 'damage': damages[i], 'rank': rank})
    return rows


def _better_row(existing, candidate, roster=None) -> bool:
    """Return True when `candidate` is a stronger match than `existing` for
    the same damage value (cross-image merge tiebreaker).

    With a roster, prefer the name that fuzzy-matches a roster member with
    the higher score — this prevents header-prose garbage like
    "Hunting Trap Damage Ranking AlejoCAT" from winning over a clean
    "AlejoCAT" just because it's longer. Without a roster, fall back to
    rank-info > non-empty > longer-name.
    """
    if existing.get('rank') is None and candidate.get('rank') is not None:
        return True
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

    if e_name and c_name and len(c_name) > len(e_name) + 2:
        return True
    return False


MATCH_AUTO_CONFIRM = 90
MATCH_LIKELY_MIN = 70
MATCH_AMBIGUOUS_DELTA = 5


# RapidOCR recognition-model language codes admins can pick from. 
# "ch" is the baked-in default (Chinese + any Latin-script characters) 
# so it always works without downloading extra models.
OCR_LANGUAGES = [
    ("ch",          "Chinese + English (default)"),
    ("en",          "English only (sharper)"),
    ("japan",       "Japanese"),
    ("korean",      "Korean"),
    ("chinese_cht", "Traditional Chinese"),
    ("latin",       "Latin (French, German, etc.)"),
    ("arabic",      "Arabic"),
    ("cyrillic",    "Cyrillic (Russian, etc.)"),
    ("devanagari",  "Devanagari (Hindi, etc.)"),
]
OCR_LANG_CODES = {code for code, _label in OCR_LANGUAGES}
OCR_LANG_LABEL = dict(OCR_LANGUAGES)
DEFAULT_OCR_LANG = "ch"

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
    """Return True when `text` contains at least one character in `lang`'s
    expected Unicode range. Latin engines (en/latin) have no such range
    check — Latin gibberish is indistinguishable from a valid Latin name
    without a dictionary. The default ch engine isn't filtered either."""
    ranges = _LANG_UNICODE_RANGES.get(lang)
    if not ranges:
        return True
    return any(any(lo <= ord(c) <= hi for lo, hi in ranges) for c in text)


def _extract_script_substrings(text: str, lang: str, *, min_script_chars: int = 2) -> list:
    """Pull out contiguous runs of `lang`-script characters (with allowed
    intra-name whitespace) from `text`. Filters single-char hits that are
    typically misread medal icons or punctuation. Returns deduped list,
    sorted longest-first.
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
        out.append(s)
    out.sort(key=len, reverse=True)
    return out


_RTL_RANGES = [(0x0590, 0x08FF), (0xFB1D, 0xFDFF), (0xFE70, 0xFEFF)]


def _has_rtl(text: str) -> bool:
    return any(any(lo <= ord(c) <= hi for lo, hi in _RTL_RANGES) for c in text)


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
    """Prepend U+200E (LRM) when text contains RTL chars so Discord renders
    the line left-aligned within an LTR-base paragraph. The RTL run still
    reads RTL within itself."""
    if not text:
        return text or ""
    return "‎" + text if _has_rtl(text) else text


def _reshape_for_chart(text) -> str:
    """Shape Arabic glyphs and convert to visual order for matplotlib, which
    does not run the bidi algorithm itself. Discord embeds should use
    `_ltr_line` instead — Discord shapes Arabic natively."""
    if not text or not _RESHAPE_AVAILABLE:
        return text or ""
    if not _has_rtl(text):
        return text
    try:
        return _bidi_get_display(_arabic_reshaper.reshape(text))
    except Exception:
        return text


# Per-language RapidOCR engine cache. Each extra language adds ~30 MB
# of RAM after first use (RapidOCR rebuilds the detection + classifier
# + recognition ONNX sessions per instance — the API doesn't expose a
# way to share det/cls across languages).
_ocr_engines = {}


def get_ocr_engine(lang: str):
    """Return a RapidOCR instance configured for the given language code.

    Falls back to the already-initialised default engine when rapidocr is
    unavailable, when `lang` is unknown, or when building a language-specific
    engine fails (e.g. the model couldn't be downloaded).
    """
    if not OCR_AVAILABLE:
        return None
    if lang == DEFAULT_OCR_LANG or lang not in OCR_LANG_CODES:
        return rapid_ocr
    engine = _ocr_engines.get(lang)
    if engine is not None:
        return engine
    try:
        from rapidocr import RapidOCR, LangRec
        # RapidOCR 3.x requires the LangRec enum, not a raw string. The enum
        # values match our OCR_LANGUAGES codes 1:1 (ch, en, japan, korean,
        # arabic, cyrillic, devanagari, latin, chinese_cht).
        engine = RapidOCR(params={"Rec.lang_type": LangRec(lang)})
        _ocr_engines[lang] = engine
        logger.info(f"Bear OCR: loaded language-specific engine for {lang!r}")
        return engine
    except Exception as e:
        logger.warning(f"Bear OCR: could not load engine for {lang!r} ({e}); using default")
        return rapid_ocr


def match_roster(detected_name: str, roster):
    """Return up to 5 best matches as [(fid, nickname, score_0_100), ...].

    `roster` is a list of (fid, nickname) tuples. Empty detected name or
    empty roster returns []. Scores below MATCH_LIKELY_MIN are dropped.
    """
    if not detected_name or not roster or not RAPIDFUZZ_AVAILABLE:
        return []
    names = [nick or "" for (_fid, nick) in roster]
    # default_process applies case-folding + whitespace normalisation so that
    # "alejocat" and "AlejoCAT" match — important for manual search input.
    results = _rf_process.extract(
        detected_name, names,
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
    rows: dict = field(default_factory=dict)


@dataclass
class EventGroup:
    """One bear event accumulated from one or more compatible screenshots."""
    trap_value: str = ""
    rallies_value: str = ""
    damage_int: int = 0
    merged_rows: dict = field(default_factory=dict)
    image_count: int = 0

    def merge(self, result: ImageResult, roster: list | None = None):
        if not self.trap_value and result.trap:
            self.trap_value = result.trap
        if not self.rallies_value and result.rallies:
            self.rallies_value = result.rallies
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
            return False
        has_agreement = has_conflict = False
        for new_dmg, new_row in result.rows.items():
            existing_row = self.merged_rows.get(new_dmg)
            if not existing_row:
                continue
            status = _row_pair_status(existing_row, new_row, roster)
            if status == 'same':
                has_agreement = True
            elif status == 'different':
                has_conflict = True
        if has_agreement:
            return True
        return not has_conflict


def _row_pair_status(row_a: dict, row_b: dict, roster: list) -> str:
    """Two rows share a damage value. Returns 'same' when both names
    confidently fuzzy-match the same roster fid, 'different' when they
    confidently match different fids, or 'unknown' when at least one
    name doesn't roster-match with high confidence."""
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
    """Deletes the source screenshot messages once every event has been
    actioned (Submit or Cancel from its ReviewView) and at least one was
    Submitted. If every event was cancelled, the screenshots stay."""
    def __init__(self, source_messages, enabled: bool):
        self.source_messages = list(source_messages)
        self.enabled = enabled
        self.pending = 0
        self.any_submitted = False

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
        if not (self.enabled and self.pending == 0 and self.any_submitted):
            return
        for msg in self.source_messages:
            try:
                await msg.delete()
            except discord.NotFound:
                pass
            except Exception as e:
                logger.warning(f"Bear auto-delete failed for message {msg.id}: {e}")


class BearSession:
    """Per-(channel, user) session that accumulates bear-hunt screenshots
    over a sliding window. Each new upload restarts the timer; Done /
    Cancel buttons or expiry produce one ReviewView per detected event."""

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
        self.any_ocr_success = False
        self.finalized = False
        # In-flight OCR state (None when idle).
        self.current_image_idx: int | None = None
        self.current_image_total: int | None = None
        self.current_phase: str | None = None  # 'ocr' or 'fallback'
        self.current_lang: str | None = None

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
        filled = round((current - 1) / total * width) if total else 0
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
        async with self.lock:
            if self.finalized:
                return
            self.source_messages.append(message)
            total = len(image_attachments)
            base_idx = self.processed_images

            async def _phase_callback(phase: str, lang: str):
                self.current_phase = phase
                self.current_lang = lang
                await self.render_progress()

            for offset, attachment in enumerate(image_attachments, start=1):
                self.current_image_idx = base_idx + offset
                self.current_image_total = base_idx + total
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
                    progress_callback=_phase_callback,
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
        await self.cog._finalize_session(self, timed_out=timed_out)

    async def cancel(self):
        async with self.lock:
            if self.finalized:
                return
            self.finalized = True
            self.stop_timer()
            _active_sessions.pop((self.channel_id, self.user_id), None)
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
        try:
            await interaction.response.defer()
        except Exception:
            pass
        await self.session.finalize(timed_out=False)

    async def _on_cancel(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
        except Exception:
            pass
        await self.session.cancel()


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


def _render_damage_chart(dates, values, *, title, ylabel="Damage"):
    """Render the bot's standard fivethirtyeight-styled damage line chart.
    Returns a discord.File pointing at an in-memory PNG, or None when
    matplotlib isn't available or rendering fails. Filename is always
    'plot.png' so callers can attach it via 'attachment://plot.png'.
    """
    if not MATPLOTLIB_AVAILABLE or not dates:
        return None
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import MaxNLocator, FuncFormatter
    try:
        plt.style.use("fivethirtyeight")
        plt.figure(figsize=(10, 7), facecolor="#1a1a2d")
        plt.plot(dates, values, marker='o', linewidth=3)
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
            "bear_ocr_lang": "TEXT DEFAULT 'ch'",
            "bear_ocr_fallback_langs": "TEXT DEFAULT ''",
            "bear_session_timeout_min": "INTEGER DEFAULT 15",
            "bear_auto_delete_screenshots": "INTEGER DEFAULT 1",
        }
        for col_name, col_type in new_columns.items():
            if col_name not in columns:
                self.alliance_cursor.execute(
                    f"ALTER TABLE alliancesettings ADD COLUMN {col_name} {col_type}"
                )

        self.alliance_conn.commit()

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
        """Route bear hunt screenshots into a per-(channel, user) session.

        First upload starts a session and posts a collecting message with
        Done/Cancel buttons. Subsequent uploads from the same user in the
        same channel attach to that session within its timeout window.
        On finalize (Done click, Cancel click, or timer expiry) the cog
        builds one ReviewView per detected event and replaces the
        collecting message in place.
        """
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

    async def _ocr_attachment_to_result(self, image_bytes: bytes, primary_lang: str,
                                        fallback_langs: list, *, filename: str = "",
                                        roster: list | None = None,
                                        progress_callback=None) -> ImageResult:
        """OCR a single screenshot (primary + any fallbacks) and return an
        ImageResult ready to feed into session clustering. `progress_callback`
        is invoked as `await callback(phase, lang)` at the start of each OCR
        phase ('ocr' for the primary engine, 'fallback' for each fallback)."""
        result = ImageResult()
        if progress_callback:
            await progress_callback('ocr', primary_lang)
        try:
            extracted_text = self._ocr_bytes(image_bytes, lang=primary_lang)
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

        img_rows = {row['damage']: row for row in parse_player_rows(repaired)}

        def _name_score(name: str) -> int:
            if not name:
                return 0
            if not roster:
                return 100
            cands = match_roster(name, roster)
            return cands[0][2] if cands else 0

        def _is_unfilled(row) -> bool:
            return _name_score(row.get('name') or '') < MATCH_LIKELY_MIN

        if fallback_langs and any(_is_unfilled(r) for r in img_rows.values()):
            for fb_lang in fallback_langs:
                if not any(_is_unfilled(r) for r in img_rows.values()):
                    break
                if progress_callback:
                    await progress_callback('fallback', fb_lang)
                try:
                    fb_text = self._ocr_bytes(image_bytes, lang=fb_lang)
                except Exception as e:
                    logger.warning(f"Bear OCR fallback {fb_lang} failed: {e}")
                    continue
                if not fb_text.strip():
                    continue
                fb_repaired = repair_ocr_digits(fb_text)
                logger.info(
                    f"Bear OCR fallback [{fb_lang}] ({filename}): {fb_repaired!r}"
                )
                if not _output_matches_lang_script(fb_repaired, fb_lang):
                    logger.info(
                        f"Bear OCR fallback [{fb_lang}] rejected: "
                        f"output contains no {fb_lang}-script characters"
                    )
                    continue
                filled_via_damage = False
                for fr in parse_player_rows(fb_repaired):
                    existing = img_rows.get(fr['damage'])
                    if not existing or not fr['name']:
                        continue
                    if _name_score(fr['name']) > _name_score(existing.get('name') or ''):
                        existing['name'] = fr['name']
                        filled_via_damage = True
                        logger.info(
                            f"Bear OCR fallback [{fb_lang}] filled "
                            f"{fr['name']!r} for damage {fr['damage']}"
                        )
                if not filled_via_damage and fb_lang not in _LATIN_ONLY_LANGS:
                    candidates = _extract_script_substrings(fb_repaired, fb_lang)
                    unfilled = sorted(
                        [r for r in img_rows.values() if _is_unfilled(r)],
                        key=lambda r: -r['damage'],
                    )
                    if len(unfilled) == 1 and candidates:
                        unfilled[0]['name'] = candidates[0]
                        logger.info(
                            f"Bear OCR fallback [{fb_lang}] filled (by-script) "
                            f"{candidates[0]!r} for damage {unfilled[0]['damage']}"
                        )
                    elif len(candidates) == len(unfilled) >= 1:
                        for row, name in zip(unfilled, candidates):
                            row['name'] = name
                            logger.info(
                                f"Bear OCR fallback [{fb_lang}] filled (by-script) "
                                f"{name!r} for damage {row['damage']}"
                            )

        result.rows = img_rows
        return result

    async def _finalize_session(self, session: BearSession, *, timed_out: bool):
        """Build a ReviewView per event in the session and replace the
        collecting message in place. Multi-event sessions get one extra
        message per additional event."""
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

        review_views: list[BearHuntReviewView] = []
        for event in session.events:
            merged_rows = dict(event.merged_rows)
            merged_rows.pop(event.damage_int, None)
            rows_sum = sum(r['damage'] for r in merged_rows.values())
            damage_int = event.damage_int
            if rows_sum > damage_int:
                damage_int = rows_sum
            for i, row in enumerate(sorted(merged_rows.values(), key=lambda r: -r['damage'])):
                row['rank'] = i + 1

            hunt_meta = {
                'date': today_date,
                'hunting_trap': int(event.trap_value) if event.trap_value and event.trap_value.isdigit() else None,
                'rallies': int(event.rallies_value) if event.rallies_value and event.rallies_value.isdigit() else None,
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
            )
            tracker.register()
            review_views.append(review)

        n_events = len(review_views)
        multi_event = n_events > 1

        def _decorate(embed: discord.Embed, idx: int) -> discord.Embed:
            prefixes = []
            if timed_out and idx == 0:
                prefixes.append(
                    f"{theme.hourglassIcon} **Session timed out after "
                    f"{session.timeout_min} min** — review and Submit when ready."
                )
            if multi_event:
                prefixes.append(
                    f"{theme.warnIcon} **Event {idx + 1} of {n_events}** — "
                    f"separate events were detected in this batch."
                )
            if prefixes:
                embed.description = "\n".join(prefixes) + "\n\n" + (embed.description or "")
            if not session.any_ocr_success and idx == 0:
                embed.title = f"{theme.warnIcon} OCR could not read the image(s) — add rows manually"
            return embed

        channel = self.bot.get_channel(session.channel_id)

        first_review = review_views[0]
        first_embed = _decorate(first_review.build_embed(), 0)
        if session.progress_msg is not None:
            try:
                await session.progress_msg.edit(embed=first_embed, view=first_review)
                first_review.message = session.progress_msg
            except Exception as e:
                logger.warning(f"Bear hunt: could not edit progress into review: {e}")
                if channel:
                    try:
                        first_review.message = await channel.send(embed=first_embed, view=first_review)
                    except Exception:
                        pass
        elif channel:
            try:
                first_review.message = await channel.send(embed=first_embed, view=first_review)
            except Exception:
                pass

        for i, review in enumerate(review_views[1:], start=1):
            if not channel:
                break
            embed = _decorate(review.build_embed(), i)
            try:
                review.message = await channel.send(embed=embed, view=review)
            except Exception as e:
                logger.warning(f"Bear hunt: could not send review for event {i + 1}: {e}")

    def _ocr_bytes(self, image_bytes: bytes, lang: str = DEFAULT_OCR_LANG) -> str:
        """OCR an image already in memory. Used by fallback passes so we
        don't re-download the attachment once per configured language.
        """
        if not OCR_AVAILABLE or not image_bytes:
            return ""
        engine = get_ocr_engine(lang)
        if engine is None:
            return ""
        image = Image.open(io.BytesIO(image_bytes))
        result = engine(np.array(image.convert('RGB')))
        if not result:
            return ""
        if hasattr(result, 'txts') and result.txts:
            return " ".join(result.txts)
        if hasattr(result, '__iter__'):
            texts = [str(item[1]) for item in result
                     if isinstance(item, (list, tuple)) and len(item) >= 2]
            return " ".join(texts) if texts else str(result)
        return str(result)

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

    async def show_bear_track_menu(self, interaction: discord.Interaction):
        """Display the bear damage tracking main menu."""
        try:
            view = BearMenuView(cog=self, original_user_id=interaction.user.id)

            embed = discord.Embed(
                title=f"{theme.chartIcon} Bear Damage Tracking",
                description=(
                    f"Track your alliance's bear damage over time and view trends.\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.chartIcon} **View Bear Damage**\n"
                    f"  Select an alliance and date range to see a damage graph\n\n"
                    f"{theme.editListIcon} **Edit Bear Damage**\n"
                    f"  Edit or delete saved damage records for your alliances\n\n"
                    f"{theme.settingsIcon} **Settings**\n"
                    f"  Configure bear channel, keywords, damage range, and permissions\n"
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


class BearHuntReviewView(discord.ui.View):
    """Review-and-edit view for OCR-extracted bear hunt data.

    Shows the hunt header plus per-player rows with their roster match
    status. Admins edit/add/delete rows, then submit — which persists the
    hunt summary to `bear_hunts` and each row to `bear_player_damage`.
    """

    ROWS_PER_PAGE = 25

    def __init__(self, cog, data_submit, *, hunt_meta, rows, roster,
                 alliance_id, alliance_name, original_user_id,
                 auto_delete_tracker=None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.data_submit = data_submit
        self.hunt_meta = hunt_meta
        self.roster = roster
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.original_user_id = original_user_id
        self.auto_delete_tracker = auto_delete_tracker
        self.message = None
        self.page = 0
        self._tracker_resolved = False

        self.rows = [self._enrich_row(r) for r in rows]
        self._sort_rows()
        self._build_components()

    async def _notify_tracker_submit(self):
        if self.auto_delete_tracker and not self._tracker_resolved:
            self._tracker_resolved = True
            await self.auto_delete_tracker.on_submit()

    async def _notify_tracker_cancel(self):
        if self.auto_delete_tracker and not self._tracker_resolved:
            self._tracker_resolved = True
            await self.auto_delete_tracker.on_cancel()

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
        embed.add_field(name="Alliance", value=self.alliance_name or f"ID {self.alliance_id}", inline=False)
        embed.add_field(name="Date", value=self.hunt_meta['date'], inline=True)
        embed.add_field(
            name="Hunting Trap",
            value=str(self.hunt_meta['hunting_trap']) if self.hunt_meta['hunting_trap'] else "-",
            inline=True,
        )
        embed.add_field(
            name="Rallies",
            value=str(self.hunt_meta['rallies']) if self.hunt_meta['rallies'] else "-",
            inline=True,
        )
        embed.add_field(
            name="Total Alliance Damage",
            value=format_damage_for_embed(self.hunt_meta['total_damage']) or "-",
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
                player = f"`{r['nickname']}` · `{r['fid']}`"
            elif status == 'likely':
                top_fid, top_nick, score = r['candidates'][0]
                player = f"`{top_nick}` ({score}%) · `{top_fid}`"
            elif status == 'ambiguous':
                tops = " / ".join(
                    f"`{c[1]}` (`{c[0]}`, {c[2]}%)"
                    for c in r['candidates'][:2]
                )
                player = f"{tops}"
            elif status == 'manual':
                player = f"`{r['nickname']}` · `{r['fid']}`"
            else:
                name = r['name'] or "unreadable"
                player = f"`{name}` — no match"
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

        buttons = [
            ("Edit Hunt Info", theme.editListIcon, discord.ButtonStyle.secondary, self._on_edit_header),
            ("Add Row", theme.addIcon, discord.ButtonStyle.secondary, self._on_add_row),
            ("Submit", theme.verifiedIcon, discord.ButtonStyle.success, self._on_submit),
            ("Save Totals Only", theme.totalIcon, discord.ButtonStyle.primary, self._on_submit_totals_only),
            ("Cancel", theme.deniedIcon, discord.ButtonStyle.secondary, self._on_cancel),
        ]
        for label, emoji, style, cb in buttons:
            btn = discord.ui.Button(label=label, emoji=emoji, style=style, row=1)
            btn.callback = cb
            self.add_item(btn)

        total_pages = self._total_pages()
        if total_pages > 1:
            prev_btn = discord.ui.Button(
                label="Prev", emoji=theme.prevIcon,
                style=discord.ButtonStyle.secondary,
                row=2, disabled=(self.page == 0),
            )
            prev_btn.callback = self._on_prev
            self.add_item(prev_btn)
            page_label = discord.ui.Button(
                label=f"Page {self.page + 1}/{total_pages}",
                style=discord.ButtonStyle.secondary,
                row=2, disabled=True,
            )
            self.add_item(page_label)
            next_btn = discord.ui.Button(
                label="Next", emoji=theme.nextIcon,
                style=discord.ButtonStyle.secondary,
                row=2, disabled=(self.page >= total_pages - 1),
            )
            next_btn.callback = self._on_next
            self.add_item(next_btn)

    async def refresh(self, interaction):
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
    """Validate the three text inputs that EditRow/AddRow modals share.

    Returns a `(parsed_row_dict, error_message)` tuple — exactly one
    side is non-None. The dict is shaped like a `BearHuntReviewView` row
    so callers can `update()` an existing row or `append` a new one.
    """
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
                f"Select an alliance, trap, and date range to view damage.\n"
                f"{theme.upperDivider}\n"
                f"Use the dropdown to pick an alliance, then choose a trap and date range.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Edit Bear Damage", style=discord.ButtonStyle.primary, emoji=theme.editListIcon, row=1)
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
            title=f"{theme.editListIcon} Edit Bear Damage",
            description=(
                f"Select an alliance to view and manage its damage records.\n"
                f"{theme.upperDivider}\n"
                f"Pick an alliance from the dropdown, then select a record to edit or delete.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.primary, emoji=theme.settingsIcon, row=2)
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
        embed = view._build_settings_embed()
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
    def __init__(self, data_submit, *, cog, original_user_id,
                 alliance_id: int | None = None, hunting_trap: int | None = None,
                 from_date: date | None = None, to_date: date | None = None):
        super().__init__(timeout=7200)
        self.data_submit = data_submit
        self.cog = cog
        self.original_user_id = original_user_id
        self.alliance_id = alliance_id
        self.hunting_trap = hunting_trap
        self.from_date = from_date
        self.to_date = to_date

        options = build_alliance_options(cog.alliance_conn)
        self.add_item(AllianceSelect(self, options, action="view"))

    def is_ready(self) -> bool:
        return all([self.alliance_id, self.hunting_trap, self.from_date, self.to_date])

    def missing_inputs(self) -> list[str]:
        missing = []
        if not self.alliance_id:
            missing.append("alliance")
        if not self.hunting_trap:
            missing.append("trap")
        if not self.from_date or not self.to_date:
            missing.append("date range")
        return missing

    async def try_redraw(self, interaction: discord.Interaction):
        if not self.is_ready():
            missing = ", ".join(self.missing_inputs())
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select: **{missing}** to draw the graph.",
                ephemeral=True
            )
            return

        embed, file = await self.data_submit.process_view(
            alliance_id=self.alliance_id,
            hunting_trap=self.hunting_trap,
            from_date=self.from_date.strftime("%Y-%m-%d"),
            to_date=self.to_date.strftime("%Y-%m-%d"),
        )

        if not embed:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No data found for the selected parameters.",
                ephemeral=True
            )
            return

        await interaction.response.edit_message(
            embed=embed, attachments=[file] if file else [], view=self
        )

    @discord.ui.button(label="Date Range", style=discord.ButtonStyle.primary, emoji=theme.calendarIcon, row=2)
    async def date_range(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await interaction.response.send_modal(DateRangeModal(self))

    @discord.ui.button(label="Trap 1", style=discord.ButtonStyle.secondary, emoji=theme.bearTrapIcon, row=2)
    async def trap_1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self._select_trap(interaction, 1)

    @discord.ui.button(label="Trap 2", style=discord.ButtonStyle.secondary, emoji=theme.bearTrapIcon, row=2)
    async def trap_2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self._select_trap(interaction, 2)

    async def _select_trap(self, interaction: discord.Interaction, trap_number: int):
        if self.hunting_trap == trap_number:
            await interaction.response.send_message(
                f"{theme.warnIcon} Already showing Trap {trap_number}.",
                ephemeral=True
            )
            return
        self.hunting_trap = trap_number
        await self.try_redraw(interaction)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=2)
    async def back_to_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)


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
    """Paginated picker for the unmatched player rows of one saved hunt.
    Selecting a row opens a modal to assign it to a roster member or
    delete it. Updates persist immediately."""

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
        embed = discord.Embed(
            title=f"{theme.warnIcon} Fix Unmatched Rows",
            description=(
                "Pick an unmatched row to assign it to a roster member, or "
                "leave the player field blank in the modal to delete it."
            ),
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

        total_pages = self._total_pages()
        if total_pages > 1:
            prev_btn = discord.ui.Button(
                label="Prev", emoji=theme.prevIcon,
                style=discord.ButtonStyle.secondary, row=1,
                disabled=(self.page == 0),
            )
            prev_btn.callback = self._on_prev
            next_btn = discord.ui.Button(
                label="Next", emoji=theme.nextIcon,
                style=discord.ButtonStyle.secondary, row=1,
                disabled=(self.page >= total_pages - 1),
            )
            next_btn.callback = self._on_next
            self.add_item(prev_btn)
            self.add_item(next_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

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

        channel_btn = discord.ui.Button(label="Change Bear Channel", style=discord.ButtonStyle.primary, emoji=theme.announceIcon, row=2, disabled=not has_alliance)
        channel_btn.callback = self._change_channel_callback
        self.add_item(channel_btn)

        keywords_btn = discord.ui.Button(label="Manage Keywords", style=discord.ButtonStyle.primary, emoji=theme.editListIcon, row=2, disabled=not has_alliance)
        keywords_btn.callback = self._manage_keywords_callback
        self.add_item(keywords_btn)

        range_btn = discord.ui.Button(label="Set Damage Range", style=discord.ButtonStyle.primary, emoji=theme.chartIcon, row=2, disabled=not has_alliance)
        range_btn.callback = self._set_range_callback
        self.add_item(range_btn)

        ocr_btn = discord.ui.Button(label="OCR Languages", style=discord.ButtonStyle.primary, emoji=theme.globeIcon, row=2, disabled=not has_alliance)
        ocr_btn.callback = self._ocr_languages_callback
        self.add_item(ocr_btn)

        timeout_btn = discord.ui.Button(label="Session Timeout", style=discord.ButtonStyle.primary, emoji=theme.hourglassIcon, row=3, disabled=not has_alliance)
        timeout_btn.callback = self._session_timeout_callback
        self.add_item(timeout_btn)

        auto_delete_btn = discord.ui.Button(label="Toggle Auto-Delete", style=discord.ButtonStyle.primary, emoji=theme.trashIcon, row=3, disabled=not has_alliance)
        auto_delete_btn.callback = self._toggle_auto_delete_callback
        self.add_item(auto_delete_btn)

        add_perm_btn = discord.ui.Button(label="Toggle Add Permission", style=discord.ButtonStyle.secondary, emoji=theme.lockIcon, row=4, disabled=not has_alliance)
        add_perm_btn.callback = self._toggle_add_callback
        self.add_item(add_perm_btn)

        view_perm_btn = discord.ui.Button(label="Toggle View Permission", style=discord.ButtonStyle.secondary, emoji=theme.eyeIcon, row=4, disabled=not has_alliance)
        view_perm_btn.callback = self._toggle_view_callback
        self.add_item(view_perm_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=4)
        back_btn.callback = self._back_callback
        self.add_item(back_btn)

    def _build_settings_embed(self) -> discord.Embed:
        description = (
            f"{theme.upperDivider}\n"
            f"Track alliance bear damage by uploading in-game screenshots.\n"
            f"- Players post bear hunt screenshots in the configured channel.\n"
            f"- The bot uses OCR to read each row's player name and damage.\n"
            f"- Multiple screenshots posted close together merge into one or more events.\n"
            f"- A review screen opens where you can fix any bad matches.\n "
            f"- Submitted hunts can be tracked and charted over a configurable range.\n"
            f"{theme.lowerDivider}"
        )

        embed = discord.Embed(
            title=f"{theme.settingsIcon} Bear Settings",
            description=description,
            color=theme.emColor1
        )

        quick_guide = (
            f"{theme.announceIcon} **Change Bear Channel** - Where the bot looks for bear screenshots\n"
            f"{theme.editListIcon} **Manage Keywords** - Words required in the typed message text (not the image). Blank = no filter.\n"
            f"{theme.chartIcon} **Set Damage Range** - How many days of data to show (0 = all)\n"
            f"{theme.globeIcon} **OCR Languages** - Primary + fallback recognition models\n"
            f"{theme.hourglassIcon} **Session Timeout** - How long to wait for more screenshots (1-60 min)\n"
            f"{theme.trashIcon} **Toggle Auto-Delete** - Remove screenshots after submission\n"
            f"{theme.lockIcon} **Toggle Permissions** - Who can add or view damage data\n"
        )
        embed.add_field(name="Quick Guide", value=quick_guide, inline=False)

        if self.alliance_id:
            settings = self.cog.get_bear_settings(self.alliance_id)
            channel_id = settings["channel_id"]
            keywords = ", ".join(settings["keywords"]) if settings["keywords"] else "None"
            damage_range = settings["damage_range"]
            view_text = "Admins only" if settings["admin_only_view"] else "Everyone"
            add_text = "Admins only" if settings["admin_only_add"] else "Everyone"
            channel_display = f"<#{channel_id}>" if channel_id else "Not set"
            primary_lang, fallback_langs = self.cog.get_ocr_language_settings(self.alliance_id)
            primary_label = OCR_LANG_LABEL.get(primary_lang, primary_lang)
            fb_labels = [OCR_LANG_LABEL.get(c, c) for c in fallback_langs]
            ocr_summary = primary_label + (f" · fallbacks: {', '.join(fb_labels)}" if fb_labels else "")
            timeout_min = settings["session_timeout_min"]
            auto_delete_text = "On" if settings["auto_delete_screenshots"] else "Off"

            current_settings = (
                f"{theme.upperDivider}\n"
                f"**Bear Channel:** {channel_display}\n"
                f"**Keywords:** {keywords}\n"
                f"**Damage History Range:** {damage_range} day(s) {'(all history)' if damage_range == 0 else ''}\n"
                f"**OCR Languages:** {ocr_summary}\n"
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
        embed = self._build_settings_embed()
        await interaction.response.edit_message(content=None, view=self, embed=embed)

    async def _change_channel_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        view = BearChannelSelectView(
            cog=self.cog,
            alliance_id=self.alliance_id,
            parent_settings_view=self,
            parent_message=interaction.message
        )
        await interaction.response.send_message(
            "Select the bear score channel for this alliance:",
            view=view,
            ephemeral=True
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
        current_range = settings["damage_range"]

        await interaction.response.send_modal(
            DamageRangeModal(self.cog, self.alliance_id, current_range, self)
        )

    async def _ocr_languages_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        view = BearOcrLanguagesView(self.cog, self.alliance_id, self.original_user_id, self)
        await interaction.response.edit_message(embed=view._build_embed(), view=view)

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
        embed = self._build_settings_embed()
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

        embed = self._build_settings_embed()
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

    def __init__(self, cog, alliance_id, original_user_id, parent_settings_view):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id
        self.original_user_id = original_user_id
        self.parent = parent_settings_view
        self._build()

    def _build(self):
        self.clear_items()
        primary, fallbacks = self.cog.get_ocr_language_settings(self.alliance_id)

        primary_opts = [
            discord.SelectOption(label=label, value=code, default=(code == primary))
            for code, label in OCR_LANGUAGES
        ]
        primary_select = discord.ui.Select(
            placeholder="Primary OCR language",
            options=primary_opts, min_values=1, max_values=1, row=0,
        )
        primary_select.callback = self._on_primary_change
        self.add_item(primary_select)

        fb_opts = [
            discord.SelectOption(label=label, value=code, default=(code in fallbacks))
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
            f"Configure OCR recognition for bear hunt screenshots.\n"
            f"{theme.upperDivider}\n"
            f"**Primary** runs first on every screenshot.\n"
            f"**Fallbacks** re-OCR the same screenshot for any row whose "
            f"name came back unreadable, so alliances with mixed-script "
            f"names (e.g. Latin + Arabic) can still resolve every player.\n"
            f"{theme.lowerDivider}\n\n"
            f"**Primary:** {primary_label}\n"
            f"**Fallbacks:** {', '.join(fb_labels) if fb_labels else '*(none)*'}\n"
        )
        if fb_labels:
            description += (
                f"\n{theme.warnIcon} Each fallback adds **~30 MB RAM** and "
                f"**~10 MB disk** per language, and slows down processing. "
                f"Only enable for the characters your alliance players names "
                f"actually use.\n"
            )
        return discord.Embed(
            title=f"{theme.globeIcon} OCR Languages",
            description=description,
            color=theme.emColor1,
        )

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

    async def _on_back(self, interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=self.parent._build_settings_embed(), view=self.parent,
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

        await self.parent_view.try_redraw(interaction)


class BearChannelSelectView(discord.ui.View):
    def __init__(self, cog, alliance_id: int, parent_settings_view: BearSettingsView, parent_message: discord.Message = None):
        super().__init__(timeout=180)
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_settings_view = parent_settings_view
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
                    settings_view = self.parent_view.parent_settings_view
                    embed = settings_view._build_settings_embed()
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
                 parent_settings_view: BearSettingsView):
        super().__init__(title="Manage Bear Keywords")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_settings_view = parent_settings_view

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

            embed = self.parent_settings_view._build_settings_embed()
            embed.description += f"\n{theme.verifiedIcon} Keywords updated."
            await safe_edit_message(interaction, embed=embed, view=self.parent_settings_view, content=None)

        except Exception as e:
            logger.error(f"KeywordsModal error: {e}")
            print(f"[ERROR] KeywordsModal error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to update keywords.", ephemeral=True
            )


class DamageRangeModal(discord.ui.Modal):
    def __init__(self, cog, alliance_id: int, current_range: int,
                 parent_settings_view: BearSettingsView):
        super().__init__(title="Set Damage History Range")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_settings_view = parent_settings_view

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

        embed = self.parent_settings_view._build_settings_embed()
        embed.description += f"\n{theme.verifiedIcon} Damage range set to {days} days."
        await safe_edit_message(interaction, embed=embed, view=self.parent_settings_view, content=None)


class SessionTimeoutModal(discord.ui.Modal):
    def __init__(self, cog, alliance_id: int, current_timeout: int,
                 parent_settings_view: BearSettingsView):
        super().__init__(title="Set Session Timeout")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_settings_view = parent_settings_view

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

        embed = self.parent_settings_view._build_settings_embed()
        embed.description += f"\n{theme.verifiedIcon} Session timeout set to {minutes} min."
        await safe_edit_message(interaction, embed=embed, view=self.parent_settings_view, content=None)


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

    async def process_view(self, *, alliance_id: int, hunting_trap: int,
                           from_date: str | None = None, to_date: str | None = None,
                           alliance_name: str | None = None):
        """Generate a view embed and chart for bear damage data."""
        if alliance_name is None:
            self.alliance_cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?",
                (alliance_id,)
            )
            row = self.alliance_cursor.fetchone()
            alliance_name = row[0] if row else f"Alliance ID: {alliance_id}"

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


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

async def setup(bot):
    await bot.add_cog(BearTrack(bot))
