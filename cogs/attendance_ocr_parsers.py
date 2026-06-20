"""OCR session classes for every attendance event: event registry, base session, and per-event parsers."""
from __future__ import annotations
import asyncio
import difflib
import functools
import logging
import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import discord

from .pimp_my_bot import theme

logger = logging.getLogger("alliance")

DEFAULT_TIMEOUT_SECONDS = 180


# ── event registry ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class EventTypeConfig:
    """Per-event-type config with per-kind fingerprint regexes for distinguishing
    registration vs result screenshots within the same event."""
    key: str
    label: str
    default_keywords: tuple[str, ...]
    fingerprint_re_by_kind: dict[str, re.Pattern]
    event_weekday: Optional[int] = None
    legion_required: bool = False


# Shared anchors for foundry/canyon registration mails — anchored on event name later.
_REGISTRATION_MARKERS = (
    r"(?:selected\s+as\s+a?\s*combatant"
    r"|Legionnaires?\s+of\s+your\s+Legion"
    r"|Combatants:\s*\d+\s*/\s*\d+"
    r"|Please\s+get\s+ready\s+to\s+enter)"
)


EVENT_TYPES: dict[str, EventTypeConfig] = {
    "foundry_battle": EventTypeConfig(
        key="foundry_battle",
        label="Foundry Battle",
        default_keywords=("Foundry Battle", "Foundry"),
        fingerprint_re_by_kind={
            "registration": re.compile(
                _REGISTRATION_MARKERS + r".*?Foundry",
                re.IGNORECASE | re.DOTALL,
            ),
            "result": re.compile(
                r"(?:Personal\s+Arsenal\s+Points|Imperial\s+Foundry\s+Control)",
                re.IGNORECASE,
            ),
        },
        event_weekday=6,
        legion_required=True,
    ),
    "canyon_clash": EventTypeConfig(
        key="canyon_clash",
        label="Canyon Clash",
        default_keywords=("Canyon Clash", "Canyon"),
        fingerprint_re_by_kind={
            "registration": re.compile(
                _REGISTRATION_MARKERS + r".*?Canyon",
                re.IGNORECASE | re.DOTALL,
            ),
            # Result-anchors avoid bare "Canyon Clash" which also appears in registration mails.
            "result": re.compile(
                r"(?:Total\s+Fuel\s+Used"
                r"|Personal\s+Point\s+Ranking"
                r"|ranked\s+No\.?\s*\d+\s+in\s+\[Canyon)",
                re.IGNORECASE,
            ),
        },
        event_weekday=5,
        legion_required=True,
    ),
    "power_rankings": EventTypeConfig(
        key="power_rankings",
        label="Power Rankings",
        default_keywords=("Power Rankings",),
        fingerprint_re_by_kind={
            "result": re.compile(r"(?:Alliance\s+Ranking|Power\s+Rankings)", re.IGNORECASE),
        },
    ),
    "alliance_showdown": EventTypeConfig(
        key="alliance_showdown",
        label="Alliance Showdown",
        default_keywords=("Alliance Showdown",),
        fingerprint_re_by_kind={
            "result": re.compile(r"Alliance\s+Showdown", re.IGNORECASE),
        },
    ),
}


def fingerprint_match(event_type: str, ocr_text: str, kind: Optional[str] = None) -> bool:
    """True if the event's fingerprint regex matches; `kind` narrows to one kind."""
    cfg = EVENT_TYPES.get(event_type)
    if cfg is None:
        return False
    if kind is not None:
        regex = cfg.fingerprint_re_by_kind.get(kind)
        return regex is not None and regex.search(ocr_text) is not None
    return any(rx.search(ocr_text) for rx in cfg.fingerprint_re_by_kind.values())


def detect_kind(event_type: str, ocr_text: str) -> Optional[str]:
    """Which kind (registration/result/...) the OCR matches, or None for a scroll page."""
    cfg = EVENT_TYPES.get(event_type)
    if cfg is None:
        return None
    for kind, regex in cfg.fingerprint_re_by_kind.items():
        if regex.search(ocr_text):
            return kind
    return None


def classify_event(ocr_text: str, enabled_events: list[str]
                   ) -> Optional[tuple[str, str]]:
    """Classify text to `(event_type, kind)` or None via fingerprint regex."""
    if not ocr_text or not enabled_events:
        return None
    for event_type in enabled_events:
        kind = detect_kind(event_type, ocr_text)
        if kind is not None:
            return (event_type, kind)
    return None


def resolve_event_date(mail_date_local: date, event_type: str,
                       *, registration: bool = False) -> tuple[date, str]:
    cfg = EVENT_TYPES.get(event_type)
    if cfg is None or cfg.event_weekday is None:
        return mail_date_local, "exact"
    if registration:
        # The registration mail arrives before the event (Foundry ~2 days,
        # Canyon ~1) — snap forward to the event's weekday so it dates the event,
        # not the sign-up day, and matches the result session.
        days = (cfg.event_weekday - mail_date_local.weekday()) % 7
        return mail_date_local + timedelta(days=days), ("exact" if days == 0 else "adjusted")
    delta = (mail_date_local.weekday() - cfg.event_weekday) % 7
    if delta == 0:
        return mail_date_local, "exact"
    if delta == 1:
        return mail_date_local - timedelta(days=1), "adjusted"
    if delta == 6:
        return mail_date_local + timedelta(days=1), "adjusted"
    return mail_date_local, "mismatch"


_HEADER_DATE_RE = re.compile(r"(?<!\d)(\d{4})[-./](\d{2})[-./](\d{2})(?!\d{3})")
_LEGION_RE = re.compile(r"\[?\s*Legion\s+(\d)\s*\]?", re.IGNORECASE)
_SCOREBOARD_ID_RE = re.compile(r"#(\d{1,5})")
_SCOREBOARD_NAME_RE = re.compile(r"\[([^\]]{1,8})\]\s*([^\d\[\n]{1,40}?)(?=\s*(?:\[|#|\d{1,3}(?:,\d{3})+|\d{4,}|Stats|MVP|Legion|$))")

# Stat labels we extract from the result mail's stats panel. Order matters for
# zipping with MVP names when OCR flattens the two columns into one line.
_RESULT_STAT_KEYS: tuple[tuple[str, str], ...] = (
    ("fuel_used",        r"Total\s+Fuel\s+Used"),
    ("squads_defeated",  r"Enemy\s+Squads\s+Defeated"),
    ("buildings",        r"Occupied\s+Buildings"),
    ("speedups",         r"Total\s+Battle\s+Speedups?\s+Used"),
    ("march_i",          r"Total\s+March\s+Accelerator\s+I\s+Used"),
    ("march_ii",         r"Total\s+March\s+Accelerator\s+II\s+Used"),
    ("retreats",         r"Total\s+Retreats"),
    ("advances",         r"Total\s+Advances"),
)

_STAT_LABELS: dict[str, str] = {
    "fuel_used":       "Fuel Used",
    "squads_defeated": "Squads Defeated",
    "buildings":       "Buildings",
    "speedups":        "Speedups",
    "march_i":         "March I",
    "march_ii":        "March II",
    "retreats":        "Retreats",
    "advances":        "Advances",
}

# Allowed event-time slots (UTC). Mirrors notification_event_types.EVENT_CONFIG.
EVENT_TIME_SLOTS: dict[str, tuple[str, ...]] = {
    "foundry_battle":  ("02:00", "12:00", "14:00", "19:00"),
    "canyon_clash":    ("02:00", "12:00", "14:00", "19:00", "21:00"),
}


def _parse_compact_int(token: str) -> Optional[int]:
    """Parse compact ('19.9M'), comma ('12,345'), EU-dot ('1.234'), or bare
    integers to int; None on failure. EU thousands = every dot followed by
    exactly 3 digits, so a real decimal like '1.5' is rejected."""
    token = token.strip()

    # Compact suffix branch — dot is the decimal separator.
    m = re.match(r"^(\d+(?:\.\d+)?)([KkMmBb])$", token.replace(",", ""))
    if m:
        val = float(m.group(1))
        mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[m.group(2).lower()]
        return int(val * mult)

    # No suffix. Strip comma always; strip dot only when it's followed by
    # exactly 3 digits (then a non-digit or end) — that's EU thousands format.
    stripped = re.sub(r"\.(?=\d{3}(?:\D|$))", "", token).replace(",", "")
    return int(stripped) if stripped.isdigit() else None


def extract_header_date(ocr_text: str) -> Optional[date]:
    m = _HEADER_DATE_RE.search(ocr_text)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def extract_legion(ocr_text: str) -> Optional[str]:
    m = _LEGION_RE.search(ocr_text)
    return f"Legion {m.group(1)}" if m else None


# ── result-mail extras: scoreboard, stats, MVPs ───────────────────────────

def _clean_scoreboard_name(raw: str, tag: str) -> str:
    """Clean an alliance name captured between `[TAG]` markers: drop a leading
    duplicate of the tag and trailing short lowercase line-wrap fragments,
    falling back to the tag itself when nothing usable remains."""
    name = raw.strip(" ,.")
    if tag and name.upper().startswith(tag.upper()):
        rest = name[len(tag):]
        if rest == "" or not rest[0].isalnum():
            name = rest.strip(" ,.")

    parts = name.split()
    while parts and parts[-1].isalpha() and parts[-1].islower() and len(parts[-1]) <= 4:
        parts.pop()
    name = " ".join(parts)

    if len(name) < 3:
        return tag
    return name


def _parse_alliance_scoreboard(text: str) -> list[dict]:
    """Extract the 3-alliance scoreboard from a result-mail header. OCR emits
    it column-wise (IDs, legions, names, scores); pair positionally — only the
    alliance ID and score are load-bearing."""
    # Slice between "battle details" and "Stats" so we don't pick up player rows.
    lower = text.lower()
    start = 0
    for marker in ("battle details", "achieving the", "victory"):
        idx = lower.find(marker)
        if idx != -1:
            start = max(start, idx + len(marker))
    end = len(text)
    for marker in ("Stats", "Legion Tally", "Personal Point Ranking", "Personal Arsenal"):
        idx = text.find(marker, start)
        if idx != -1:
            end = min(end, idx)
    segment = text[start:end]

    ids = [int(m.group(1)) for m in _SCOREBOARD_ID_RE.finditer(segment)]
    if not ids:
        return []

    legions = re.findall(r"Legion\s+(\d)", segment)
    tag_matches = list(re.finditer(r"\[([^\]]{1,8})\]", segment))

    # Names: text between each tag's closing `]` and the next `[` / digit run.
    names: list[str] = []
    for i, m in enumerate(tag_matches):
        name_start = m.end()
        name_end = tag_matches[i + 1].start() if i + 1 < len(tag_matches) else len(segment)
        # Stop at the first big digit run — those are the scores.
        digit_match = re.search(r"\d{3,}", segment[name_start:name_end])
        if digit_match:
            name_end = name_start + digit_match.start()
        raw_name = segment[name_start:name_end].strip(" ,.")
        names.append(_clean_scoreboard_name(raw_name, tag=m.group(1)))

    scores = [int(m.group(0).replace(",", ""))
              for m in re.finditer(r"\d{1,3}(?:,\d{3})+|\d{4,}", segment)]

    n = min(len(ids), len(scores)) if scores else 0
    rows = []
    for i in range(n):
        rows.append({
            "rank": ids[i],
            "legion": f"Legion {legions[i]}" if i < len(legions) else None,
            "tag": tag_matches[i].group(1) if i < len(tag_matches) else None,
            "name": names[i] if i < len(names) else None,
            "score": scores[i],
        })
    rows.sort(key=lambda r: -(r["score"] or 0))
    return rows


_SCORE_TOKEN_RE = re.compile(r"^\d{1,3}(?:,\d{3})+$|^\d{4,}$")
_RANK_TOKEN_RE = re.compile(r"^#(\d{1,5})$")
_LEGION_TOKEN_RE = re.compile(r"^Legion\s+(\d)$", re.IGNORECASE)
_TAG_TOKEN_RE = re.compile(r"\[([^\]]{1,8})\]")


def _box_centroid(box) -> Optional[tuple[float, float]]:
    """Centroid of a 4-corner bounding box. Returns None for malformed input."""
    try:
        xs = [pt[0] for pt in box]
        ys = [pt[1] for pt in box]
        return sum(xs) / len(xs), sum(ys) / len(ys)
    except (TypeError, IndexError, ZeroDivisionError):
        return None


_LEGION_TAG_RE = re.compile(r"^Legion\s*\d", re.IGNORECASE)

# Tokens that appear in the Stats/MVP/footer regions below the scoreboard
# cards and must never be glued onto an alliance name.
_NON_NAME_KEYWORDS = (
    "total fuel", "enemy squads", "occupied buildings", "battle speedups",
    "march accelerator", "total retreats", "total advances",
    "personal point ranking", "personal arsenal", "battle details",
    "achievement", "rewards", "delete", "stats", "mvp",
    "congratulations", "of your alliance ranked",
)


def _is_non_name_text(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in _NON_NAME_KEYWORDS)


def _parse_alliance_scoreboard_spatial(blocks: list) -> list[dict]:
    """Box-aware scoreboard parser: cluster chunks into alliance cards by each
    `[TAG]`'s x-position, bounded vertically by the score so footer/stats text
    can't leak into a name."""
    if not blocks:
        return []
    enriched = []
    for text, box in blocks:
        c = _box_centroid(box)
        if c is None:
            continue
        enriched.append({"text": text.strip(), "cx": c[0], "cy": c[1]})
    if not enriched:
        return []

    # Real alliance tags only — skip [Legion N] which appears in mail body text.
    anchors = []
    for b in enriched:
        m = _TAG_TOKEN_RE.search(b["text"])
        if not m:
            continue
        tag = m.group(1).strip()
        if _LEGION_TAG_RE.match(tag):
            continue
        anchors.append({**b, "tag": tag})
    if not anchors:
        return []
    anchors.sort(key=lambda a: a["cx"])

    DEFAULT_HALF = 150.0
    rows = []
    for i, anchor in enumerate(anchors):
        half_left = (anchor["cx"] - anchors[i - 1]["cx"]) / 2 if i > 0 else DEFAULT_HALF
        half_right = (anchors[i + 1]["cx"] - anchor["cx"]) / 2 if i + 1 < len(anchors) else DEFAULT_HALF
        col_min, col_max = anchor["cx"] - half_left, anchor["cx"] + half_right

        # Pass 1: find this card's score — the topmost formatted number
        # below the tag in this column. Acts as the bottom edge of the card.
        score = None
        score_cy = None
        for b in enriched:
            if not (col_min <= b["cx"] <= col_max):
                continue
            if _SCORE_TOKEN_RE.match(b["text"]) and b["cy"] > anchor["cy"]:
                if score_cy is None or b["cy"] < score_cy:
                    score = int(b["text"].replace(",", ""))
                    score_cy = b["cy"]
        # Without a score this isn't a real scoreboard card — drop it.
        if score is None:
            continue

        rank = legion = None
        name_parts: list[tuple[float, str]] = []
        for b in enriched:
            if not (col_min <= b["cx"] <= col_max):
                continue
            t = b["text"]
            if (m := _RANK_TOKEN_RE.match(t)) and b["cy"] < anchor["cy"]:
                rank = int(m.group(1))
                continue
            if (m := _LEGION_TOKEN_RE.match(t)) and b["cy"] < anchor["cy"]:
                legion = f"Legion {m.group(1)}"
                continue
            # Name fragments must sit strictly between the tag and the score.
            if not (anchor["cy"] - 5 <= b["cy"] < score_cy):
                continue
            if _SCORE_TOKEN_RE.match(t):
                continue
            if _is_non_name_text(t):
                continue
            stripped = _TAG_TOKEN_RE.sub("", t).strip(" .,")
            if stripped:
                name_parts.append((b["cy"], stripped))

        # Top-to-bottom; mid-word wraps (both ends lowercase) join without
        # a space, word-boundary wraps join with one.
        name_parts.sort()
        name = ""
        for j, (_, part) in enumerate(name_parts):
            if j == 0:
                name = part
            elif name and name[-1].islower() and part[0].islower():
                name += part
            else:
                name += " " + part

        rows.append({
            "rank": rank, "legion": legion, "tag": anchor["tag"],
            "name": name or anchor["tag"], "score": score,
        })
    # `rank` is the alliance number (#449, #312, etc.) — a permanent
    # identifier, not the event placement. Placement is by score desc.
    rows.sort(key=lambda r: -(r["score"] or 0))
    return rows


def _parse_stats_panel(text: str) -> dict[str, int]:
    """Extract stat_key → int for each known stat label in the result mail."""
    stats: dict[str, int] = {}
    for key, pattern in _RESULT_STAT_KEYS:
        m = re.search(pattern + r"\s*:?\s*([\d.,]+[KkMmBb]?)", text)
        if not m:
            continue
        parsed = _parse_compact_int(m.group(1))
        if parsed is not None:
            stats[key] = parsed
    return stats


def _parse_mvps(text: str) -> list[dict]:
    """Extract per-stat MVP entries as `{stat_key, name, value}`: find every
    `<name>: <value>` pair outside the stat-label spans and pair each stat with
    the nearest one within ±150 chars."""
    # 1) Mark stat-label spans (label + its own value) so we don't capture
    #    sub-tokens of "Total Fuel Used: 19.9M" as an MVP name.
    stat_spans: list[tuple[int, int, str]] = []
    for key, pattern in _RESULT_STAT_KEYS:
        full_re = re.compile(pattern + r"\s*:?\s*[\d.,]+[KkMmBb]?", re.IGNORECASE)
        for m in full_re.finditer(text):
            stat_spans.append((m.start(), m.end(), key))

    def _overlaps_stat_span(start: int, end: int) -> bool:
        return any(s_start < end and start < s_end for s_start, s_end, _ in stat_spans)

    # 2) Find every plausible "<name>: <value>" pair that's OUTSIDE every stat span.
    name_value_re = re.compile(r"([A-Za-z_][\w\-' ]{1,30}?)\s*:\s*([\d.,]+[KkMmBb]?)")
    candidates: list[tuple[int, int, str, str]] = []
    for m in name_value_re.finditer(text):
        if _overlaps_stat_span(m.start(), m.end()):
            continue
        candidates.append((m.start(), m.end(), m.group(1).strip(), m.group(2).strip()))

    # 3) Pair each stat with the nearest non-overlapping MVP candidate.
    mvps: list[dict] = []
    used_candidate_idxs: set[int] = set()
    for stat_start, stat_end, stat_key in stat_spans:
        anchor = (stat_start + stat_end) // 2
        best_idx = None
        best_dist = 10**9
        for idx, (c_start, c_end, _name, _value) in enumerate(candidates):
            if idx in used_candidate_idxs:
                continue
            mid = (c_start + c_end) // 2
            dist = abs(mid - anchor)
            if dist < best_dist and dist <= 150:
                best_dist = dist
                best_idx = idx
        if best_idx is None:
            continue
        _, _, name, value = candidates[best_idx]
        parsed = _parse_compact_int(value)
        if parsed is None:
            continue
        used_candidate_idxs.add(best_idx)
        # Tidy the name: strip stray punctuation.
        name = re.sub(r"[^\w\s\-\']", "", name).strip()
        if not name:
            continue
        mvps.append({"stat_key": stat_key, "name": name, "value": parsed})
    return mvps


# ── OCR + roster helpers ──────────────────────────────────────────────────

# Three formats we accept per OCR'd number token. Compact-suffix MUST come
# first so '12.345M' isn't greedily eaten by the dot-as-thousand-separator
# branch (which would yield 12345 instead of 12_345_000).
_FORMATTED_NUMBER_RE = re.compile(
    r"\d+(?:\.\d+)?[KkMmBb]\b"        # 807.3M, 1.0B, 3K
    r"|\d{1,3}(?:[,\.]\d{3})+"        # 1,234,567 or 1.234.567 (EU thousands)
    r"|(?<![A-Za-z0-9])\d{4,}(?![A-Za-z0-9])"  # bare 12345, not digits glued to
                                               # letters ('lord235342323' is a name)
)
_RANK_PREFIX_RE = re.compile(r"^\s*(\d{1,4})\b")

_PARSE_STOPWORDS = frozenset({
    "mail", "delete", "ranking", "rankings", "personal", "arsenal",
    "points", "point", "alliance", "showdown", "combatants", "substitutes",
    "page", "expires", "rewards", "stats", "mvp", "of", "your", "the", "in",
    "chief", "power", "ko", "contribution", "claimed", "no",
})

# Section markers that mark the start of the player-row data. The parser
# slices the OCR text at the LAST occurrence of any marker so the trailing
# header noise (column labels, blurb text) is discarded.
_SECTION_MARKERS = (
    "Personal Arsenal Points",
    "Personal Point Ranking",
    "Combatants",
    "Chief Power",
    "Power Rankings",
    "Ranking",
)

# Markers that signal the end of player-row data — anything after gets
# trimmed off so the mail-expiry timestamp ("Expires in 2026-06-14...")
# doesn't get parsed as a phantom player row with value 2026.
_END_MARKERS = (
    "Expires in",
    "Battle Overview",
    "Master Bonus",
    "Expert Bonus",
)

# Markers identifying a result mail's header page (scoreboard/tally/rewards),
# whose alliance-level totals must not be parsed as player rows.
_RESULT_HEADER_MARKERS = (
    "congratulations", "legion tally", "control rewards", "gathering rewards",
    "loot rewards", "ko rewards", "sieges by mercenaries",
)
_RESULT_LEADERBOARD_MARKERS = ("personal arsenal points", "personal point ranking")


def _is_result_header_page(text: str) -> bool:
    """True for a result mail's header page (header markers, no leaderboard
    marker) — skip row parsing there. Leaderboard and continuation pages
    return False."""
    tl = text.lower()
    return (not any(m in tl for m in _RESULT_LEADERBOARD_MARKERS)
            and any(m in tl for m in _RESULT_HEADER_MARKERS))


def _trim_to_data_section(text: str) -> str:
    """Trim start to the rightmost section marker and end to the earliest
    end marker. If no markers match, leave the corresponding boundary alone."""
    text_lower = text.lower()
    best_start = 0
    for marker in _SECTION_MARKERS:
        idx = text_lower.rfind(marker.lower())
        if idx != -1:
            end_of_marker = idx + len(marker)
            if end_of_marker > best_start:
                best_start = end_of_marker
    # End trim: first end-marker AFTER the start trim wins.
    best_end = len(text)
    for marker in _END_MARKERS:
        idx = text_lower.find(marker.lower(), best_start)
        if idx != -1 and idx < best_end:
            best_end = idx
    return text[best_start:best_end]


def _alliance_ocr_langs(alliance_id, names) -> tuple[str, list]:
    """Primary + fallback OCR languages for the alliance. Shared with Bear
    Tracking's OCR-language setting (one place per alliance). Fallbacks are
    auto-derived from `names` when auto-manage is on, else the manual config."""
    from . import bear_track
    default = bear_track.DEFAULT_OCR_LANG
    try:
        with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
            row = conn.execute(
                "SELECT bear_ocr_lang FROM alliancesettings WHERE alliance_id = ?",
                (alliance_id,)).fetchone()
    except Exception:
        return default, []
    if not row:
        return default, []
    primary = (row[0] or default).strip() or default
    if primary not in bear_track.OCR_LANG_CODES:
        primary = default
    fbs = bear_track.auto_managed_fallbacks(alliance_id, names, primary=primary)
    return primary, fbs


async def ocr_value_rows(image_bytes: bytes, *, roster, alliance_id, session=None, parse=None, primary_text: str | None = None, progress_callback=None):
    """OCR (name, value) rows with multi-language fallback for non-Latin names;
    returns (rows, primary_text). `parse` overrides the row parser (Power
    Rankings passes `_parse_power_rows`). `primary_text` reuses a primary read
    the caller already did, avoiding a redundant primary OCR pass."""
    from . import bear_track
    if parse is None:
        parse = _parse_player_value_rows
    primary, fallbacks = _alliance_ocr_langs(alliance_id, [n for _f, n in (roster or [])])
    if not fallbacks or not roster:
        text = primary_text if primary_text is not None else await bear_track.ocr_bytes(image_bytes, lang=primary, session=session)
        rows = parse(bear_track.repair_ocr_digits(text)) if text.strip() else []
        return rows, text

    def is_unfilled(r):
        fid, _ = fuzzy_match_name(r.get("name") or "", roster, alliance_id=alliance_id)
        return fid is None

    def merge(rows, fb_rows, fb_text, _lang):
        # Cheap value-merge first (works when the fallback engine happens to read
        # the number too). Group by value so rows sharing a value aren't dropped.
        fb_by_value: dict = {}
        for fr in fb_rows:
            fb_by_value.setdefault(fr["value"], []).append(fr)
        for r in rows:
            if not is_unfilled(r):
                continue
            for fr in fb_by_value.get(r["value"], ()):
                if fr.get("name"):
                    fid, _ = fuzzy_match_name(fr["name"], roster, alliance_id=alliance_id)
                    if fid is not None:
                        r["name"] = fr["name"]
                        break
        # Non-Latin engines read names well but mangle numbers, so value-merge
        # usually misses them. Fall back to bear's anchor-based position fill
        # (Latin names from the primary pass anchor the script substrings). It
        # sorts on 'damage' and mutates 'name', so alias 'damage' to our value.
        if any(is_unfilled(r) for r in rows):
            for r in rows:
                r["damage"] = r["value"]
            try:
                # id() keys so rows sharing a value aren't collapsed.
                bear_track.fill_unfilled_by_position(
                    {id(r): r for r in rows}, fb_text, _lang, "attendance", roster)
            finally:
                for r in rows:
                    r.pop("damage", None)

    return await bear_track.ocr_rows_with_fallback(
        image_bytes, primary_lang=primary, fallback_langs=fallbacks,
        parse=parse, is_unfilled=is_unfilled, merge=merge,
        session=session, primary_text=primary_text, progress_callback=progress_callback)


def find_formatted_numbers(text: str) -> list[tuple[int, int, int]]:
    out = []
    for m in _FORMATTED_NUMBER_RE.finditer(text):
        value = _parse_compact_int(m.group(0))
        if value is not None:
            out.append((m.start(), m.end(), value))
    return out


def _parse_player_value_rows(text: str, *, capture_tail: bool = False) -> list[dict]:
    """Parse (name, value) rows position-based, not line-based: OCR flattens the
    screenshot to one line, so the name is the chunk before each number (data
    section only). `capture_tail` (results only) recovers trailing 0-scorers."""
    rows = []
    text = _trim_to_data_section(text)
    prev_end = 0
    for start, end, value in find_formatted_numbers(text):
        chunk = text[prev_end:start].strip()
        prev_end = end
        # Trailing rank digit (e.g., "AlejoCAT 5" → "AlejoCAT")
        chunk = re.sub(r"\s+\d{1,3}\s*$", "", chunk)
        # Strip alliance-rank sticker tokens like "R4"
        chunk = re.sub(r"\bR\d+\b", "", chunk)
        # Detach trailing punctuation from header words ("Points:" → "Points").
        # Keep ',' and '.' — they're thousands separators, so splitting them
        # would shatter an OCR-garbled power value ("A36,17o,548") that the
        # number regex missed, hiding the row boundary it marks.
        chunk = re.sub(r"[:;!?]+", " ", chunk)
        tokens = [
            t for t in chunk.split()
            if t.lower() not in _PARSE_STOPWORDS
            and any(c.isalpha() for c in t)
            and len(t) >= 2
        ]
        if not tokens:
            continue
        # Keep the full trailing name (no last-3 cap that dropped "Bow" from
        # "Bow to thy Lord"), but split off any previous-row bleed at a garbled
        # number boundary.
        name = _name_from_tokens(tokens)
        if len(name) < 2:
            continue
        rows.append({"name": name, "value": value})
    # Trailing 0-scorers the value-anchored loop misses (their value is a lone 0
    # or OCR-dropped, leaving only "Name Rank").
    if capture_tail:
        rows.extend(_parse_tail_rows(text[prev_end:]))
    return rows


def _parse_tail_rows(tail: str) -> list[dict]:
    """Trailing 'Name Rank [Value]' rows whose value OCR read as 0 or dropped.
    Rank-ordered mails list 0-scorers last, so a missing value means 0 (absent)."""
    tail = re.sub(r"\bR\d+\b", "", tail)  # drop alliance-rank stickers
    rows = []
    name_toks: list[str] = []
    toks = tail.split()
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.isdigit():
            if name_toks:  # first number after a name is its rank
                value = 0
                if i + 1 < len(toks) and toks[i + 1].isdigit():
                    value = int(toks[i + 1])
                    i += 1
                name = _name_from_tokens(name_toks)
                if len(name) >= 2:
                    rows.append({"name": name, "value": value})
                name_toks = []
        elif (len(t) >= 2 and any(c.isalpha() for c in t)
              and t.lower() not in _PARSE_STOPWORDS):
            name_toks.append(t)
        i += 1
    return rows


def _name_noise(name: str) -> int:
    """Count digit-bearing tokens — a proxy for OCR junk like '48Z' or 'lII3'."""
    return sum(1 for t in (name or "").split() if any(c.isdigit() for c in t))


def _cleaner_name(new: str, old: str) -> bool:
    """True if `new` is a better OCR capture than `old`: fewer digit-noise
    tokens first, then the longer (more complete) name."""
    nn, on = _name_noise(new), _name_noise(old)
    if nn != on:
        return nn < on
    return len(new or "") > len(old or "")


# Name-like: a letter then word chars only ("BB_300", "lord235342323"). A
# garbled power value carries comma/period separators or is digit-led, so it
# still trips the digit-majority check below.
_NAMELIKE_TOKEN_RE = re.compile(r"^[A-Za-z]\w*$")


def _looks_like_garbled_number(token: str) -> bool:
    """A garbled power value (long, digit-majority) that the number regex missed;
    a name-like token (even with digits/underscore) is never one."""
    if _NAMELIKE_TOKEN_RE.match(token):
        return False
    digits = sum(c.isdigit() for c in token)
    return len(token) >= 5 and digits * 2 >= len(token)


def _name_from_tokens(tokens: list[str]) -> str:
    """The name is the trailing run of tokens after the last garbled-number
    token — when a power isn't detected as a row separator the chunk bleeds in
    from the previous row(s)."""
    cut = 0
    for idx, t in enumerate(tokens):
        if _looks_like_garbled_number(t):
            cut = idx + 1
    name_tokens = tokens[cut:]
    # After bleed, drop a leading rank-sticker artifact (R1–R5 OCR'd as 'Rs'/'Re').
    if cut and len(name_tokens) > 1 and len(name_tokens[0]) == 2 and name_tokens[0][0] in "Rr":
        name_tokens = name_tokens[1:]
    return " ".join(name_tokens).strip()


def _dedup_into(target: list[dict], new_row: dict) -> None:
    """Append with smart dedup: same value + overlapping (substring) names means
    the same player — keep the cleaner capture (fewer digit-noise tokens, then
    the longer name)."""
    new_name_norm = _normalize_for_match(new_row.get("name") or "")
    new_val = new_row.get("value")
    for i, r in enumerate(target):
        if r.get("value") != new_val:
            continue
        existing_norm = _normalize_for_match(r.get("name") or "")
        if not new_name_norm or not existing_norm:
            continue
        if (new_name_norm == existing_norm
                or new_name_norm in existing_norm
                or existing_norm in new_name_norm):
            new_noise = _name_noise(new_row["name"])
            old_noise = _name_noise(r["name"])
            if (new_noise < old_noise
                    or (new_noise == old_noise
                        and len(new_row["name"]) > len(r["name"]))):
                target[i] = new_row
            return
    target.append(new_row)


def load_alliance_roster(alliance_id: int) -> list[tuple[int, str]]:
    with sqlite3.connect("db/users.sqlite", timeout=30.0) as conn:
        rows = conn.execute(
            "SELECT fid, nickname FROM users WHERE alliance = ?",
            (str(alliance_id),),
        ).fetchall()
    return [(int(fid), nick or "") for fid, nick in rows if fid]


_skeleton_fn = None


def _skeleton(s: str) -> str:
    """Fold decorated-gamertag homoglyphs (Greek/Cyrillic/styled lookalikes) to
    their Latin equivalents, reusing bear_track's table so the two stay in sync.
    Bound lazily to respect the module's deferred bear_track import."""
    global _skeleton_fn
    if _skeleton_fn is None:
        from . import bear_track
        _skeleton_fn = bear_track._skeleton
    return _skeleton_fn(s)


@functools.lru_cache(maxsize=8192)
def _normalize_for_match(s: str) -> str:
    """Fold homoglyphs to Latin, lowercase, drop non-alphanumeric noise — so a
    decorated 'ROγAL' matches 'ROYAL'. Genuine non-Latin scripts (Arabic, CJK)
    have no Latin lookalike and pass through unchanged."""
    return re.sub(r"[^\w]", "", _skeleton(s), flags=re.UNICODE).casefold()


def _score_status(score: float) -> Optional[str]:
    """Convert a similarity score (0.0-1.0) to a status tier label.
    Returns None if below the review threshold (not a viable candidate)."""
    if score >= 0.95:
        return "auto"
    if score >= 0.80:
        return "likely"
    if score >= 0.65:
        return "review"
    return None


def _pair_similarity(detected_norm: str, nick_norm: str) -> float:
    """Score one detected-vs-roster pair, both already normalised.
    Returns 1.0 for exact match, then substring-containment boost, else
    SequenceMatcher ratio.
    """
    if detected_norm == nick_norm:
        return 1.0
    if nick_norm in detected_norm or detected_norm in nick_norm:
        shorter_len = min(len(nick_norm), len(detected_norm))
        longer_len = max(len(nick_norm), len(detected_norm))
        return max(0.85, shorter_len / longer_len)
    return difflib.SequenceMatcher(None, detected_norm, nick_norm).ratio()


def fuzzy_match_candidates(detected: str, roster: list[tuple[int, str]],
                           *, limit: int = 5, alliance_id: Optional[int] = None
                           ) -> list[tuple[int, str, float, str]]:
    """Top-N viable fuzzy matches `[(fid, nickname, score, status), ...]`, score
    descending (empty if none ≥ review threshold). A learned alias fills in when
    no direct match is confident, but a strong direct match always wins."""
    if not detected:
        return []
    scored = _score_against_roster(detected, roster)

    if alliance_id is not None and (not scored or scored[0][3] != "auto"):
        alias_fid = alias_lookup(alliance_id, detected, roster)
        if alias_fid is not None:
            nick = next((n for f, n in roster if f == alias_fid), "")
            scored = [(alias_fid, nick, 1.0, "auto")] + [c for c in scored if c[0] != alias_fid]

    # Previous-row bleed lands the real name as a trailing sub-phrase
    # ("L6 Morte2" → "Morte2", "SPAFDRT Leoz" → "Leoz"). Only when the full
    # string didn't match confidently, retry the trailing suffixes (longest
    # first) and adopt the first that matches at the strict `auto` threshold —
    # so a junk prefix doesn't sink an otherwise-real row.
    if not scored or scored[0][3] != "auto":
        tokens = detected.split()
        for start in range(1, len(tokens)):
            sub = _score_against_roster(" ".join(tokens[start:]), roster)
            if sub and sub[0][3] == "auto":
                return sub[:limit]

    return scored[:limit]


def _score_against_roster(detected: str, roster: list[tuple[int, str]]
                          ) -> list[tuple[int, str, float, str]]:
    """Score one name against every roster nick; viable candidates sorted by
    score descending."""
    detected_norm = _normalize_for_match(detected)
    if not detected_norm:
        return []
    scored: list[tuple[int, str, float, str]] = []
    for fid, nick in roster:
        nick_norm = _normalize_for_match(nick)
        if not nick_norm:
            continue
        score = _pair_similarity(detected_norm, nick_norm)
        status = _score_status(score)
        if status is None:
            continue
        scored.append((fid, nick, score, status))
    scored.sort(key=lambda c: -c[2])
    return scored


def assign_unique_fids(raw_rows: list[dict], roster: list[tuple[int, str]],
                       *, alliance_id: Optional[int] = None) -> list[dict]:
    """Greedy global fid assignment across rows competing for the same roster
    entries. Rows (need `name`/`value`) come back enriched with
    `fid`/`nickname`/`status`/`candidates`; higher-value rows win ties, and
    distinct same-named players resolve to distinct fids."""
    nick_by_fid = {f: n for f, n in roster}
    enriched: list[dict] = []
    for raw in raw_rows:
        cands = fuzzy_match_candidates(raw["name"], roster, alliance_id=alliance_id)
        enriched.append({
            **raw,
            "candidates": cands,
            "fid": None,
            "nickname": None,
            "status": "no_match",
        })

    # Higher-value row wins ties.
    row_priority = {
        idx: rank for rank, idx in enumerate(
            sorted(range(len(enriched)),
                   key=lambda i: -int(enriched[i].get("value") or 0))
        )
    }
    pool = [
        (score, row_priority[row_idx], row_idx, fid, nick, status)
        for row_idx, row in enumerate(enriched)
        for fid, nick, score, status in row["candidates"]
    ]
    pool.sort(key=lambda c: (-c[0], c[1]))

    assigned_fids: set[int] = set()
    for _score, _prio, row_idx, fid, nick, status in pool:
        row = enriched[row_idx]
        if row["fid"] is not None or fid in assigned_fids:
            continue
        row["fid"] = fid
        row["nickname"] = nick_by_fid.get(fid, nick)
        row["status"] = status
        assigned_fids.add(fid)
    return enriched


def fuzzy_match_name(detected: str, roster: list[tuple[int, str]],
                     *, alliance_id: Optional[int] = None) -> tuple[Optional[int], str]:
    """Single-best fuzzy match against the roster as `(fid, status)` — a
    convenience wrapper over `fuzzy_match_candidates` for callers that don't
    need multi-candidate dedup."""
    if not detected:
        return None, "no_name"
    if not _normalize_for_match(detected):
        return None, "no_name"
    candidates = fuzzy_match_candidates(detected, roster, limit=1, alliance_id=alliance_id)
    if not candidates:
        return None, "no_match"
    fid, _nick, _score, status = candidates[0]
    return fid, status


def update_users_power(fid: int, power: int, ts_iso: str) -> None:
    with sqlite3.connect("db/users.sqlite", timeout=30.0) as conn:
        conn.execute(
            "UPDATE users SET power = ?, power_updated_at = ? WHERE fid = ?",
            (power, ts_iso, fid),
        )
        conn.commit()


def update_users_combat_power(fid: int, combat_power: int, ts_iso: str) -> None:
    with sqlite3.connect("db/users.sqlite", timeout=30.0) as conn:
        conn.execute(
            "UPDATE users SET combat_power = ?, combat_power_updated_at = ? WHERE fid = ?",
            (combat_power, ts_iso, fid),
        )
        conn.commit()


# ── attendance session DB helpers ─────────────────────────────────────────

_ATT_DB = "db/attendance.sqlite"

# Min similarity to reuse a stored alias key when OCR drifts between screenshots.
_OCR_ALIAS_FUZZY_MIN = 0.92


def _init_ocr_alias_table() -> None:
    """Learned OCR→player aliases: maps the normalized OCR text of a name the
    bot can't read (decorated/homoglyph gamertags) to the player an admin
    resolved it to, so it only has to be fixed by hand once."""
    try:
        with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ocr_name_alias (
                    alliance_id INTEGER NOT NULL,
                    ocr_key     TEXT    NOT NULL,
                    fid         INTEGER NOT NULL,
                    raw_name    TEXT,
                    updated_at  TEXT,
                    PRIMARY KEY (alliance_id, ocr_key)
                )
            """)
            conn.commit()
    except Exception as e:
        logger.warning(f"AttendanceOCR: could not init ocr_name_alias table: {e}")


_init_ocr_alias_table()


def learn_name_alias(alliance_id, ocr_name, fid) -> None:
    """Remember that this alliance's OCR text `ocr_name` resolves to `fid`, so a
    decorated name only has to be matched by hand once. No-op for blank/short
    keys or non-roster (placeholder/negative) fids."""
    if not alliance_id or not fid or int(fid) <= 0 or not ocr_name:
        return
    key = _normalize_for_match(ocr_name)
    if len(key) < 2:  # too little signal to key on reliably
        return
    try:
        with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
            conn.execute("""
                INSERT INTO ocr_name_alias (alliance_id, ocr_key, fid, raw_name, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(alliance_id, ocr_key) DO UPDATE SET
                    fid = excluded.fid,
                    raw_name = excluded.raw_name,
                    updated_at = excluded.updated_at
            """, (int(alliance_id), key, int(fid), ocr_name,
                  datetime.now(timezone.utc).isoformat(timespec="seconds")))
            conn.commit()
        _ALIAS_CACHE.pop(int(alliance_id), None)
    except Exception as e:
        logger.warning(f"AttendanceOCR: could not learn alias for {ocr_name!r}: {e}")


_ALIAS_CACHE: dict[int, list] = {}


def _load_alliance_aliases(alliance_id: int) -> list:
    """Cached alias rows per alliance; invalidated by learn_name_alias on write."""
    aid = int(alliance_id)
    cached = _ALIAS_CACHE.get(aid)
    if cached is not None:
        return cached
    try:
        with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
            rows = conn.execute(
                "SELECT ocr_key, fid FROM ocr_name_alias WHERE alliance_id = ?",
                (aid,)).fetchall()
    except Exception as e:
        logger.warning(f"AttendanceOCR: alias load failed: {e}")
        return []
    _ALIAS_CACHE[aid] = rows
    return rows


def alias_lookup(alliance_id, detected, roster) -> Optional[int]:
    """A learned alias for `detected` → fid, or None. Exact key first, then a
    fuzzy pass over this alliance's keys (OCR drifts slightly between
    screenshots). Only returns members still on the roster."""
    if not alliance_id or not detected:
        return None
    key = _normalize_for_match(detected)
    if len(key) < 2:
        return None
    roster_fids = {f for f, _ in roster}
    rows = _load_alliance_aliases(alliance_id)
    for ocr_key, fid in rows:
        if ocr_key == key and fid in roster_fids:
            return fid
    best_fid, best_score = None, 0.0
    for ocr_key, fid in rows:
        if fid not in roster_fids:
            continue
        score = difflib.SequenceMatcher(None, key, ocr_key).ratio()
        if score > best_score:
            best_fid, best_score = fid, score
    return best_fid if best_score >= _OCR_ALIAS_FUZZY_MIN else None


def rematch_unmatched_rows(session_id: str, alliance_id: int) -> int:
    """Re-match this session's unmatched (placeholder) rows against the current
    roster + aliases, reattributing those that now match confidently and aren't
    already taken. Returns the count resolved (use after a roster sync)."""
    roster = load_alliance_roster(alliance_id)
    if not roster:
        return 0
    resolved = 0
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        rows = conn.execute(
            "SELECT player_id, player_name FROM attendance_records WHERE session_id = ?",
            (session_id,)).fetchall()
        taken: set[int] = set()
        unmatched: list[tuple[str, str]] = []
        for pid, pname in rows:
            try:
                ipid = int(pid)
            except (TypeError, ValueError):
                continue
            if ipid > 0:
                taken.add(ipid)
            else:
                unmatched.append((pid, pname or ""))
        for pid, pname in unmatched:
            fid, status = fuzzy_match_name(pname, roster, alliance_id=alliance_id)
            if fid is None or status != "auto" or fid in taken:
                continue  # only reattribute on a confident, unambiguous match
            nick = next((n for f, n in roster if f == fid), None)
            conn.execute(
                "UPDATE attendance_records SET player_id = ?, player_name = ? "
                "WHERE session_id = ? AND player_id = ?",
                (str(fid), nick or pname, session_id, pid))
            taken.add(fid)
            resolved += 1
        conn.commit()
    return resolved


def _record_session(*, session_id, event_type, event_date, date_confidence,
                    event_subtype, alliance_id, awaiting_result=1):
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        conn.execute(
            "INSERT INTO attendance_sessions "
            "(session_id, event_type, event_date, event_date_confidence, "
            " event_subtype, alliance_id, awaiting_result, origin) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'ocr')",
            (session_id, event_type,
             event_date.isoformat() if event_date else None,
             date_confidence, event_subtype, alliance_id, awaiting_result),
        )
        conn.commit()


def _find_closed_session(*, event_type, event_date, event_subtype, alliance_id) -> Optional[str]:
    """Return the session_id of an already-closed session matching this event,
    or None. Lets re-uploads of a finished event reopen its review."""
    if event_date is None:
        return None
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        row = conn.execute(
            "SELECT session_id FROM attendance_sessions "
            "WHERE event_type = ? AND alliance_id = ? AND awaiting_result = 0 "
            "AND COALESCE(event_subtype, '') = COALESCE(?, '') "
            "AND ABS(julianday(event_date) - julianday(?)) <= 1 "
            "ORDER BY ABS(julianday(event_date) - julianday(?)) LIMIT 1",
            (event_type, alliance_id, event_subtype,
             event_date.isoformat(), event_date.isoformat()),
        ).fetchone()
    return row[0] if row else None


def _find_open_session(*, event_type, event_date, event_subtype, alliance_id) -> Optional[str]:
    """Read-only lookup of an OPEN session_id matching this event (or None), so a
    result upload picks up the registered roster from its registration session."""
    if event_date is None:
        return None
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        row = conn.execute(
            "SELECT session_id FROM attendance_sessions "
            "WHERE event_type = ? AND alliance_id = ? AND awaiting_result = 1 "
            "AND COALESCE(event_subtype, '') = COALESCE(?, '') "
            "AND ABS(julianday(event_date) - julianday(?)) <= 3 "
            "ORDER BY ABS(julianday(event_date) - julianday(?)) LIMIT 1",
            (event_type, alliance_id, event_subtype,
             event_date.isoformat(), event_date.isoformat()),
        ).fetchone()
    return row[0] if row else None


def _load_existing_session_data(session_id: str) -> dict:
    """Read everything previously stored for a session: header, scoreboard,
    stats, MVPs, player rows. Returns a dict matching the fields a
    `_PointsSession` would populate from a fresh OCR pass.
    """
    out: dict = {
        "alliance_rank": None,
        "event_time": None,
        "event_date": None,
        "event_subtype": None,
        "alliance_scores": [],
        "stats": {},
        "mvps": [],
        "rows": [],                # status='present'
        "registered_rows": [],     # status='registered' — pre-event combatants
    }
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        hdr = conn.execute(
            "SELECT alliance_rank, event_time, event_date, event_subtype "
            "FROM attendance_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if hdr is None:
            return out
        out["alliance_rank"] = hdr[0]
        out["event_time"] = hdr[1]
        if hdr[2]:
            try:
                out["event_date"] = date.fromisoformat(hdr[2])
            except ValueError:
                out["event_date"] = None
        out["event_subtype"] = hdr[3]

        out["alliance_scores"] = [
            {"rank": r[0], "legion": r[1], "tag": r[2],
             "name": r[3], "score": r[4]}
            for r in conn.execute(
                "SELECT rank, legion, tag, name, score FROM attendance_session_scoreboard "
                "WHERE session_id = ? ORDER BY rank",
                (session_id,),
            ).fetchall()
        ]

        out["stats"] = {
            k: v for k, v in conn.execute(
                "SELECT stat_key, stat_value FROM attendance_session_stats "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        }

        out["mvps"] = [
            {"stat_key": r[0], "name": r[1], "value": r[2],
             "fid_str": r[3]}  # carry forward for fidelity; usually re-resolved at submit
            for r in conn.execute(
                "SELECT stat_key, mvp_name, mvp_value, mvp_fid FROM attendance_session_mvps "
                "WHERE session_id = ? ORDER BY stat_key",
                (session_id,),
            ).fetchall()
        ]

        out["rows"] = [
            {"fid": int(r[0]) if r[0] and str(r[0]).lstrip("-").isdigit() else None,
             "name": r[1], "value": int(r[2]) if r[2] is not None else 0}
            for r in conn.execute(
                "SELECT player_id, player_name, points FROM attendance_records "
                "WHERE session_id = ? AND status IN ('present', 'absent') "
                "ORDER BY points DESC",
                (session_id,),
            ).fetchall()
        ]
        out["registered_rows"] = [
            {"fid": int(r[0]) if r[0] and str(r[0]).lstrip("-").isdigit() else None,
             "name": r[1], "value": int(r[2]) if r[2] is not None else 0}
            for r in conn.execute(
                "SELECT player_id, player_name, points FROM attendance_records "
                "WHERE session_id = ? AND status = 'registered' ORDER BY points DESC",
                (session_id,),
            ).fetchall()
        ]
    return out


def _find_or_create_session(*, event_type, event_date, event_subtype, alliance_id,
                            date_confidence):
    if event_date is None:
        sid = str(uuid.uuid4())
        _record_session(
            session_id=sid, event_type=event_type, event_date=event_date,
            date_confidence=date_confidence, event_subtype=event_subtype,
            alliance_id=alliance_id,
        )
        return sid
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        row = conn.execute(
            "SELECT session_id FROM attendance_sessions "
            "WHERE event_type = ? AND alliance_id = ? AND awaiting_result = 1 "
            "AND COALESCE(event_subtype, '') = COALESCE(?, '') "
            "AND ABS(julianday(event_date) - julianday(?)) <= 2 "
            "ORDER BY ABS(julianday(event_date) - julianday(?)) LIMIT 1",
            (event_type, alliance_id, event_subtype,
             event_date.isoformat(), event_date.isoformat()),
        ).fetchone()
    if row:
        return row[0]
    sid = str(uuid.uuid4())
    _record_session(
        session_id=sid, event_type=event_type, event_date=event_date,
        date_confidence=date_confidence, event_subtype=event_subtype,
        alliance_id=alliance_id,
    )
    return sid


def _ocr_session_label(event_date, event_subtype) -> str:
    """Friendly prefix shown in the legacy Attendance UI's session list. The
    legacy UI appends ` [event_type]` after this, so we deliberately omit the
    event_type to avoid the duplicated `canyon_clash 2026-05-16 [canyon_clash]`."""
    bits = []
    if event_subtype:
        bits.append(event_subtype)
    if event_date:
        bits.append(event_date.isoformat() if hasattr(event_date, "isoformat") else str(event_date))
    return " · ".join(bits) if bits else "Screenshot Upload"


def _record_attendance_row(*, session_id, event_type, event_date, event_subtype,
                           alliance_id, fid, name, status, points,
                           alliance_rank=None):
    session_name = _ocr_session_label(event_date, event_subtype)
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO attendance_records "
            "(session_id, session_name, event_type, event_date, player_id, player_name, "
            " alliance_id, alliance_name, status, points, event_subtype, alliance_rank) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (session_id, session_name, event_type,
             event_date.isoformat() if event_date else None,
             str(fid), name, str(alliance_id), "", status, points,
             event_subtype, alliance_rank),
        )
        conn.commit()


def _upsert_attendance_row(*, session_id, event_type, event_date, event_subtype,
                           alliance_id, fid, name, status, points):
    session_name = _ocr_session_label(event_date, event_subtype)
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        existing = conn.execute(
            "SELECT 1 FROM attendance_records WHERE session_id = ? AND player_id = ?",
            (session_id, str(fid)),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE attendance_records SET status = ?, points = ?, "
                "player_name = ?, event_subtype = COALESCE(event_subtype, ?) "
                "WHERE session_id = ? AND player_id = ?",
                (status, points, name, event_subtype, session_id, str(fid)),
            )
        else:
            conn.execute(
                "INSERT INTO attendance_records "
                "(session_id, session_name, event_type, event_date, player_id, player_name, "
                " alliance_id, alliance_name, status, points, event_subtype, was_walk_in) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (session_id, session_name, event_type,
                 event_date.isoformat() if event_date else None,
                 str(fid), name, str(alliance_id), "", status, points,
                 event_subtype, 1),
            )
        conn.commit()


def _mark_registered_as_absent(session_id: str, except_fids: set[int]) -> list[dict]:
    no_shows = []
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        rows = conn.execute(
            "SELECT player_id, player_name FROM attendance_records "
            "WHERE session_id = ? AND status = 'registered'",
            (session_id,),
        ).fetchall()
        for pid, pname in rows:
            try:
                fid = int(pid)
            except (TypeError, ValueError):
                continue
            if fid in except_fids:
                continue
            no_shows.append({"fid": fid, "name": pname})
            conn.execute(
                "UPDATE attendance_records SET status = 'absent' "
                "WHERE session_id = ? AND player_id = ?",
                (session_id, pid),
            )
        conn.commit()
    return no_shows


def _close_session(session_id: str) -> None:
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        conn.execute(
            "UPDATE attendance_sessions SET awaiting_result = 0, "
            "closed_at = CURRENT_TIMESTAMP WHERE session_id = ?",
            (session_id,),
        )
        conn.commit()


def delete_session(session_id: str) -> None:
    """Remove a session and all its rows (records, scoreboard, stats, MVPs).
    users.power/combat_power are independent and left untouched."""
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        for tbl in ("attendance_records", "attendance_session_scoreboard",
                    "attendance_session_stats", "attendance_session_mvps",
                    "attendance_sessions"):
            conn.execute(f"DELETE FROM {tbl} WHERE session_id = ?", (session_id,))
        conn.commit()


def _unmatched_id_floor(session_id: str) -> int:
    """Most-negative existing player_id for this session, or 0 if none. Callers
    decrement from this to allocate a fresh per-session placeholder id."""
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        row = conn.execute(
            "SELECT MIN(CAST(player_id AS INTEGER)) FROM attendance_records "
            "WHERE session_id = ? AND CAST(player_id AS INTEGER) < 0",
            (session_id,),
        ).fetchone()
    return row[0] if row and row[0] is not None else 0


# ── base session class ────────────────────────────────────────────────────

class OcrUploadSession:
    """Generic OCR upload session: collects screenshots, runs subclass-provided parsing/review."""

    event_label = "OCR Upload"

    def __init__(self, *, cog, channel: discord.TextChannel, uploader_id: int,
                 event_type: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS):
        self.cog = cog
        self.channel = channel
        self.uploader_id = uploader_id
        self.event_type = event_type
        self.timeout_seconds = timeout_seconds

        self.image_attachments: list[discord.Attachment] = []
        self.progress_message: Optional[discord.Message] = None
        self.cancelled = False
        self.finalized = False
        self._timer_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        # Progress counters mirror bear_track so the embed can show "image X of Y"
        # while parsing — total bumps before the lock so concurrent uploads
        # immediately update the visible total instead of after the current batch.
        self.known_total_images = 0
        self.processed_images = 0
        self.current_image_idx: Optional[int] = None
        self.current_phase: Optional[str] = None   # 'ocr' or 'fallback'
        self.current_lang: Optional[str] = None

    async def start(self, attachments: list[discord.Attachment], *, status_message=None):
        if status_message is not None:
            # Reuse the "Reading your screenshots…" ack already shown to the
            # uploader, updating it in place instead of posting a second embed.
            self.progress_message = status_message
            await self.render_progress()
        else:
            self.progress_message = await self.channel.send(
                embed=self.build_progress_embed(),
                view=_ProgressView(self),
            )
        await self.add_attachments(attachments)
        self.restart_timer()

    async def add_attachments(self, attachments: list[discord.Attachment]):
        self.known_total_images += len(attachments)
        if self.progress_message is not None:
            await self.render_progress()
        async with self._lock:
            self.image_attachments.extend(attachments)
            await self._process_attachments(attachments)
            self.restart_timer()
            self.current_image_idx = None
            self.current_phase = None
            self.current_lang = None
            await self.render_progress()

    async def finalize(self, *, timed_out: bool = False):
        if self.finalized or self.cancelled:
            return
        self.finalized = True
        self.delete_snapshot()
        self.stop_timer()
        try:
            # Wait for any in-flight batch so we render the full parsed set, not a
            # partial one (e.g. Done clicked while a second batch is still OCRing).
            async with self._lock:
                await self.render_review(timed_out=timed_out)
        except Exception:
            logger.exception("OcrUploadSession: failed to render review")

    async def cancel(self, *, by_user: bool = False):
        if self.finalized or self.cancelled:
            return
        self.cancelled = True
        self.delete_snapshot()
        self.stop_timer()
        if self.progress_message:
            try:
                await self.progress_message.edit(
                    embed=discord.Embed(
                        title=f"{theme.deniedIcon} Upload cancelled",
                        description="Session cancelled by user." if by_user else "Session timed out with no images.",
                        color=theme.emColor2,
                    ),
                    view=None,
                )
            except Exception:
                pass

    def restart_timer(self):
        self.stop_timer()
        self._timer_task = asyncio.create_task(self._timer_run())

    def stop_timer(self):
        if self._timer_task and not self._timer_task.done():
            self._timer_task.cancel()
        self._timer_task = None

    async def _timer_run(self):
        try:
            await asyncio.sleep(self.timeout_seconds)
            if not self.finalized and not self.cancelled:
                await self.finalize(timed_out=True)
        except asyncio.CancelledError:
            pass

    async def render_progress(self):
        if self.progress_message is None:
            return
        try:
            await self.progress_message.edit(
                embed=self.build_progress_embed(),
                view=_ProgressView(self),
            )
        except discord.NotFound:
            self.progress_message = None

    def build_progress_embed(self) -> discord.Embed:
        label = EVENT_TYPES[self.event_type].label if self.event_type in EVENT_TYPES else self.event_type
        if self.current_image_idx is not None and self.known_total_images:
            bar = self._progress_bar(self.current_image_idx, self.known_total_images)
            phase_label = "fallback OCR" if self.current_phase == 'fallback' else "running OCR"
            lang_label = self._short_lang_label(self.current_lang)
            lang_part = f" ({lang_label})" if lang_label else ""
            progress_line = (
                f"{bar} **`{self.current_image_idx} of {self.known_total_images}`** "
                f"{theme.processingIcon} {phase_label}{lang_part}…\n"
            )
        else:
            done = min(self.processed_images, self.known_total_images)
            progress_line = (
                f"**{theme.importIcon} Images processed:** "
                f"`{done} of {self.known_total_images}`\n"
            )
        embed = discord.Embed(
            title=f"{theme.documentIcon} Screenshot Upload — {label}",
            description=(
                f"{theme.upperDivider}\n"
                f"**{theme.userIcon} Uploader:** <@{self.uploader_id}>\n"
                f"{progress_line}"
                f"{self.extra_progress_lines()}"
                f"{theme.lowerDivider}\n"
            ),
            color=theme.emColor1,
        )
        from . import onnx_lifecycle  # low-mem OCR is slow; reassure the uploader
        if onnx_lifecycle.LOW_MEM_MODE and self.current_image_idx is not None:
            embed.set_footer(text="Please wait, this can take a while...")
        return embed

    async def _phase_callback(self, phase: str, lang: str):
        """Update the live OCR phase/language so the progress embed shows which
        language pass is running (mirrors bear_track's indicator)."""
        self.current_phase = phase
        self.current_lang = lang
        await self.render_progress()

    @staticmethod
    def _progress_bar(current: int, total: int) -> str:
        width = max(min(total, 12), 6)
        filled = max(0, min(width, round(current / total * width))) if total else 0
        return "▰" * filled + "▱" * (width - filled)

    @staticmethod
    def _short_lang_label(lang: Optional[str]) -> str:
        if not lang:
            return ""
        return {"en": "English"}.get(lang, lang.title())

    # --- Crash resume: snapshot parsed state after each image, restore on restart ---
    _SNAPSHOT_SKIP = {'cog', 'channel', 'progress_message', '_timer_task', '_lock',
                      'image_attachments', 'session_view'}

    def _snapshot_key(self) -> str:
        return f"att:{self.channel.id}:{self.uploader_id}"

    def snapshot_payload(self) -> dict:
        import datetime as _dt
        import json as _json
        p: dict = {'channel_id': self.channel.id}
        for k, v in self.__dict__.items():
            if k in self._SNAPSHOT_SKIP:
                continue
            if isinstance(v, (_dt.date, _dt.datetime)):
                p[k] = {'__date__': v.isoformat()}
            elif isinstance(v, (str, int, float, bool, type(None), list, dict)):
                try:
                    _json.dumps(v)
                    p[k] = v
                except Exception:
                    pass
        return p

    def restore_payload(self, p: dict) -> None:
        import datetime as _dt
        for k, v in p.items():
            if k == 'channel_id':
                continue
            if isinstance(v, dict) and '__date__' in v:
                try:
                    v = _dt.date.fromisoformat(v['__date__'])
                except Exception:
                    try:
                        v = _dt.datetime.fromisoformat(v['__date__'])
                    except Exception:
                        continue
            setattr(self, k, v)

    def save_snapshot(self) -> None:
        from . import ocr_resume
        ocr_resume.save(self._snapshot_key(), 'attendance', self.snapshot_payload())

    def delete_snapshot(self) -> None:
        from . import ocr_resume
        ocr_resume.delete(self._snapshot_key())

    async def resume(self) -> None:
        """Re-post the progress message for a session recovered after a restart."""
        kept = len(getattr(self, 'rows', None) or []) + len(getattr(self, 'result_rows', None) or []) \
            + len(getattr(self, 'registered_rows', None) or [])
        embed = discord.Embed(
            title=f"{theme.documentIcon} Recovered your upload",
            description=(
                f"{theme.upperDivider}\n"
                f"The bot restarted mid-upload. **{kept}** parsed entr{'y' if kept == 1 else 'ies'} kept.\n"
                f"Click **Done Uploading** to review and submit, or re-upload any screenshots that "
                f"weren't processed yet.\n"
                f"{theme.lowerDivider}\n"
            ),
            color=theme.emColor1,
        )
        self.progress_message = await self.channel.send(embed=embed, view=_ProgressView(self))
        self.restart_timer()

    def extra_progress_lines(self) -> str:
        return ""

    async def _process_attachments(self, attachments: list[discord.Attachment]):
        raise NotImplementedError

    async def render_review(self, *, timed_out: bool):
        raise NotImplementedError


async def _safe_defer(interaction: discord.Interaction) -> None:
    """Best-effort interaction ack: the 3s window can lapse while parsing, and
    `finalize`/`cancel` edit the progress message directly, so a failed ack must
    never abort the action."""
    try:
        await interaction.response.defer()
    except (discord.HTTPException, discord.InteractionResponded):
        pass


class _ProgressView(discord.ui.View):
    def __init__(self, session: OcrUploadSession):
        # Long timeout so the Done Uploading button stays clickable while
        # the admin reads through the parsed counts before deciding.
        super().__init__(timeout=7200)
        self.session = session

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.session.uploader_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the uploader can finalize this session.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Done Uploading", style=discord.ButtonStyle.success,
                       emoji=f"{theme.verifiedIcon}")
    async def done(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await _safe_defer(interaction)
        await self.session.finalize(timed_out=False)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger,
                       emoji=f"{theme.deniedIcon}")
    async def cancel(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await _safe_defer(interaction)
        await self.session.cancel(by_user=True)

    async def on_timeout(self):
        """Collapse a stuck progress message into a small expiry notice
        instead of leaving a long counts-list with dead buttons."""
        sess = self.session
        if sess.finalized or sess.cancelled or sess.progress_message is None:
            return
        try:
            await sess.progress_message.edit(
                embed=discord.Embed(
                    title=f"{theme.warnIcon} Upload session expired",
                    description=(
                        "No activity for a while, so this upload was "
                        "discarded. **Upload the screenshots again** to "
                        "start a new session."
                    ),
                    color=theme.emColor2,
                ),
                view=None,
            )
        except (discord.NotFound, discord.HTTPException):
            pass
        sess.cog.end_session(sess.channel.id, sess.uploader_id)


# ── shared review/summary embed ───────────────────────────────────────────

# ── Power Rankings session ────────────────────────────────────────────────

class PowerRankingsSession(OcrUploadSession):
    """Power Rankings: stitch scroll screenshots, read Power (multi-language),
    then route through the shared review as a power snapshot (writes users.power,
    no attendance). Rows dedup by value + overlapping name across the stitched
    scroll screenshots."""

    db_event_type = "power_rankings"
    registration_value_label = "Power"   # unused — no sign-up phase
    result_value_label = "Power"
    simple_results = True                # no registration / present-absent concept
    power_only_snapshot = True           # Submit updates users.power, writes no attendance

    def __init__(self, *, alliance_id: int, **kwargs):
        super().__init__(**kwargs)
        self.alliance_id = alliance_id
        self.rows: list[dict] = []
        self.result_rows: list[dict] = []
        self.registered_rows: list[dict] = []
        # A power snapshot reflects the roster's power right now — default to
        # today (editable via Set Date); the screens carry no event date.
        self.detected_date = datetime.now(timezone.utc).date()
        self.detected_legion = None
        self.detected_time = None
        self.date_confidence = None
        self.alliance_rank: Optional[int] = None
        # Empty so the shared review/persist treats this as a plain result-only
        # event (no scoreboard / battle stats / MVPs).
        self.alliance_scores: list = []
        self.stats: dict = {}
        self.mvps: list = []

    async def _process_attachments(self, attachments: list[discord.Attachment]):
        roster = load_alliance_roster(self.alliance_id)
        for att in attachments:
            self.current_image_idx = self.processed_images + 1
            await self.render_progress()
            try:
                data = await att.read()
                rows, _text = await ocr_value_rows(
                    data, roster=roster, alliance_id=self.alliance_id,
                    parse=_parse_power_rows, progress_callback=self._phase_callback)
            except Exception:
                self.processed_images += 1
                continue
            # Dedup by value + overlapping name (keep distinct same-power players).
            for row in rows:
                _dedup_into(self.rows, row)
            self.processed_images += 1
            await self.render_progress()
            self.save_snapshot()
        # Sorted by power desc — the canonical ranking — for the review.
        self.result_rows = [
            {"name": r["name"], "value": r["power"]}
            for r in sorted(self.rows, key=lambda r: -r["power"])
        ]

    def extra_progress_lines(self) -> str:
        return f"**{theme.listIcon} Players parsed so far:** `{len(self.rows)}`\n"

    async def render_review(self, *, timed_out: bool):
        from .attendance_ocr_review import EventReviewView
        view = EventReviewView(
            self,
            registration_value_label=self.registration_value_label,
            result_value_label=self.result_value_label,
        )
        if self.progress_message:
            try:
                await self.progress_message.edit(embed=view.build_embed(), view=view)
            except discord.NotFound:
                pass


def _parse_power_rows(text: str) -> list[dict]:
    """Parse Power Rankings rows from OCR text. The top 3 ranks display medal
    icons (no rank digit in the OCR), so a leading rank digit is optional;
    rank is None when not found.
    """
    rows = []
    text = _trim_to_data_section(text)
    prev_end = 0
    for start, end, value in find_formatted_numbers(text):
        chunk = text[prev_end:start].strip()
        prev_end = end
        if value < 1_000_000:
            continue
        rank = None
        m = _RANK_PREFIX_RE.match(chunk)
        if m:
            try:
                rank = int(m.group(1))
            except ValueError:
                rank = None
            if rank is not None and rank > 1000:
                continue
            chunk = chunk[m.end():].strip()
        chunk = re.sub(r"\bR\d+\b", "", chunk)
        tokens = [
            t for t in chunk.split()
            if t.lower() not in _PARSE_STOPWORDS
            and any(c.isalpha() for c in t)
            and len(t) >= 2
        ]
        if not tokens:
            continue
        # Full trailing name (no last-3 cap), with previous-row bleed split off
        # at a garbled-number boundary.
        name = _name_from_tokens(tokens)
        if len(name) < 2:
            continue
        # `value` mirrors `power` so the shared OCR fallback merge/match (keyed on
        # name+value) works; `power` is kept for the session's dedup + persistence.
        rows.append({"rank": rank, "name": name, "power": value, "value": value})
    return rows


# ── Foundry / Canyon unified session: registration + result in one ────────

class _PointsSession(OcrUploadSession):
    """Unified Foundry/Canyon session. Collects registration + result screenshots
    in one upload and routes each parsed row into `registered_rows` or
    `result_rows` by per-attachment kind detection."""

    db_event_type: str = ""
    registration_value_label: str = "Power"
    result_value_label: str = "Points"
    update_fn_registration = staticmethod(lambda fid, val, ts: None)

    def __init__(self, *, alliance_id: int, **kwargs):
        super().__init__(**kwargs)
        self.alliance_id = alliance_id
        self.registered_rows: list[dict] = []
        self.result_rows: list[dict] = []
        self._last_kind: Optional[str] = None
        self.detected_date = None
        self.detected_legion = None
        self.detected_time: Optional[str] = None
        self.date_confidence = None
        self.alliance_rank: Optional[int] = None
        self.alliance_scores: list[dict] = []
        self.stats: dict[str, int] = {}
        self.mvps: list[dict] = []

    async def _process_attachments(self, attachments: list[discord.Attachment]):
        from . import bear_track
        roster = load_alliance_roster(self.alliance_id)
        for att in attachments:
            self.current_image_idx = self.processed_images + 1
            await self.render_progress()
            try:
                data = await att.read()
                await self._phase_callback('ocr', bear_track.DEFAULT_OCR_LANG)
                blocks = await bear_track.ocr_bytes_with_boxes(
                    data, lang=bear_track.DEFAULT_OCR_LANG)
                text = " ".join(t for t, _ in blocks)
            except Exception:
                self.processed_images += 1
                continue

            # Scroll pages match no fingerprint — inherit the last classified kind.
            kind = detect_kind(self.db_event_type, text)
            if kind is None:
                kind = self._last_kind or "result"
            else:
                self._last_kind = kind

            if self.detected_date is None:
                d = extract_header_date(text)
                if d:
                    self.detected_date, self.date_confidence = resolve_event_date(
                        d, self.db_event_type, registration=(kind == "registration"))
            if self.detected_legion is None:
                self.detected_legion = extract_legion(text)

            target = self.result_rows if kind == "result" else self.registered_rows
            header_page = False
            if kind == "result":
                self._merge_result_metadata(text, blocks=blocks)
                header_page = _is_result_header_page(text)
            # Header pages would yield bogus rows from alliance-level totals.
            if not header_page:
                # Multi-language fallback so non-Latin names (e.g. Arabic) read
                # correctly instead of OCRing as garbage that splits one player
                # into duplicate unmatched rows across overlapping screenshots.
                # Tail 0-capture only for results (0 = absent); registration CP is
                # always present so a tail row there is just a misread, not a 0.
                parse = (lambda t: _parse_player_value_rows(t, capture_tail=True)) \
                    if kind == "result" else _parse_player_value_rows
                try:
                    rows, _ = await ocr_value_rows(
                        data, roster=roster, alliance_id=self.alliance_id,
                        parse=parse, primary_text=text, progress_callback=self._phase_callback)
                except Exception:
                    rows = parse(text)
                for row in rows:
                    _dedup_into(target, row)
            self.processed_images += 1
            await self.render_progress()
            self.save_snapshot()

    def _merge_result_metadata(self, text: str, blocks: Optional[list] = None):
        """Capture alliance rank / scoreboard / stats / MVPs from a result-mail header."""
        if self.alliance_rank is None:
            m = _ALLIANCE_RANK_RE.search(text)
            if m:
                try:
                    self.alliance_rank = int(m.group(1))
                except ValueError:
                    pass
            else:
                # Foundry has no "ranked No. N" — derive 1/2 from win/loss wording.
                self.alliance_rank = _foundry_rank_from_outcome(text)
        if not self.alliance_scores:
            scoreboard = []
            if blocks:
                scoreboard = _parse_alliance_scoreboard_spatial(blocks)
            if not scoreboard:
                scoreboard = _parse_alliance_scoreboard(text)
            if scoreboard:
                self.alliance_scores = scoreboard
        if not self.stats:
            parsed_stats = _parse_stats_panel(text)
            if parsed_stats:
                self.stats = parsed_stats
        if not self.mvps:
            parsed_mvps = _parse_mvps(text)
            if parsed_mvps:
                self.mvps = parsed_mvps

    def extra_progress_lines(self) -> str:
        # Order: event identity first (legion / date / rank), then row counts —
        # the identity gives the user something stable to look at while OCR
        # is still streaming new row counts beneath it.
        bits = []
        if self.detected_legion:
            bits.append(f"**{theme.shieldIcon} Legion:** `{self.detected_legion}`")
        if self.detected_date:
            bits.append(f"**{theme.calendarIcon} Event date:** `{self.detected_date.isoformat()}`")
        if self.alliance_rank is not None:
            bits.append(f"**{theme.crownIcon} Alliance rank:** `No. {self.alliance_rank}`")
        if self.registered_rows:
            bits.append(f"**{theme.listIcon} Registered:** `{len(self.registered_rows)}`")
        if self.result_rows:
            bits.append(f"**{theme.listIcon} Results:** `{len(self.result_rows)}`")
        return "\n".join(bits) + "\n" if bits else ""

    async def render_review(self, *, timed_out: bool):
        from .attendance_ocr_review import EventReviewView

        # Enrichment paths: re-uploading a CLOSED session, OR uploading
        # registration AFTER the result already closed the event (out-of-order)
        # — both merge into that session. Uploading the result that closes an
        # OPEN registration is the common in-order workflow.
        existing_closed_id = (
            _find_closed_session(
                event_type=self.db_event_type,
                event_date=self.detected_date,
                event_subtype=self.detected_legion,
                alliance_id=self.alliance_id,
            ) if (self.result_rows or self.registered_rows) else None
        )
        existing_open_id = None
        if existing_closed_id is None and self.result_rows:
            existing_open_id = _find_open_session(
                event_type=self.db_event_type,
                event_date=self.detected_date,
                event_subtype=self.detected_legion,
                alliance_id=self.alliance_id,
            )

        if existing_closed_id:
            self._merge_existing(_load_existing_session_data(existing_closed_id))
        elif existing_open_id:
            self._merge_open_registration(_load_existing_session_data(existing_open_id))

        view = EventReviewView(
            self,
            registration_value_label=self.registration_value_label,
            result_value_label=self.result_value_label,
            existing_session_id=existing_closed_id,
            enriching_open_session_id=existing_open_id,
        )
        if self.progress_message:
            try:
                await self.progress_message.edit(embed=view.build_embed(), view=view)
            except discord.NotFound:
                pass

    def _merge_existing(self, existing: dict):
        """Overlay newly-OCR'd data on top of an existing CLOSED session's data; new wins."""
        if self.alliance_rank is None:
            self.alliance_rank = existing.get("alliance_rank")
        if self.detected_time is None:
            self.detected_time = existing.get("event_time")
        if self.detected_date is None:
            self.detected_date = existing.get("event_date")
        if self.detected_legion is None:
            self.detected_legion = existing.get("event_subtype")

        if not self.alliance_scores:
            self.alliance_scores = existing.get("alliance_scores", [])
        if not self.stats:
            self.stats = existing.get("stats", {})
        if not self.mvps:
            self.mvps = existing.get("mvps", [])

        merged = list(self.result_rows)
        seen_fids = {r["fid"] for r in self.result_rows if r.get("fid")}
        seen_names = {
            _normalize_for_match(r.get("name") or "")
            for r in self.result_rows if not r.get("fid")
        }
        for r in existing.get("rows", []):
            fid = r.get("fid")
            name_key = _normalize_for_match(r.get("name") or "")
            if fid and fid in seen_fids:
                continue
            if not fid and name_key in seen_names:
                continue
            merged.append({"name": r.get("name") or "", "value": int(r.get("value") or 0)})
        self.result_rows = merged

    def _merge_open_registration(self, existing: dict):
        """A result mail for an event with registration already on file: pull the
        registered roster + header fields from the DB so the review is complete
        and submit closes that session instead of forking a parallel one."""
        if self.alliance_rank is None:
            self.alliance_rank = existing.get("alliance_rank")
        if self.detected_time is None:
            self.detected_time = existing.get("event_time")
        if self.detected_date is None:
            self.detected_date = existing.get("event_date")
        if self.detected_legion is None:
            self.detected_legion = existing.get("event_subtype")

        if not self.registered_rows:
            self.registered_rows = [
                {"name": r.get("name") or "", "value": int(r.get("value") or 0)}
                for r in existing.get("registered_rows", [])
            ]


class FoundryBattleSession(_PointsSession):
    db_event_type = "foundry_battle"
    registration_value_label = "Combat Power"
    result_value_label = "Personal Arsenal Points"
    update_fn_registration = staticmethod(update_users_combat_power)


class CanyonClashSession(_PointsSession):
    db_event_type = "canyon_clash"
    registration_value_label = "Power"
    result_value_label = "Personal Points"
    update_fn_registration = staticmethod(update_users_power)


# ── Alliance Showdown session ─────────────────────────────────────────────

_ALLIANCE_RANK_RE = re.compile(
    r"(?:rank(?:ed|ing)?\s+No\.?\s+|Alliance\s+Rank(?:ing)?[:\s]+)(\d{1,3})",
    re.IGNORECASE,
)
_FOUNDRY_WIN_RE = re.compile(r"\b(?:prevailed|victory|congratulations)\b", re.IGNORECASE)
_FOUNDRY_LOSS_RE = re.compile(r"\b(?:defeat(?:ed)?|unfortunately|better\s+luck)\b", re.IGNORECASE)


def _foundry_rank_from_outcome(text: str):
    """Foundry has no 'ranked No. N' line — win=1, loss=2, None if neither."""
    if _FOUNDRY_WIN_RE.search(text):
        return 1
    if _FOUNDRY_LOSS_RE.search(text):
        return 2
    return None


class AllianceShowdownSession(OcrUploadSession):
    """Alliance Showdown final-rankings parser: per-player points, routed through
    the shared review (result-only) for the same fix-at-upload + edit-later
    lifecycle as Foundry/Canyon."""

    db_event_type = "alliance_showdown"
    registration_value_label = "Power"        # unused — Showdown has no sign-up phase
    result_value_label = "Showdown Points"
    simple_results = True                      # no registration / present-absent concept

    def __init__(self, *, alliance_id: int, **kwargs):
        super().__init__(**kwargs)
        self.alliance_id = alliance_id
        self.registered_rows: list[dict] = []
        self.result_rows: list[dict] = []
        self.detected_date = None
        self.detected_legion = None
        self.detected_time = None
        self.date_confidence = None
        self.alliance_rank: Optional[int] = None
        # Empty so the shared review/persist treats this as a plain result-only
        # event (no scoreboard / battle stats / MVPs).
        self.alliance_scores: list = []
        self.stats: dict = {}
        self.mvps: list = []

    async def _process_attachments(self, attachments: list[discord.Attachment]):
        roster = load_alliance_roster(self.alliance_id)
        for att in attachments:
            self.current_image_idx = self.processed_images + 1
            await self.render_progress()
            try:
                data = await att.read()
                rows, text = await ocr_value_rows(
                    data, roster=roster, alliance_id=self.alliance_id,
                    progress_callback=self._phase_callback)
            except Exception:
                self.processed_images += 1
                continue
            if self.detected_date is None:
                d = extract_header_date(text)
                if d:
                    self.detected_date, self.date_confidence = resolve_event_date(d, self.event_type)
            # NOTE: the mail's "ranking No. N" is the *recipient's* personal rank,
            # not an alliance-vs-alliance rank — irrelevant, so we don't capture it.
            for row in rows:
                _dedup_into(self.result_rows, row)
            self.processed_images += 1
            await self.render_progress()
            self.save_snapshot()

    def extra_progress_lines(self) -> str:
        bits = [f"**{theme.listIcon} Players parsed:** `{len(self.result_rows)}`"]
        if self.detected_date:
            bits.append(f"**{theme.calendarIcon} Event date:** `{self.detected_date.isoformat()}`")
        return "\n".join(bits) + "\n"

    async def render_review(self, *, timed_out: bool):
        from .attendance_ocr_review import EventReviewView
        view = EventReviewView(
            self,
            registration_value_label=self.registration_value_label,
            result_value_label=self.result_value_label,
        )
        if self.progress_message:
            try:
                await self.progress_message.edit(embed=view.build_embed(), view=view)
            except discord.NotFound:
                pass


# ── factory ───────────────────────────────────────────────────────────────

SESSION_CLASSES: dict[str, type[OcrUploadSession]] = {
    "power_rankings": PowerRankingsSession,
    "foundry_battle": FoundryBattleSession,
    "canyon_clash": CanyonClashSession,
    "alliance_showdown": AllianceShowdownSession,
}


def build_session(event_type: str, *, cog, channel, uploader, alliance_id: int) -> Optional[OcrUploadSession]:
    cls = SESSION_CLASSES.get(event_type)
    if cls is None:
        return None
    return cls(
        cog=cog, channel=channel, uploader_id=uploader.id,
        event_type=event_type, alliance_id=alliance_id,
    )


def build_session_from_snapshot(cog, channel, payload: dict) -> Optional[OcrUploadSession]:
    """Recreate a session from a crash snapshot, pre-loaded with its parsed rows."""
    cls = SESSION_CLASSES.get(payload.get('event_type'))
    if cls is None:
        return None
    sess = cls(cog=cog, channel=channel, uploader_id=payload['uploader_id'],
               event_type=payload['event_type'], alliance_id=payload.get('alliance_id'))
    sess.restore_payload(payload)
    return sess
