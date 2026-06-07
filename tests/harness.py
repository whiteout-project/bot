"""Test harness wrapping the bear-track parsing pipeline.

Imports `bot/cogs/bear_track.py` directly so changes to the bot's parsing
logic are exercised by the test suite without spinning up Discord.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

BOT_DIR = Path(__file__).resolve().parent.parent  # repo root (this file is <repo>/tests/)
COGS_DIR = BOT_DIR / "cogs"
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _load_bear_track():
    """Load `bear_track` as a member of the `cogs` package so its relative
    imports (`from .pimp_my_bot import ...`) resolve. Avoids the cog-loading
    / discord.py code paths but still has the package-level imports work.
    """
    if str(BOT_DIR) not in sys.path:
        sys.path.insert(0, str(BOT_DIR))
    # `import cogs.bear_track` triggers the package's relative imports
    # cleanly, including pimp_my_bot, login_handler etc.
    import cogs.bear_track as bear_track  # type: ignore
    return bear_track


bt = _load_bear_track()


def load_roster(name: str) -> list[tuple[int, str]]:
    """Load a fixture roster as list of (fid, nick) tuples."""
    path = FIXTURES / "rosters" / f"{name}.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    return [(int(m["fid"]), m["nick"]) for m in data["members"]]


def parse_text(ocr_text: str, roster: Optional[list] = None) -> dict[str, Any]:
    """Run the full text-level pipeline (digit repair → trap/rallies/total
    extraction → row parsing → roster matching) on a raw OCR string and
    return a structured result for assertion.
    """
    repaired = bt.repair_ocr_digits(ocr_text)
    trap, rallies, total_damage = bt.extract_bear_hunt_stats(repaired)
    raw_rows = bt.parse_player_rows(repaired)

    enriched = []
    for r in raw_rows:
        candidates = bt.match_roster(r["name"], roster) if roster else []
        status = bt.classify_match(candidates)
        top = candidates[0] if candidates else None
        enriched.append({
            "name": r["name"],
            "damage": r["damage"],
            "rank": r["rank"],
            "match_fid": top[0] if top else None,
            "match_score": top[2] if top else 0,
            "status": status,
        })
    return {
        "trap": trap,
        "rallies": rallies,
        "total_damage": total_damage,
        "rows": enriched,
    }


@dataclass
class FixtureCase:
    screenshot_path: Path
    expected_path: Path
    roster_name: str

    @property
    def expected(self) -> dict:
        return json.loads(self.expected_path.read_text(encoding="utf-8"))

    @property
    def roster(self) -> list[tuple[int, str]]:
        return load_roster(self.roster_name)


def discover_fixtures() -> list[FixtureCase]:
    """Yield (screenshot, expected, roster_name) for every PNG that has a
    matching expected JSON. Roster name comes from the expected JSON's
    `roster` field; defaults to "default".
    """
    out = []
    for shot in sorted((FIXTURES / "screenshots").glob("*.png")):
        expected_path = FIXTURES / "expected" / f"{shot.stem}.json"
        if not expected_path.exists():
            continue
        expected = json.loads(expected_path.read_text(encoding="utf-8"))
        roster_name = expected.get("roster", "default")
        out.append(FixtureCase(shot, expected_path, roster_name))
    return out


def _ocr_bytes_sync(image_bytes: bytes, lang: str) -> str:
    """bear_track.ocr_bytes is async in the current bot; run it to completion
    so the synchronous pipeline below can consume the text."""
    import asyncio
    return asyncio.run(bt.ocr_bytes(image_bytes, lang=lang))


def run_ocr_pipeline(image_bytes: bytes, primary_lang: str = "ch",
                     fallback_langs: list[str] | None = None,
                     roster: list | None = None) -> dict:
    """Run the full image-level OCR pipeline (primary engine + fallbacks +
    by-script position-aware fill) and return the resulting rows.

    Mirrors what `_ocr_attachment_to_result` does in the cog, but pulled
    out of the async/Discord context.
    """
    fallback_langs = fallback_langs or []
    primary_text = _ocr_bytes_sync(image_bytes, primary_lang)
    repaired = bt.repair_ocr_digits(primary_text)
    trap, rallies, total = bt.extract_bear_hunt_stats(repaired)
    date = bt.extract_hunt_date(repaired) or ""
    rows = {row["damage"]: row for row in bt.parse_player_rows(repaired)}

    # Run fallbacks the same way the cog does, in order.
    for fb_lang in fallback_langs:
        if not any(bt.is_row_unfilled(r, roster) for r in rows.values()):
            break
        fb_text = _ocr_bytes_sync(image_bytes, fb_lang)
        if not fb_text.strip():
            continue
        fb_repaired = bt.repair_ocr_digits(fb_text)
        if not bt._output_matches_lang_script(fb_repaired, fb_lang):
            continue
        fb_rows = bt.parse_player_rows(fb_repaired)
        if fb_lang in bt._RTL_LANGS:
            for fr in fb_rows:
                if fr.get("name"):
                    fr["name"] = bt._reverse_for_rtl(fr["name"], fb_lang)
        filled_via_damage = bt.merge_fallback_rows_by_damage(rows, fb_rows, roster, fb_lang)
        if not filled_via_damage and fb_lang not in bt._LATIN_ONLY_LANGS:
            bt.fill_unfilled_by_position(rows, fb_repaired, fb_lang, "test", roster)
    return {
        "trap": trap,
        "rallies": rallies,
        "total_damage": total,
        "date": date,
        "rows": list(rows.values()),
    }
