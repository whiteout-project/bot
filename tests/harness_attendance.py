"""Test harness wrapping the attendance OCR pipeline.

Imports `bot/cogs/attendance_ocr_parsers.py` directly so changes to the
attendance parsers are exercised by the test suite without spinning up
Discord. Layer-1 tests use the parser functions directly on inline text;
layer-2 tests run real OCR via bear_track on PNG fixtures.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

BOT_DIR = Path(__file__).resolve().parent.parent  # repo root (this file is <repo>/tests/)
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "attendance"


def _load_modules():
    """Load the attendance OCR parser module and bear_track (for real OCR).
    `import cogs.X` triggers the package's relative imports cleanly.
    """
    if str(BOT_DIR) not in sys.path:
        sys.path.insert(0, str(BOT_DIR))
    import cogs.attendance_ocr_parsers as parsers  # type: ignore
    import cogs.bear_track as bear_track  # type: ignore
    return parsers, bear_track


parsers, bear_track = _load_modules()


def load_roster(name: str = "default") -> list[tuple[int, str]]:
    """Load a fixture roster as a list of (fid, nick) tuples."""
    path = FIXTURES / "rosters" / f"{name}.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    return [(int(m["fid"]), m["nick"]) for m in data["members"]]


def ocr_image(path: Path, lang: str = "en") -> str:
    """Run the bot's OCR engine on a PNG fixture and return the raw text."""
    import asyncio
    data = path.read_bytes()
    return asyncio.run(bear_track.ocr_bytes(data, lang=lang))


@dataclass
class FixtureCase:
    screenshot_path: Path
    event_type: str
    legion: Optional[str]  # "Legion 1" / "Legion 2" / None
    expected_path: Optional[Path] = None
    roster_name: str = "default"

    @property
    def expected(self) -> Optional[dict]:
        if self.expected_path and self.expected_path.exists():
            return json.loads(self.expected_path.read_text(encoding="utf-8"))
        return None

    @property
    def roster(self) -> list[tuple[int, str]]:
        return load_roster(self.roster_name)


# Hand-curated mapping of fixture filename → (event_type, legion).
# Each tuple says: which parser should pick this up, and which legion (if any).
_FIXTURE_META: dict[str, tuple[str, Optional[str]]] = {
    "foundry_battle_l1_001.png": ("foundry_battle", "Legion 1"),
    "foundry_battle_l1_002.png": ("foundry_battle", "Legion 1"),
    "foundry_battle_l1_003.png": ("foundry_battle", "Legion 1"),
    "foundry_battle_l1_004.png": ("foundry_battle", "Legion 1"),
    "foundry_battle_l2_001.png": ("foundry_battle", "Legion 2"),
    "foundry_battle_l2_002.png": ("foundry_battle", "Legion 2"),
    "foundry_battle_l2_003.png": ("foundry_battle", "Legion 2"),
    "foundry_battle_l2_004.png": ("foundry_battle", "Legion 2"),
    "canyon_clash_l1_001.png": ("canyon_clash", "Legion 1"),
    "canyon_clash_l1_002.png": ("canyon_clash", "Legion 1"),
    "canyon_clash_l1_003.png": ("canyon_clash", "Legion 1"),
    "canyon_clash_l2_001.png": ("canyon_clash", "Legion 2"),
    "canyon_clash_l2_002.png": ("canyon_clash", "Legion 2"),
    "canyon_clash_l2_003.png": ("canyon_clash", "Legion 2"),
    "alliance_showdown_001.png": ("alliance_showdown", None),
    "alliance_showdown_002.png": ("alliance_showdown", None),
    "alliance_showdown_003.png": ("alliance_showdown", None),
    "alliance_showdown_004.png": ("alliance_showdown", None),
    "alliance_showdown_005.png": ("alliance_showdown", None),
    "alliance_showdown_006.png": ("alliance_showdown", None),
    "alliance_showdown_007.png": ("alliance_showdown", None),
    "alliance_showdown_008.png": ("alliance_showdown", None),
    "power_rankings_001.png": ("power_rankings", None),
    "power_rankings_002.png": ("power_rankings", None),
    "power_rankings_003.png": ("power_rankings", None),
    "power_rankings_004.png": ("power_rankings", None),
    "power_rankings_005.png": ("power_rankings", None),
    "power_rankings_006.png": ("power_rankings", None),
    "power_rankings_007.png": ("power_rankings", None),
    "power_rankings_008.png": ("power_rankings", None),
    "power_rankings_009.png": ("power_rankings", None),
    "power_rankings_010.png": ("power_rankings", None),
    "power_rankings_011.png": ("power_rankings", None),
    "power_rankings_012.png": ("power_rankings", None),
    "power_rankings_013.png": ("power_rankings", None),
    # Registration mails for foundry/canyon now classify under the unified
    # event_type (foundry_battle / canyon_clash) — the per-screenshot
    # "kind" is what distinguishes registration from result data inside a
    # session, not the event_type itself.
    "foundry_registration_001.png": ("foundry_battle", "Legion 1"),
    "foundry_registration_002.png": ("foundry_battle", "Legion 1"),
    "foundry_registration_003.png": ("foundry_battle", "Legion 1"),
    "canyon_registration_001.png": ("canyon_clash", "Legion 1"),
    "canyon_registration_002.png": ("canyon_clash", "Legion 1"),
    "canyon_registration_003.png": ("canyon_clash", "Legion 1"),
}


def discover_fixtures(event_type: Optional[str] = None) -> list[FixtureCase]:
    """Yield FixtureCase objects for every screenshot, optionally filtered by event_type."""
    out = []
    shots_dir = FIXTURES / "screenshots"
    expected_dir = FIXTURES / "expected"
    for shot in sorted(shots_dir.glob("*.png")):
        meta = _FIXTURE_META.get(shot.name)
        if meta is None:
            continue
        et, legion = meta
        if event_type is not None and et != event_type:
            continue
        expected_path = expected_dir / f"{shot.stem}.json"
        out.append(FixtureCase(
            screenshot_path=shot, event_type=et, legion=legion,
            expected_path=expected_path if expected_path.exists() else None,
        ))
    return out


def fuzzy_match(name: str, roster: Optional[list] = None) -> tuple[Optional[int], str]:
    """Direct passthrough to the parser's matcher for test convenience."""
    return parsers.fuzzy_match_name(name, roster or [])
