"""
cogs/event_registry.py — Centralized game-event registry

Single source of truth for every in-game event referenced across the bot.

Why this file exists
--------------------
Prior to this module, each cog maintained its own event list with inconsistent
naming. The same real-world events appeared under different names depending on
which part of the bot you were looking at:

    System                    Event name used
    ─────────────────────────────────────────────────────────────────────
    discipline.py             "Foundry Battle", "Bear Hunt",
                              "Sun Fire Castle", "Frost Dragon Tyrant",
                              "State vs State", "Icefife Warhymn League"
    attendance.py             "Foundry", "Bear Trap",
                              "Castle Battle", "Frostdragon Tyrant"
    notification_event_types  "Foundry Battle", "Bear Trap",
                              "Castle Battle", "SvS"
    attendance_ocr_parsers    key="foundry_battle", label="Foundry Battle"

This module defines one canonical name per event and derives the per-cog
lists from it, so a single edit here propagates everywhere.

How to use
----------
Replace per-cog event lists with imports from this module:

    # discipline.py
    from .event_registry import DISCIPLINE_EVENTS as EVENTS

    # attendance.py  (drop-in replacements)
    from .event_registry import ATTENDANCE_EVENT_TYPES as EVENT_TYPES
    from .event_registry import ATTENDANCE_LEGION_EVENT_TYPES as LEGION_EVENT_TYPES

    # notification_event_types.py
    # The EVENT_CONFIG dict there carries too much per-event metadata to replace
    # directly, but the keys should be kept in sync with display_name values here.

    # attendance_ocr_parsers.py
    # The EventTypeConfig dataclass carries regex / weekday metadata beyond
    # what this registry tracks. The `label` field in each EventTypeConfig
    # MUST match the `display_name` here; use the shared `key` to cross-reference.

Adding a new event
------------------
1. Add one dict to REGISTRY below with all required fields.
2. Set the appropriate flags (discipline / attendance / notifications / ocr_key).
3. If the event has an OCR parser, add the EventTypeConfig in
   attendance_ocr_parsers.py using the same `key` and matching `label`.
4. If the event has notifications, add/update the entry in
   notification_event_types.py using the same `display_name` as the key.

Data-migration note
-------------------
The event name is stored as a plain string in both db/discipline.sqlite
(infractions.event) and db/attendance.sqlite (attendance_records.event_type).
Renaming an event here does NOT automatically update existing rows.

Where this module introduces a canonical name that differs from what a cog
previously used, a `legacy_names` list documents the old strings so that a
migration script can locate and rename them:

    UPDATE infractions SET event = 'Bear Trap' WHERE event = 'Bear Hunt';

Running such a migration is optional — old records will still display
correctly using whatever string was stored; only new records will use the
canonical name.
"""

from __future__ import annotations

# ── Registry ──────────────────────────────────────────────────────────────────
#
# Each entry is a dict with the following fields:
#
#   key           str        Stable snake_case identifier used as the DB key in
#                            the OCR system and as a cross-reference handle.
#   display_name  str        Canonical in-game name shown to users in all menus.
#   short_name    str        Abbreviated name for space-constrained contexts
#                            (e.g. button labels, embeds with limited width).
#   discipline    bool       Appears in the Discipline Log event picker.
#   attendance    bool       Appears in the Attendance manual event picker.
#   notifications bool       Has a corresponding entry in notification_event_types.
#   legion        bool       Requires legion / sub-group selection (Foundry, Canyon).
#   ocr_key       str|None   Key in attendance_ocr_parsers.EVENT_TYPES, or None.
#   legacy_names  list[str]  Previous names used in other cogs — kept for
#                            migration reference only; not used at runtime.

REGISTRY: list[dict] = [
    # ── Foundry Battle ────────────────────────────────────────────────────────
    # notifications uses "Foundry Battle"; OCR key is "foundry_battle"
    # attendance.py previously used the short name "Foundry"
    {
        "key":           "foundry_battle",
        "display_name":  "Foundry Battle",
        "short_name":    "Foundry",
        "discipline":    True,
        "attendance":    True,
        "notifications": True,
        "legion":        True,
        "ocr_key":       "foundry_battle",
        "legacy_names":  ["Foundry"],           # attendance.py
    },
    # ── Canyon Clash ──────────────────────────────────────────────────────────
    # Consistent across all systems.
    {
        "key":           "canyon_clash",
        "display_name":  "Canyon Clash",
        "short_name":    "Canyon Clash",
        "discipline":    True,
        "attendance":    True,
        "notifications": True,
        "legion":        True,
        "ocr_key":       "canyon_clash",
        "legacy_names":  [],
    },
    # ── Crazy Joe ─────────────────────────────────────────────────────────────
    # Consistent across all systems.
    {
        "key":           "crazy_joe",
        "display_name":  "Crazy Joe",
        "short_name":    "Crazy Joe",
        "discipline":    True,
        "attendance":    True,
        "notifications": True,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
    # ── Bear Trap ─────────────────────────────────────────────────────────────
    # Canonical name is "Bear Trap" (used by attendance + notifications).
    # discipline.py previously used "Bear Hunt".
    {
        "key":           "bear_trap",
        "display_name":  "Bear Trap",
        "short_name":    "Bear Trap",
        "discipline":    True,
        "attendance":    True,
        "notifications": True,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  ["Bear Hunt"],         # discipline.py
    },
    # ── Castle Battle ─────────────────────────────────────────────────────────
    # Canonical name is "Castle Battle" (used by attendance + notifications).
    # discipline.py previously used "Sun Fire Castle".
    {
        "key":           "castle_battle",
        "display_name":  "Castle Battle",
        "short_name":    "Castle Battle",
        "discipline":    True,
        "attendance":    True,
        "notifications": True,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  ["Sun Fire Castle"],   # discipline.py
    },
    # ── Frost Dragon Tyrant ───────────────────────────────────────────────────
    # discipline.py used "Frost Dragon Tyrant" (two words, capitalised).
    # attendance.py used "Frostdragon Tyrant" (one word).
    # "Frost Dragon Tyrant" is the clearer form; adopted as canonical.
    {
        "key":           "frost_dragon_tyrant",
        "display_name":  "Frost Dragon Tyrant",
        "short_name":    "Frost Dragon",
        "discipline":    True,
        "attendance":    True,
        "notifications": False,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  ["Frostdragon Tyrant"], # attendance.py
    },
    # ── State vs State ────────────────────────────────────────────────────────
    # notifications uses the abbreviation "SvS"; discipline uses the full name.
    # Full name adopted as canonical for clarity; short_name carries "SvS".
    {
        "key":           "state_vs_state",
        "display_name":  "State vs State",
        "short_name":    "SvS",
        "discipline":    True,
        "attendance":    False,
        "notifications": True,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  ["SvS"],               # notification_event_types.py
    },
    # ── Fortress Battle ───────────────────────────────────────────────────────
    # Consistent between discipline and notifications.
    {
        "key":           "fortress_battle",
        "display_name":  "Fortress Battle",
        "short_name":    "Fortress",
        "discipline":    True,
        "attendance":    False,
        "notifications": True,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
    # ── Brother in Arms ───────────────────────────────────────────────────────
    {
        "key":           "brother_in_arms",
        "display_name":  "Brother in Arms",
        "short_name":    "BiA",
        "discipline":    True,
        "attendance":    False,
        "notifications": False,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
    # ── Alliance Championship ─────────────────────────────────────────────────
    {
        "key":           "alliance_championship",
        "display_name":  "Alliance Championship",
        "short_name":    "Alliance Championship",
        "discipline":    True,
        "attendance":    False,
        "notifications": False,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
    # ── Icefire Warhymn League ────────────────────────────────────────────────
    # discipline.py contained a typo: "Icefife Warhymn League".
    # Corrected to "Icefire Warhymn League" here.
    {
        "key":           "icefire_warhymn_league",
        "display_name":  "Icefire Warhymn League",
        "short_name":    "Warhymn",
        "discipline":    True,
        "attendance":    False,
        "notifications": False,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  ["Icefife Warhymn League"],  # discipline.py typo
    },
    # ── Player Behavior ───────────────────────────────────────────────────────
    # Not a game event — used in the discipline log for general conduct issues.
    {
        "key":           "player_behavior",
        "display_name":  "Player Behavior",
        "short_name":    "Behavior",
        "discipline":    True,
        "attendance":    False,
        "notifications": False,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
    # ── Power Rankings ────────────────────────────────────────────────────────
    # OCR-only; does not appear in manual attendance or discipline pickers.
    {
        "key":           "power_rankings",
        "display_name":  "Power Rankings",
        "short_name":    "Power Rankings",
        "discipline":    False,
        "attendance":    False,
        "notifications": False,
        "legion":        False,
        "ocr_key":       "power_rankings",
        "legacy_names":  [],
    },
    # ── Alliance Showdown ─────────────────────────────────────────────────────
    # OCR-only; does not appear in manual attendance or discipline pickers.
    {
        "key":           "alliance_showdown",
        "display_name":  "Alliance Showdown",
        "short_name":    "Alliance Showdown",
        "discipline":    False,
        "attendance":    False,
        "notifications": False,
        "legion":        False,
        "ocr_key":       "alliance_showdown",
        "legacy_names":  [],
    },
    # ── Frostfire Mine ────────────────────────────────────────────────────────
    # Notifications only; not currently tracked in attendance or discipline.
    {
        "key":           "frostfire_mine",
        "display_name":  "Frostfire Mine",
        "short_name":    "Frostfire Mine",
        "discipline":    False,
        "attendance":    False,
        "notifications": True,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
    # ── Mercenary Prestige ────────────────────────────────────────────────────
    # Notifications only; not currently tracked in attendance or discipline.
    {
        "key":           "mercenary_prestige",
        "display_name":  "Mercenary Prestige",
        "short_name":    "Mercenary",
        "discipline":    False,
        "attendance":    False,
        "notifications": True,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
    # ── Other ─────────────────────────────────────────────────────────────────
    # Catch-all for manually logged attendance sessions that don't fit an
    # established event type. Not a real game event.
    {
        "key":           "other",
        "display_name":  "Other",
        "short_name":    "Other",
        "discipline":    False,
        "attendance":    True,
        "notifications": False,
        "legion":        False,
        "ocr_key":       None,
        "legacy_names":  [],
    },
]


# ── Lookup indexes (built once at import time) ─────────────────────────────────

_BY_KEY:  dict[str, dict] = {e["key"]: e for e in REGISTRY}
_BY_NAME: dict[str, dict] = {e["display_name"]: e for e in REGISTRY}


def get_event(key: str) -> dict | None:
    """Return the registry entry for a stable key (e.g. 'foundry_battle')."""
    return _BY_KEY.get(key)


def get_event_by_name(display_name: str) -> dict | None:
    """Return the registry entry for a canonical display name."""
    return _BY_NAME.get(display_name)


# ── Pre-filtered lists for drop-in cog compatibility ──────────────────────────

DISCIPLINE_EVENTS: list[str] = [
    e["display_name"] for e in REGISTRY if e["discipline"]
]

ATTENDANCE_EVENT_TYPES: list[str] = [
    e["display_name"] for e in REGISTRY if e["attendance"]
]

ATTENDANCE_LEGION_EVENT_TYPES: list[str] = [
    e["display_name"] for e in REGISTRY if e["attendance"] and e["legion"]
]

NOTIFICATION_EVENT_NAMES: list[str] = [
    e["display_name"] for e in REGISTRY if e["notifications"]
]

OCR_EVENT_KEYS: list[str] = [
    e["ocr_key"] for e in REGISTRY if e["ocr_key"] is not None
]
