"""Review UI for unified Foundry/Canyon sessions: registration, result, or both.

EventReviewView auto-detects mode from which row lists are populated; partial
sessions stay open so a later upload of the missing half enriches the record.
"""
from __future__ import annotations
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import discord

from .pimp_my_bot import theme
from .bear_track import _isolate_rtl, _ltr_line
from .login_handler import LoginHandler
from .attendance_ocr_parsers import (
    EVENT_TYPES,
    _STAT_LABELS,
    load_alliance_roster,
    fuzzy_match_name,
    assign_unique_fids,
    update_users_combat_power,
    update_users_power,
    _record_attendance_row,
    _upsert_attendance_row,
    _mark_registered_as_absent,
    _close_session,
    delete_session,
    _find_or_create_session,
    _record_session,
    _unmatched_id_floor,
    _normalize_for_match,
    learn_name_alias,
)

logger = logging.getLogger("alliance")

_STATUS_ICON = {
    "auto":     theme.verifiedIcon,
    "likely":   theme.warnIcon,
    "review":   theme.questionIcon,
    "manual":   theme.verifiedIcon,
    "no_match": theme.deniedIcon,
    "no_name":  theme.deniedIcon,
}

# Attendance is the headline status in complete/enriching mode — separate
# from the underlying roster-match quality so a green ✅ here always means
# "this player showed up", never "we identified them confidently".
_ATTENDANCE_ICON = {
    "present":      theme.verifiedIcon,
    "absent":       theme.deniedIcon,
    "needs_review": theme.warnIcon,
    "registered":   theme.timeIcon,
}

# Higher = better. Used when merging reg+result rows for the same fid to
# pick the stronger match label.
_MATCH_PRIORITY = {"no_match": 0, "no_name": 0, "review": 1,
                   "likely": 2, "auto": 3, "manual": 3}


def _format_int(n: Optional[int]) -> str:
    return f"{int(n):,}" if n is not None else "—"


def _parse_value_input(raw: Optional[str]) -> Optional[int]:
    """Digits-only int from a user value field; None if it had no digits."""
    digits = re.sub(r"[^\d]", "", raw or "")
    return int(digits) if digits else None


def _add_paginated_field(embed: discord.Embed, header: str, lines: list[str],
                         *, max_fields: int = 4, budget: int = 1000) -> None:
    """Split a long list of lines across multiple embed fields. Discord caps
    each field at 1024 chars; we leave headroom and cap total fields so we
    don't blow the 6000-char embed budget either."""
    if not lines:
        return
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        if current and current_len + len(line) + 1 > budget:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
            if len(chunks) >= max_fields - 1:
                # Last field — pack everything remaining (still respecting 1024)
                rest = "\n".join([line] + lines[lines.index(line) + 1:])
                if len(rest) > 1024:
                    rest = rest[:1010] + "\n…(truncated)"
                chunks.append(rest)
                break
        current.append(line)
        current_len += len(line) + 1
    else:
        if current:
            chunks.append("\n".join(current))

    for i, chunk in enumerate(chunks):
        # Continuation fields use a zero-width space as their name so Discord
        # doesn't render the same header twice.
        name = header if i == 0 else "​"
        embed.add_field(name=name[:256], value=chunk, inline=False)


def _format_compact(n: Optional[int]) -> str:
    if n is None:
        return "—"
    n = int(n)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


class EventReviewView(discord.ui.View):
    """Review for a Foundry/Canyon event — mode auto-detected from the row lists."""

    ROWS_PER_PAGE = 25

    def __init__(self, session, *,
                 registration_value_label: str,
                 result_value_label: str,
                 existing_session_id: Optional[str] = None,
                 enriching_open_session_id: Optional[str] = None,
                 edit_mode: bool = False):
        super().__init__(timeout=7200)
        self.session = session
        # Opened from the settings/Mark flow to edit a saved session: each edit
        # autosaves, so we show Back instead of Submit/Cancel.
        self.edit_mode = edit_mode
        # "Simple" events (e.g. Alliance Showdown) have no registration and no
        # present/absent concept — everyone takes part; we only record who scored.
        # Suppresses the registration/absent framing in the review.
        self.simple = bool(getattr(session, "simple_results", False))
        self.registration_value_label = registration_value_label
        self.result_value_label = result_value_label
        # Set when re-uploading screenshots for a CLOSED event — Submit
        # updates the closed record in place.
        self.existing_session_id = existing_session_id
        # Set when this upload's result data matches an OPEN registration
        # session whose registered_rows we've loaded into memory — Submit
        # closes that session.
        self.enriching_open_session_id = enriching_open_session_id
        self.roster = load_alliance_roster(session.alliance_id)
        self.registered_rows = self._enrich_rows(session.registered_rows, kind="registration")
        self.result_rows = self._enrich_rows(session.result_rows, kind="result")
        # Move results off a duplicate-name twin that didn't register, so an
        # extra roster twin isn't pulled in as a phantom player.
        reg_fids = {r["fid"] for r in self.registered_rows if r.get("fid")}
        self._reconcile_result_twins(reg_fids)
        # Cache for the merged per-player view — building it is O(n²) and it's
        # consulted ~6× per render (embed, footer, dropdown count, components).
        # Invalidated in refresh(), which runs after every row-mutating edit.
        self._merged_cache: Optional[list[dict]] = None
        self.page = 0
        # When on, the dropdown + visible list show only rows still needing a
        # player assigned — so admins can fix a big roster without paging all.
        self.show_unmatched_only = False
        self._build_components()

    def _reconcile_result_twins(self, reg_fids: set) -> None:
        """A result row matched to a duplicate-name twin who didn't register is
        the same player as an unmatched registered twin — move it onto that fid
        so a 2-player event can't surface a phantom 3rd."""
        used = {r["fid"] for r in self.result_rows if r.get("fid")}
        open_reg = reg_fids - used
        for r in self.result_rows:
            if not open_reg:
                return
            fid = r.get("fid")
            if fid in reg_fids or not r.get("candidates"):
                continue
            for cand_fid, cand_nick, _score, status in r["candidates"]:
                if cand_fid in open_reg:
                    used.discard(fid)
                    r["fid"] = cand_fid
                    r["nickname"] = self._lookup_nickname(cand_fid) or cand_nick
                    r["status"] = status
                    used.add(cand_fid)
                    open_reg.discard(cand_fid)
                    break

    def _enrich_rows(self, raw_rows: list[dict], *, kind: str) -> list[dict]:
        enriched = assign_unique_fids(
            raw_rows, self.roster, alliance_id=self.session.alliance_id)
        for r in enriched:
            r["_kind"] = kind
        return enriched

    def _lookup_nickname(self, fid: Optional[int]) -> Optional[str]:
        if fid is None:
            return None
        for f, nick in self.roster:
            if f == fid:
                return nick
        return None

    @property
    def has_registration(self) -> bool:
        return bool(self.registered_rows)

    @property
    def has_result(self) -> bool:
        return bool(self.result_rows)

    @property
    def mode(self) -> str:
        if self.has_registration and self.has_result:
            return "complete"
        if self.has_registration:
            return "registration"
        return "result"

    @property
    def all_rows(self) -> list[dict]:
        return self.registered_rows + self.result_rows

    def _global_to_local(self, global_idx: int) -> tuple[list[dict], int]:
        if global_idx < len(self.registered_rows):
            return self.registered_rows, global_idx
        return self.result_rows, global_idx - len(self.registered_rows)

    def _build_merged_view(self) -> list[dict]:
        """Per-player rows for complete/enriching mode. Each player appears
        once with registered + result values side-by-side, sorted with the
        attendees on top and Absent at the bottom. Cached per render."""
        if self._merged_cache is not None:
            return self._merged_cache
        by_fid: dict[int, dict] = {}
        nameless: list[dict] = []

        def _empty(template: dict, source: str, idx: int) -> dict:
            return {
                "fid": template["fid"],
                "nickname": template["nickname"],
                "name": template["name"],
                "match_status": template["status"],
                "registered_value": None,
                "result_value": None,
                "_reg_idx": idx if source == "reg" else None,
                "_res_idx": idx if source == "res" else None,
            }

        for i, r in enumerate(self.registered_rows):
            entry = _empty(r, "reg", i)
            entry["registered_value"] = r["value"]
            if r["fid"]:
                by_fid[r["fid"]] = entry
            else:
                nameless.append(entry)

        for i, r in enumerate(self.result_rows):
            if r["fid"] and r["fid"] in by_fid:
                existing = by_fid[r["fid"]]
                existing["result_value"] = r["value"]
                existing["_res_idx"] = i
                if (_MATCH_PRIORITY.get(r["status"], 0)
                        > _MATCH_PRIORITY.get(existing["match_status"], 0)):
                    existing["match_status"] = r["status"]
                    existing["nickname"] = r["nickname"] or existing["nickname"]
            elif r["fid"]:
                entry = _empty(r, "res", i)
                entry["result_value"] = r["value"]
                by_fid[r["fid"]] = entry
            else:
                # Try to fold into an existing reg-side nameless row whose
                # OCR'd name is the same.
                norm = _normalize_for_match(r["name"])
                target = None
                for n in nameless:
                    if (n["result_value"] is None
                            and _normalize_for_match(n["name"]) == norm):
                        target = n
                        break
                if target is not None:
                    target["result_value"] = r["value"]
                    target["_res_idx"] = i
                else:
                    entry = _empty(r, "res", i)
                    entry["result_value"] = r["value"]
                    nameless.append(entry)

        rows = list(by_fid.values()) + nameless
        for row in rows:
            if row["result_value"] is not None and row["registered_value"] is not None:
                row["attendance"] = "present"
            elif row["result_value"] is not None:
                # Game rule: a FID can only join one Foundry/Canyon per cycle,
                # so a result row with no registration match is by definition
                # an anomaly — missed Substitute, duplicate name, OCR mismatch,
                # or no registration uploaded.
                row["attendance"] = "needs_review"
            else:
                row["attendance"] = "absent"

        att_order = {"present": 0, "needs_review": 1, "absent": 2}
        rows.sort(key=lambda r: (
            att_order.get(r["attendance"], 3),
            -(r["result_value"] or r["registered_value"] or 0),
        ))
        self._merged_cache = rows
        return rows

    def _status_banner(self) -> str:
        scope = self._event_scope_phrase()
        if getattr(self.session, "power_only_snapshot", False):
            return (
                f"{theme.infoIcon} **Power snapshot** — Submit updates each matched "
                f"player's Power. Unmatched rows are skipped; no attendance is recorded."
            )
        if self.simple:
            return (
                f"{theme.infoIcon} **Scored players** — everyone in the alliance "
                f"takes part; this records who actually scored. Players who didn't "
                f"score simply aren't listed."
            )
        if self.enriching_open_session_id:
            return (
                f"{theme.verifiedIcon} **Matched existing event registration** — "
                f"Submit closes the event and marks no-shows Absent."
            )
        if self.mode == "complete":
            return (f"{theme.verifiedIcon} **Registration + Result captured** — "
                    "Submit closes this event.")
        if self.mode == "registration":
            return (
                f"{theme.warnIcon} **Registration only** — Submit saves this "
                f"{scope}; the result mail you upload later will close it."
            )
        return (f"{theme.warnIcon} **Result only** — no matching registration "
                "found. Submit records results without an Absent flip.")

    def _event_scope_phrase(self) -> str:
        """Short human phrase identifying this session (legion + date)."""
        bits = []
        if self.session.detected_legion:
            bits.append(self.session.detected_legion)
        if self.session.detected_date:
            bits.append(f"on {self.session.detected_date.isoformat()}")
        return " ".join(bits) if bits else "event"

    def build_embed(self) -> discord.Embed:
        cfg = EVENT_TYPES.get(self.session.event_type)
        label = cfg.label if cfg else self.session.event_type

        title_bits = [f"{theme.verifiedIcon} {label}"]
        if self.session.detected_legion:
            title_bits.append(f"· {self.session.detected_legion}")
        if self.session.detected_date:
            date_part = self.session.detected_date.isoformat()
            if self.session.detected_time:
                date_part += f" {self.session.detected_time} UTC"
            title_bits.append(f"· {date_part}")

        if self.simple:
            legend = (f"• Icons: {theme.verifiedIcon} matched · "
                      f"{theme.warnIcon} check · {theme.deniedIcon} unmatched")
        else:
            legend = f"• Icons: {theme.verifiedIcon} present · {theme.deniedIcon} absent"
        desc_lines = [
            f"{theme.upperDivider}",
            self._status_banner(),
            "",
            "• Edit any rows that need fixing, then Submit.",
            "• Empty the Name field on edit to delete a row.",
            legend,
        ]
        if self.existing_session_id is not None:
            desc_lines.append(
                f"\n{theme.refreshIcon} **Editing existing record** — Submit will "
                "UPDATE the stored data, merging your changes with what was saved before."
            )
        if self.session.detected_time is None:
            allowed = _allowed_time_slots(self.session.db_event_type)
            if allowed:
                desc_lines.append(
                    f"\n{theme.warnIcon} **Event time not set** — use the "
                    f"**Time** button to pick a UTC slot ({', '.join(allowed)})."
                )
        # Status counts up top where they're actually seen (was an easily-missed
        # footer). Lists what still needs the admin's attention before submit.
        attention = self._build_footer_bits()
        if attention:
            desc_lines.append(f"\n**{theme.warnIcon} Needs attention**")
            desc_lines.extend(f"• {bit}" for bit in attention)

        if self.session.alliance_rank is not None:
            desc_lines.append(
                f"\n**{theme.crownIcon} Alliance ranked No. {self.session.alliance_rank}**"
            )

        if self.session.alliance_scores:
            desc_lines.append(f"\n**{theme.shieldIcon} Scoreboard**")
            medal = ["🥇", "🥈", "🥉"]
            for i, sc in enumerate(self.session.alliance_scores):
                m = medal[i] if i < len(medal) else "•"
                tag = f"[{sc['tag']}]" if sc.get("tag") else ""
                name = sc.get("name") or ""
                legion = f" · {sc['legion']}" if sc.get("legion") else ""
                desc_lines.append(
                    f"{m} #{sc['rank']} {tag}{name} — `{_format_int(sc.get('score'))}`{legion}"
                )

        if self.session.stats:
            desc_lines.append(f"\n**{theme.chartIcon} Battle Stats**")
            stat_pairs = [
                f"{_STAT_LABELS.get(k, k)}: `{_format_compact(v)}`"
                for k, v in self.session.stats.items()
            ]
            for i in range(0, len(stat_pairs), 4):
                desc_lines.append(" · ".join(stat_pairs[i:i + 4]))

        if self.session.mvps:
            # Group by MVP name so a one-line summary reads compactly:
            #   MVPs: MIMOUN (Fuel, Squads, Buildings) · HOGER KURDI (Speedups) · …
            by_name: dict[str, list[str]] = {}
            for mvp in self.session.mvps:
                stat_label = _STAT_LABELS.get(mvp["stat_key"], mvp["stat_key"])
                by_name.setdefault(mvp["name"], []).append(stat_label)
            mvp_bits = [f"**{name}** ({', '.join(stats)})"
                        for name, stats in by_name.items()]
            desc_lines.append(f"\n**{theme.crownIcon} MVPs:** " + " · ".join(mvp_bits))

        # The visible list: filtered view when the Unmatched-only toggle is on;
        # otherwise complete mode inlines the merged table into the description
        # (4096 chars vs a 1024 field), and other modes use the fields below.
        if self.show_unmatched_only:
            self._append_filtered_lines(desc_lines)
        elif self.mode == "complete":
            self._append_merged_player_lines(desc_lines)
        else:
            self._append_section_lines(desc_lines)

        desc_lines.append(f"{theme.lowerDivider}")

        embed = discord.Embed(
            title=" ".join(title_bits),
            description="\n".join(desc_lines),
            color=theme.emColor3,
        )

        if not self.all_rows:
            embed.add_field(
                name="Players",
                value="*No player rows detected. Use Add Row to add manually.*",
                inline=False,
            )
        return embed

    def _append_section_lines(self, desc_lines: list[str]) -> None:
        """Non-complete mode shows exactly one populated section (registered XOR
        result). Rendered into the description (4096 budget) so a full page fits
        without the awkward mid-page '(truncated)' — pagination shows the rest."""
        if self.has_registration:
            rows = self.registered_rows
            label = f"{theme.userIcon} Registered ({self.registration_value_label})"
        else:
            rows = self.result_rows
            label = f"{theme.chartIcon} Results ({self.result_value_label})"
        if not rows:
            return
        start = self.page * self.ROWS_PER_PAGE
        page_rows = rows[start:start + self.ROWS_PER_PAGE]
        desc_lines.append(f"\n**{label} · {len(rows)}**")
        budget_remaining = 4096 - sum(len(l) + 1 for l in desc_lines) - 120
        for offset, r in enumerate(page_rows):
            i = start + offset + 1  # global row number, stable across pages
            icon = _STATUS_ICON.get(r["status"], "")
            if r["status"] in ("auto", "manual") and r["fid"]:
                player = f"`{_isolate_rtl(r['nickname'])}` · `{r['fid']}`"
            elif r["status"] in ("likely", "review") and r["fid"]:
                player = f"`{_isolate_rtl(r['nickname'])}` ({r['status']}) · `{r['fid']}`"
            else:
                player = f"`{_isolate_rtl(r['name'])}` — no match"
            line = _ltr_line(f"**#{i}** {icon} {player} — `{_format_int(r['value'])}`")
            if budget_remaining - len(line) - 1 < 0:
                break  # rest is on the next page — pagination, not a truncation note
            desc_lines.append(line)
            budget_remaining -= len(line) + 1

    def _reg_tag(self) -> str:
        return "CP" if self.registration_value_label == "Combat Power" else "Pwr"

    def _fmt_reg(self, value) -> str:
        # Combat Power is precise and small; don't abbreviate it.
        if self.registration_value_label == "Combat Power":
            return _format_int(value)
        return _format_compact(value)

    def _append_merged_player_lines(self, desc_lines: list[str]) -> None:
        """One row per player into the description (present → needs-review →
        absent), truncating with a hint near the 4096-char budget."""
        merged = self._build_merged_view()
        if not merged:
            return
        present = sum(1 for r in merged if r["attendance"] == "present")
        absent = sum(1 for r in merged if r["attendance"] == "absent")
        needs_review = sum(1 for r in merged if r["attendance"] == "needs_review")
        header_bits = [f"{theme.verifiedIcon} {present} present"]
        if absent:
            header_bits.append(f"{theme.deniedIcon} {absent} absent")
        if needs_review:
            header_bits.append(f"{theme.warnIcon} {needs_review} needs review")
        desc_lines.append(
            f"\n**Players · {len(merged)} ({' · '.join(header_bits)})**"
        )

        # Page the visible list to match the dropdown so Prev/Next move through it.
        start = self.page * self.ROWS_PER_PAGE
        page_rows = merged[start:start + self.ROWS_PER_PAGE]
        rendered = 0
        budget_remaining = 4096 - sum(len(l) + 1 for l in desc_lines) - 100
        for offset, r in enumerate(page_rows):
            i = start + offset + 1
            att = _ATTENDANCE_ICON.get(r["attendance"], "")
            display = _isolate_rtl(r["nickname"] or r["name"] or "?")
            ms = r["match_status"]
            if not r["fid"] or r["fid"] < 0:
                qual = " *(unmatched)*"
            elif ms in ("likely", "review"):
                qual = " *(unsure)*"
            else:
                qual = ""
            bits = [f"**#{i}** {att} `{display}`{qual}"]
            if r["fid"] and r["fid"] > 0:
                bits.append(f"`{r['fid']}`")
            if r["registered_value"] is not None:
                bits.append(f"`{self._fmt_reg(r['registered_value'])}` {self._reg_tag()}")
            if r["result_value"] is not None:
                bits.append(f"`{_format_compact(r['result_value'])}` Pts")
            elif r["attendance"] == "absent":
                bits.append("`Absent`")
            line = _ltr_line(" · ".join(bits))
            if budget_remaining - len(line) - 1 < 0:
                desc_lines.append(f"_…and {len(page_rows) - rendered} more on this "
                                  "page — open Edit a player row to see all_")
                return
            desc_lines.append(line)
            budget_remaining -= len(line) + 1
            rendered += 1

    def _append_filtered_lines(self, desc_lines: list[str]) -> None:
        """Render the Unmatched-only list (page slice) into the description.
        Names only — the admin matches by name via the dropdown."""
        entries = self._edit_entries()  # already filtered to unmatched
        total = len(entries)
        desc_lines.append(f"\n**{theme.deniedIcon} Unmatched rows · {total}**")
        if total == 0:
            desc_lines.append(
                f"{theme.verifiedIcon} All rows are matched — nothing to assign."
            )
            return
        start = self.page * self.ROWS_PER_PAGE
        page_rows = entries[start:start + self.ROWS_PER_PAGE]
        budget_remaining = 4096 - sum(len(l) + 1 for l in desc_lines) - 120
        for offset, (kind, _key, row, _um) in enumerate(page_rows):
            name = _isolate_rtl(row.get("nickname") or row.get("name") or "(unreadable)")
            if kind == "merged":
                vbits = []
                if row.get("registered_value") is not None:
                    vbits.append(f"{self._fmt_reg(row['registered_value'])} {self._reg_tag()}")
                if row.get("result_value") is not None:
                    vbits.append(f"{_format_compact(row['result_value'])} Pts")
                tail = " · ".join(vbits) if vbits else "—"
            else:
                tail = f"`{_format_int(row['value'])}`"
            line = _ltr_line(f"{theme.deniedIcon} `{name}` — {tail}")
            if budget_remaining - len(line) - 1 < 0:
                desc_lines.append(
                    f"_…and {total - (start + offset)} more — use the dropdown_"
                )
                return
            desc_lines.append(line)
            budget_remaining -= len(line) + 1

    def _build_footer_bits(self) -> list[str]:
        bits = []
        if self.mode == "complete":
            merged = self._build_merged_view()
            needs_review = sum(1 for r in merged if r["attendance"] == "needs_review")
            absent = sum(1 for r in merged if r["attendance"] == "absent")
            unmatched = sum(1 for r in merged if not r["fid"] or r["fid"] < 0)
            low_conf = sum(
                1 for r in merged
                if r["fid"] and r["fid"] > 0 and r["match_status"] in ("likely", "review")
            )
            if needs_review:
                bits.append(f"{needs_review} needs review")
            if unmatched:
                bits.append(f"{unmatched} unmatched — use Edit a player row to assign")
            if low_conf:
                bits.append(f"{low_conf} low-confidence — verify before submit")
            if absent:
                bits.append(f"{absent} will be marked Absent")
            return bits

        unmatched = sum(1 for r in self.all_rows if not r["fid"])
        low_conf = sum(1 for r in self.all_rows if r["status"] == "review")
        absent_count = self._would_be_absent_count()
        if unmatched:
            bits.append(f"{unmatched} unmatched — use Edit a player row to assign")
        if low_conf:
            bits.append(f"{low_conf} low-confidence — verify before submit")
        if absent_count:
            bits.append(f"{absent_count} will be marked Absent")
        return bits

    def _would_be_absent_count(self) -> int:
        """Count registered rows that will flip to Absent on submit (0 in registration-only mode)."""
        if not self.has_result:
            return 0
        if self.mode == "complete":
            present_fids = {r["fid"] for r in self.result_rows if r["fid"]}
            return sum(1 for r in self.registered_rows
                       if r["fid"] and r["fid"] not in present_fids)
        if not self.session.detected_date:
            return 0
        present_fids = {r["fid"] for r in self.result_rows if r["fid"]}
        with sqlite3.connect("db/attendance.sqlite", timeout=30.0) as conn:
            rows = conn.execute(
                "SELECT DISTINCT player_id FROM attendance_records ar "
                "JOIN attendance_sessions s ON ar.session_id = s.session_id "
                "WHERE s.event_type = ? AND s.alliance_id = ? AND s.awaiting_result = 1 "
                "AND COALESCE(s.event_subtype, '') = COALESCE(?, '') "
                "AND ABS(julianday(s.event_date) - julianday(?)) <= 2 "
                "AND ar.status = 'registered'",
                (self.session.db_event_type, self.session.alliance_id,
                 self.session.detected_legion,
                 self.session.detected_date.isoformat()),
            ).fetchall()
        registered_fids = {int(r[0]) for r in rows if r[0] and r[0].isdigit()}
        return len(registered_fids - present_fids)

    # ── components ────────────────────────────────────────────────────────

    @staticmethod
    def _fid_unmatched(fid) -> bool:
        return not fid or fid < 0

    def _unmatched_total(self) -> int:
        """How many rows still need a player assigned (across the whole list)."""
        if self.mode == "complete":
            return sum(1 for mr in self._build_merged_view()
                       if self._fid_unmatched(mr["fid"]))
        return sum(1 for r in self.all_rows if self._fid_unmatched(r.get("fid")))

    def _edit_entries(self) -> list[tuple]:
        """Editable rows in display order (honoring the Unmatched-only filter)
        as (kind, key, row, unmatched); 'kind:key' is the dropdown value mapping
        back to the underlying row."""
        out = []
        if self.mode == "complete":
            for idx, mr in enumerate(self._build_merged_view()):
                um = self._fid_unmatched(mr["fid"])
                if self.show_unmatched_only and not um:
                    continue
                out.append(("merged", idx, mr, um))
        else:
            for gi in range(len(self.all_rows)):
                bucket, li = self._global_to_local(gi)
                r = bucket[li]
                um = self._fid_unmatched(r.get("fid"))
                if self.show_unmatched_only and not um:
                    continue
                out.append(("raw", gi, r, um))
        return out

    def _dropdown_count(self) -> int:
        """Count behind pagination — the filtered visible entries."""
        return len(self._edit_entries())

    def _total_pages(self) -> int:
        return max(1, (self._dropdown_count() + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE)

    def _build_components(self):
        self.clear_items()
        # Clamp the page in case the filtered list shrank under the cursor
        # (e.g. the last unmatched row on this page was just assigned).
        self.page = max(0, min(self.page, self._total_pages() - 1))
        entries = self._edit_entries()
        total = len(entries)
        if total:
            start = self.page * self.ROWS_PER_PAGE
            end = min(start + self.ROWS_PER_PAGE, total)
            options = []
            for kind, key, row, um in entries[start:end]:
                if kind == "merged":
                    name_part = row["nickname"] or row["name"] or "(unreadable)"
                    tag = " (unmatched)" if um else (
                        " (unsure)" if row["match_status"] in ("likely", "review") else "")
                    label = _ltr_line(f"#{key + 1} {name_part}{tag}")[:100]
                    desc_bits = []
                    if row["registered_value"] is not None:
                        desc_bits.append(f"{self._fmt_reg(row['registered_value'])} {self._reg_tag()}")
                    if row["result_value"] is not None:
                        desc_bits.append(f"{_format_compact(row['result_value'])} Pts")
                    elif row["attendance"] == "absent":
                        desc_bits.append("Absent")
                    desc = " · ".join(desc_bits)[:100]
                    options.append(discord.SelectOption(
                        label=label, value=f"merged:{key}", description=desc,
                    ))
                else:
                    _bucket, local_idx = self._global_to_local(key)
                    name_part = row["nickname"] or row["name"] or "(unreadable)"
                    fid_part = f" · {row['fid']}" if row.get("fid") else ""
                    tag = " (unmatched)" if um else ""
                    label = _ltr_line(f"#{local_idx + 1} {name_part}{fid_part}{tag}")[:100]
                    desc = f"{_format_int(row['value'])} · {row['status']}"[:100]
                    options.append(discord.SelectOption(
                        label=label, value=f"raw:{key}", description=desc,
                    ))
            placeholder = ("Edit an unmatched row…" if self.show_unmatched_only
                           else "Edit a player row…")
            select = discord.ui.Select(
                placeholder=placeholder, options=options, row=0,
            )
            select.callback = self._on_edit_row
            self.add_item(select)

        # Row 1: pagination — sits directly under the dropdown so navigation
        # is right next to what it affects. Auto-hidden when only 1 page.
        total_pages = self._total_pages()
        if total_pages > 1:
            prev = discord.ui.Button(
                label="Prev", emoji=theme.prevIcon,
                style=discord.ButtonStyle.secondary, row=1,
                disabled=self.page == 0,
            )
            prev.callback = self._on_prev
            self.add_item(prev)
            page_label = discord.ui.Button(
                label=f"Page {self.page + 1}/{total_pages}",
                style=discord.ButtonStyle.secondary, row=1, disabled=True,
            )
            self.add_item(page_label)
            nxt = discord.ui.Button(
                label="Next", emoji=theme.nextIcon,
                style=discord.ButtonStyle.secondary, row=1,
                disabled=self.page >= total_pages - 1,
            )
            nxt.callback = self._on_next
            self.add_item(nxt)

        # Row 2: Time / Add Row / Edit Event Info
        if _allowed_time_slots(self.session.db_event_type):
            time_label = self.session.detected_time or "not set"
            time_btn = discord.ui.Button(
                label=f"Time: {time_label}",
                emoji=theme.timeIcon,
                style=(discord.ButtonStyle.success if self.session.detected_time
                       else discord.ButtonStyle.secondary),
                row=2,
            )
            time_btn.callback = self._on_set_time
            self.add_item(time_btn)
        # Unmatched-only filter — shown when there's something to filter (or the
        # filter is already on, so you can switch back).
        um_total = self._unmatched_total()
        if um_total or self.show_unmatched_only:
            on = self.show_unmatched_only
            label = f"Unmatched: {'On' if on else 'Off'}"
            toggle = discord.ui.Button(
                label=label, emoji=theme.eyeIcon,
                style=discord.ButtonStyle.success if on else discord.ButtonStyle.secondary,
                row=2,
            )
            toggle.callback = self._on_toggle_unmatched
            self.add_item(toggle)
        # A power snapshot has no event fields but the date — label it plainly.
        header_label = ("Set Date" if getattr(self.session, "power_only_snapshot", False)
                        else "Edit Event Info")
        for label, emoji, style, cb in (
            ("Add Row", theme.addIcon, discord.ButtonStyle.secondary, self._on_add_row),
            (header_label, theme.editListIcon, discord.ButtonStyle.secondary, self._on_edit_header),
        ):
            btn = discord.ui.Button(label=label, emoji=emoji, style=style, row=2)
            btn.callback = cb
            self.add_item(btn)

        # Row 3: Back (edit mode autosaves) — or Submit / Cancel (upload review).
        if self.edit_mode:
            row3 = [
                ("Back", theme.backIcon, discord.ButtonStyle.secondary, self._on_back_edit),
                ("Delete Event", theme.trashIcon, discord.ButtonStyle.danger, self._on_delete_event),
            ]
        else:
            row3 = [
                ("Submit", theme.verifiedIcon, discord.ButtonStyle.success, self._on_submit),
                ("Cancel", theme.deniedIcon, discord.ButtonStyle.danger, self._on_cancel),
            ]
        for label, emoji, style, cb in row3:
            btn = discord.ui.Button(label=label, emoji=emoji, style=style, row=3)
            btn.callback = cb
            self.add_item(btn)

    # ── interaction guards ────────────────────────────────────────────────

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.session.uploader_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the uploader can edit this review.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        """Replace the big review embed with a small expiry notice so the
        channel doesn't keep a stale table sitting around after a walked-away
        review."""
        # Skip if the review was already resolved (submitted/cancelled).
        if (self.session.channel.id, self.session.uploader_id) not in self.session.cog.active_sessions:
            return
        if self.session.progress_message:
            try:
                await self.session.progress_message.edit(
                    embed=discord.Embed(
                        title=f"{theme.warnIcon} Review session expired",
                        description=(
                            "No activity in this review for a while, so the "
                            "session was discarded. **Upload the screenshots "
                            "again** to start a fresh review."
                        ),
                        color=theme.emColor2,
                    ),
                    view=None,
                )
            except (discord.NotFound, discord.HTTPException):
                pass
        self.session.cog.end_session(
            self.session.channel.id, self.session.uploader_id,
        )

    # ── callbacks ─────────────────────────────────────────────────────────

    async def _on_edit_row(self, interaction: discord.Interaction):
        raw = interaction.data["values"][0]
        kind, _, idx_str = raw.partition(":")
        try:
            idx = int(idx_str)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Bad row reference.", ephemeral=True,
            )
            return
        if kind == "merged":
            merged = self._build_merged_view()
            if idx >= len(merged):
                await interaction.response.send_message(
                    f"{theme.deniedIcon} That row no longer exists.", ephemeral=True,
                )
                return
            await interaction.response.send_modal(_EditMergedRowModal(self, merged[idx]))
        else:
            if idx >= len(self.all_rows):
                await interaction.response.send_message(
                    f"{theme.deniedIcon} That row no longer exists.", ephemeral=True,
                )
                return
            await interaction.response.send_modal(_EditRowModal(self, idx))

    async def _on_add_row(self, interaction: discord.Interaction):
        if self.mode == "complete":
            # Ask which bucket the row goes into.
            view = _AddRowBucketView(self)
            await interaction.response.send_message(
                content=f"{theme.questionIcon} Add as a Registered or Result row?",
                view=view, ephemeral=True,
            )
            return
        # Single-mode sessions: target the bucket that's populated.
        target = "registration" if self.mode == "registration" else "result"
        await interaction.response.send_modal(_AddRowModal(self, kind=target))

    async def _on_edit_header(self, interaction: discord.Interaction):
        await interaction.response.send_modal(_EditEventInfoModal(self))

    async def _on_set_time(self, interaction: discord.Interaction):
        allowed = _allowed_time_slots(self.session.db_event_type)
        if not allowed:
            await interaction.response.send_message(
                f"{theme.warnIcon} No time slots configured for this event type.",
                ephemeral=True,
            )
            return
        view = _TimeSlotPickerView(self, allowed)
        await interaction.response.send_message(
            content=f"{theme.timeIcon} Pick the UTC slot this event ran at:",
            view=view, ephemeral=True,
        )

    async def _on_prev(self, interaction: discord.Interaction):
        if self.page > 0:
            self.page -= 1
        await self.refresh(interaction)

    async def _on_next(self, interaction: discord.Interaction):
        if self.page < self._total_pages() - 1:
            self.page += 1
        await self.refresh(interaction)

    async def _on_toggle_unmatched(self, interaction: discord.Interaction):
        self.show_unmatched_only = not self.show_unmatched_only
        self.page = 0
        await self.refresh(interaction)

    async def _on_cancel(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title=f"{theme.deniedIcon} Upload cancelled",
            description="Review discarded; nothing was saved.",
            color=theme.emColor2,
        )
        await interaction.response.edit_message(embed=embed, view=None)
        self.session.cog.end_session(self.session.channel.id, self.session.uploader_id)
        self.stop()

    async def _on_back_edit(self, interaction: discord.Interaction):
        """Edit-mode exit — edits already autosaved, so just return to the list."""
        attendance_cog = self.session.cog.bot.get_cog("Attendance")
        if attendance_cog:
            await attendance_cog.show_session_selection_for_marking(
                interaction, self.session.alliance_id)

    async def _on_delete_event(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title=f"{theme.warnIcon} Delete this event?",
            description=(
                "This permanently removes the event and its recorded attendance. "
                "Player Power / Combat Power is **not** affected.\n\n"
                "_To restore it, re-upload the screenshots._"
            ),
            color=theme.emColor2,
        )
        await interaction.response.edit_message(
            embed=embed, view=_ConfirmDeleteEventView(self))

    async def _do_delete_event(self, interaction: discord.Interaction):
        delete_session(self.existing_session_id)
        self.stop()
        attendance_cog = self.session.cog.bot.get_cog("Attendance")
        if attendance_cog:
            await attendance_cog.show_session_selection_for_marking(
                interaction, self.session.alliance_id)

    async def _save_edit(self, interaction: discord.Interaction):
        """Refresh the embed, then persist immediately when editing a saved
        session (so edits stick without a Submit step). Used by the row/header
        edit modals, not by pagination/toggle."""
        await self.refresh(interaction)
        if self.edit_mode:
            try:
                self._persist()
            except Exception as e:
                logger.exception("Attendance edit autosave failed")
                try:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} Couldn't save the edit: {e}", ephemeral=True)
                except Exception:
                    pass

    async def _on_submit(self, interaction: discord.Interaction):
        try:
            session_id, absent_rows = self._persist()
        except Exception as e:
            logger.exception("EventReview submit failed")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Submit failed: {e}", ephemeral=True,
            )
            return
        if getattr(self.session, "power_only_snapshot", False):
            embed = self._build_power_snapshot_embed()
        else:
            embed = self._build_scoreboard_embed(session_id, absent_rows)
        await interaction.response.edit_message(embed=embed, view=None)
        self.session.cog.end_session(self.session.channel.id, self.session.uploader_id)
        self.stop()

    async def refresh(self, interaction: discord.Interaction):
        # A row-mutating edit happened before refresh — drop the cached merged
        # view so the rebuild below reflects the change.
        self._merged_cache = None
        self._build_components()
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=self.build_embed(), view=self)
        else:
            await interaction.response.edit_message(embed=self.build_embed(), view=self)

    # ── persistence ───────────────────────────────────────────────────────

    def _persist(self) -> tuple[str, list[dict]]:
        """Write session + scoreboard + stats + MVPs + players by mode, returning
        (session_id, absent_rows). Registration-only stays open; result-only and
        complete close after marking absentees."""
        update_fn = (
            update_users_combat_power
            if self.session.db_event_type == "foundry_battle"
            else update_users_power
        )
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

        # Power Rankings is a power snapshot, not an event — write users.power for
        # matched rows and record no attendance.
        if getattr(self.session, "power_only_snapshot", False):
            return self._persist_power_snapshot(ts), []

        if self.mode == "registration":
            return self._persist_registration_only(update_fn, ts), []

        if self.existing_session_id is not None:
            session_id = self.existing_session_id
            with sqlite3.connect("db/attendance.sqlite", timeout=30.0) as conn:
                conn.execute(
                    "UPDATE attendance_sessions SET awaiting_result = 1 "
                    "WHERE session_id = ?", (session_id,))
                conn.commit()
        else:
            session_id = _find_or_create_session(
                event_type=self.session.db_event_type,
                event_date=self.session.detected_date,
                event_subtype=self.session.detected_legion,
                alliance_id=self.session.alliance_id,
                date_confidence=self.session.date_confidence,
            )

        with sqlite3.connect("db/attendance.sqlite", timeout=30.0) as conn:
            conn.execute(
                "UPDATE attendance_sessions "
                "SET alliance_rank = ?, event_time = ? WHERE session_id = ?",
                (self.session.alliance_rank, self.session.detected_time, session_id),
            )
            self._write_scoreboard(conn, session_id)
            self._write_stats(conn, session_id)
            self._write_mvps(conn, session_id)
            conn.commit()

        # Write registered rows first so 'present' upserts can flip them.
        # The name→neg-fid map lets unmatched result rows fold onto the
        # matching unmatched reg row (same as the merged-view does visually),
        # so the registered row updates to 'present' instead of being flipped
        # to 'absent' AND a duplicate 'needs review' row being created.
        unmatched_reg_by_name: dict[str, list[int]] = {}
        if self.has_registration:
            unmatched_reg_by_name = self._write_registered_rows(session_id, update_fn, ts)

        next_unmatched = _unmatched_id_floor(session_id) - 1
        present_fids = set()
        for r in self.result_rows:
            if r["fid"]:
                present_fids.add(r["fid"])
                learn_name_alias(self.session.alliance_id, r["name"], r["fid"])
                row_fid: int = r["fid"]
            else:
                norm = _normalize_for_match(r["name"])
                matched_neg = None
                if norm and unmatched_reg_by_name.get(norm):
                    matched_neg = unmatched_reg_by_name[norm].pop(0)
                if matched_neg is not None:
                    row_fid = matched_neg
                    present_fids.add(row_fid)
                else:
                    row_fid = next_unmatched
                    next_unmatched -= 1
            _upsert_attendance_row(
                session_id=session_id,
                event_type=self.session.db_event_type,
                event_date=self.session.detected_date,
                event_subtype=self.session.detected_legion,
                alliance_id=self.session.alliance_id,
                fid=row_fid,
                name=r["nickname"] or r["name"],
                status="present",
                points=r["value"],
            )
        absent_rows = _mark_registered_as_absent(session_id, except_fids=present_fids)
        _close_session(session_id)
        return session_id, absent_rows

    def _persist_power_snapshot(self, ts: str) -> Optional[str]:
        """Power Rankings: update users.power for each matched row, learn the
        alias, and record NO attendance. Returns None (there is no session)."""
        count = 0
        for r in self.result_rows:
            if r["fid"]:
                update_users_power(r["fid"], r["value"], ts)
                learn_name_alias(self.session.alliance_id, r["name"], r["fid"])
                count += 1
        self._power_updated_count = count
        return None

    def _persist_registration_only(self, update_fn, ts: str) -> str:
        session_id = _find_or_create_session(
            event_type=self.session.db_event_type,
            event_date=self.session.detected_date,
            event_subtype=self.session.detected_legion,
            alliance_id=self.session.alliance_id,
            date_confidence=self.session.date_confidence,
        )
        self._write_registered_rows(session_id, update_fn, ts)
        with sqlite3.connect("db/attendance.sqlite", timeout=30.0) as conn:
            conn.execute(
                "UPDATE attendance_sessions "
                "SET alliance_rank = ?, event_time = ? WHERE session_id = ?",
                (self.session.alliance_rank, self.session.detected_time, session_id),
            )
            conn.commit()
        return session_id

    def _write_registered_rows(self, session_id: str, update_fn, ts: str
                                ) -> dict[str, list[int]]:
        """Wipe + reinsert registered rows; return normalized-name → placeholder
        negative-fids for unmatched reg rows, so the result pass can fold
        unmatched results onto the same row by name."""
        with sqlite3.connect("db/attendance.sqlite", timeout=30.0) as conn:
            conn.execute(
                "DELETE FROM attendance_records WHERE session_id = ? AND status = 'registered'",
                (session_id,),
            )
            conn.commit()
        name_to_neg: dict[str, list[int]] = {}
        next_unmatched = -1
        for r in self.registered_rows:
            if r["fid"]:
                update_fn(r["fid"], r["value"], ts)
                learn_name_alias(self.session.alliance_id, r["name"], r["fid"])
                row_fid: int = r["fid"]
            else:
                row_fid = next_unmatched
                next_unmatched -= 1
                norm = _normalize_for_match(r["name"])
                if norm:
                    name_to_neg.setdefault(norm, []).append(row_fid)
            _record_attendance_row(
                session_id=session_id,
                event_type=self.session.db_event_type,
                event_date=self.session.detected_date,
                event_subtype=self.session.detected_legion,
                alliance_id=self.session.alliance_id,
                fid=row_fid,
                name=r["nickname"] or r["name"],
                status="registered",
                points=r["value"],
            )
        return name_to_neg

    def _own_alliance_name(self) -> Optional[str]:
        with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
            row = conn.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?",
                (self.session.alliance_id,),
            ).fetchone()
        return row[0] if row else None

    def _is_own_scoreboard_entry(self, sc: dict) -> bool:
        own = self._own_alliance_name() or ""
        if not own:
            return False
        own_clean = re.sub(r"[\[\]]", "", own).casefold()
        tag = (sc.get("tag") or "").casefold()
        name = (sc.get("name") or "").casefold()
        if tag and tag in own_clean:
            return True
        if name and (name == own_clean or name in own_clean or own_clean in name):
            return True
        return False

    def _write_scoreboard(self, conn, session_id: str):
        conn.execute(
            "DELETE FROM attendance_session_scoreboard WHERE session_id = ?",
            (session_id,),
        )
        for sc in self.session.alliance_scores:
            is_own = self._is_own_scoreboard_entry(sc)
            conn.execute(
                "INSERT OR REPLACE INTO attendance_session_scoreboard "
                "(session_id, rank, legion, tag, name, score, is_own_alliance) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (session_id, sc["rank"], sc.get("legion"), sc.get("tag"),
                 sc.get("name"), sc.get("score"), int(is_own)),
            )

    def _write_stats(self, conn, session_id: str):
        conn.execute(
            "DELETE FROM attendance_session_stats WHERE session_id = ?",
            (session_id,),
        )
        for stat_key, value in self.session.stats.items():
            conn.execute(
                "INSERT OR REPLACE INTO attendance_session_stats "
                "(session_id, stat_key, stat_value) VALUES (?, ?, ?)",
                (session_id, stat_key, value),
            )

    def _write_mvps(self, conn, session_id: str):
        conn.execute(
            "DELETE FROM attendance_session_mvps WHERE session_id = ?",
            (session_id,),
        )
        for mvp in self.session.mvps:
            fid, _status = fuzzy_match_name(mvp["name"], self.roster, alliance_id=self.session.alliance_id)
            conn.execute(
                "INSERT OR REPLACE INTO attendance_session_mvps "
                "(session_id, stat_key, mvp_name, mvp_value, mvp_fid) "
                "VALUES (?, ?, ?, ?, ?)",
                (session_id, mvp["stat_key"], mvp["name"], mvp["value"],
                 str(fid) if fid else None),
            )

    # ── scoreboard embed ──────────────────────────────────────────────────

    def _total_absent_for_session(self, session_id: str) -> int:
        with sqlite3.connect("db/attendance.sqlite", timeout=30.0) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM attendance_records "
                "WHERE session_id = ? AND status = 'absent'",
                (session_id,),
            ).fetchone()
        return int(row[0]) if row else 0

    def _previous_event_total(self, session_id: str) -> Optional[int]:
        with sqlite3.connect("db/attendance.sqlite", timeout=30.0) as conn:
            prior = conn.execute(
                "SELECT session_id FROM attendance_sessions "
                "WHERE event_type = ? AND alliance_id = ? "
                "AND COALESCE(event_subtype, '') = COALESCE(?, '') "
                "AND session_id != ? "
                "AND event_date < COALESCE(?, '9999-12-31') "
                "ORDER BY event_date DESC LIMIT 1",
                (self.session.db_event_type, self.session.alliance_id,
                 self.session.detected_legion, session_id,
                 self.session.detected_date.isoformat() if self.session.detected_date else None),
            ).fetchone()
            if not prior:
                return None
            row = conn.execute(
                "SELECT SUM(points) FROM attendance_records "
                "WHERE session_id = ? AND status = 'present'",
                (prior[0],),
            ).fetchone()
            return int(row[0]) if row and row[0] is not None else None

    def _build_power_snapshot_embed(self) -> discord.Embed:
        cfg = EVENT_TYPES.get(self.session.event_type)
        label = cfg.label if cfg else self.session.event_type
        updated = getattr(self, "_power_updated_count", 0)
        unmatched = [r for r in self.result_rows if not r["fid"]]
        desc = [
            f"{theme.upperDivider}",
            f"{theme.chartIcon} **Power updated for `{updated}` player"
            f"{'s' if updated != 1 else ''}.**",
        ]
        if unmatched:
            desc.append(
                f"{theme.warnIcon} `{len(unmatched)}` unmatched row"
                f"{'s' if len(unmatched) != 1 else ''} skipped — re-upload to assign them."
            )
        desc.append(f"{theme.lowerDivider}")
        return discord.Embed(
            title=f"{theme.verifiedIcon} {label} — Power Recorded",
            description="\n".join(desc),
            color=theme.emColor1,
        )

    def _build_scoreboard_embed(self, session_id: str, absent_rows: list[dict]) -> discord.Embed:
        cfg = EVENT_TYPES.get(self.session.event_type)
        label = cfg.label if cfg else self.session.event_type

        title_bits = [f"{theme.verifiedIcon} {label}"]
        if self.session.detected_legion:
            title_bits.append(f"· {self.session.detected_legion}")
        if self.session.detected_date:
            date_part = self.session.detected_date.isoformat()
            if self.session.detected_time:
                date_part += f" {self.session.detected_time} UTC"
            title_bits.append(f"· {date_part}")

        desc = [f"{theme.upperDivider}"]
        if self.mode == "registration":
            desc.append(f"{theme.warnIcon} Saved as **registration only** — "
                        "event will close when the result mail is uploaded.\n")
        if self.session.alliance_rank is not None:
            desc.append(f"**{theme.crownIcon} Alliance ranked No. {self.session.alliance_rank}**\n")

        if self.session.alliance_scores:
            desc.append(f"**{theme.shieldIcon} Alliance Scoreboard**")
            medal = ["🥇", "🥈", "🥉"]
            for i, sc in enumerate(self.session.alliance_scores):
                m = medal[i] if i < len(medal) else "•"
                tag = f"[{sc['tag']}]" if sc.get("tag") else ""
                name = sc.get("name") or ""
                own = " **(you)**" if self._is_own_scoreboard_entry(sc) else ""
                desc.append(
                    f"{m} #{sc['rank']} {tag}{name}{own} — `{_format_int(sc.get('score'))}`"
                )
            desc.append("")

        if self.session.stats:
            desc.append(f"**{theme.chartIcon} Battle Stats**")
            stat_pairs = [
                f"{_STAT_LABELS.get(k, k)}: `{_format_compact(v)}`"
                for k, v in self.session.stats.items()
            ]
            for i in range(0, len(stat_pairs), 4):
                desc.append(" · ".join(stat_pairs[i:i + 4]))
            desc.append("")

        if self.session.mvps:
            desc.append(f"**{theme.crownIcon} MVPs**")
            for mvp in self.session.mvps:
                stat_label = _STAT_LABELS.get(mvp["stat_key"], mvp["stat_key"])
                desc.append(
                    f"• **{mvp['name']}** — {stat_label} `{_format_compact(mvp['value'])}`"
                )
            desc.append("")

        # Counts come from the merged per-player view (same numbers the review
        # showed at submit), so the post-submit summary matches what the user
        # clicked Submit on.
        is_registration_only = (self.mode == "registration")
        merged = self._build_merged_view() if not is_registration_only else []
        present_count = sum(1 for r in merged if r["attendance"] == "present")
        absent_count = sum(1 for r in merged if r["attendance"] == "absent")
        needs_review_count = sum(1 for r in merged if r["attendance"] == "needs_review")
        matched_reg = [r for r in self.registered_rows if r["fid"]]
        reg_unmatched = len(self.registered_rows) - len(matched_reg)

        analytics_lines = [f"**{theme.membersIcon} Participation & Performance**"]
        if is_registration_only:
            line = f"`{len(self.registered_rows)}` combatants in the registration mail"
            if reg_unmatched:
                line += (f" — `{len(matched_reg)}` matched to roster, "
                         f"`{reg_unmatched}` unmatched")
            analytics_lines.append(line)
        elif absent_count:
            registered_count = present_count + absent_count
            analytics_lines.append(
                f"`{present_count}/{registered_count}` registered players scored "
                f"· `{absent_count}` marked Absent"
            )
        else:
            analytics_lines.append(f"`{present_count}` players scored")

        if needs_review_count and not is_registration_only:
            analytics_lines.append(
                f"{theme.warnIcon} `{needs_review_count}` row(s) need review — "
                "present without a matching registration"
            )

        total_all = sum(r["value"] for r in self.result_rows)
        if total_all > 0:
            count_all = len(self.result_rows)
            avg = total_all // max(count_all, 1)
            analytics_lines.append(
                f"Total: `{_format_compact(total_all)}` {self.result_value_label} · "
                f"Average: `{_format_compact(avg)}`"
            )
            top = max(self.result_rows, key=lambda r: r["value"])
            top_name = top["nickname"] or top["name"]
            analytics_lines.append(
                f"Top contributor: **{top_name}** — `{_format_int(top['value'])}`"
            )
            prev_total = self._previous_event_total(session_id)
            if prev_total and prev_total > 0:
                delta_pct = (total_all - prev_total) / prev_total * 100
                arrow = "📈" if delta_pct >= 0 else "📉"
                analytics_lines.append(
                    f"{arrow} `{delta_pct:+.1f}%` vs previous event "
                    f"(was `{_format_compact(prev_total)}`)"
                )
        elif is_registration_only and self.registered_rows:
            total_reg = sum(r["value"] for r in self.registered_rows)
            avg = total_reg // max(len(self.registered_rows), 1)
            top = max(self.registered_rows, key=lambda r: r["value"])
            top_name = top["nickname"] or top["name"]
            analytics_lines.append(
                f"Total: `{_format_compact(total_reg)}` {self.registration_value_label} · "
                f"Average: `{_format_compact(avg)}`"
            )
            analytics_lines.append(
                f"Highest: **{top_name}** — `{_format_int(top['value'])}`"
            )

        desc.append("\n".join(analytics_lines))
        desc.append("")

        # In registration-only mode the "by power" list is just the registration
        # roster sorted by their stored combatant power — it's not a scoring
        # contest. The real ranking happens after the result mail.
        primary_rows = self.result_rows if self.has_result else self.registered_rows
        if primary_rows:
            if self.has_result:
                section_title = f"Top Scorers ({self.result_value_label})"
            else:
                section_title = f"Registered Players by {self.registration_value_label}"
            desc.append(f"**{theme.listIcon} {section_title}**")
            top_n = sorted(primary_rows, key=lambda r: -r["value"])[:10]
            for i, r in enumerate(top_n, start=1):
                player = r["nickname"] or r["name"]
                desc.append(f"`#{i:>2}` **{player}** — `{_format_int(r['value'])}`")
            if len(primary_rows) > 10:
                desc.append(f"_…and {len(primary_rows) - 10} more_")

        desc.append(f"{theme.lowerDivider}")

        return discord.Embed(
            title=" ".join(title_bits),
            description="\n".join(desc),
            color=theme.emColor3,
        )


class _ConfirmDeleteEventView(discord.ui.View):
    """Ephemeral-style inline confirm for deleting a saved event."""

    def __init__(self, parent: "EventReviewView"):
        super().__init__(timeout=60)
        self.parent = parent
        confirm = discord.ui.Button(
            label="Delete", emoji=theme.trashIcon, style=discord.ButtonStyle.danger)
        confirm.callback = self._confirm
        self.add_item(confirm)
        cancel = discord.ui.Button(
            label="Cancel", emoji=theme.backIcon, style=discord.ButtonStyle.secondary)
        cancel.callback = self._cancel
        self.add_item(cancel)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await self.parent.interaction_check(interaction)

    async def _confirm(self, interaction: discord.Interaction):
        self.stop()
        await self.parent._do_delete_event(interaction)

    async def _cancel(self, interaction: discord.Interaction):
        self.stop()
        await interaction.response.edit_message(
            embed=self.parent.build_embed(), view=self.parent)


# ── modals ────────────────────────────────────────────────────────────────

async def _resolve_player_field(interaction: discord.Interaction,
                                view: "EventReviewView", text: str):
    """Resolve an ID or name to (fid, nickname, status, note). An ID not in the
    roster is looked up via the player API (like /w); `note` is an ephemeral
    message for the caller to surface."""
    text = (text or "").strip()
    if text.isdigit():
        fid = int(text)
        nick = view._lookup_nickname(fid)
        if nick:
            return fid, nick, "manual", f"{theme.verifiedIcon} Matched ID `{fid}` to **{_isolate_rtl(nick)}**."
        # Not in the roster — pull the name from the player API.
        if not interaction.response.is_done():
            await interaction.response.defer()
        try:
            result = await LoginHandler().fetch_player_data(str(fid))
        except Exception as e:
            logger.error(f"Attendance player lookup failed for {fid}: {e}")
            print(f"[ERROR] Attendance player lookup failed for {fid}: {e}")
            return fid, None, "no_match", f"{theme.deniedIcon} Lookup failed for ID `{fid}` — try again shortly."
        if result.get("status") == "success" and result.get("data"):
            nick = result["data"].get("nickname") or str(fid)
            state = _ensure_player_in_alliance(fid, result["data"], view.session.alliance_id)
            view.roster = load_alliance_roster(view.session.alliance_id)
            disp = _isolate_rtl(nick)
            if state == "added":
                note = f"{theme.verifiedIcon} Added **{disp}** (ID `{fid}`) to the alliance and matched the row."
            elif state == "other_alliance":
                note = f"{theme.warnIcon} Matched ID `{fid}` to **{disp}** — already in another alliance, not moved."
            else:
                note = f"{theme.verifiedIcon} Matched ID `{fid}` to **{disp}**."
            return fid, nick, "manual", note
        reason = {"rate_limited": "API rate limit reached — try again shortly.",
                  "not_found": f"No player found with ID `{fid}`."}.get(
            result.get("status") or "", "Lookup failed.")
        return fid, None, "no_match", f"{theme.deniedIcon} {reason}"
    if text:
        f, st = fuzzy_match_name(text, view.roster, alliance_id=view.session.alliance_id)
        if f is not None:
            nick = view._lookup_nickname(f)
            return f, nick, st, f"{theme.verifiedIcon} Matched to **{_isolate_rtl(nick or text)}**."
        return None, None, "no_match", (
            f"{theme.warnIcon} No player matched `{text}` — left unmatched.")
    return None, None, "no_match", None


def _ensure_player_in_alliance(fid: int, data: dict, alliance_id) -> str:
    """Add a looked-up player to this alliance if untracked. Returns 'added',
    'exists', or 'other_alliance' — never moves a player from another alliance."""
    nick = data.get("nickname") or str(fid)
    with sqlite3.connect("db/users.sqlite", timeout=30.0) as conn:
        row = conn.execute("SELECT alliance FROM users WHERE fid = ?", (fid,)).fetchone()
        if row is not None:
            return "exists" if str(row[0]) == str(alliance_id) else "other_alliance"
        conn.execute(
            "INSERT INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, alliance) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (fid, nick, data.get("stove_lv", 0), str(data.get("kid", "")),
             data.get("stove_lv_content", ""), str(alliance_id)),
        )
        conn.commit()
        return "added"


class _EditMergedRowModal(discord.ui.Modal):
    """Edit a player across both reg and result buckets at once. Used in
    complete mode so the dropdown can show one option per merged player.
    Empty name = delete both underlying rows.
    """

    def __init__(self, view: EventReviewView, merged_row: dict):
        display = merged_row.get("nickname") or merged_row.get("name") or "player"
        super().__init__(title=f"Edit {display}"[:45])
        self.view = view
        self.merged_row = merged_row
        self.reg_idx = merged_row.get("_reg_idx")
        self.res_idx = merged_row.get("_res_idx")

        self.player_input = discord.ui.TextInput(
            label="Player (ID or name — blank to delete)",
            default=display, required=False, max_length=80,
        )
        self.add_item(self.player_input)
        # Offer a field for every phase the event has, not just the buckets this
        # row already fills — so a missing Combat Power / result can be added here.
        if view.has_registration:
            self.reg_value_input: Optional[discord.ui.TextInput] = discord.ui.TextInput(
                label=f"Reg power ({view.registration_value_label})"[:45],
                default=str(merged_row.get("registered_value") or 0),
                required=False, max_length=15,
            )
            self.add_item(self.reg_value_input)
        else:
            self.reg_value_input = None
        if view.has_result:
            self.res_value_input: Optional[discord.ui.TextInput] = discord.ui.TextInput(
                label=f"Result pts ({view.result_value_label})"[:45],
                default=str(merged_row.get("result_value") or 0),
                required=False, max_length=15,
            )
            self.add_item(self.res_value_input)
        else:
            self.res_value_input = None

    async def on_submit(self, interaction: discord.Interaction):
        if not self.player_input.value.strip():
            for bucket, idx in self._underlying_targets():
                if idx is not None and idx < len(bucket):
                    del bucket[idx]
            await self.view._save_edit(interaction)
            return

        fid, nickname, status, note = await _resolve_player_field(
            interaction, self.view, self.player_input.value)
        name = self.player_input.value.strip()
        # Update the row in place (keeps its OCR name for the alias DB); if a
        # bucket has no row yet but a value was entered, create one.
        if self.reg_value_input is not None:
            self._apply(self.view.registered_rows, self.reg_idx,
                        _parse_value_input(self.reg_value_input.value) or 0,
                        "registration", fid, nickname, status, name)
        if self.res_value_input is not None:
            self._apply(self.view.result_rows, self.res_idx,
                        _parse_value_input(self.res_value_input.value) or 0,
                        "result", fid, nickname, status, name)
        await self.view._save_edit(interaction)
        if note:
            await interaction.followup.send(note, ephemeral=True)

    @staticmethod
    def _apply(bucket, idx, value, kind, fid, nickname, status, name) -> None:
        if idx is not None and idx < len(bucket):
            bucket[idx].update(
                {"value": value, "fid": fid, "nickname": nickname, "status": status})
        elif value:
            bucket.append({"name": nickname or name, "value": value, "fid": fid,
                           "nickname": nickname, "status": status, "_kind": kind})

    def _underlying_targets(self) -> list[tuple[list[dict], Optional[int]]]:
        return [
            (self.view.registered_rows, self.reg_idx),
            (self.view.result_rows, self.res_idx),
        ]


class _EditRowModal(discord.ui.Modal):
    """Edit one row's name/fid/value; empty name deletes."""

    def __init__(self, view: EventReviewView, global_idx: int):
        super().__init__(title=f"Edit Row #{global_idx + 1}"[:45])
        self.view = view
        self.global_idx = global_idx
        bucket, local_idx = view._global_to_local(global_idx)
        self.bucket = bucket
        self.local_idx = local_idx
        row = bucket[local_idx]
        value_label = (view.registration_value_label
                       if row["_kind"] == "registration"
                       else view.result_value_label)

        self.player_input = discord.ui.TextInput(
            label="Player (ID or name — blank to delete)",
            default=row.get("nickname") or row.get("name") or "",
            required=False, max_length=80,
        )
        self.value_input = discord.ui.TextInput(
            label=f"Value ({value_label})"[:45],
            default=str(row.get("value") or 0),
            required=True, max_length=15,
        )
        self.add_item(self.player_input)
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not self.player_input.value.strip():
            del self.bucket[self.local_idx]
            await self.view._save_edit(interaction)
            return
        value = _parse_value_input(self.value_input.value)
        if value is None:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Value must be a whole number.", ephemeral=True,
            )
            return

        fid, nickname, status, note = await _resolve_player_field(
            interaction, self.view, self.player_input.value)
        # Update in place so the original OCR name (alias key) and _kind survive.
        self.bucket[self.local_idx].update(
            {"value": value, "fid": fid, "nickname": nickname, "status": status})
        await self.view._save_edit(interaction)
        if note:
            await interaction.followup.send(note, ephemeral=True)


class _AddRowModal(discord.ui.Modal):
    """Add a manual row to one bucket."""

    def __init__(self, view: EventReviewView, *, kind: str):
        super().__init__(title=f"Add {kind.title()} Row"[:45])
        self.view = view
        self.kind = kind
        value_label = (view.registration_value_label
                       if kind == "registration"
                       else view.result_value_label)
        self.player_input = discord.ui.TextInput(
            label="Player (ID or name)",
            required=True, max_length=80,
        )
        self.value_input = discord.ui.TextInput(
            label=f"Value ({value_label})"[:45],
            required=True, max_length=15,
        )
        self.add_item(self.player_input)
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        player_text = self.player_input.value.strip()
        if not player_text:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Provide a player ID or name.", ephemeral=True,
            )
            return
        value = _parse_value_input(self.value_input.value)
        if value is None:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Value must be a whole number.", ephemeral=True,
            )
            return

        fid, nickname, status, note = await _resolve_player_field(
            interaction, self.view, player_text)
        new_row = {
            "name": nickname or player_text,
            "value": value,
            "fid": fid,
            "nickname": nickname,
            "status": status,
            "_kind": self.kind,
        }
        bucket = (self.view.registered_rows if self.kind == "registration"
                  else self.view.result_rows)
        bucket.append(new_row)
        await self.view._save_edit(interaction)
        if note:
            await interaction.followup.send(note, ephemeral=True)


class _AddRowBucketView(discord.ui.View):
    """Bucket picker shown only in complete mode."""

    def __init__(self, parent: EventReviewView):
        super().__init__(timeout=120)
        self.parent = parent

        reg_btn = discord.ui.Button(
            label="Registered", style=discord.ButtonStyle.primary,
            emoji=theme.userIcon,
        )
        reg_btn.callback = self._on_registered
        self.add_item(reg_btn)

        res_btn = discord.ui.Button(
            label="Result", style=discord.ButtonStyle.primary,
            emoji=theme.chartIcon,
        )
        res_btn.callback = self._on_result
        self.add_item(res_btn)

    async def _on_registered(self, interaction: discord.Interaction):
        await interaction.response.send_modal(_AddRowModal(self.parent, kind="registration"))

    async def _on_result(self, interaction: discord.Interaction):
        await interaction.response.send_modal(_AddRowModal(self.parent, kind="result"))


class _EditEventInfoModal(discord.ui.Modal):
    """Edit event-level fields: date (always), plus legion + alliance rank for
    events that have them. Simple events (e.g. Showdown) get a date-only form."""

    def __init__(self, view: EventReviewView):
        session = view.session
        power_only = getattr(session, "power_only_snapshot", False)
        super().__init__(title="Set Date" if power_only else "Edit Event Info")
        self.view = view
        self.date_input = discord.ui.TextInput(
            label="Snapshot date (YYYY-MM-DD)" if power_only else "Event date (YYYY-MM-DD)",
            default=session.detected_date.isoformat() if session.detected_date else "",
            required=False, max_length=10,
        )
        self.add_item(self.date_input)
        # Legion / alliance rank don't apply to simple events — date only.
        self.legion_input = None
        self.rank_input = None
        if not view.simple:
            self.legion_input = discord.ui.TextInput(
                label="Legion (1 or 2)",
                default=session.detected_legion.replace("Legion ", "") if session.detected_legion else "",
                required=False, max_length=2,
            )
            self.rank_input = discord.ui.TextInput(
                label="Alliance rank (No. ?)",
                default=str(session.alliance_rank) if session.alliance_rank is not None else "",
                required=False, max_length=3,
            )
            self.add_item(self.legion_input)
            self.add_item(self.rank_input)

    async def on_submit(self, interaction: discord.Interaction):
        session = self.view.session
        date_raw = self.date_input.value.strip()
        if date_raw:
            try:
                session.detected_date = datetime.fromisoformat(date_raw).date()
                session.date_confidence = "manual"
            except ValueError:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Date must be YYYY-MM-DD.", ephemeral=True,
                )
                return
        else:
            session.detected_date = None

        if self.legion_input is not None:
            legion_raw = self.legion_input.value.strip()
            if legion_raw in ("1", "2"):
                session.detected_legion = f"Legion {legion_raw}"
            elif not legion_raw:
                session.detected_legion = None

        if self.rank_input is not None:
            rank_raw = self.rank_input.value.strip()
            if rank_raw.isdigit():
                session.alliance_rank = int(rank_raw)
            elif not rank_raw:
                session.alliance_rank = None

        await self.view._save_edit(interaction)


class _TimeSlotPickerView(discord.ui.View):
    """Ephemeral UTC time-slot dropdown."""

    def __init__(self, parent: EventReviewView, slots: tuple[str, ...]):
        super().__init__(timeout=120)
        self.parent = parent
        current = parent.session.detected_time
        options = [
            discord.SelectOption(
                label=slot, value=slot, description="UTC",
                default=(slot == current),
            )
            for slot in slots
        ]
        options.append(discord.SelectOption(
            label="Clear (no time set)", value="__clear__", description="Remove the time",
        ))
        select = discord.ui.Select(
            placeholder="Pick a UTC slot…",
            options=options, min_values=1, max_values=1,
        )
        select.callback = self._picked
        self.add_item(select)

    async def _picked(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        self.parent.session.detected_time = None if value == "__clear__" else value
        await interaction.response.edit_message(
            content=f"{theme.verifiedIcon} Event time "
                    f"{'cleared' if value == '__clear__' else f'set to `{value}` UTC'}.",
            view=None,
        )
        if self.parent.session.progress_message:
            try:
                self.parent._build_components()
                await self.parent.session.progress_message.edit(
                    embed=self.parent.build_embed(), view=self.parent,
                )
            except discord.NotFound:
                pass


def _allowed_time_slots(db_event_type: str) -> tuple[str, ...]:
    from .attendance_ocr_parsers import EVENT_TIME_SLOTS
    return EVENT_TIME_SLOTS.get(db_event_type, ())
