"""No-Shows: alliance-wide registered-no-show ranking for Foundry / Canyon Clash."""
import asyncio
import sqlite3
from datetime import date, timedelta

import discord

from .bear_track import _isolate_rtl, _ltr_line
from .pimp_my_bot import theme, check_interaction_user, safe_edit_message, disable_expired_view

_ATT_DB = "db/attendance.sqlite"
_USERS_DB = "db/users.sqlite"

# attendance_sessions stores internal event keys; the UI shows the labels below.
NO_SHOW_EVENT_TYPES = ["foundry_battle", "canyon_clash"]
_EVENT_LABEL_BY_KEY = {"foundry_battle": "Foundry", "canyon_clash": "Canyon Clash"}

# Cycle states for the list view's event/window filter buttons.
_EVENT_STATES = [tuple(NO_SHOW_EVENT_TYPES)] + [(e,) for e in NO_SHOW_EVENT_TYPES]
_EVENT_LABELS = ["Both"] + [_EVENT_LABEL_BY_KEY[e] for e in NO_SHOW_EVENT_TYPES]
_WINDOW_STATES = [90, 30, None]
_WINDOW_LABELS = {90: "90d", 30: "30d", None: "All"}


def _format_rank_line(rank, r):
    """One ranked list line; RTL names are isolated so they can't reorder the stats."""
    name = _isolate_rtl(r.get("display_name") or r["name"])
    marker = " · former" if r.get("left_alliance") else ""
    return _ltr_line(
        f"**{rank}.** {name}{marker} (`{r['fid']}`) - {r['no_shows']} no-shows | "
        f"{r['attended']} attended | {r['excused']} excused ({int(r['rate'] * 100)}%)"
    )


def compute_no_shows(alliance_id, event_types, since_date=None, min_events=3):
    """Ranked per-player no-show aggregates over closed sessions of the given event types."""
    if not event_types:
        return []
    placeholders = ",".join("?" for _ in event_types)
    sql = (
        "SELECT ar.player_id, MAX(ar.player_name) AS name, "
        "SUM(ar.status='present') AS attended, "
        "SUM(ar.status='absent' AND ar.excused=0) AS no_shows, "
        "SUM(ar.status='absent' AND ar.excused=1) AS excused "
        "FROM attendance_records ar "
        "JOIN attendance_sessions s ON s.session_id = ar.session_id "
        "WHERE s.alliance_id = ? AND s.awaiting_result = 0 "
        f"AND s.event_type IN ({placeholders}) "
        "AND (? IS NULL OR s.event_date >= ?) "
        "GROUP BY ar.player_id"
    )
    params = [alliance_id, *event_types, since_date, since_date]
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        rows = conn.execute(sql, params).fetchall()

    out = []
    for player_id, name, attended, no_shows, excused in rows:
        attended, no_shows, excused = int(attended or 0), int(no_shows or 0), int(excused or 0)
        tracked = attended + no_shows + excused
        if tracked < min_events:
            continue
        denom = attended + no_shows
        rate = (no_shows / denom) if denom else 0.0
        try:
            fid = int(player_id)
        except (TypeError, ValueError):
            continue
        out.append({"fid": fid, "name": name or str(fid), "attended": attended,
                    "no_shows": no_shows, "excused": excused, "tracked": tracked, "rate": rate})
    out.sort(key=lambda r: (-r["no_shows"], -r["rate"], r["name"].lower()))
    return out


def player_no_show_incidents(alliance_id, player_id, event_types, since_date=None):
    """One player's absent records (excused or not) for the excuse drill-down."""
    if not event_types:
        return []
    placeholders = ",".join("?" for _ in event_types)
    sql = (
        "SELECT s.session_id, s.event_type, s.event_date, ar.excused, ar.excused_reason "
        "FROM attendance_records ar "
        "JOIN attendance_sessions s ON s.session_id = ar.session_id "
        "WHERE s.alliance_id = ? AND s.awaiting_result = 0 AND ar.player_id = ? "
        f"AND ar.status = 'absent' AND s.event_type IN ({placeholders}) "
        "AND (? IS NULL OR s.event_date >= ?) "
        "ORDER BY s.event_date DESC"
    )
    params = [alliance_id, player_id, *event_types, since_date, since_date]
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [{"session_id": sid, "event_type": et, "event_date": ed,
             "excused": bool(exc), "reason": reason} for sid, et, ed, exc, reason in rows]


def set_excused(session_id, player_id, excused, reason=None):
    """Flag or clear excused on one absence row."""
    with sqlite3.connect(_ATT_DB, timeout=30.0) as conn:
        conn.execute(
            "UPDATE attendance_records SET excused = ?, excused_reason = ? "
            "WHERE session_id = ? AND player_id = ?",
            (1 if excused else 0, reason if excused else None, session_id, player_id),
        )
        conn.commit()


def _enrich_names(rows, alliance_id):
    """Attach current nickname + alliance-membership flag from users.sqlite.
    Cross-DB, so it stays in the view layer rather than compute_no_shows."""
    if not rows:
        return rows
    fids = [r["fid"] for r in rows]
    nick_by_fid = {}
    current = set()
    try:
        with sqlite3.connect(_USERS_DB, timeout=30.0) as conn:
            placeholders = ",".join("?" * len(fids))
            for fid, nick, alliance in conn.execute(
                f"SELECT fid, nickname, alliance FROM users WHERE fid IN ({placeholders})",
                fids,
            ).fetchall():
                nick_by_fid[fid] = nick or ""
                if str(alliance) == str(alliance_id):
                    current.add(fid)
    except sqlite3.OperationalError:
        pass
    for r in rows:
        r["display_name"] = nick_by_fid.get(r["fid"]) or r["name"]
        r["left_alliance"] = r["fid"] not in current
    return rows


def _load_rows(alliance_id, event_types, since_date, min_events):
    """Compute + enrich in one thread hop since the second step depends on the first."""
    rows = compute_no_shows(alliance_id, event_types, since_date, min_events)
    return _enrich_names(rows, alliance_id)


# ── list view ────────────────────────────────────────────────────────────

class NoShowsView(discord.ui.View):
    """Alliance-wide ranked no-show list with event/window filters, paging,
    CSV export, and a per-player select into the excuse drill-down."""

    PAGE_SIZE = 15

    def __init__(self, cog, user_id, alliance_id, alliance_name):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self._event_idx = 0
        self._window_idx = 0
        self.min_events = 3
        self.page = 0
        self.rows = []
        self.message = None
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await check_interaction_user(interaction, self.user_id)

    async def on_timeout(self):
        await disable_expired_view(self)

    @property
    def event_types(self):
        return list(_EVENT_STATES[self._event_idx])

    @property
    def window_days(self):
        return _WINDOW_STATES[self._window_idx]

    def _total_pages(self) -> int:
        return max(1, (len(self.rows) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)

    async def _load(self):
        since = None if self.window_days is None else (date.today() - timedelta(days=self.window_days)).isoformat()
        self.rows = await asyncio.to_thread(
            _load_rows, self.alliance_id, self.event_types, since, self.min_events)
        self.page = min(self.page, self._total_pages() - 1)
        self._build_components()

    def build_embed(self) -> discord.Embed:
        total_pages = self._total_pages()
        start = self.page * self.PAGE_SIZE
        page_rows = self.rows[start:start + self.PAGE_SIZE]

        lines = [f"{theme.upperDivider}"]
        if not page_rows:
            lines.append(f"{theme.warnIcon} No no-shows recorded for these filters.")
        else:
            for i, r in enumerate(page_rows, start=start + 1):
                lines.append(_format_rank_line(i, r))
        lines.append(f"{theme.lowerDivider}")

        event_label = _EVENT_LABELS[self._event_idx]
        window_label = _WINDOW_LABELS[self.window_days]
        embed = discord.Embed(
            title=f"{theme.deniedIcon} No-Shows - {self.alliance_name}",
            description="\n".join(lines),
            color=theme.emColor1,
        )
        embed.set_footer(
            text=f"Event: {event_label} · Window: {window_label} · Min {self.min_events}+ events · "
                 f"Page {self.page + 1}/{total_pages} · {len(self.rows)} player(s)"
        )
        return embed

    def _build_components(self):
        self.clear_items()
        total_pages = self._total_pages()
        start = self.page * self.PAGE_SIZE
        page_rows = self.rows[start:start + self.PAGE_SIZE]

        event_label = _EVENT_LABELS[self._event_idx]
        event_icons = {"Both": theme.listIcon}
        for _name, _icon in zip(_EVENT_LABELS[1:], (theme.foundryIcon, theme.canyonClashIcon)):
            event_icons[_name] = _icon
        event_icon = event_icons.get(event_label, theme.listIcon)
        event_btn = discord.ui.Button(label=f"Event: {event_label}", emoji=event_icon,
                                      style=discord.ButtonStyle.secondary, row=0)
        event_btn.callback = self._on_event_toggle
        self.add_item(event_btn)

        window_label = _WINDOW_LABELS[self.window_days]
        window_btn = discord.ui.Button(label=f"Window: {window_label}", emoji=theme.calendarIcon,
                                       style=discord.ButtonStyle.secondary, row=0)
        window_btn.callback = self._on_window_toggle
        self.add_item(window_btn)

        export_btn = discord.ui.Button(label="Export CSV", emoji=theme.exportIcon,
                                       style=discord.ButtonStyle.secondary, row=0)
        export_btn.callback = self._export_csv
        self.add_item(export_btn)

        prev_btn = discord.ui.Button(emoji=theme.prevIcon, style=discord.ButtonStyle.secondary,
                                     row=1, disabled=self.page == 0)
        prev_btn.callback = self._on_prev
        self.add_item(prev_btn)
        page_btn = discord.ui.Button(label=f"{self.page + 1}/{total_pages}",
                                     style=discord.ButtonStyle.secondary, row=1, disabled=True)
        self.add_item(page_btn)
        next_btn = discord.ui.Button(emoji=theme.nextIcon, style=discord.ButtonStyle.secondary,
                                     row=1, disabled=self.page >= total_pages - 1)
        next_btn.callback = self._on_next
        self.add_item(next_btn)

        if page_rows:
            options = []
            for r in page_rows:
                name = r.get("display_name") or r["name"]
                options.append(discord.SelectOption(
                    label=name[:100], value=str(r["fid"]),
                    description=f"{r['no_shows']} no-shows - {r['attended']} attended - ID {r['fid']}"[:100],
                ))
            select = discord.ui.Select(placeholder="Select a player to review...", options=options, row=2)
            select.callback = self._on_select_player
            self.add_item(select)

        back_btn = discord.ui.Button(label="Back", emoji=theme.backIcon,
                                     style=discord.ButtonStyle.secondary, row=3)
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    async def _on_event_toggle(self, interaction: discord.Interaction):
        self._event_idx = (self._event_idx + 1) % len(_EVENT_STATES)
        self.page = 0
        await self._load()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    async def _on_window_toggle(self, interaction: discord.Interaction):
        self._window_idx = (self._window_idx + 1) % len(_WINDOW_STATES)
        self.page = 0
        await self._load()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    async def _on_prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self._build_components()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    async def _on_next(self, interaction: discord.Interaction):
        self.page = min(self._total_pages() - 1, self.page + 1)
        self._build_components()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    async def _on_select_player(self, interaction: discord.Interaction):
        fid = int(interaction.data["values"][0])
        row = next((r for r in self.rows if r["fid"] == fid), None)
        name = (row.get("display_name") or row["name"]) if row else f"ID {fid}"
        view = PlayerExcuseView(self.cog, self.user_id, self.alliance_id, fid, name,
                                self.event_types, self.window_days, self)
        await view._load()
        await safe_edit_message(interaction, embed=view.build_embed(), view=view)
        view.message = await interaction.original_response()

    async def _on_back(self, interaction: discord.Interaction):
        await self.cog.show_attendance_hub(interaction, self.alliance_id)
        self.stop()

    async def _export_csv(self, interaction: discord.Interaction):
        import csv
        import io
        await interaction.response.defer(ephemeral=True)
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["FID", "Name", "No-Shows", "Attended", "Excused", "Tracked", "No-Show Rate %"])
        for r in self.rows:
            w.writerow([r["fid"], r.get("display_name") or r["name"], r["no_shows"], r["attended"],
                        r["excused"], r["tracked"], round(r["rate"] * 100)])
        data_bytes = io.BytesIO(buf.getvalue().encode("utf-8"))
        file = discord.File(data_bytes, filename=f"no_shows_{self.alliance_id}.csv")
        try:
            await interaction.user.send(file=file)
            await interaction.followup.send(
                f"{theme.verifiedIcon} No-Shows CSV sent to your DMs.", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send(
                f"{theme.deniedIcon} I couldn't DM you - enable DMs from server members.", ephemeral=True)


# ── excuse drill-down ──────────────────────────────────────────────────────

class ExcuseReasonModal(discord.ui.Modal, title="Excuse No-Show"):
    def __init__(self, parent_view, session_id):
        super().__init__()
        self.parent_view = parent_view
        self.session_id = session_id
        self.reason = discord.ui.TextInput(
            label="Reason (optional)", required=False, max_length=200,
            placeholder="e.g. told us in advance, real-life emergency")
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        await asyncio.to_thread(
            set_excused, self.session_id, self.parent_view.fid, True,
            (self.reason.value or "").strip() or None)
        self.parent_view.changed = True
        await self.parent_view.refresh(interaction)


class PlayerExcuseView(discord.ui.View):
    """One player's absences for a set of filters, with per-incident excuse
    toggles. Back reloads the parent list so its counts reflect any changes."""

    PAGE_SIZE = 20

    def __init__(self, cog, user_id, alliance_id, fid, name, event_types, window_days, parent_view):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.alliance_id = alliance_id
        self.fid = fid
        self.name = name
        self.event_types = event_types
        self.window_days = window_days
        self.parent_view = parent_view
        self.changed = False
        self.incidents = []
        self.page = 0
        self.message = None
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await check_interaction_user(interaction, self.user_id)

    async def on_timeout(self):
        await disable_expired_view(self)

    def _total_pages(self) -> int:
        return max(1, (len(self.incidents) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)

    async def _load(self):
        since = None if self.window_days is None else (date.today() - timedelta(days=self.window_days)).isoformat()
        self.incidents = await asyncio.to_thread(
            player_no_show_incidents, self.alliance_id, self.fid, self.event_types, since)
        self.page = min(self.page, self._total_pages() - 1)
        self._build_components()

    def build_embed(self) -> discord.Embed:
        total_pages = self._total_pages()
        start = self.page * self.PAGE_SIZE
        page_items = self.incidents[start:start + self.PAGE_SIZE]

        lines = [f"{theme.upperDivider}"]
        if not page_items:
            lines.append(f"{theme.warnIcon} No absences recorded for this player under the current filters.")
        else:
            for inc in page_items:
                icon = theme.verifiedIcon if inc["excused"] else theme.deniedIcon
                reason_suffix = f" - _{inc['reason']}_" if inc["excused"] and inc["reason"] else ""
                event_label = _EVENT_LABEL_BY_KEY.get(inc['event_type'], inc['event_type'])
                lines.append(f"{icon} {event_label} - {inc['event_date']}{reason_suffix}")
        lines.append(f"{theme.lowerDivider}")

        embed = discord.Embed(
            title=f"{theme.deniedIcon} No-Shows - {_isolate_rtl(self.name)}",
            description="\n".join(lines),
            color=theme.emColor1,
        )
        embed.set_footer(text=f"ID {self.fid} · Page {self.page + 1}/{total_pages} · "
                               f"Click an incident to toggle excused")
        return embed

    def _build_components(self):
        self.clear_items()
        start = self.page * self.PAGE_SIZE
        page_items = self.incidents[start:start + self.PAGE_SIZE]

        for idx, inc in enumerate(page_items):
            row = idx // 5
            label = f"{_EVENT_LABEL_BY_KEY.get(inc['event_type'], inc['event_type'])} {inc['event_date']}"[:80]
            if inc["excused"]:
                btn = discord.ui.Button(
                    label=f"Un-excuse: {label}", emoji=theme.verifiedIcon,
                    style=discord.ButtonStyle.success, row=row)
            else:
                btn = discord.ui.Button(
                    label=f"Excuse: {label}", emoji=theme.deniedIcon,
                    style=discord.ButtonStyle.secondary, row=row)
            btn.callback = self._make_toggle_callback(inc["session_id"], inc["excused"])
            self.add_item(btn)

        nav_row = 4
        total_pages = self._total_pages()
        if total_pages > 1:
            prev_btn = discord.ui.Button(emoji=theme.prevIcon, style=discord.ButtonStyle.secondary,
                                         row=nav_row, disabled=self.page == 0)
            prev_btn.callback = self._on_prev
            self.add_item(prev_btn)
            next_btn = discord.ui.Button(emoji=theme.nextIcon, style=discord.ButtonStyle.secondary,
                                         row=nav_row, disabled=self.page >= total_pages - 1)
            next_btn.callback = self._on_next
            self.add_item(next_btn)

        back_btn = discord.ui.Button(label="Back", emoji=theme.backIcon,
                                     style=discord.ButtonStyle.secondary, row=nav_row)
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    def _make_toggle_callback(self, session_id, currently_excused):
        async def callback(interaction: discord.Interaction):
            if currently_excused:
                await asyncio.to_thread(set_excused, session_id, self.fid, False)
                self.changed = True
                await self.refresh(interaction)
            else:
                await interaction.response.send_modal(ExcuseReasonModal(self, session_id))
        return callback

    async def refresh(self, interaction: discord.Interaction):
        await self._load()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    async def _on_prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self._build_components()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    async def _on_next(self, interaction: discord.Interaction):
        self.page = min(self._total_pages() - 1, self.page + 1)
        self._build_components()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self)

    async def _on_back(self, interaction: discord.Interaction):
        if self.changed:
            await self.parent_view._load()
        else:
            self.parent_view._build_components()
        await safe_edit_message(interaction, embed=self.parent_view.build_embed(), view=self.parent_view)
        self.stop()
