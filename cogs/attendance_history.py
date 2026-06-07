"""Per-player attendance history — a read-only, ID-keyed lens over
`attendance_records` (every other view is event-centric). Shows one player's
participation across all events over time plus current Power / Combat Power;
lookup by ID so former/transferred members still appear.
"""
import logging
import sqlite3
from datetime import datetime, timezone

import discord

from .pimp_my_bot import theme
from .attendance_ocr_parsers import EVENT_TYPES
from .bear_track import _isolate_rtl, _ltr_line

logger = logging.getLogger(__name__)

ATTENDANCE_DB = "db/attendance.sqlite"
USERS_DB = "db/users.sqlite"

# Status -> (icon, label). Mirrors the review legend.
_STATUS = {
    "present": ("verifiedIcon", "Present"),
    "absent": ("deniedIcon", "Absent"),
    "registered": ("hourglassIcon", "Registered"),
    "needs_review": ("warnIcon", "Needs review"),
    "no_match": ("warnIcon", "Unmatched"),
}
# Event types that have a sign-up phase, so present/(present+absent) is a
# meaningful attendance ratio. Others (Showdown) are plain participation counts.
_RATIO_EVENTS = {"foundry_battle", "canyon_clash"}


def _event_label(event_type: str, subtype: str | None, session_name: str | None = None) -> str:
    cfg = EVENT_TYPES.get(event_type)
    label = cfg.label if cfg else (event_type or "Event")
    # "Other" is uninformative — show the admin-given session name in brackets.
    if (event_type or "").lower() == "other" and session_name and session_name.lower() != "other":
        label = f"{label} ({session_name})"
    return f"{label} · {subtype}" if subtype else label


def _fmt_date(raw) -> str:
    if not raw:
        return "—"
    s = str(raw)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return s[:10]


def _compact_power(n) -> str:
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "—"
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


def _relative(ts_iso) -> str:
    if not ts_iso:
        return ""
    try:
        dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00"))
        return f" · <t:{int(dt.timestamp())}:R>"
    except (TypeError, ValueError):
        return ""


# ── queries ────────────────────────────────────────────────────────────────

def history_players(alliance_id: int) -> list[dict]:
    """Players with at least one matched attendance record under this alliance
    (lookup by ID, so former/transferred members are included). Returns dicts
    sorted by most-recent activity: {fid, nickname, current, events, last}."""
    try:
        with sqlite3.connect(ATTENDANCE_DB, timeout=30.0) as conn:
            rows = conn.execute(
                "SELECT player_id, COUNT(*), MAX(event_date), MAX(player_name) "
                "FROM attendance_records "
                "WHERE alliance_id = ? AND CAST(player_id AS INTEGER) > 0 "
                "GROUP BY player_id",
                (str(alliance_id),),
            ).fetchall()
    except sqlite3.OperationalError:
        return []
    if not rows:
        return []

    fids = [int(r[0]) for r in rows]
    nick_by_fid: dict[int, str] = {}
    current: set[int] = set()
    try:
        with sqlite3.connect(USERS_DB, timeout=30.0) as conn:
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

    players = []
    for pid, count, last, last_name in rows:
        fid = int(pid)
        players.append({
            "fid": fid,
            "nickname": nick_by_fid.get(fid) or last_name or f"ID {fid}",
            "current": fid in current,
            "events": count,
            "last": last,
        })
    players.sort(key=lambda p: (str(p["last"] or ""), p["events"]), reverse=True)
    return players


def player_timeline(alliance_id: int, fid: int) -> list[dict]:
    try:
        with sqlite3.connect(ATTENDANCE_DB, timeout=30.0) as conn:
            rows = conn.execute(
                "SELECT event_type, event_date, event_subtype, status, points, "
                "alliance_rank, player_name, session_name FROM attendance_records "
                "WHERE alliance_id = ? AND player_id = ? ORDER BY event_date DESC",
                (str(alliance_id), str(fid)),
            ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [
        {
            "event_type": r[0], "event_date": r[1], "event_subtype": r[2],
            "status": r[3], "points": r[4] or 0, "alliance_rank": r[5],
            "player_name": r[6], "session_name": r[7],
        }
        for r in rows
    ]


def player_power(fid: int) -> dict:
    out = {"nickname": None, "alliance": None, "power": None, "power_at": None,
           "combat_power": None, "combat_power_at": None}
    try:
        with sqlite3.connect(USERS_DB, timeout=30.0) as conn:
            row = conn.execute(
                "SELECT nickname, alliance, power, power_updated_at, combat_power, "
                "combat_power_updated_at FROM users WHERE fid = ?",
                (fid,),
            ).fetchone()
    except sqlite3.OperationalError:
        return out
    if row:
        (out["nickname"], out["alliance"], out["power"], out["power_at"],
         out["combat_power"], out["combat_power_at"]) = row
    return out


def summarize(timeline: list[dict]) -> dict:
    """Per-event-type rollup. Ratio events report present/(present+absent);
    others report a participation count."""
    by_type: dict[str, dict] = {}
    for r in timeline:
        et = r["event_type"]
        d = by_type.setdefault(et, {"present": 0, "absent": 0, "other": 0,
                                    "points": 0, "count": 0})
        d["count"] += 1
        d["points"] += r["points"] or 0
        if r["status"] == "present":
            d["present"] += 1
        elif r["status"] == "absent":
            d["absent"] += 1
        else:
            d["other"] += 1
    return by_type


# ── player picker ────────────────────────────────────────────────────────

class HistoryPlayerSelectView(discord.ui.View):
    """Paginated single-select of players who have attendance history, with a
    name/ID search. Former members are flagged."""

    PAGE_SIZE = 25

    def __init__(self, cog, user_id, alliance_id, alliance_name, players):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.players = players
        self.filter_text = ""
        self.page = 0
        self._build()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who opened this view can use it.",
                ephemeral=True,
            )
            return False
        return True

    def _filtered(self):
        if not self.filter_text:
            return self.players
        f = self.filter_text.casefold()
        return [p for p in self.players
                if f in p["nickname"].casefold() or f in str(p["fid"])]

    def _build(self):
        self.clear_items()
        items = self._filtered()
        total_pages = max(1, (len(items) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.page = min(self.page, total_pages - 1)
        start = self.page * self.PAGE_SIZE
        page_items = items[start:start + self.PAGE_SIZE]

        if page_items:
            options = []
            for p in page_items:
                label = _ltr_line(f"{p['nickname']}")[:100]
                suffix = "" if p["current"] else " · former"
                desc = f"ID {p['fid']} · {p['events']} events{suffix}"[:100]
                options.append(discord.SelectOption(
                    label=label or f"ID {p['fid']}", value=str(p["fid"]), description=desc))
            select = discord.ui.Select(placeholder="Select a player…", options=options, row=0)
            select.callback = self._on_select
            self.add_item(select)

        search_btn = discord.ui.Button(
            label="Search", emoji=theme.searchIcon,
            style=discord.ButtonStyle.primary, row=1)
        search_btn.callback = self._on_search
        self.add_item(search_btn)

        if self.filter_text:
            clear_btn = discord.ui.Button(
                label="Clear", emoji=theme.deniedIcon,
                style=discord.ButtonStyle.secondary, row=1)
            clear_btn.callback = self._on_clear
            self.add_item(clear_btn)

        if total_pages > 1:
            prev_btn = discord.ui.Button(
                emoji=theme.prevIcon, style=discord.ButtonStyle.secondary,
                row=2, disabled=self.page == 0)
            prev_btn.callback = self._on_prev
            self.add_item(prev_btn)
            page_btn = discord.ui.Button(
                label=f"{self.page + 1}/{total_pages}",
                style=discord.ButtonStyle.secondary, row=2, disabled=True)
            self.add_item(page_btn)
            next_btn = discord.ui.Button(
                emoji=theme.nextIcon, style=discord.ButtonStyle.secondary,
                row=2, disabled=self.page >= total_pages - 1)
            next_btn.callback = self._on_next
            self.add_item(next_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=3)
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    def build_embed(self) -> discord.Embed:
        items = self._filtered()
        desc = [
            f"{theme.upperDivider}",
            f"Pick a player to see their attendance history across all events.",
            f"{theme.membersIcon} **{len(items)}** player(s) with history"
            + (f" matching `{self.filter_text}`" if self.filter_text else "")
            + f" in **{self.alliance_name}**.",
            f"{theme.lowerDivider}",
        ]
        return discord.Embed(
            title=f"{theme.listIcon} Player History — Select Player",
            description="\n".join(desc), color=theme.emColor1,
        )

    async def _on_select(self, interaction: discord.Interaction):
        fid = int(interaction.data["values"][0])
        view = PlayerHistoryView(
            self.cog, self.user_id, self.alliance_id, self.alliance_name,
            fid, parent=self)
        await view.render(interaction)

    async def _on_search(self, interaction: discord.Interaction):
        await interaction.response.send_modal(_HistorySearchModal(self))

    async def _on_clear(self, interaction: discord.Interaction):
        self.filter_text = ""
        self.page = 0
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_next(self, interaction: discord.Interaction):
        self.page += 1
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_back(self, interaction: discord.Interaction):
        await self.cog.show_attendance_hub(interaction, self.alliance_id)


class _HistorySearchModal(discord.ui.Modal):
    def __init__(self, view: HistoryPlayerSelectView):
        super().__init__(title="Search Players")
        self.view = view
        self.query = discord.ui.TextInput(
            label="Name or ID", required=False, max_length=50,
            default=view.filter_text)
        self.add_item(self.query)

    async def on_submit(self, interaction: discord.Interaction):
        self.view.filter_text = self.query.value.strip()
        self.view.page = 0
        self.view._build()
        await interaction.response.edit_message(
            embed=self.view.build_embed(), view=self.view)


# ── history view ───────────────────────────────────────────────────────────

class PlayerHistoryView(discord.ui.View):
    PAGE_SIZE = 10

    def __init__(self, cog, user_id, alliance_id, alliance_name, fid, parent=None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.fid = fid
        self.parent = parent
        self.page = 0
        self.event_filter = None  # None = all event types
        self.timeline = player_timeline(alliance_id, fid)
        self.power = player_power(fid)
        self._build()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who opened this view can use it.",
                ephemeral=True,
            )
            return False
        return True

    def _nickname(self) -> str:
        if self.power.get("nickname"):
            return self.power["nickname"]
        return self.timeline[0]["player_name"] if self.timeline else f"ID {self.fid}"

    def _filtered(self):
        if not self.event_filter:
            return self.timeline
        return [r for r in self.timeline if r["event_type"] == self.event_filter]

    def _event_types(self) -> list[str]:
        seen = []
        for r in self.timeline:
            if r["event_type"] not in seen:
                seen.append(r["event_type"])
        return seen

    def _build(self):
        self.clear_items()
        types = self._event_types()
        if len(types) > 1:
            options = [discord.SelectOption(
                label="All events", value="__all__", default=self.event_filter is None)]
            for et in types:
                options.append(discord.SelectOption(
                    label=_event_label(et, None)[:100], value=et,
                    default=self.event_filter == et))
            select = discord.ui.Select(placeholder="Filter by event…", options=options, row=0)
            select.callback = self._on_filter
            self.add_item(select)

        total_pages = self._total_pages()
        if total_pages > 1:
            prev_btn = discord.ui.Button(
                emoji=theme.prevIcon, style=discord.ButtonStyle.secondary,
                row=1, disabled=self.page == 0)
            prev_btn.callback = self._on_prev
            self.add_item(prev_btn)
            page_btn = discord.ui.Button(
                label=f"{self.page + 1}/{total_pages}",
                style=discord.ButtonStyle.secondary, row=1, disabled=True)
            self.add_item(page_btn)
            next_btn = discord.ui.Button(
                emoji=theme.nextIcon, style=discord.ButtonStyle.secondary,
                row=1, disabled=self.page >= total_pages - 1)
            next_btn.callback = self._on_next
            self.add_item(next_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    def _total_pages(self) -> int:
        n = len(self._filtered())
        return max(1, (n + self.PAGE_SIZE - 1) // self.PAGE_SIZE)

    def _summary_lines(self) -> list[str]:
        summary = summarize(self.timeline)
        if not summary:
            return [f"{theme.warnIcon} No attendance records yet."]
        parts = []
        for et, d in summary.items():
            label = _event_label(et, None)
            if et in _RATIO_EVENTS:
                denom = d["present"] + d["absent"]
                rate = f"{d['present']}/{denom}" if denom else f"{d['present']}"
                parts.append(f"**{label}:** {rate}")
            else:
                parts.append(f"**{label}:** {d['count']} event{'s' if d['count'] != 1 else ''}")
        total_points = sum(d["points"] for d in summary.values())
        lines = list(parts)  # one event per line
        if total_points:
            lines.append(f"{theme.chartIcon} Total points across events: `{total_points:,}`")
        return lines

    def build_embed(self) -> discord.Embed:
        nick = _isolate_rtl(self._nickname())
        loc = self.alliance_name
        if self.power.get("alliance") is not None and str(self.power["alliance"]) != str(self.alliance_id):
            loc += " · former"
        header = [
            f"{theme.upperDivider}",
            f"{theme.userIcon} **{nick}** · `ID {self.fid}` · {loc}",
        ]
        pwr = []
        if self.power.get("power") is not None:
            pwr.append(f"{theme.chartIcon} Power `{_compact_power(self.power['power'])}`"
                       f"{_relative(self.power['power_at'])}")
        if self.power.get("combat_power") is not None:
            pwr.append(f"{theme.shieldIcon} Combat `{_compact_power(self.power['combat_power'])}`"
                       f"{_relative(self.power['combat_power_at'])}")
        if pwr:
            header.append(" · ".join(pwr))
        header.append("")
        header.extend(self._summary_lines())
        header.append(f"{theme.lowerDivider}")

        rows = self._filtered()
        total_pages = self._total_pages()
        self.page = min(self.page, total_pages - 1)
        start = self.page * self.PAGE_SIZE
        body = []
        for r in rows[start:start + self.PAGE_SIZE]:
            icon_attr, _label = _STATUS.get(r["status"], ("listIcon", r["status"]))
            icon = getattr(theme, icon_attr, "")
            line = (f"`{_fmt_date(r['event_date'])}` {icon} "
                    f"{_event_label(r['event_type'], r['event_subtype'], r.get('session_name'))}")
            extra = []
            if r["points"]:
                extra.append(f"`{r['points']:,}`")
            if r["alliance_rank"]:
                extra.append(f"rank #{r['alliance_rank']}")
            if extra:
                line += " — " + " · ".join(extra)
            body.append(_ltr_line(line))
        if not body:
            body = [f"{theme.warnIcon} No records for this filter."]

        embed = discord.Embed(
            title=f"{theme.listIcon} Attendance History",
            description="\n".join(header) + "\n" + "\n".join(body),
            color=theme.emColor1,
        )
        legend = (f"{theme.verifiedIcon} present · {theme.deniedIcon} absent · "
                  f"{theme.hourglassIcon} registered · {theme.warnIcon} needs review")
        embed.set_footer(text=f"Page {self.page + 1}/{total_pages} · {legend}")
        return embed

    async def render(self, interaction: discord.Interaction):
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=self.build_embed(), view=self)
        else:
            await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_filter(self, interaction: discord.Interaction):
        val = interaction.data["values"][0]
        self.event_filter = None if val == "__all__" else val
        self.page = 0
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_next(self, interaction: discord.Interaction):
        self.page += 1
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_back(self, interaction: discord.Interaction):
        if self.parent is not None:
            self.parent._build()
            await interaction.response.edit_message(
                embed=self.parent.build_embed(), view=self.parent)
        else:
            await self.cog.show_attendance_hub(interaction, self.alliance_id)
