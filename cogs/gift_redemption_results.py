"""Gift code Redemption History viewer: per-account outcomes (redeemed / already
redeemed / failed) for a chosen code, paginated and filterable by alliance."""

import asyncio
import sqlite3
import logging

import discord

from .pimp_my_bot import theme, safe_edit_message, check_interaction_user, disable_expired_view
from .permission_handler import PermissionManager

logger = logging.getLogger('gift')

PAGE_SIZE = 20

# Reuse the proven RTL helpers shared with bear_track / attendance: an LRM prefix
# keeps the whole line left-to-right (so icons/numbers/parens stay in order) while
# each RTL name is wrapped in an FSI…PDI isolate.
from .bear_track import _isolate_rtl, _ltr_line


SUCCESS_STATUSES = {"SUCCESS", "SAME TYPE EXCHANGE"}
ALREADY_STATUSES = {"RECEIVED"}
# The code itself is bad/expired — not a per-account outcome. Shown as a banner.
CODE_LEVEL_STATUSES = {"CDK_NOT_FOUND", "USAGE_LIMIT", "TIME_ERROR"}

STATUS_LABELS = {
    "SUCCESS": "Redeemed",
    "SAME TYPE EXCHANGE": "Redeemed",
    "RECEIVED": "Already redeemed",
    "CONNECTION_ERROR": "Connection error",
    "CAPTCHA_INVALID": "Captcha failed",
    "ERROR": "Error",
}

BUCKET_ORDER = ("success", "already", "failed")
BUCKET_META = {
    "success": ("Redeemed", "verifiedIcon"),
    "already": ("Already Redeemed", "giftIcon"),
    "failed": ("Failed", "deniedIcon"),
}


def _bucket(status: str) -> str:
    if status in SUCCESS_STATUSES:
        return "success"
    if status in ALREADY_STATUSES:
        return "already"
    if status in CODE_LEVEL_STATUSES:
        return "code"
    return "failed"


def _load_results(giftcode: str, cursor_rows):
    """Merge redemption rows (giftcode.sqlite) with nicknames/alliance."""
    fids = [r[0] for r in cursor_rows]
    users = {}
    if fids:
        with sqlite3.connect('db/users.sqlite', timeout=30.0) as uconn:
            ucur = uconn.cursor()
            for i in range(0, len(fids), 500):
                chunk = fids[i:i + 500]
                placeholders = ",".join("?" * len(chunk))
                ucur.execute(
                    f"SELECT fid, nickname, alliance FROM users WHERE fid IN ({placeholders})",
                    chunk,
                )
                for fid, nickname, alliance in ucur.fetchall():
                    users[fid] = (nickname, alliance)

    rows = []
    for fid, status, ts in cursor_rows:
        nickname, alliance = users.get(fid, (None, None))
        rows.append({
            "fid": fid,
            "nickname": nickname or str(fid),
            "alliance": str(alliance) if alliance is not None else None,
            "status": status,
            "bucket": _bucket(status),
            "ts": ts,
        })
    return rows


async def show_redeem_results(cog, interaction: discord.Interaction):
    """Entry point from the main Gift Code menu."""
    is_admin, _ = PermissionManager.is_admin(interaction.user.id)
    if not is_admin:
        msg = f"{theme.deniedIcon} Only bot admins can view redeem history."
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
        return

    cog.cursor.execute("SELECT giftcode, date FROM gift_codes ORDER BY date DESC LIMIT 25")
    codes = cog.cursor.fetchall()
    alliances, is_global = PermissionManager.get_admin_alliances(
        interaction.user.id, interaction.guild_id or 0
    )
    allowed_ids = None if is_global else {str(aid) for aid, _ in alliances}

    view = RedeemHistoryView(cog, interaction.user.id, codes, alliances, allowed_ids)
    await safe_edit_message(interaction, embed=view.build_embed(), view=view, content=None)
    view.message = await interaction.original_response()


class RedeemHistoryView(discord.ui.View):
    """One-screen Redemption History: code dropdown + alliance filter + status
    toggles + pagination. Everything but the code dropdown and Back is disabled
    until a code is selected."""

    def __init__(self, cog, user_id, codes, alliances, allowed_ids):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.codes = codes                  # [(giftcode, date), ...]
        self.alliances = alliances          # [(alliance_id, name), ...]
        self.allowed_ids = allowed_ids      # None => global (all)
        self.message = None

        self.selected_code = None
        self.all_rows = []                  # permission-scoped rows for selected code
        self.alliance_filter = None         # None => all accessible
        self.active_buckets = set(BUCKET_ORDER)
        self.page = 0
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await check_interaction_user(interaction, self.user_id)

    # --- data helpers ---------------------------------------------------
    def _alliance_rows(self):
        if self.alliance_filter is None:
            return self.all_rows
        return [r for r in self.all_rows if r["alliance"] == self.alliance_filter]

    def _counts(self, rows):
        counts = {"success": 0, "already": 0, "failed": 0, "code": 0}
        for r in rows:
            counts[r["bucket"]] = counts.get(r["bucket"], 0) + 1
        return counts

    def _visible_rows(self, rows):
        return [r for r in rows if r["bucket"] in self.active_buckets]

    def _alliance_name(self, alliance_id):
        for aid, name in self.alliances:
            if str(aid) == str(alliance_id):
                return name
        return f"Alliance {alliance_id}"

    # --- rendering ------------------------------------------------------
    def build_embed(self) -> discord.Embed:
        if not self.codes:
            return discord.Embed(
                title=f"{theme.archiveIcon} Redemption History",
                description="No gift codes have been added yet.",
                color=theme.emColor1,
            )
        if self.selected_code is None:
            return discord.Embed(
                title=f"{theme.archiveIcon} Redemption History",
                description=(
                    "Pick a gift code above to see which accounts redeemed it, "
                    "already had it, or failed."
                ),
                color=theme.emColor1,
            )

        arows = self._alliance_rows()
        counts = self._counts(arows)
        visible = self._visible_rows(arows)

        total_pages = max(1, (len(visible) + PAGE_SIZE - 1) // PAGE_SIZE)
        self.page = max(0, min(self.page, total_pages - 1))
        page_rows = visible[self.page * PAGE_SIZE:(self.page + 1) * PAGE_SIZE]

        header = (
            f"{theme.verifiedIcon} Redeemed: `{counts['success']}`  "
            f"{theme.giftIcon} Already: `{counts['already']}`  "
            f"{theme.deniedIcon} Failed: `{counts['failed']}`"
        )

        lines = []
        if counts["code"]:
            lines.append(
                f"{theme.warnIcon} This code looks invalid or expired for "
                f"`{counts['code']}` account(s) (bad/expired code)."
            )
            lines.append("")

        if not page_rows:
            lines.append("_No accounts match the current filters._")
        else:
            icons = {b: getattr(theme, BUCKET_META[b][1]) for b in BUCKET_ORDER}
            for r in page_rows:
                label = STATUS_LABELS.get(r["status"], r["status"])
                name = _isolate_rtl(r["nickname"])
                lines.append(_ltr_line(f"{icons[r['bucket']]} **{name}** (`{r['fid']}`) - {label}"))

        scope = "All alliances" if self.alliance_filter is None else self._alliance_name(self.alliance_filter)
        return discord.Embed(
            title=f"{theme.archiveIcon} Redemption History - {self.selected_code}",
            description=(
                f"{header}\n"
                f"{theme.upperDivider}\n"
                + "\n".join(lines)
                + f"\n{theme.lowerDivider}\n"
                f"Scope: **{scope}**  ·  Page **{self.page + 1}/{total_pages}**"
            ),
            color=theme.emColor1,
        )

    def _build_components(self):
        self.clear_items()
        has_code = self.selected_code is not None

        # Row 0: gift code picker (always active).
        if self.codes:
            options = [
                discord.SelectOption(
                    label=code[:100],
                    description=(f"Added {date}" if date else None),
                    value=code,
                    default=code == self.selected_code,
                )
                for code, date in self.codes
            ]
            code_select = discord.ui.Select(
                placeholder="Pick a gift code", options=options, row=0,
            )
            code_select.callback = self._on_code
            self.add_item(code_select)

        # Row 1: alliance filter (only when more than one is accessible).
        if self.alliances and len(self.alliances) > 1:
            opts = [discord.SelectOption(
                label="All alliances", value="__all__",
                default=self.alliance_filter is None,
            )]
            for aid, name in self.alliances[:24]:
                opts.append(discord.SelectOption(
                    label=name[:100], value=str(aid),
                    default=str(aid) == str(self.alliance_filter),
                ))
            asel = discord.ui.Select(
                placeholder="Filter by alliance", options=opts, row=1, disabled=not has_code,
            )
            asel.callback = self._on_alliance
            self.add_item(asel)

        # Row 2: status filter toggles with live counts (disabled until a code).
        counts = self._counts(self._alliance_rows())
        for bucket in BUCKET_ORDER:
            title, icon_name = BUCKET_META[bucket]
            on = bucket in self.active_buckets
            btn = discord.ui.Button(
                label=f"{title} ({counts[bucket]})",
                emoji=getattr(theme, icon_name),
                style=discord.ButtonStyle.success if on else discord.ButtonStyle.secondary,
                row=2,
                disabled=not has_code,
            )
            btn.callback = self._make_toggle(bucket)
            self.add_item(btn)

        # Row 3: pagination (disabled until a code).
        prev_btn = discord.ui.Button(label="Prev", emoji=f"{theme.prevIcon}", style=discord.ButtonStyle.primary, row=3, disabled=not has_code)
        prev_btn.callback = self._prev
        self.add_item(prev_btn)
        next_btn = discord.ui.Button(label="Next", emoji=f"{theme.nextIcon}", style=discord.ButtonStyle.primary, row=3, disabled=not has_code)
        next_btn.callback = self._next
        self.add_item(next_btn)

        # Row 4: Back only (returns to the gift menu).
        back_btn = discord.ui.Button(label="Back", emoji=f"{theme.backIcon}", style=discord.ButtonStyle.secondary, row=4)
        back_btn.callback = self._back
        self.add_item(back_btn)

    async def _refresh(self, interaction: discord.Interaction):
        self._build_components()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self, content=None)

    # --- callbacks ------------------------------------------------------
    async def _on_code(self, interaction: discord.Interaction):
        self.selected_code = interaction.data["values"][0]
        await interaction.response.defer()
        self.cog.cursor.execute(
            "SELECT fid, status, last_attempt_at FROM user_giftcodes WHERE giftcode = ?",
            (self.selected_code,),
        )
        cursor_rows = self.cog.cursor.fetchall()
        rows = await asyncio.to_thread(_load_results, self.selected_code, cursor_rows)
        if self.allowed_ids is None:
            self.all_rows = rows
        else:
            self.all_rows = [r for r in rows if r["alliance"] in self.allowed_ids]
        self.alliance_filter = None
        self.active_buckets = set(BUCKET_ORDER)
        self.page = 0
        self._build_components()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self, content=None)

    async def _on_alliance(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        self.alliance_filter = None if value == "__all__" else value
        self.page = 0
        await self._refresh(interaction)

    def _make_toggle(self, bucket):
        async def _toggle(interaction: discord.Interaction):
            if bucket in self.active_buckets:
                self.active_buckets.discard(bucket)
            else:
                self.active_buckets.add(bucket)
            self.page = 0
            await self._refresh(interaction)
        return _toggle

    async def _prev(self, interaction: discord.Interaction):
        self.page -= 1
        await self._refresh(interaction)

    async def _next(self, interaction: discord.Interaction):
        self.page += 1
        await self._refresh(interaction)

    async def _back(self, interaction: discord.Interaction):
        await self.cog.show_gift_menu(interaction)

    async def on_timeout(self):
        await disable_expired_view(self)
