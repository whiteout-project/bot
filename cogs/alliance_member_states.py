"""Member state management, under Alliance Management -> Member States."""

import asyncio
import logging

import discord

from . import gift_state_resolver as gsr
from .bot_level_mapping import parse_state
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, safe_edit_message, check_interaction_user, MenuView

logger = logging.getLogger('gift')

PAGE_SIZE = 25


def _count(n, word):
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def _scan_lines(cog):
    """Live state-scan progress lines, or nothing when no scan is queued."""
    running = cog.kingdom_scan_status()
    if not running:
        return []
    checked = running['total'] - len(running['remaining'])
    label = "Auto-scan" if running.get('mode') == 'auto' else "Scanning now"
    out = [f"\n{theme.hourglassIcon} {label}: `{checked}/{running['total']}` members checked, "
           f"`{running.get('resolved', 0)}` found."]
    if running.get('status') == 'queued' and not running.get('started'):
        out.append("└ Waiting for its turn in the queue.")
    elif running.get('status') == 'queued' and checked < running['total']:
        out.append("└ Paused while other work runs. It picks up where it left off.")
    elif running.get('waiting'):
        out.append("└ Waiting while the bot is busy reading screenshots or near its game API limit.")
    elif running.get('probe_total'):
        out.append(f"└ `{running['probe']}/{running['probe_total']}` states checked "
                   f"for the member being scanned.")
    if running.get('mode') != 'auto':
        out.append("└ Detailed progress is posted in each alliance's log channel.")
    return out


def _catchup_lines(cog):
    """Live 'still redeeming missed codes' line, or nothing when no catch-up is queued."""
    catching = cog.member_catchup_status()
    if not catching:
        return []
    return [f"\n{theme.giftIcon} Redeeming missed codes: "
            f"`{catching['done']}/{catching['codes']}` for `{catching['members']}` member(s)."]


def _hub_counts():
    survey = gsr.survey_alliance_bindings()
    unbound = [r for r in survey if r["current_kid"] is None and not r["multistate"]]
    return {
        **gsr.member_fix_counts(),
        'unbound': len(unbound),
        'bindable': sum(1 for r in unbound if r["proposed_kid"] is not None),
    }


def parse_scan_range(lo_text, hi_text):
    """(min, max) from the range modal, None for both blank (default range), or the error text."""
    if not lo_text.strip() and not hi_text.strip():
        return None
    try:
        lo, hi = int(lo_text.strip()), int(hi_text.strip())
    except ValueError:
        return "Enter whole numbers only, like `1` and `2000`."
    if lo < 1 or hi > gsr.SCAN_MAX_KINGDOM:
        return f"State numbers run from 1 to {gsr.SCAN_MAX_KINGDOM}."
    if lo > hi:
        return "The lowest state can't be higher than the highest state."
    return lo, hi


def _queue_result_text(queued):
    if queued is None:
        return "A state scan is already running."
    if queued == 0:
        return "Nobody needs a scan right now."
    return (f"Scanning {_count(queued, 'member')} now. It steps aside whenever a gift code needs "
            f"redeeming, so it can take a while.")


async def _is_global_admin(interaction: discord.Interaction) -> bool:
    """Every action here spans all alliances, so it's Global-only; denies with an ephemeral."""
    _, is_global = PermissionManager.is_admin(interaction.user.id)
    if not is_global:
        await interaction.response.send_message(
            f"{theme.deniedIcon} Only global administrators can manage member states.",
            ephemeral=True,
        )
    return is_global


async def show_state_management(cog, interaction: discord.Interaction):
    if not await _is_global_admin(interaction):
        return
    view = StateManagementView(cog, interaction.user.id)
    await safe_edit_message(interaction, embed=await view.build_embed(), view=view, content=None)


class _MenuView(MenuView):
    """Shared plumbing: dynamic buttons, refresh in place, Back to the hub."""

    def __init__(self, cog, user_id):
        super().__init__(user_id)
        self.cog = cog
        self.last_result = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Re-checked per click: the menu lives for hours and admin rights can change meanwhile.
        return await _is_global_admin(interaction) and await super().interaction_check(interaction)

    def _add_button(self, label, emoji, style, row, callback, disabled=False):
        button = discord.ui.Button(label=label, emoji=f"{emoji}", style=style, row=row, disabled=disabled)
        button.callback = callback
        self.add_item(button)

    async def refresh(self, interaction: discord.Interaction):
        await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)

    async def _back_to_hub(self, interaction: discord.Interaction):
        hub = StateManagementView(self.cog, self.original_user_id)
        await safe_edit_message(interaction, embed=await hub.build_embed(), view=hub, content=None)


class _PagedView(_MenuView):
    def __init__(self, cog, user_id, page=0):
        super().__init__(cog, user_id)
        self.page = page
        self.rows = []
        self._keep_rows = False

    async def _rows(self, load):
        """The loaded rows on a page flip, a fresh load on every other render."""
        if not self._keep_rows:
            self.rows = await asyncio.to_thread(load)
        self._keep_rows = False
        return self.rows

    def _page_rows(self):
        pages = max(1, (len(self.rows) + PAGE_SIZE - 1) // PAGE_SIZE)
        self.page = max(0, min(self.page, pages - 1))
        if pages > 1:
            self._add_button("Prev", theme.prevIcon, discord.ButtonStyle.secondary, 1,
                             self._on_prev, disabled=self.page == 0)
            self._add_button("Next", theme.nextIcon, discord.ButtonStyle.secondary, 1,
                             self._on_next, disabled=self.page >= pages - 1)
        return self.rows[self.page * PAGE_SIZE:(self.page + 1) * PAGE_SIZE]

    async def _on_prev(self, interaction: discord.Interaction):
        self.page -= 1
        self._keep_rows = True
        await self.refresh(interaction)

    async def _on_next(self, interaction: discord.Interaction):
        self.page += 1
        self._keep_rows = True
        await self.refresh(interaction)


class StateManagementView(_MenuView):
    """Member States hub: status at a glance and the three sub-menus."""

    async def build_embed(self):
        counts = await asyncio.to_thread(_hub_counts)
        settings = await asyncio.to_thread(gsr.get_scan_settings)
        to_fix = counts['wrong'] + counts['missing']

        self.clear_items()
        self._add_button(f"Members to Fix ({to_fix})", theme.membersIcon,
                         discord.ButtonStyle.danger if to_fix else discord.ButtonStyle.secondary,
                         0, self._open(MembersToFixView))
        self._add_button("Alliance States", theme.allianceIcon, discord.ButtonStyle.primary,
                         0, self._open(AllianceKingdomsView))
        self._add_button(f"State Scan: {'On' if settings['enabled'] else 'Off'}", theme.searchIcon,
                         discord.ButtonStyle.primary, 0, self._open(KingdomScanView))
        self._add_button("Back", theme.backIcon, discord.ButtonStyle.secondary, 1, self._on_back)

        scan = (f"On, states {settings['min']}-{settings['max']}" if settings['enabled'] else "Off")
        lines = [
            "Gift code redemption needs each member's state. You can fix missing or wrong states here.\n",
            f"{theme.upperDivider}",
            f"{theme.membersIcon} **Members to fix:** `{counts['wrong']}` wrong state, "
            f"`{counts['missing']}` no state",
            f"{theme.allianceIcon} **Alliances without a state:** `{counts['unbound']}` "
            f"({counts['bindable']} can be auto-bound)",
            f"{theme.searchIcon} **State scan:** {scan}",
        ]
        lines.extend(_scan_lines(self.cog))
        lines.extend(_catchup_lines(self.cog))
        lines += [
            f"{theme.lowerDivider}",
            f"{theme.membersIcon} **Members to Fix**",
            "└ Set states by hand, copy the alliance's state, or scan for them.",
            f"{theme.allianceIcon} **Alliance States**",
            "└ Give alliances their state and mark alliances that span several states.",
            f"{theme.searchIcon} **State Scan**",
            "└ Let the bot find missing states by itself while it has nothing else to do.",
        ]
        return discord.Embed(title=f"{theme.stateIcon} Member States",
                             description="\n".join(lines), color=theme.emColor1)

    def _open(self, view_class):
        async def callback(interaction: discord.Interaction):
            view = view_class(self.cog, self.original_user_id)
            await safe_edit_message(interaction, embed=await view.build_embed(), view=view, content=None)
        return callback

    async def _on_back(self, interaction: discord.Interaction):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu:
            await main_menu.show_alliance_management(interaction)


class MembersToFixView(_PagedView):
    """Members with no state on file, or whose state the game rejected (error 40020)."""

    @staticmethod
    def option_description(fid, kid, reason):
        if reason == 'wrong':
            return f"ID {fid} - state {kid} was rejected"[:100]
        return f"ID {fid} - no state on file"[:100]

    async def build_embed(self):
        await self._rows(gsr.members_to_fix)
        wrong = sum(1 for r in self.rows if r[3] == 'wrong')
        missing = len(self.rows) - wrong
        self.clear_items()

        page_rows = self._page_rows()
        if page_rows:
            select = discord.ui.Select(
                placeholder="Pick a member to set their state", row=0,
                options=[discord.SelectOption(label=str(nick or fid)[:100], value=str(fid),
                                              description=self.option_description(fid, kid, reason))
                         for fid, nick, kid, reason in page_rows])
            select.callback = self._on_select
            self.add_item(select)
            self._add_button("Set All to State", theme.membersIcon,
                             discord.ButtonStyle.success, 2, self._on_set_all, disabled=wrong == 0)
            self._add_button("Use Alliance State", theme.allianceIcon, discord.ButtonStyle.primary,
                             2, self._on_use_alliance, disabled=missing == 0)
            self._add_button("Scan These Now", theme.searchIcon, discord.ButtonStyle.primary,
                             2, self._on_scan)
            self._add_button("Dismiss Warnings", theme.trashIcon, discord.ButtonStyle.secondary,
                             2, self._on_dismiss, disabled=wrong == 0)
        self._add_button("Back", theme.backIcon, discord.ButtonStyle.secondary, 3, self._back_to_hub)

        lines = [
            "These members have no state on file, or the game rejected their state the last time "
            "a code was redeemed for them. A rejected state usually means they transferred or left the game.\n",
            f"{theme.upperDivider}",
            f"{theme.stateIcon} **Wrong state:** `{wrong}`",
            f"{theme.membersIcon} **No state:** `{missing}`\n",
            f"{theme.userIcon} **Pick a member**",
            "└ Type their state. They get the codes they missed right away.",
            f"{theme.membersIcon} **Set All to State**",
            "└ Put every member with a wrong state in one state, for a group that transferred together.",
            f"{theme.allianceIcon} **Use Alliance State**",
            "└ Give members with no state their alliance's state right away.",
            f"{theme.searchIcon} **Scan These Now**",
            "└ Try state numbers for each member in the background until one fits. This can take hours.",
            f"{theme.trashIcon} **Dismiss Warnings**",
            "└ Clear the wrong-state warnings. Members who are still wrong show up again after the next redemption.",
        ]
        lines.extend(_scan_lines(self.cog))
        lines.extend(_catchup_lines(self.cog))
        if not self.rows:
            lines.append(f"\n{theme.verifiedIcon} Every member has a working state right now.")
        if self.last_result:
            lines.append(f"\n{theme.verifiedIcon} {self.last_result}")
        lines.append(f"{theme.lowerDivider}")
        return discord.Embed(title=f"{theme.membersIcon} Members to Fix",
                             description="\n".join(lines), color=theme.emColor1)

    async def _on_select(self, interaction: discord.Interaction):
        fid = int(interaction.data["values"][0])
        match = next((r for r in self.rows if r[0] == fid), None)
        await interaction.response.send_modal(
            MemberStateModal(self, fid, (match[1] if match else None) or str(fid),
                             match[2] if match else None))

    async def _on_set_all(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            BulkStateModal(self, [r[0] for r in self.rows if r[3] == 'wrong']))

    async def _on_use_alliance(self, interaction: discord.Interaction):
        n, caught = await asyncio.to_thread(self.cog.assign_alliance_state_to_missing)
        self.last_result = (
            f"Gave {_count(n, 'member')} their alliance's state."
            + (f" Redeeming the codes {caught} of them missed." if caught else "")
            if n else "Their alliances don't have a state yet either. Give the alliances a state first."
        )
        await self.refresh(interaction)

    async def _on_scan(self, interaction: discord.Interaction):
        self.last_result = _queue_result_text(self.cog.queue_kingdom_scan('now'))
        await self.refresh(interaction)

    async def _on_dismiss(self, interaction: discord.Interaction):
        wrong = [r[0] for r in self.rows if r[3] == 'wrong']
        await asyncio.to_thread(gsr.clear_state_mismatch_many, wrong)
        self.cog.logger.info(f"GiftOps: admin dismissed {len(wrong)} wrong-state warning(s)")
        self.last_result = f"Dismissed {_count(len(wrong), 'warning')}."
        await self.refresh(interaction)


class AllianceKingdomsView(_PagedView):
    """Each alliance's home state, auto-bind, and the multistate flag."""

    async def build_embed(self):
        await self._rows(gsr.survey_alliance_bindings)
        self.rows.sort(key=lambda r: (not r["multistate"], str(r["name"]).lower()))
        self.clear_items()

        page_rows = self._page_rows()
        if page_rows:
            options = []
            for r in page_rows:
                on = r["multistate"]
                status = "Multistate" if on else (
                    f"State {r['current_kid']}" if r["current_kid"] else "No state")
                options.append(discord.SelectOption(
                    label=str(r["name"])[:100], value=str(r["alliance_id"]),
                    description=f"{status}. Pick to {'unmark' if on else 'mark'} as multistate"[:100],
                    emoji=theme.verifiedIcon if on else None,
                ))
            select = discord.ui.Select(placeholder="Pick an alliance to mark or unmark it as multistate",
                                       options=options, row=0)
            select.callback = self._on_select
            self.add_item(select)
        self._add_button("Auto-bind Alliances", theme.chartIcon, discord.ButtonStyle.primary, 2, self._on_bind)
        self._add_button("Back", theme.backIcon, discord.ButtonStyle.secondary, 2, self._back_to_hub)

        bound = sum(1 for r in self.rows if r["current_kid"] is not None and not r["multistate"])
        multi = sum(1 for r in self.rows if r["multistate"])
        lines = [
            "Members with no state of their own use their alliance's state. Only players from "
            "that state can be added or moved into the alliance.\n",
            f"{theme.upperDivider}",
            f"{theme.allianceIcon} **With a state:** `{bound}` · **Without:** "
            f"`{len(self.rows) - bound - multi}` · **Multistate:** `{multi}`\n",
            f"{theme.chartIcon} **Auto-bind Alliances**",
            "└ Give each alliance the state most of its members are in.",
            f"{theme.allianceIcon} **Pick an alliance**",
            "└ Mark or unmark it as multistate. Multistate alliances are never auto-bound, and their "
            "members keep their own states.",
            f"\n{theme.infoIcon} Too few known members to auto-bind? Pick the alliance under "
            f"Alliance Management and use **Set State**.",
        ]
        if self.last_result:
            lines.append(f"\n{theme.verifiedIcon} {self.last_result}")
        lines.append(f"{theme.lowerDivider}")
        return discord.Embed(title=f"{theme.allianceIcon} Alliance States",
                             description="\n".join(lines), color=theme.emColor1)

    async def _on_select(self, interaction: discord.Interaction):
        alliance_id = int(interaction.data["values"][0])
        currently = await asyncio.to_thread(gsr.is_multistate, alliance_id)
        await asyncio.to_thread(gsr.set_multistate, alliance_id, not currently)
        self.last_result = None
        await self.refresh(interaction)

    async def _on_bind(self, interaction: discord.Interaction):
        result = await asyncio.to_thread(self.cog.bind_alliance_states)
        b, m = len(result["bound"]), len(result["multistate"])
        self.last_result = (
            f"Gave {_count(b, 'alliance')} a state and marked {m} as multistate."
            if (b or m) else "No alliance had a clear majority to bind."
        )
        await self.refresh(interaction)


class KingdomScanView(_MenuView):
    """The background state scan: on/off and the range of states it checks."""

    async def build_embed(self):
        settings = await asyncio.to_thread(gsr.get_scan_settings)
        waiting, needing = await asyncio.to_thread(gsr.scan_counts)
        on = settings['enabled']
        self.clear_items()
        self._add_button(f"Auto-scan: {'On' if on else 'Off'}", theme.searchIcon,
                         discord.ButtonStyle.success if on else discord.ButtonStyle.secondary,
                         0, self._on_toggle)
        self._add_button("Set Range", theme.stateIcon, discord.ButtonStyle.primary, 0, self._on_range)
        self._add_button("Back", theme.backIcon, discord.ButtonStyle.secondary, 1, self._back_to_hub)

        source = "set by you" if settings['custom'] else "1 to the highest state on file"
        lines = [
            "The bot finds each member's state by trying state numbers one at a time until the gift "
            "code service accepts the member's ID. It tries the likely states first, then every state in the range below. It only "
            "runs while the bot is idle and pauses for gift codes, member syncs and screenshot "
            "reading (OCR).\n",
            f"{theme.upperDivider}",
            f"{theme.searchIcon} **Auto-scan:** {'On' if on else 'Off'}",
            f"{theme.stateIcon} **Range:** states {settings['min']}-{settings['max']} ({source})",
            f"{theme.membersIcon} **Waiting to be scanned:** `{waiting}`",
            f"{theme.deniedIcon} **Not found, tried again after {gsr.SCAN_RETRY_DAYS} days:** "
            f"`{needing - waiting}`",
        ]
        lines.extend(_scan_lines(self.cog))
        lines += [
            f"{theme.lowerDivider}",
            f"{theme.searchIcon} **Auto-scan**",
            "└ When it's on, the bot scans members who need a state whenever it's idle.",
            f"{theme.stateIcon} **Set Range**",
            "└ Limit the scan to the state numbers your members could be in.",
            f"\n{theme.infoIcon} A full scan can take several hours per member. Members it finds "
            f"get the codes they missed.",
        ]
        if self.last_result:
            lines.append(f"\n{theme.verifiedIcon} {self.last_result}")
        return discord.Embed(title=f"{theme.searchIcon} State Scan",
                             description="\n".join(lines), color=theme.emColor1)

    async def _on_toggle(self, interaction: discord.Interaction):
        on = not await asyncio.to_thread(gsr.scan_enabled)
        await asyncio.to_thread(gsr.set_scan_enabled, on)
        logger.info(f"GiftOps: admin turned the state auto-scan {'on' if on else 'off'}")
        self.last_result = ("Auto-scan is on. It starts within 5 minutes once the bot is idle."
                            if on else "Auto-scan is off. A running auto-scan stops at its next state check.")
        await self.refresh(interaction)

    async def _on_range(self, interaction: discord.Interaction):
        settings = await asyncio.to_thread(gsr.get_scan_settings)
        await interaction.response.send_modal(ScanRangeModal(self, settings))


class ScanRangeModal(discord.ui.Modal):
    """Lowest and highest state the scan checks."""

    def __init__(self, parent_view, settings):
        super().__init__(title="State Scan Range")
        self.parent_view = parent_view
        hint = "Leave both empty for 1 to the highest state on file"
        lo, hi = (str(settings['min']), str(settings['max'])) if settings['custom'] else (None, None)
        self.lo = discord.ui.TextInput(label="Lowest state", default=lo, placeholder=hint,
                                       required=False, max_length=5)
        self.hi = discord.ui.TextInput(label="Highest state", default=hi, placeholder=hint,
                                       required=False, max_length=5)
        self.add_item(self.lo)
        self.add_item(self.hi)

    async def on_submit(self, interaction: discord.Interaction):
        if not await _is_global_admin(interaction):
            return
        if not await check_interaction_user(interaction, self.parent_view.original_user_id):
            return
        parsed = parse_scan_range(self.lo.value, self.hi.value)
        if isinstance(parsed, str):
            await interaction.response.send_message(f"{theme.deniedIcon} {parsed}", ephemeral=True)
            return
        if parsed is None:
            await asyncio.to_thread(gsr.clear_scan_range)
            logger.info("GiftOps: admin reset the state scan range to the default")
            self.parent_view.last_result = "State scan range set back to 1 to the highest state on file."
            await self.parent_view.refresh(interaction)
            return
        lo, hi = parsed
        await asyncio.to_thread(gsr.set_scan_range, lo, hi)
        logger.info(f"GiftOps: admin set the state scan range to {lo}-{hi}")
        self.parent_view.last_result = f"State scan range set to {lo}-{hi}."
        await self.parent_view.refresh(interaction)


class MemberStateModal(discord.ui.Modal):
    """Set one member's state by hand."""
    def __init__(self, parent_view, fid, nickname, current_kid):
        super().__init__(title="Set Member State")
        self.parent_view = parent_view
        self.fid = fid
        self.nickname = nickname
        self.state = discord.ui.TextInput(
            label=f"State for {str(nickname)[:30]}",
            placeholder="The state number the member is in now, e.g. 911",
            default=str(current_kid) if current_kid is not None else None,
            required=True, max_length=5,
        )
        self.add_item(self.state)

    async def on_submit(self, interaction: discord.Interaction):
        if not await _is_global_admin(interaction):
            return
        if not await check_interaction_user(interaction, self.parent_view.original_user_id):
            return
        kid = parse_state(self.state.value)
        if kid is None:
            await interaction.response.send_message(
                f"{theme.deniedIcon} `{self.state.value.strip()}` isn't a state number. "
                f"Enter digits only, like `911`.",
                ephemeral=True)
            return
        from . import gift_redemption
        cog = self.parent_view.cog
        await asyncio.to_thread(gsr.set_user_kid, self.fid, kid)
        cog.logger.info(f"GiftOps: admin set state {kid} for FID {self.fid} ({self.nickname})")
        queued = gift_redemption.enqueue_member_redemption(cog, self.fid, self.nickname)
        self.parent_view.last_result = (
            f"{self.nickname} is now in state {kid}. "
            + (f"Redeeming {queued} code(s) they missed." if queued
               else "They already have every code.")
        )
        await self.parent_view.refresh(interaction)


class BulkStateModal(discord.ui.Modal):
    """Set one state on every listed member - for a batch that transferred together."""
    def __init__(self, parent_view, fids):
        super().__init__(title="Set State for All")
        self.parent_view = parent_view
        self.fids = fids
        self.state = discord.ui.TextInput(
            label=f"State for all {len(fids)} member(s)",
            placeholder="The state they're all in now, e.g. 911",
            required=True, max_length=5,
        )
        self.add_item(self.state)

    async def on_submit(self, interaction: discord.Interaction):
        if not await _is_global_admin(interaction):
            return
        if not await check_interaction_user(interaction, self.parent_view.original_user_id):
            return
        from . import gift_redemption
        kid = parse_state(self.state.value)
        if kid is None:
            await interaction.response.send_message(
                f"{theme.deniedIcon} `{self.state.value.strip()}` isn't a state number. "
                f"Enter digits only, like `911`.",
                ephemeral=True)
            return
        cog = self.parent_view.cog
        await asyncio.to_thread(gsr.set_user_kid_many, self.fids, kid)
        caught = sum(1 for fid in self.fids if gift_redemption.enqueue_member_redemption(cog, fid))
        cog.logger.info(f"GiftOps: admin set state {kid} for {len(self.fids)} listed member(s)")
        self.parent_view.last_result = (
            f"Set {len(self.fids)} member(s) to state {kid}."
            + (f" Catching up {caught} on missed codes." if caught else "")
        )
        await self.parent_view.refresh(interaction)
