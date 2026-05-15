"""
Alliance member operations. Handles member imports, removals, and transfers.
"""
import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import asyncio
import time
import logging
from typing import List, Optional
from datetime import datetime
import os
import csv
import io
from .login_handler import LoginHandler
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, safe_edit_message
from .process_queue import MEMBER_ADD, PreemptedException

logger = logging.getLogger('alliance')


class _MemberAddProgress:
    # Drops the target message after MAX_FAILS edits fail in a row so callers
    # can fall through to a DM once the ephemeral interaction's 15-min webhook
    # token expires mid-operation.
    MAX_FAILS = 3

    def __init__(self, message):
        self.message = message
        self._fails = 0

    async def edit(self, embed):
        if self.message is None:
            return
        try:
            await self.message.edit(embed=embed)
            self._fails = 0
        except Exception as e:
            self._fails += 1
            if self._fails >= self.MAX_FAILS:
                logger.warning(
                    f"member_add: dropping progress message after {self._fails} failed edits "
                    f"(likely expired interaction: {e}); switching to headless"
                )
                self.message = None

_RTL_RANGES = [(0x0590, 0x08FF), (0xFB1D, 0xFDFF), (0xFE70, 0xFEFF)]


def _has_rtl(text: str) -> bool:
    return bool(text) and any(
        any(lo <= ord(c) <= hi for lo, hi in _RTL_RANGES) for c in text
    )


def _isolate_rtl(text: str) -> str:
    """Wrap RTL chars in FSI\u2026PDI so they don't reorder surrounding tokens."""
    if not _has_rtl(text):
        return text or ""
    return f"\u2068{text}\u2069"


def _ltr_line(text: str) -> str:
    """Prepend LRM so the whole line stays left-to-right when it contains
    RTL chars (FSI alone only protects neighbouring tokens, not the line)."""
    if not text:
        return text or ""
    return "\u200e" + text if _has_rtl(text) else text


def fix_rtl(text):
    return _isolate_rtl(text)


class MemberFilterModal(discord.ui.Modal):
    def __init__(self, parent_view: "MemberListView"):
        super().__init__(title="Filter Members")
        self.parent_view = parent_view
        self.name_input = discord.ui.TextInput(
            label="Name contains",
            default=parent_view.filter_name,
            required=False,
            max_length=100,
        )
        self.id_input = discord.ui.TextInput(
            label="ID contains",
            default=parent_view.filter_id,
            required=False,
            max_length=20,
        )
        self.state_input = discord.ui.TextInput(
            label="State (exact match)",
            default=parent_view.filter_state,
            required=False,
            max_length=10,
        )
        self.add_item(self.name_input)
        self.add_item(self.id_input)
        self.add_item(self.state_input)

    async def on_submit(self, interaction: discord.Interaction):
        self.parent_view.filter_name = self.name_input.value.strip()
        self.parent_view.filter_id = self.id_input.value.strip()
        self.parent_view.filter_state = self.state_input.value.strip()
        self.parent_view.current_page = 0
        self.parent_view._build_components()
        await interaction.response.edit_message(
            embed=self.parent_view.build_embed(),
            view=self.parent_view,
        )


class MemberListView(discord.ui.View):
    PAGE_SIZE = 20
    SORTS = [
        ("FC \u2193",      lambda m: (-m['furnace_lv'], m['nickname'].casefold()), False),
        ("FC \u2191",      lambda m: (m['furnace_lv'], m['nickname'].casefold()),  False),
        ("Name A→Z",  lambda m: m['nickname'].casefold(),                      False),
        ("Name Z→A",  lambda m: m['nickname'].casefold(),                      True),
        ("ID \u2191",      lambda m: m['fid'],                                       False),
        ("State \u2191",   lambda m: (m['kid'], -m['furnace_lv']),                  False),
    ]

    def __init__(self, members, alliance_id, alliance_name, cog, author_id):
        super().__init__(timeout=7200)
        self.all_members = [
            {'fid': fid, 'nickname': nick or '', 'furnace_lv': fl or 0, 'kid': kid}
            for fid, nick, fl, kid in members
        ]
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.cog = cog
        self.author_id = author_id
        self.message = None
        self.current_page = 0
        self.sort_idx = 0
        self.filter_name = ""
        self.filter_id = ""
        self.filter_state = ""
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who opened this view can use these controls.",
                ephemeral=True,
            )
            return False
        return True

    def _has_filter(self) -> bool:
        return bool(self.filter_name or self.filter_id or self.filter_state)

    def _filtered(self):
        out = self.all_members
        if self.filter_name:
            n = self.filter_name.casefold()
            out = [m for m in out if n in m['nickname'].casefold()]
        if self.filter_id:
            out = [m for m in out if self.filter_id in str(m['fid'])]
        if self.filter_state:
            try:
                target = int(self.filter_state)
                out = [m for m in out if m['kid'] == target]
            except ValueError:
                pass
        return out

    def _sorted_filtered(self):
        label, key, reverse = self.SORTS[self.sort_idx]
        return sorted(self._filtered(), key=key, reverse=reverse)

    def _build_components(self):
        self.clear_items()
        items = self._sorted_filtered()
        total_pages = max(1, (len(items) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self.current_page >= total_pages:
            self.current_page = max(0, total_pages - 1)

        prev_btn = discord.ui.Button(
            emoji=theme.prevIcon,
            style=discord.ButtonStyle.secondary,
            disabled=self.current_page == 0,
            row=0,
        )
        prev_btn.callback = self._on_prev
        self.add_item(prev_btn)

        sort_btn = discord.ui.Button(
            label=f"Sort: {self.SORTS[self.sort_idx][0]}",
            style=discord.ButtonStyle.secondary,
            row=0,
        )
        sort_btn.callback = self._on_sort
        self.add_item(sort_btn)

        next_btn = discord.ui.Button(
            emoji=theme.nextIcon,
            style=discord.ButtonStyle.secondary,
            disabled=self.current_page >= total_pages - 1,
            row=0,
        )
        next_btn.callback = self._on_next
        self.add_item(next_btn)

        filter_btn = discord.ui.Button(
            label="Filter",
            emoji=theme.searchIcon,
            style=discord.ButtonStyle.primary,
            row=1,
        )
        filter_btn.callback = self._on_filter
        self.add_item(filter_btn)

        if self._has_filter():
            clear_btn = discord.ui.Button(
                label="Clear Filter",
                emoji=theme.deniedIcon,
                style=discord.ButtonStyle.secondary,
                row=1,
            )
            clear_btn.callback = self._on_clear
            self.add_item(clear_btn)

        export_btn = discord.ui.Button(
            label="Export",
            emoji=theme.exportIcon,
            style=discord.ButtonStyle.success,
            row=1,
        )
        export_btn.callback = self._on_export
        self.add_item(export_btn)

    def build_embed(self) -> discord.Embed:
        items = self._sorted_filtered()
        total = len(items)
        total_pages = max(1, (total + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        start = self.current_page * self.PAGE_SIZE
        page_items = items[start:start + self.PAGE_SIZE]

        all_count = len(self.all_members)
        if self.all_members:
            max_fl = max(m['furnace_lv'] for m in self.all_members)
            avg_fl = sum(m['furnace_lv'] for m in self.all_members) / all_count
            max_label = self.cog.level_mapping.get(max_fl, str(max_fl))
            avg_label = self.cog.level_mapping.get(int(avg_fl), str(int(avg_fl)))
        else:
            max_label = avg_label = "-"

        header = [
            f"{theme.upperDivider}",
            (f"{theme.chartIcon} **Total:** `{all_count}`  ·  "
             f"**Highest:** `{max_label}`  ·  **Avg:** `{avg_label}`"),
            f"{theme.listIcon} **Sort:** `{self.SORTS[self.sort_idx][0]}`",
        ]
        if self._has_filter():
            parts = []
            if self.filter_name:
                parts.append(f"name~`{self.filter_name}`")
            if self.filter_id:
                parts.append(f"id~`{self.filter_id}`")
            if self.filter_state:
                parts.append(f"state=`{self.filter_state}`")
            header.append(
                f"{theme.searchIcon} **Filter:** {' · '.join(parts)}  →  `{total}` match"
            )
        header.append(f"{theme.lowerDivider}")

        if not page_items:
            body = f"\n{theme.deniedIcon} No members match the current filter."
        else:
            rows = []
            for offset, m in enumerate(page_items, start=start + 1):
                level = self.cog.level_mapping.get(m['furnace_lv'], str(m['furnace_lv']))
                raw_nick = m['nickname'] or "(no name)"
                nick = _isolate_rtl(raw_nick)
                line1 = _ltr_line(f"`{offset:>3}.` {theme.userIcon} **{nick}**")
                line2 = f"     `{level}` · `ID {m['fid']}` · `State {m['kid']}`"
                rows.append(f"{line1}\n{line2}")
            body = "\n".join(rows)

        embed = discord.Embed(
            title=f"{theme.userIcon} {self.alliance_name} \u2014 Member List",
            description="\n".join(header) + "\n" + body,
            color=theme.emColor1,
        )
        embed.set_footer(text=f"Page {self.current_page + 1}/{total_pages}")
        return embed

    async def _rerender(self, interaction: discord.Interaction):
        self._build_components()
        await interaction.response.edit_message(
            embed=self.build_embed(), view=self
        )

    async def _on_prev(self, interaction: discord.Interaction):
        if self.current_page > 0:
            self.current_page -= 1
        await self._rerender(interaction)

    async def _on_next(self, interaction: discord.Interaction):
        items = self._sorted_filtered()
        total_pages = max(1, (len(items) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self.current_page < total_pages - 1:
            self.current_page += 1
        await self._rerender(interaction)

    async def _on_sort(self, interaction: discord.Interaction):
        self.sort_idx = (self.sort_idx + 1) % len(self.SORTS)
        self.current_page = 0
        await self._rerender(interaction)

    async def _on_filter(self, interaction: discord.Interaction):
        await interaction.response.send_modal(MemberFilterModal(self))

    async def _on_clear(self, interaction: discord.Interaction):
        self.filter_name = self.filter_id = self.filter_state = ""
        self.current_page = 0
        await self._rerender(interaction)

    async def _on_export(self, interaction: discord.Interaction):
        items = self._sorted_filtered()
        if not items:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Nothing to export \u2014 current filter matches no members.",
                ephemeral=True,
            )
            return
        column_embed = discord.Embed(
            title=f"{theme.chartIcon} Select Export Columns",
            description=(
                f"**Alliance:** {self.alliance_name}\n"
                f"**Members in export:** {len(items)}"
                + (" (filtered view)" if self._has_filter() else "")
                + "\n\nClick the buttons to toggle columns on/off.\n"
                "All columns are selected by default."
            ),
            color=theme.emColor1,
        )
        column_view = ExportColumnSelectView(
            self.alliance_id, self.alliance_name, self.cog,
            include_alliance=False,
            prefiltered_members=items,
        )
        await interaction.response.send_message(
            embed=column_embed, view=column_view, ephemeral=True
        )

    async def on_timeout(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class ManageMembersView(MemberListView):
    """One-stop alliance member view: filter, sort, multi-select, then act.

    Adds a multi-select dropdown of the current page plus action buttons
    (Remove / Transfer / Add / Select IDs / Export) on top of the existing
    filterable list. Selection persists across pages via `pending_selections`.
    """

    def __init__(self, members, alliance_id, alliance_name, cog, author_id,
                 alliances=None):
        self.pending_selections: set = set()
        self.alliances = alliances or []  # [(aid, name, count?), ...] for Transfer
        super().__init__(members, alliance_id, alliance_name, cog, author_id)

    @property
    def members(self):
        """3-tuple compatibility shim for IDMultiSelectModal."""
        return [(m['fid'], m['nickname'], m['furnace_lv']) for m in self.all_members]

    def _build_main_embed(self) -> discord.Embed:
        """IDMultiSelectModal compatibility — same content as build_embed."""
        return self.build_embed()

    def update_select_menu(self):
        """IDMultiSelectModal compatibility — rebuild everything."""
        self._build_components()

    def update_action_buttons(self):
        """IDMultiSelectModal compatibility — covered by _build_components."""

    def build_embed(self) -> discord.Embed:
        embed = super().build_embed()
        if self.pending_selections:
            embed.description = (
                f"{embed.description}\n\n"
                f"**{theme.pinIcon} Selected: {len(self.pending_selections)} member(s)**"
            )
        return embed

    def _build_components(self):
        self.clear_items()
        items = self._sorted_filtered()
        total_pages = max(1, (len(items) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self.current_page >= total_pages:
            self.current_page = max(0, total_pages - 1)
        page_items = items[self.current_page * self.PAGE_SIZE:
                           (self.current_page + 1) * self.PAGE_SIZE]

        # Row 0: multi-select dropdown of current page (if anything to show)
        if page_items:
            options = []
            for m in page_items[:25]:
                fid = m['fid']
                level = self.cog.level_mapping.get(m['furnace_lv'], str(m['furnace_lv']))
                is_selected = fid in self.pending_selections
                options.append(discord.SelectOption(
                    label=(m['nickname'] or "(no name)")[:50],
                    value=str(fid),
                    description=f"ID: {fid} · {level} · State {m['kid']}"[:100],
                    emoji=theme.verifiedIcon if is_selected else theme.userIcon,
                    default=is_selected,
                ))
            select = discord.ui.Select(
                placeholder=f"{theme.membersIcon} Select members on this page (Page "
                            f"{self.current_page + 1}/{total_pages})",
                options=options,
                min_values=0,
                max_values=len(options),
                row=0,
            )
            select.callback = self._on_select
            self.add_item(select)

        # Row 1: pagination + sort
        prev_btn = discord.ui.Button(
            emoji=theme.prevIcon,
            style=discord.ButtonStyle.secondary,
            disabled=self.current_page == 0,
            row=1,
        )
        prev_btn.callback = self._on_prev
        self.add_item(prev_btn)

        sort_btn = discord.ui.Button(
            label=f"Sort: {self.SORTS[self.sort_idx][0]}",
            style=discord.ButtonStyle.secondary,
            row=1,
        )
        sort_btn.callback = self._on_sort
        self.add_item(sort_btn)

        next_btn = discord.ui.Button(
            emoji=theme.nextIcon,
            style=discord.ButtonStyle.secondary,
            disabled=self.current_page >= total_pages - 1,
            row=1,
        )
        next_btn.callback = self._on_next
        self.add_item(next_btn)

        # Row 2: filter / [clear filter] / add / select IDs
        filter_btn = discord.ui.Button(
            label="Filter", emoji=theme.searchIcon,
            style=discord.ButtonStyle.primary, row=2,
        )
        filter_btn.callback = self._on_filter
        self.add_item(filter_btn)

        if self._has_filter():
            clear_btn = discord.ui.Button(
                label="Clear Filter", emoji=theme.deniedIcon,
                style=discord.ButtonStyle.secondary, row=2,
            )
            clear_btn.callback = self._on_clear
            self.add_item(clear_btn)

        add_btn = discord.ui.Button(
            label="Add Members", emoji=theme.addIcon,
            style=discord.ButtonStyle.success, row=2,
        )
        add_btn.callback = self._on_add_members
        self.add_item(add_btn)

        select_ids_btn = discord.ui.Button(
            label="Select IDs", emoji=theme.searchIcon,
            style=discord.ButtonStyle.secondary, row=2,
        )
        select_ids_btn.callback = self._on_select_ids
        self.add_item(select_ids_btn)

        # Row 3: action buttons (gated on selection)
        has_selection = bool(self.pending_selections)

        remove_btn = discord.ui.Button(
            label=f"Remove ({len(self.pending_selections)})" if has_selection else "Remove",
            emoji=theme.minusIcon,
            style=discord.ButtonStyle.danger,
            disabled=not has_selection,
            row=3,
        )
        remove_btn.callback = self._on_remove_selected
        self.add_item(remove_btn)

        # Need 2+ alliances to transfer
        can_transfer = has_selection and len(self.alliances) >= 2
        transfer_btn = discord.ui.Button(
            label=f"Transfer ({len(self.pending_selections)})" if has_selection else "Transfer",
            emoji=theme.transferIcon,
            style=discord.ButtonStyle.primary,
            disabled=not can_transfer,
            row=3,
        )
        transfer_btn.callback = self._on_transfer_selected
        self.add_item(transfer_btn)

        export_btn = discord.ui.Button(
            label="Export", emoji=theme.exportIcon,
            style=discord.ButtonStyle.success, row=3,
        )
        export_btn.callback = self._on_export
        self.add_item(export_btn)

        # Row 4: Back to alliance hub
        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=4,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    async def _on_select(self, interaction: discord.Interaction):
        select = next(c for c in self.children if isinstance(c, discord.ui.Select))
        items = self._sorted_filtered()
        page_items = items[self.current_page * self.PAGE_SIZE:
                           (self.current_page + 1) * self.PAGE_SIZE]
        page_fids = {m['fid'] for m in page_items}
        # Replace this page's contribution with the new selection
        self.pending_selections -= page_fids
        for value in select.values:
            self.pending_selections.add(int(value))
        await self._rerender(interaction)

    async def _on_add_members(self, interaction: discord.Interaction):
        await interaction.response.send_modal(AddMemberModal(self.alliance_id))

    async def _on_select_ids(self, interaction: discord.Interaction):
        await interaction.response.send_modal(IDMultiSelectModal(self))

    async def _on_remove_selected(self, interaction: discord.Interaction):
        if not self.pending_selections:
            return
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{theme.warnIcon} Confirm Removal",
                description=(
                    f"Remove **{len(self.pending_selections)}** member(s) "
                    f"from **{self.alliance_name}**?\n\n"
                    f"This is permanent and cannot be undone."
                ),
                color=theme.emColor2,
            ),
            view=_RemoveSelectedConfirmView(self),
            ephemeral=True,
        )

    async def _on_transfer_selected(self, interaction: discord.Interaction):
        if not self.pending_selections or len(self.alliances) < 2:
            return
        target_options = [
            discord.SelectOption(
                label=name[:50],
                value=str(aid),
                description=f"ID: {aid}" + (f" · {count} members" if count is not None else ""),
                emoji=theme.allianceIcon,
            )
            for aid, name, count in (
                (a[0], a[1], a[2] if len(a) > 2 else None) for a in self.alliances
            )
            if aid != self.alliance_id
        ]
        if not target_options:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No other alliances available to transfer to.",
                ephemeral=True,
            )
            return

        target_select = discord.ui.Select(
            placeholder=f"{theme.pinIcon} Choose the target alliance…",
            options=target_options,
        )
        target_view = discord.ui.View(timeout=300)
        target_view.add_item(target_select)

        parent_view = self
        async def target_callback(target_interaction: discord.Interaction):
            try:
                target_alliance_id = int(target_select.values[0])
                fids = list(parent_view.pending_selections)
                with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                    cur = alliance_db.cursor()
                    cur.execute(
                        "SELECT name FROM alliance_list WHERE alliance_id = ?",
                        (target_alliance_id,),
                    )
                    target_name = cur.fetchone()[0]
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cur = users_db.cursor()
                    placeholders = ",".join("?" * len(fids))
                    cur.execute(
                        f"UPDATE users SET alliance = ? WHERE fid IN ({placeholders})",
                        [target_alliance_id, *fids],
                    )
                    users_db.commit()

                logger.info(
                    f"Bulk transfer: {len(fids)} members from "
                    f"{parent_view.alliance_name} to {target_name}"
                )

                # Refresh parent view's data — drop transferred members from local cache
                transferred = set(fids)
                parent_view.all_members = [
                    m for m in parent_view.all_members if m['fid'] not in transferred
                ]
                parent_view.pending_selections.clear()
                parent_view._build_components()

                await target_interaction.response.edit_message(
                    embed=discord.Embed(
                        title=f"{theme.verifiedIcon} Transfer Complete",
                        description=(
                            f"Moved **{len(fids)}** member(s) from "
                            f"**{parent_view.alliance_name}** to **{target_name}**.\n\n"
                            f"_The Manage Members list behind this dialog has been "
                            f"refreshed._"
                        ),
                        color=theme.emColor3,
                    ),
                    view=_TransferDoneView(),
                )
                if parent_view.message:
                    try:
                        await parent_view.message.edit(
                            embed=parent_view.build_embed(), view=parent_view,
                        )
                    except discord.HTTPException:
                        pass
            except Exception as e:
                logger.error(f"Transfer error: {e}")
                print(f"Transfer error: {e}")
                await target_interaction.response.edit_message(
                    embed=discord.Embed(
                        title=f"{theme.deniedIcon} Error",
                        description="An error occurred during the transfer operation.",
                        color=theme.emColor2,
                    ),
                    view=None,
                )

        target_select.callback = target_callback
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{theme.pinIcon} Target Alliance",
                description=(
                    f"Transferring **{len(self.pending_selections)}** member(s) "
                    f"out of **{self.alliance_name}**.\n\nPick a target alliance below."
                ),
                color=theme.emColor1,
            ),
            view=target_view,
            ephemeral=True,
        )

    async def _on_back(self, interaction: discord.Interaction):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu:
            await main_menu.show_alliance_hub(interaction, self.alliance_id)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Main Menu module not found.", ephemeral=True
            )


class _TransferDoneView(discord.ui.View):
    """One-button view shown on the Transfer Members success ephemeral.
    The Manage Members list (parent ephemeral) has already been refreshed
    in place; this just lets the user dismiss the success dialog cleanly."""

    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label="Close", emoji=theme.verifiedIcon,
                       style=discord.ButtonStyle.secondary)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.defer()
            await interaction.delete_original_response()
        except Exception:
            try:
                await interaction.response.edit_message(view=None)
            except Exception:
                pass


class _RemoveSelectedConfirmView(discord.ui.View):
    """Yes/no confirmation for ManageMembersView's Remove Selected action."""

    def __init__(self, parent_view: ManageMembersView):
        super().__init__(timeout=60)
        self.parent_view = parent_view

    @discord.ui.button(label="Confirm Remove", emoji=theme.minusIcon,
                       style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        fids = list(self.parent_view.pending_selections)
        if not fids:
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Nothing Selected",
                    description="No members are selected.",
                    color=theme.emColor2,
                ),
                view=None,
            )
            return
        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cur = users_db.cursor()
                placeholders = ",".join("?" * len(fids))
                cur.execute(
                    f"DELETE FROM users WHERE fid IN ({placeholders})", fids
                )
                users_db.commit()
            logger.info(
                f"Bulk remove: {len(fids)} members from "
                f"{self.parent_view.alliance_name}"
            )
            removed = set(fids)
            self.parent_view.all_members = [
                m for m in self.parent_view.all_members if m['fid'] not in removed
            ]
            self.parent_view.pending_selections.clear()
            self.parent_view._build_components()
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.verifiedIcon} Members Removed",
                    description=(
                        f"Removed **{len(fids)}** member(s) from "
                        f"**{self.parent_view.alliance_name}**."
                    ),
                    color=theme.emColor3,
                ),
                view=None,
            )
            if self.parent_view.message:
                try:
                    await self.parent_view.message.edit(
                        embed=self.parent_view.build_embed(),
                        view=self.parent_view,
                    )
                except discord.HTTPException:
                    pass
        except Exception as e:
            logger.error(f"Remove error: {e}")
            print(f"Remove error: {e}")
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Error",
                    description=f"Failed to remove members: {e}",
                    color=theme.emColor2,
                ),
                view=None,
            )

    @discord.ui.button(label="Cancel", emoji=theme.deniedIcon,
                       style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.deniedIcon} Removal Cancelled",
                description="No members were removed.",
                color=theme.emColor4,
            ),
            view=None,
        )


class AllianceMemberOperations(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn_alliance = sqlite3.connect('db/alliance.sqlite', timeout=30.0, check_same_thread=False)
        self.c_alliance = self.conn_alliance.cursor()

        self.conn_users = sqlite3.connect('db/users.sqlite', timeout=30.0, check_same_thread=False)
        self.c_users = self.conn_users.cursor()
        
        self.level_mapping = {
            31: "F30 - 1", 32: "F30 - 2", 33: "F30 - 3", 34: "F30 - 4",
            35: "FC 1", 36: "FC 1 - 1", 37: "FC 1 - 2", 38: "FC 1 - 3", 39: "FC 1 - 4",
            40: "FC 2", 41: "FC 2 - 1", 42: "FC 2 - 2", 43: "FC 2 - 3", 44: "FC 2 - 4",
            45: "FC 3", 46: "FC 3 - 1", 47: "FC 3 - 2", 48: "FC 3 - 3", 49: "FC 3 - 4",
            50: "FC 4", 51: "FC 4 - 1", 52: "FC 4 - 2", 53: "FC 4 - 3", 54: "FC 4 - 4",
            55: "FC 5", 56: "FC 5 - 1", 57: "FC 5 - 2", 58: "FC 5 - 3", 59: "FC 5 - 4",
            60: "FC 6", 61: "FC 6 - 1", 62: "FC 6 - 2", 63: "FC 6 - 3", 64: "FC 6 - 4",
            65: "FC 7", 66: "FC 7 - 1", 67: "FC 7 - 2", 68: "FC 7 - 3", 69: "FC 7 - 4",
            70: "FC 8", 71: "FC 8 - 1", 72: "FC 8 - 2", 73: "FC 8 - 3", 74: "FC 8 - 4",
            75: "FC 9", 76: "FC 9 - 1", 77: "FC 9 - 2", 78: "FC 9 - 3", 79: "FC 9 - 4",
            80: "FC 10", 81: "FC 10 - 1", 82: "FC 10 - 2", 83: "FC 10 - 3", 84: "FC 10 - 4"
        }

        self.fl_emojis = {
            range(35, 40): "<:fc1:1326751863764156528>",
            range(40, 45): "<:fc2:1326751886954594315>",
            range(45, 50): "<:fc3:1326751903912034375>",
            range(50, 55): "<:fc4:1326751938674692106>",
            range(55, 60): "<:fc5:1326751952750776331>",
            range(60, 65): "<:fc6:1326751966184869981>",
            range(65, 70): "<:fc7:1326751983939489812>",
            range(70, 75): "<:fc8:1326751996707082240>",
            range(75, 80): "<:fc9:1326752008505528331>",
            range(80, 85): "<:fc10:1326752023001174066>"
        }

        # Log directory for audit logs (member additions)
        self.log_directory = 'log'
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)
        self.log_file = os.path.join(self.log_directory, 'alliance_memberlog.txt')

        # Initialize login handler for centralized API management
        self.login_handler = LoginHandler()

    def get_fl_emoji(self, fl_level: int) -> str:
        for level_range, emoji in self.fl_emojis.items():
            if fl_level in level_range:
                return emoji
        return f"{theme.levelIcon}"

    async def show_add_members_for(self, interaction: discord.Interaction, alliance_id: int):
        """Open the Add Members modal for a known alliance (no picker step)."""
        await interaction.response.send_modal(AddMemberModal(alliance_id))

    async def show_view_members_for(self, interaction: discord.Interaction, alliance_id: int):
        """Build the Member List directly for a known alliance (no picker step).
        Posts the list publicly in the channel and replaces the source ephemeral
        with a 'list posted below' acknowledgement, matching the entry-point flow."""
        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
            cursor = alliance_db.cursor()
            cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?",
                (alliance_id,),
            )
            row = cursor.fetchone()
        if not row:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance not found.", ephemeral=True
            )
            return
        alliance_name = row[0]

        with sqlite3.connect('db/users.sqlite') as users_db:
            cursor = users_db.cursor()
            cursor.execute(
                "SELECT fid, nickname, furnace_lv, kid FROM users WHERE alliance = ?",
                (alliance_id,),
            )
            members = cursor.fetchall()

        if not members:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No members found in this alliance.",
                ephemeral=True,
            )
            return

        member_view = MemberListView(
            members, alliance_id, alliance_name, self, interaction.user.id,
        )

        await interaction.response.edit_message(
            content=f"{theme.verifiedIcon} Member list has been generated and posted below.",
            embed=None,
            view=None,
        )
        message = await interaction.channel.send(
            embed=member_view.build_embed(),
            view=member_view,
        )
        member_view.message = message

    async def show_manage_members_for(self, interaction: discord.Interaction, alliance_id: int):
        """Open the unified Manage Members view (filter, sort, multi-select, act)."""
        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
            cursor = alliance_db.cursor()
            cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,)
            )
            row = cursor.fetchone()
        if not row:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance not found.", ephemeral=True
            )
            return
        alliance_name = row[0]

        with sqlite3.connect('db/users.sqlite') as users_db:
            cursor = users_db.cursor()
            cursor.execute(
                "SELECT fid, nickname, furnace_lv, kid FROM users WHERE alliance = ?",
                (alliance_id,),
            )
            members = cursor.fetchall()

        # Alliances accessible to this admin — used for Transfer Selected target picker
        accessible, _ = PermissionManager.get_admin_alliances(
            interaction.user.id, interaction.guild_id
        )
        alliances_with_counts = []
        for aid, name in accessible:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cur = users_db.cursor()
                cur.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (aid,))
                count = cur.fetchone()[0] or 0
            alliances_with_counts.append((aid, name, count))

        view = ManageMembersView(
            members, alliance_id, alliance_name, self,
            interaction.user.id, alliances=alliances_with_counts,
        )
        await safe_edit_message(
            interaction, embed=view.build_embed(), view=view, content=None,
        )
        try:
            view.message = await interaction.original_response()
        except Exception:
            pass

    async def show_remove_members_for(self, interaction: discord.Interaction, alliance_id: int):
        """Show member-selection for removal in a known alliance (no picker step)."""
        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
            cursor = alliance_db.cursor()
            cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?",
                (alliance_id,),
            )
            row = cursor.fetchone()
        if not row:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance not found.", ephemeral=True
            )
            return
        alliance_name = row[0]

        with sqlite3.connect('db/users.sqlite') as users_db:
            cursor = users_db.cursor()
            cursor.execute(
                "SELECT fid, nickname, furnace_lv FROM users "
                "WHERE alliance = ? ORDER BY furnace_lv DESC, nickname",
                (alliance_id,),
            )
            members = cursor.fetchall()

        if not members:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No members found in this alliance.",
                ephemeral=True,
            )
            return

        max_fl = max(m[2] for m in members)
        avg_fl = sum(m[2] for m in members) / len(members)

        member_embed = discord.Embed(
            title=f"{theme.userIcon} {alliance_name} - Member Selection",
            description=(
                "```ml\n"
                "Alliance Statistics\n"
                "══════════════════════════\n"
                f"{theme.chartIcon} Total Member     : {len(members)}\n"
                f"{theme.levelIcon} Highest Level    : "
                f"{self.level_mapping.get(max_fl, str(max_fl))}\n"
                f"{theme.chartIcon} Average Level    : "
                f"{self.level_mapping.get(int(avg_fl), str(int(avg_fl)))}\n"
                "══════════════════════════\n"
                "```\n"
                "Select the member you want to delete:"
            ),
            color=theme.emColor2,
        )

        alliances_with_counts = [(alliance_id, alliance_name, len(members))]
        member_view = MemberSelectView(
            members,
            alliance_name,
            self,
            is_remove_operation=True,
            alliance_id=alliance_id,
            alliances=alliances_with_counts,
        )

        async def member_callback(member_interaction: discord.Interaction,
                                  selected_fids=None, delete_all=False):
            if delete_all or (selected_fids and len(selected_fids) == len(members)):
                confirm_embed = discord.Embed(
                    title=f"{theme.warnIcon} Confirmation Required",
                    description=(
                        f"A total of **{len(members)}** members will be deleted.\n"
                        f"Do you confirm?"
                    ),
                    color=theme.emColor2,
                )
                confirm_view = discord.ui.View()
                confirm_button = discord.ui.Button(
                    label=f"{theme.verifiedIcon} Confirm",
                    style=discord.ButtonStyle.danger,
                    custom_id="confirm_all",
                )
                cancel_button = discord.ui.Button(
                    label=f"{theme.deniedIcon} Cancel",
                    style=discord.ButtonStyle.secondary,
                    custom_id="cancel_all",
                )
                confirm_view.add_item(confirm_button)
                confirm_view.add_item(cancel_button)

                async def confirm_callback(confirm_interaction: discord.Interaction):
                    if confirm_interaction.data["custom_id"] == "confirm_all":
                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            cursor.execute(
                                "SELECT fid, nickname FROM users WHERE alliance = ?",
                                (alliance_id,),
                            )
                            removed_members = cursor.fetchall()
                            cursor.execute(
                                "DELETE FROM users WHERE alliance = ?",
                                (alliance_id,),
                            )
                            users_db.commit()

                        try:
                            with sqlite3.connect('db/settings.sqlite') as settings_db:
                                cursor = settings_db.cursor()
                                cursor.execute(
                                    "SELECT channel_id FROM alliance_logs "
                                    "WHERE alliance_id = ?",
                                    (alliance_id,),
                                )
                                alliance_log_result = cursor.fetchone()

                                if alliance_log_result and alliance_log_result[0]:
                                    log_embed = discord.Embed(
                                        title=f"{theme.trashIcon} Mass Member Removal",
                                        description=(
                                            f"**Alliance:** {alliance_name}\n"
                                            f"**Administrator:** "
                                            f"{confirm_interaction.user.name} "
                                            f"(`{confirm_interaction.user.id}`)\n"
                                            f"**Date:** "
                                            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                                            f"**Total Members Removed:** "
                                            f"{len(removed_members)}\n\n"
                                            "**Removed Members:**\n```\n"
                                            + "\n".join(
                                                f"ID{idx+1}: {fid}"
                                                for idx, (fid, _) in enumerate(removed_members[:20])
                                            )
                                            + (
                                                f"\n... and {len(removed_members) - 20} more"
                                                if len(removed_members) > 20 else ""
                                            )
                                            + "\n```"
                                        ),
                                        color=theme.emColor2,
                                    )
                                    try:
                                        ch_id = int(alliance_log_result[0])
                                        ch = self.bot.get_channel(ch_id)
                                        if ch:
                                            await ch.send(embed=log_embed)
                                    except Exception as e:
                                        logger.error(f"Alliance Log Sending Error: {e}")
                                        print(f"Alliance Log Sending Error: {e}")
                        except Exception as e:
                            logger.error(f"Log record error: {e}")
                            print(f"Log record error: {e}")

                        success_embed = discord.Embed(
                            title=f"{theme.verifiedIcon} Members Deleted",
                            description=(
                                f"A total of **{len(removed_members)}** members "
                                f"have been successfully deleted."
                            ),
                            color=theme.emColor3,
                        )
                        await confirm_interaction.response.edit_message(
                            embed=success_embed, view=None
                        )
                    else:
                        cancel_embed = discord.Embed(
                            title=f"{theme.deniedIcon} Operation Cancelled",
                            description="Member deletion operation has been cancelled.",
                            color=theme.emColor4,
                        )
                        await confirm_interaction.response.edit_message(
                            embed=cancel_embed, view=None
                        )

                confirm_button.callback = confirm_callback
                cancel_button.callback = confirm_callback
                await member_interaction.response.edit_message(
                    embed=confirm_embed, view=confirm_view
                )

            elif selected_fids:
                try:
                    with sqlite3.connect('db/users.sqlite') as users_db:
                        cursor = users_db.cursor()
                        placeholders = ','.join('?' * len(selected_fids))
                        cursor.execute(
                            f"SELECT fid, nickname FROM users WHERE fid IN ({placeholders})",
                            selected_fids,
                        )
                        removed_members = cursor.fetchall()
                        cursor.execute(
                            f"DELETE FROM users WHERE fid IN ({placeholders})",
                            selected_fids,
                        )
                        users_db.commit()

                    try:
                        with sqlite3.connect('db/settings.sqlite') as settings_db:
                            cursor = settings_db.cursor()
                            cursor.execute(
                                "SELECT channel_id FROM alliance_logs "
                                "WHERE alliance_id = ?",
                                (alliance_id,),
                            )
                            alliance_log_result = cursor.fetchone()
                            if alliance_log_result and alliance_log_result[0]:
                                log_embed = discord.Embed(
                                    title=f"{theme.trashIcon} Bulk Member Removal",
                                    description=(
                                        f"**Alliance:** {alliance_name}\n"
                                        f"**Administrator:** "
                                        f"{member_interaction.user.name} "
                                        f"(`{member_interaction.user.id}`)\n"
                                        f"**Date:** "
                                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                                        f"**Total Members Removed:** "
                                        f"{len(removed_members)}\n\n"
                                        "**Removed Members:**\n```\n"
                                        + "\n".join(
                                            f"ID{idx+1}: {fid} - {nickname}"
                                            for idx, (fid, nickname) in enumerate(removed_members[:20])
                                        )
                                        + (
                                            f"\n... and {len(removed_members) - 20} more"
                                            if len(removed_members) > 20 else ""
                                        )
                                        + "\n```"
                                    ),
                                    color=theme.emColor2,
                                )
                                try:
                                    ch_id = int(alliance_log_result[0])
                                    ch = self.bot.get_channel(ch_id)
                                    if ch:
                                        await ch.send(embed=log_embed)
                                except Exception as e:
                                    logger.error(f"Alliance Log Sending Error: {e}")
                                    print(f"Alliance Log Sending Error: {e}")
                    except Exception as e:
                        logger.error(f"Log record error: {e}")
                        print(f"Log record error: {e}")

                    success_embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Members Deleted",
                        description=(
                            f"Successfully deleted **{len(removed_members)}** member(s)."
                        ),
                        color=theme.emColor3,
                    )
                    try:
                        await member_interaction.response.edit_message(
                            embed=success_embed, view=None
                        )
                    except Exception:
                        await member_interaction.edit_original_response(
                            embed=success_embed, view=None
                        )

                except Exception as e:
                    logger.error(f"Error in bulk member removal: {e}")
                    print(f"Error in bulk member removal: {e}")
                    error_embed = discord.Embed(
                        title=f"{theme.deniedIcon} Error",
                        description="An error occurred during member removal.",
                        color=theme.emColor2,
                    )
                    try:
                        await member_interaction.response.send_message(
                            embed=error_embed, ephemeral=True
                        )
                    except Exception:
                        await member_interaction.followup.send(
                            embed=error_embed, ephemeral=True
                        )

        member_view.callback = member_callback
        await interaction.response.edit_message(embed=member_embed, view=member_view)

    async def show_export_members(self, interaction: discord.Interaction):
        """Direct entry to Export Members flow (skip the operations sub-menu)."""
        try:
            alliances, is_global = PermissionManager.get_admin_alliances(
                interaction.user.id,
                interaction.guild_id
            )

            if not alliances:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No alliance found with your permissions.",
                    ephemeral=True
                )
                return

            select_embed = discord.Embed(
                title=f"{theme.chartIcon} Alliance Selection - Export Members",
                description=(
                    f"Select the alliance to export members from:\n\n"
                    f"**Permission Details**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.userIcon} **Access Level:** `{'Global Admin' if is_global else 'Alliance Admin'}`\n"
                    f"{theme.searchIcon} **Access Type:** `{'All Alliances' if is_global else 'Assigned Alliances'}`\n"
                    f"{theme.chartIcon} **Available Alliances:** `{len(alliances)}`\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            alliances_with_counts = []
            for alliance_id, name in alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            view = AllianceSelectViewWithAll(alliances_with_counts, self)

            async def select_callback(select_interaction: discord.Interaction):
                selected_value = view.current_select.values[0]

                if selected_value == "all":
                    alliance_id = "all"
                    alliance_name = "ALL ALLIANCES"
                    column_embed = discord.Embed(
                        title=f"{theme.chartIcon} Select Export Columns",
                        description=(
                            f"**Export Type:** ALL ALLIANCES\n"
                            f"**Total Alliances:** {len(alliances_with_counts)}\n\n"
                            "Click the buttons to toggle columns on/off.\n"
                            "All columns are selected by default.\n\n"
                            "**Available Columns:**\n"
                            "• **Alliance** - Alliance name\n"
                            "• **ID** - Member ID\n"
                            "• **Name** - Member's nickname\n"
                            "• **FC Level** - Furnace level\n"
                            "• **State** - State ID"
                        ),
                        color=theme.emColor1
                    )
                    column_view = ExportColumnSelectView(alliance_id, alliance_name, self, include_alliance=True)
                else:
                    alliance_id = int(selected_value)
                    alliance_name = next((name for aid, name, _ in alliances_with_counts if aid == alliance_id), "Unknown")
                    column_embed = discord.Embed(
                        title=f"{theme.chartIcon} Select Export Columns",
                        description=(
                            f"**Alliance:** {alliance_name}\n\n"
                            "Click the buttons to toggle columns on/off.\n"
                            "All columns are selected by default.\n\n"
                            "**Available Columns:**\n"
                            "• **ID** - Member ID\n"
                            "• **Name** - Member's nickname\n"
                            "• **FC Level** - Furnace level\n"
                            "• **State** - State ID"
                        ),
                        color=theme.emColor1
                    )
                    column_view = ExportColumnSelectView(alliance_id, alliance_name, self, include_alliance=False)

                await select_interaction.response.edit_message(embed=column_embed, view=column_view)

            view.callback = select_callback
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error in show_export_members: {e}")
            print(f"Error in show_export_members: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred during the export process.",
                ephemeral=True
            )

    async def show_transfer_members(self, interaction: discord.Interaction):
        """Direct entry to Transfer Members flow (skip the operations sub-menu)."""
        try:
            alliances, is_global = PermissionManager.get_admin_alliances(
                interaction.user.id,
                interaction.guild_id
            )

            if not alliances:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No alliance found with your permissions.",
                    ephemeral=True
                )
                return

            select_embed = discord.Embed(
                title=f"{theme.refreshIcon} Alliance Selection - Member Transfer",
                description=(
                    f"Select the **source** alliance from which you want to transfer members:\n\n"
                    f"**Permission Details**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.userIcon} **Access Level:** `{'Global Admin' if is_global else 'Alliance Admin'}`\n"
                    f"{theme.searchIcon} **Access Type:** `{'All Alliances' if is_global else 'Assigned Alliances'}`\n"
                    f"{theme.chartIcon} **Available Alliances:** `{len(alliances)}`\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            alliances_with_counts = []
            for alliance_id, name in alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            view = AllianceSelectView(alliances_with_counts, self)

            async def source_callback(source_interaction: discord.Interaction):
                try:
                    source_alliance_id = int(view.current_select.values[0])

                    with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                        cursor = alliance_db.cursor()
                        cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (source_alliance_id,))
                        source_alliance_name = cursor.fetchone()[0]

                    with sqlite3.connect('db/users.sqlite') as users_db:
                        cursor = users_db.cursor()
                        cursor.execute("""
                            SELECT fid, nickname, furnace_lv
                            FROM users
                            WHERE alliance = ?
                            ORDER BY furnace_lv DESC, nickname
                        """, (source_alliance_id,))
                        members = cursor.fetchall()

                    if not members:
                        await source_interaction.response.send_message(
                            f"{theme.deniedIcon} No members found in this alliance.",
                            ephemeral=True
                        )
                        return

                    max_fl = max(member[2] for member in members)
                    avg_fl = sum(member[2] for member in members) / len(members)

                    member_embed = discord.Embed(
                        title=f"{theme.userIcon} {source_alliance_name} - Member Selection",
                        description=(
                            f"```ml\n"
                            f"Alliance Statistics\n"
                            f"══════════════════════════\n"
                            f"{theme.chartIcon} Total Members    : {len(members)}\n"
                            f"{theme.levelIcon} Highest Level    : {self.level_mapping.get(max_fl, str(max_fl))}\n"
                            f"{theme.chartIcon} Average Level    : {self.level_mapping.get(int(avg_fl), str(int(avg_fl)))}\n"
                            f"══════════════════════════\n"
                            f"```\n"
                            f"Select the member to transfer:\n\n"
                            f"**Selection Methods**\n"
                            f"{theme.num1Icon} Pick members from the dropdown below\n"
                            f"{theme.num2Icon} Click **Select IDs** to add one or more IDs at once\n"
                            f"{theme.middleDivider}"
                        ),
                        color=theme.emColor1
                    )

                    member_view = MemberSelectView(
                        members,
                        source_alliance_name,
                        self,
                        is_remove_operation=False,
                        alliance_id=source_alliance_id,
                        alliances=alliances_with_counts
                    )

                    async def member_callback(member_interaction: discord.Interaction, selected_fids=None):
                        if not selected_fids:
                            await member_interaction.response.send_message("No members selected", ephemeral=True)
                            return

                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            placeholders = ','.join('?' * len(selected_fids))
                            cursor.execute(f"SELECT fid, nickname FROM users WHERE fid IN ({placeholders})", selected_fids)
                            selected_members = cursor.fetchall()

                        member_list = "\n".join([f"• {nickname} (ID: {fid})" for fid, nickname in selected_members[:10]])
                        if len(selected_members) > 10:
                            member_list += f"\n... and {len(selected_members) - 10} more"

                        target_embed = discord.Embed(
                            title=f"{theme.pinIcon} Target Alliance Selection",
                            description=(
                                f"**Transferring {len(selected_fids)} member(s):**\n"
                                f"{member_list}\n\n"
                                f"Select the target alliance:"
                            ),
                            color=theme.emColor1
                        )

                        target_options = [
                            discord.SelectOption(
                                label=f"{name[:50]}",
                                value=str(alliance_id),
                                description=f"ID: {alliance_id} | Members: {count}",
                                emoji=theme.allianceIcon
                            ) for alliance_id, name, count in alliances_with_counts
                            if alliance_id != source_alliance_id
                        ]

                        target_select = discord.ui.Select(
                            placeholder=f"{theme.pinIcon} Select target alliance...",
                            options=target_options
                        )

                        target_view = discord.ui.View()
                        target_view.add_item(target_select)

                        async def target_callback(target_interaction: discord.Interaction):
                            target_alliance_id = int(target_select.values[0])

                            try:
                                with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                                    cursor = alliance_db.cursor()
                                    cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (target_alliance_id,))
                                    target_alliance_name = cursor.fetchone()[0]

                                with sqlite3.connect('db/users.sqlite') as users_db:
                                    cursor = users_db.cursor()
                                    placeholders = ','.join('?' * len(selected_fids))
                                    cursor.execute(
                                        f"UPDATE users SET alliance = ? WHERE fid IN ({placeholders})",
                                        [target_alliance_id] + selected_fids
                                    )
                                    users_db.commit()

                                success_embed = discord.Embed(
                                    title=f"{theme.verifiedIcon} Transfer Successful",
                                    description=(
                                        f"**Members Transferred:** {len(selected_fids)}\n"
                                        f"{theme.allianceOldIcon} **Source:** {source_alliance_name}\n"
                                        f"{theme.allianceIcon} **Target:** {target_alliance_name}\n\n"
                                        f"**Transferred Members:**\n{member_list}"
                                    ),
                                    color=theme.emColor3
                                )

                                await target_interaction.response.edit_message(
                                    embed=success_embed,
                                    view=None
                                )

                                logger.info(
                                    f"Bulk transfer: {len(selected_fids)} members from {source_alliance_name} to {target_alliance_name}"
                                )

                            except Exception as e:
                                logger.error(f"Transfer error: {e}")
                                print(f"Transfer error: {e}")
                                error_embed = discord.Embed(
                                    title=f"{theme.deniedIcon} Error",
                                    description="An error occurred during the transfer operation.",
                                    color=theme.emColor2
                                )
                                await target_interaction.response.edit_message(
                                    embed=error_embed,
                                    view=None
                                )

                        target_select.callback = target_callback
                        try:
                            await member_interaction.response.edit_message(
                                embed=target_embed,
                                view=target_view
                            )
                        except Exception:
                            await member_interaction.edit_original_response(
                                embed=target_embed,
                                view=target_view
                            )

                    member_view.callback = member_callback
                    await source_interaction.response.edit_message(
                        embed=member_embed,
                        view=member_view
                    )

                except Exception as e:
                    logger.error(f"Source callback error: {e}")
                    print(f"Source callback error: {e}")
                    await source_interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred. Please try again.",
                        ephemeral=True
                    )

            view.callback = source_callback
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error in show_transfer_members: {e}")
            print(f"Error in show_transfer_members: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred during the transfer operation.",
                ephemeral=True
            )

    async def handle_member_operations(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title=f"{theme.userIcon} Alliance Member Operations",
            description=(
                "Please select an operation from below:\n\n"
                "**Available Operations:**\n"
                f"{theme.newIcon} `Add Members` - Add new members (supports IDs, CSV/TSV imports)\n"
                f"{theme.refreshIcon} `Transfer Members` - Transfer members to another alliance\n"
                f"{theme.minusIcon} `Remove Members` - Remove members from alliance\n"
                f"{theme.userIcon} `View Members` - View alliance member list\n"
                f"{theme.chartIcon} `Export Members` - Export member data to CSV/TSV\n"
                f"{theme.backIcon} `Back` - Return to Alliances"
            ),
            color=theme.emColor1
        )
        
        embed.set_footer(text="Select an option to continue")

        class MemberOperationsView(discord.ui.View):
            def __init__(self, cog):
                super().__init__(timeout=7200)
                self.cog = cog
                self.bot = cog.bot

            @discord.ui.button(
                label="Add Members",
                emoji=theme.addIcon,
                style=discord.ButtonStyle.success,
                custom_id="add_member",
                row=0
            )
            async def add_member_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
                try:
                    alliances, is_global = PermissionManager.get_admin_alliances(
                        button_interaction.user.id,
                        button_interaction.guild_id
                    )

                    if not alliances:
                        await button_interaction.response.send_message(
                            f"{theme.deniedIcon} No alliances found for your permissions.",
                            ephemeral=True
                        )
                        return

                    select_embed = discord.Embed(
                        title=f"{theme.listIcon} Alliance Selection",
                        description=(
                            f"Please select an alliance to add members:\n\n"
                            f"**Permission Details**\n"
                            f"{theme.upperDivider}\n"
                            f"{theme.userIcon} **Access Level:** `{'Global Admin' if is_global else 'Alliance Admin'}`\n"
                            f"{theme.searchIcon} **Access Type:** `{'All Alliances' if is_global else 'Assigned Alliances'}`\n"
                            f"{theme.chartIcon} **Available Alliances:** `{len(alliances)}`\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor3
                    )

                    alliances_with_counts = []
                    for alliance_id, name in alliances:
                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                            member_count = cursor.fetchone()[0]
                            alliances_with_counts.append((alliance_id, name, member_count))

                    view = AllianceSelectView(alliances_with_counts, self.cog)
                    
                    async def select_callback(interaction: discord.Interaction):
                        alliance_id = int(view.current_select.values[0])
                        await interaction.response.send_modal(AddMemberModal(alliance_id))

                    view.callback = select_callback
                    await button_interaction.response.send_message(
                        embed=select_embed,
                        view=view,
                        ephemeral=True
                    )

                except Exception as e:
                    logger.error(f"Error in add_member_button: {e}")
                    print(f"Error in add_member_button: {e}")
                    await button_interaction.response.send_message(
                        "An error occurred while processing your request.", 
                        ephemeral=True
                    )

            @discord.ui.button(
                label="Remove Members",
                emoji=theme.minusIcon,
                style=discord.ButtonStyle.danger,
                custom_id="remove_member",
                row=0
            )
            async def remove_member_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
                try:
                    alliances, is_global = PermissionManager.get_admin_alliances(
                        button_interaction.user.id,
                        button_interaction.guild_id
                    )

                    if not alliances:
                        await button_interaction.response.send_message(
                            f"{theme.deniedIcon} Your authorized alliance was not found.",
                            ephemeral=True
                        )
                        return

                    select_embed = discord.Embed(
                        title=f"{theme.trashIcon} Alliance Selection - Member Deletion",
                        description=(
                            f"Please select an alliance to remove members:\n\n"
                            f"**Permission Details**\n"
                            f"{theme.upperDivider}\n"
                            f"{theme.userIcon} **Access Level:** `{'Global Admin' if is_global else 'Alliance Admin'}`\n"
                            f"{theme.searchIcon} **Access Type:** `{'All Alliances' if is_global else 'Assigned Alliances'}`\n"
                            f"{theme.chartIcon} **Available Alliances:** `{len(alliances)}`\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor2
                    )

                    alliances_with_counts = []
                    for alliance_id, name in alliances:
                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                            member_count = cursor.fetchone()[0]
                            alliances_with_counts.append((alliance_id, name, member_count))

                    view = AllianceSelectView(alliances_with_counts, self.cog)
                    
                    async def select_callback(interaction: discord.Interaction):
                        alliance_id = int(view.current_select.values[0])
                        
                        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                            cursor = alliance_db.cursor()
                            cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                            alliance_name = cursor.fetchone()[0]
                        
                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            cursor.execute("""
                                SELECT fid, nickname, furnace_lv 
                                FROM users 
                                WHERE alliance = ? 
                                ORDER BY furnace_lv DESC, nickname
                            """, (alliance_id,))
                            members = cursor.fetchall()
                            
                        if not members:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon} No members found in this alliance.", 
                                ephemeral=True
                            )
                            return

                        max_fl = max(member[2] for member in members)
                        avg_fl = sum(member[2] for member in members) / len(members)

                        member_embed = discord.Embed(
                            title=f"{theme.userIcon} {alliance_name} -  Member Selection",
                            description=(
                                "```ml\n"
                                "Alliance Statistics\n"
                                "══════════════════════════\n"
                                f"{theme.chartIcon} Total Member     : {len(members)}\n"
                                f"{theme.levelIcon} Highest Level    : {self.cog.level_mapping.get(max_fl, str(max_fl))}\n"
                                f"{theme.chartIcon} Average Level    : {self.cog.level_mapping.get(int(avg_fl), str(int(avg_fl)))}\n"
                                "══════════════════════════\n"
                                "```\n"
                                "Select the member you want to delete:"
                            ),
                            color=theme.emColor2
                        )

                        member_view = MemberSelectView(
                            members,
                            alliance_name,
                            self.cog,
                            is_remove_operation=True,
                            alliance_id=alliance_id,
                            alliances=alliances_with_counts
                        )

                        async def member_callback(member_interaction: discord.Interaction, selected_fids=None, delete_all=False):
                            # Handle multi-select or delete all
                            if delete_all or (selected_fids and len(selected_fids) == len(members)):
                                # Delete all members
                                selected_value = "all"
                                confirm_embed = discord.Embed(
                                    title=f"{theme.warnIcon} Confirmation Required",
                                    description=f"A total of **{len(members)}** members will be deleted.\nDo you confirm?",
                                    color=theme.emColor2
                                )
                                
                                confirm_view = discord.ui.View()
                                confirm_button = discord.ui.Button(
                                    label=f"{theme.verifiedIcon} Confirm", 
                                    style=discord.ButtonStyle.danger, 
                                    custom_id="confirm_all"
                                )
                                cancel_button = discord.ui.Button(
                                    label=f"{theme.deniedIcon} Cancel", 
                                    style=discord.ButtonStyle.secondary, 
                                    custom_id="cancel_all"
                                )
                                
                                confirm_view.add_item(confirm_button)
                                confirm_view.add_item(cancel_button)

                                async def confirm_callback(confirm_interaction: discord.Interaction):
                                    if confirm_interaction.data["custom_id"] == "confirm_all":
                                        with sqlite3.connect('db/users.sqlite') as users_db:
                                            cursor = users_db.cursor()
                                            cursor.execute("SELECT fid, nickname FROM users WHERE alliance = ?", (alliance_id,))
                                            removed_members = cursor.fetchall()
                                            cursor.execute("DELETE FROM users WHERE alliance = ?", (alliance_id,))
                                            users_db.commit()
                                        
                                        try:
                                            with sqlite3.connect('db/settings.sqlite') as settings_db:
                                                cursor = settings_db.cursor()
                                                cursor.execute("""
                                                    SELECT channel_id 
                                                    FROM alliance_logs 
                                                    WHERE alliance_id = ?
                                                """, (alliance_id,))
                                                alliance_log_result = cursor.fetchone()
                                                
                                                if alliance_log_result and alliance_log_result[0]:
                                                    log_embed = discord.Embed(
                                                        title=f"{theme.trashIcon} Mass Member Removal",
                                                        description=(
                                                            f"**Alliance:** {alliance_name}\n"
                                                            f"**Administrator:** {confirm_interaction.user.name} (`{confirm_interaction.user.id}`)\n"
                                                            f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                                                            f"**Total Members Removed:** {len(removed_members)}\n\n"
                                                            "**Removed Members:**\n"
                                                            "```\n" + 
                                                            "\n".join([f"ID{idx+1}: {fid}" for idx, (fid, _) in enumerate(removed_members[:20])]) +
                                                            (f"\n... ve {len(removed_members) - 20} ID more" if len(removed_members) > 20 else "") +
                                                            "\n```"
                                                        ),
                                                        color=theme.emColor2
                                                    )
                                                    
                                                    try:
                                                        alliance_channel_id = int(alliance_log_result[0])
                                                        alliance_log_channel = self.bot.get_channel(alliance_channel_id)
                                                        if alliance_log_channel:
                                                            await alliance_log_channel.send(embed=log_embed)
                                                    except Exception as e:
                                                        logger.error(f"Alliance Log Sending Error: {e}")
                                                        print(f"Alliance Log Sending Error: {e}")
                                        except Exception as e:
                                            logger.error(f"Log record error: {e}")
                                            print(f"Log record error: {e}")
                                        
                                        success_embed = discord.Embed(
                                            title=f"{theme.verifiedIcon} Members Deleted",
                                            description=f"A total of **{len(removed_members)}** members have been successfully deleted.",
                                            color=theme.emColor3
                                        )
                                        await confirm_interaction.response.edit_message(embed=success_embed, view=None)
                                    else:
                                        cancel_embed = discord.Embed(
                                            title=f"{theme.deniedIcon} Operation Cancelled",
                                            description="Member deletion operation has been cancelled.",
                                            color=theme.emColor4
                                        )
                                        await confirm_interaction.response.edit_message(embed=cancel_embed, view=None)

                                confirm_button.callback = confirm_callback
                                cancel_button.callback = confirm_callback
                                
                                await member_interaction.response.edit_message(
                                    embed=confirm_embed,
                                    view=confirm_view
                                )
                            
                            elif selected_fids:
                                # Bulk delete selected members
                                try:
                                    with sqlite3.connect('db/users.sqlite') as users_db:
                                        cursor = users_db.cursor()
                                        placeholders = ','.join('?' * len(selected_fids))
                                        cursor.execute(f"SELECT fid, nickname FROM users WHERE fid IN ({placeholders})", selected_fids)
                                        removed_members = cursor.fetchall()

                                        cursor.execute(f"DELETE FROM users WHERE fid IN ({placeholders})", selected_fids)
                                        users_db.commit()

                                    try:
                                        with sqlite3.connect('db/settings.sqlite') as settings_db:
                                            cursor = settings_db.cursor()
                                            cursor.execute("""
                                                SELECT channel_id
                                                FROM alliance_logs
                                                WHERE alliance_id = ?
                                            """, (alliance_id,))
                                            alliance_log_result = cursor.fetchone()

                                            if alliance_log_result and alliance_log_result[0]:
                                                log_embed = discord.Embed(
                                                    title=f"{theme.trashIcon} Bulk Member Removal",
                                                    description=(
                                                        f"**Alliance:** {alliance_name}\n"
                                                        f"**Administrator:** {member_interaction.user.name} (`{member_interaction.user.id}`)\n"
                                                        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                                                        f"**Total Members Removed:** {len(removed_members)}\n\n"
                                                        "**Removed Members:**\n"
                                                        "```\n" +
                                                        "\n".join([f"ID{idx+1}: {fid} - {nickname}" for idx, (fid, nickname) in enumerate(removed_members[:20])]) +
                                                        (f"\n... and {len(removed_members) - 20} more" if len(removed_members) > 20 else "") +
                                                        "\n```"
                                                    ),
                                                    color=theme.emColor2
                                                )

                                                try:
                                                    alliance_channel_id = int(alliance_log_result[0])
                                                    alliance_log_channel = self.bot.get_channel(alliance_channel_id)
                                                    if alliance_log_channel:
                                                        await alliance_log_channel.send(embed=log_embed)
                                                except Exception as e:
                                                    logger.error(f"Alliance Log Sending Error: {e}")
                                                    print(f"Alliance Log Sending Error: {e}")
                                    except Exception as e:
                                        logger.error(f"Log record error: {e}")
                                        print(f"Log record error: {e}")

                                    success_embed = discord.Embed(
                                        title=f"{theme.verifiedIcon} Members Deleted",
                                        description=f"Successfully deleted **{len(removed_members)}** member(s).",
                                        color=theme.emColor3
                                    )
                                    try:
                                        await member_interaction.response.edit_message(embed=success_embed, view=None)
                                    except Exception:
                                        await member_interaction.edit_original_response(embed=success_embed, view=None)

                                except Exception as e:
                                    logger.error(f"Error in bulk member removal: {e}")
                                    print(f"Error in bulk member removal: {e}")
                                    error_embed = discord.Embed(
                                        title=f"{theme.deniedIcon} Error",
                                        description="An error occurred during member removal.",
                                        color=theme.emColor2
                                    )
                                    try:
                                        await member_interaction.response.send_message(embed=error_embed, ephemeral=True)
                                    except Exception:
                                        await member_interaction.followup.send(embed=error_embed, ephemeral=True)

                        member_view.callback = member_callback
                        await interaction.response.edit_message(
                            embed=member_embed,
                            view=member_view
                        )

                    view.callback = select_callback
                    await button_interaction.response.send_message(
                        embed=select_embed,
                        view=view,
                        ephemeral=True
                    )

                except Exception as e:
                    logger.error(f"Error in remove_member_button: {e}")
                    print(f"Error in remove_member_button: {e}")
                    await button_interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred during the member deletion process.",
                        ephemeral=True
                    )

            @discord.ui.button(
                label="View Members",
                emoji=theme.membersIcon,
                style=discord.ButtonStyle.primary,
                custom_id="view_members",
                row=1
            )
            async def view_members_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
                try:
                    alliances, is_global = PermissionManager.get_admin_alliances(
                        button_interaction.user.id,
                        button_interaction.guild_id
                    )

                    if not alliances:
                        await button_interaction.response.send_message(
                            f"{theme.deniedIcon} No alliance found that you have permission for.",
                            ephemeral=True
                        )
                        return

                    select_embed = discord.Embed(
                        title=f"{theme.userIcon} Alliance Selection",
                        description=(
                            f"Please select an alliance to view members:\n\n"
                            f"**Permission Details**\n"
                            f"{theme.upperDivider}\n"
                            f"{theme.userIcon} **Access Level:** `{'Global Admin' if is_global else 'Alliance Admin'}`\n"
                            f"{theme.searchIcon} **Access Type:** `{'All Alliances' if is_global else 'Assigned Alliances'}`\n"
                            f"{theme.chartIcon} **Available Alliances:** `{len(alliances)}`\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor1
                    )

                    alliances_with_counts = []
                    for alliance_id, name in alliances:
                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                            member_count = cursor.fetchone()[0]
                            alliances_with_counts.append((alliance_id, name, member_count))

                    view = AllianceSelectView(alliances_with_counts, self.cog)
                    
                    async def select_callback(interaction: discord.Interaction):
                        alliance_id = int(view.current_select.values[0])
                        
                        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                            cursor = alliance_db.cursor()
                            cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                            alliance_name = cursor.fetchone()[0]
                        
                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            cursor.execute("""
                                SELECT fid, nickname, furnace_lv, kid
                                FROM users
                                WHERE alliance = ?
                            """, (alliance_id,))
                            members = cursor.fetchall()

                        if not members:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon} No members found in this alliance.",
                                ephemeral=True
                            )
                            return

                        member_view = MemberListView(
                            members, alliance_id, alliance_name,
                            self.cog, interaction.user.id,
                        )

                        await interaction.response.edit_message(
                            content=f"{theme.verifiedIcon} Member list has been generated and posted below.",
                            embed=None,
                            view=None,
                        )

                        message = await interaction.channel.send(
                            embed=member_view.build_embed(),
                            view=member_view,
                        )
                        member_view.message = message

                    view.callback = select_callback
                    await button_interaction.response.send_message(
                        embed=select_embed,
                        view=view,
                        ephemeral=True
                    )

                except Exception as e:
                    logger.error(f"Error in view_members_button: {e}")
                    print(f"Error in view_members_button: {e}")
                    if not button_interaction.response.is_done():
                        await button_interaction.response.send_message(
                            f"{theme.deniedIcon} An error occurred while displaying the member list.",
                            ephemeral=True
                        )

            @discord.ui.button(
                label="Export Members",
                emoji=theme.chartIcon,
                style=discord.ButtonStyle.primary,
                custom_id="export_members",
                row=1
            )
            async def export_members_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
                await self.cog.show_export_members(button_interaction)
            
            @discord.ui.button(
                label="Back",
                emoji=theme.backIcon,
                style=discord.ButtonStyle.secondary,
                row=2
            )
            async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
                await self.cog.back_to_alliance_management(interaction)

            @discord.ui.button(label="Transfer Members", emoji=theme.retryIcon, style=discord.ButtonStyle.primary, row=0)
            async def transfer_member_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
                await self.cog.show_transfer_members(button_interaction)

        view = MemberOperationsView(self)
        await interaction.response.edit_message(embed=embed, view=view)

    def _get_queue_size(self) -> int:
        """Get the current ProcessQueue size, or 0 if unavailable."""
        process_queue = self.bot.get_cog('ProcessQueue')
        if not process_queue:
            return 0
        return process_queue.get_queue_info().get('queue_size', 0)

    async def handle_member_add_process(self, process):
        details = process.get('details', {})
        alliance_id = details.get('alliance_id')
        alliance_name = details.get('alliance_name')
        ids = details.get('ids')
        invoker_id = details.get('invoker_id')
        invoker_name = details.get('invoker_name', 'unknown')

        if not alliance_id or not ids:
            logger.error(f"member_add process {process['id']} missing alliance_id or ids")
            return

        process_queue = self.bot.get_cog('ProcessQueue')
        runtime = process_queue.get_runtime_context(process['id']) if process_queue else {}
        interaction = runtime.get('interaction')

        message = None
        if interaction is not None:
            try:
                message = await interaction.original_response()
            except Exception as e:
                logger.warning(f"member_add process {process['id']}: could not fetch progress message ({e}); running headless")

        if message is None:
            logger.info(f"member_add process {process['id']}: running headless (no live message)")

        await self._process_add_user(
            message, alliance_id, alliance_name, ids, invoker_id, invoker_name,
            process_id=process['id'], resumed_state=details.get('resumed_state'),
        )

    async def add_user(self, interaction: discord.Interaction, alliance_id: str, ids: str):
        self.c_alliance.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
        alliance_name = self.c_alliance.fetchone()
        if alliance_name:
            alliance_name = alliance_name[0]
        else:
            await interaction.response.send_message("Alliance not found.", ephemeral=True)
            return

        if not await self.is_admin(interaction.user.id):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        process_queue = self.bot.get_cog('ProcessQueue')
        if not process_queue:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Process Queue module not found.",
                ephemeral=True
            )
            return

        member_count = len(ids.split(',') if ',' in ids else ids.split('\n'))

        # Always send the progress embed up front (whether queued or starting now)
        embed = discord.Embed(
            title=f"{theme.userIcon} User Addition Progress",
            description=f"Processing {member_count} members for **{alliance_name}**...\n\n**Progress:** `0/{member_count}`",
            color=theme.emColor1
        )
        embed.add_field(
            name=f"\n{theme.verifiedIcon} Successfully Added (0/{member_count})",
            value="-",
            inline=False
        )
        embed.add_field(
            name=f"{theme.deniedIcon} Failed (0/{member_count})",
            value="-",
            inline=False
        )
        embed.add_field(
            name=f"{theme.warnIcon} Already Exists (0/{member_count})",
            value="-",
            inline=False
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

        # Enqueue the operation, attaching the live interaction for progress updates
        process_id = process_queue.enqueue(
            action='member_add',
            priority=MEMBER_ADD,
            alliance_id=int(alliance_id) if str(alliance_id).isdigit() else None,
            details={
                'alliance_id': str(alliance_id),
                'alliance_name': alliance_name,
                'ids': ids,
                'invoker_id': interaction.user.id,
                'invoker_name': interaction.user.name,
            },
        )
        process_queue.attach_runtime_context(process_id, {
            'interaction': interaction,
        })

        # If anything is ahead of us, surface queue position + DM-fallback note
        # so the admin knows it's normal for the embed to sit idle for a while.
        try:
            pos = process_queue.get_position(process_id)
            if pos and pos > 1:
                qs = self._get_queue_size()
                embed.description = (
                    f"Processing {member_count} members for **{alliance_name}**...\n\n"
                    f"{theme.listIcon} **Queue position:** {pos} of {qs}\n"
                    f"{theme.warnIcon} If processing takes longer than ~14 minutes "
                    f"(Discord ephemeral expiry), the final result will be DM'd to you.\n\n"
                    f"**Progress:** `0/{member_count}`"
                )
                await interaction.edit_original_response(embed=embed)
        except Exception:
            pass  # non-fatal — the operation still runs

    async def _process_add_user(self, message: Optional[discord.Message], alliance_id: str, alliance_name: str,
                                ids: str, invoker_id: Optional[int], invoker_name: str,
                                process_id: Optional[int] = None,
                                resumed_state: Optional[dict] = None):
        progress = _MemberAddProgress(message)
        ids_list = []
        
        # Check if this is CSV/TSV data with headers
        lines = [line.strip() for line in ids.split('\n') if line.strip()]
        if lines and any(delimiter in lines[0] for delimiter in [',', '\t']):
            # Detect delimiter
            delimiter = '\t' if '\t' in lines[0] else ','
            
            # Try to parse as CSV/TSV
            try:
                reader = csv.reader(io.StringIO(ids), delimiter=delimiter)
                rows = list(reader)
                
                if rows and len(rows) > 1:
                    # Get headers
                    headers = [h.strip().lower() for h in rows[0]]
                    
                    # Find ID column - look for 'id', 'fid'
                    id_col_index = None
                    for i, header in enumerate(headers):
                        if header in ['id', 'fid']:
                            id_col_index = i
                            break
                    
                    if id_col_index is not None:
                        # Extract IDs from data rows
                        for row in rows[1:]:
                            if len(row) > id_col_index and row[id_col_index].strip():
                                # Clean the ID
                                fid = ''.join(c for c in row[id_col_index] if c.isdigit())
                                if fid:
                                    ids_list.append(fid)
                        
                    else:
                        # No header found, treat first row as data if it looks like IDs
                        if rows[0] and rows[0][0].strip().isdigit():
                            for row in rows:
                                if row and row[0].strip():
                                    fid = ''.join(c for c in row[0] if c.isdigit())
                                    if fid:
                                        ids_list.append(fid)
            except Exception:
                pass  # Fall back to simple parsing
        
        # If CSV/TSV parsing didn't work or wasn't applicable, use simple parsing
        if not ids_list:
            if '\n' in ids:
                ids_list = [fid.strip() for fid in ids.split('\n') if fid.strip()]
            else:
                ids_list = [fid.strip() for fid in ids.split(",") if fid.strip()]

        # Pre-check which IDs already exist in the database
        already_in_db = []
        fids_to_process = []
        
        for fid in ids_list:
            self.c_users.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
            existing = self.c_users.fetchone()
            if existing:
                # Member already exists in database
                already_in_db.append((fid, existing[0]))
            else:
                # Member doesn't exist at all
                fids_to_process.append(fid)
        
        total_users = len(ids_list)

        # Build a fresh progress embed. When `message` is provided, we edit it in place;
        # when it's None (headless / crash recovery), we just update the local copy and
        # DM the final version to the invoker at the end.
        embed = discord.Embed(
            title=f"{theme.userIcon} User Addition Progress",
            description=f"Processing {total_users} members for **{alliance_name}**...\n\n**Progress:** `0/{total_users}`",
            color=theme.emColor1,
        )
        embed.add_field(name=f"\n{theme.verifiedIcon} Successfully Added (0/{total_users})", value="-", inline=False)
        embed.add_field(name=f"{theme.deniedIcon} Failed (0/{total_users})", value="-", inline=False)
        embed.add_field(name=f"{theme.warnIcon} Already Exists (0/{total_users})", value="-", inline=False)

        # Check API availability before starting
        embed.description = f"{theme.searchIcon} Checking API availability..."
        await progress.edit(embed)

        await self.login_handler.check_apis_availability()

        if not self.login_handler.available_apis:
            # No APIs available
            embed.description = f"{theme.deniedIcon} Both APIs are unavailable. Cannot proceed."
            embed.color = discord.Color.red()
            await progress.edit(embed)
            await self._notify_invoker_if_headless(progress.message, invoker_id, embed)
            return
        
        # Get processing rate from login handler
        rate_text = self.login_handler.get_processing_rate()
        
        # Update embed with rate information
        qs = self._get_queue_size()
        queue_info = f"\n{theme.listIcon} **Operations in queue:** {qs}" if qs > 0 else ""
        embed.description = f"Processing {total_users} members...\n{rate_text}{queue_info}\n\n**Progress:** `0/{total_users}`"
        embed.color = discord.Color.blue()
        await progress.edit(embed)

        # On resume after preempt, credit prior-run successes to Successfully-Added
        # so the counters don't visually reset to zero.
        prior_added_fids = set(resumed_state.get('added_fids', [])) if resumed_state else set()
        prior_error_fids = resumed_state.get('error_fids', []) if resumed_state else []

        added_users = [(fid, nick) for (fid, nick) in already_in_db if fid in prior_added_fids]
        already_exists_users = [(fid, nick) for (fid, nick) in already_in_db if fid not in prior_added_fids]
        error_users = list(prior_error_fids)

        added_count = len(added_users)
        error_count = len(error_users)
        already_exists_count = len(already_exists_users)

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file_path = os.path.join(self.log_directory, 'add_memberlog.txt')
        
        # Determine input format
        input_format = "Simple IDs"
        if '\t' in ids and ',' not in ids.split('\n')[0] if '\n' in ids else ids:
            input_format = "TSV Format"
        elif ',' in ids and len(ids.split(',')[0]) > 10:
            input_format = "CSV Format"
        
        try:
            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                log_file.write(f"\n{'='*50}\n")
                log_file.write(f"Date: {timestamp}\n")
                log_file.write(f"Administrator: {invoker_name} (ID: {invoker_id})\n")
                log_file.write(f"Alliance: {alliance_name} (ID: {alliance_id})\n")
                log_file.write(f"Input Format: {input_format}\n")
                # Avoid nested f-strings for Python 3.9+ compatibility
                if len(ids_list) <= 20:
                    ids_display = ', '.join(ids_list)
                else:
                    ids_display = f"{', '.join(ids_list[:20])}... ({len(ids_list)} total)"
                log_file.write(f"IDs to Process: {ids_display}\n")
                log_file.write(f"Total Members to Process: {total_users}\n")
                log_file.write(f"API Mode: {self.login_handler.get_mode_text()}\n")
                log_file.write(f"Available APIs: {self.login_handler.available_apis}\n")
                log_file.write(f"Operations in Queue: {self._get_queue_size()}\n")
                log_file.write('-'*50 + '\n')

            if added_count > 0:
                embed.set_field_at(
                    0,
                    name=f"{theme.verifiedIcon} Successfully Added ({added_count}/{total_users})",
                    value="User list cannot be displayed due to exceeding 70 users" if len(added_users) > 70
                    else ", ".join([n for _, n in added_users]) or "-",
                    inline=False
                )
            if error_count > 0:
                embed.set_field_at(
                    1,
                    name=f"{theme.deniedIcon} Failed ({error_count}/{total_users})",
                    value="Error list cannot be displayed due to exceeding 70 users" if len(error_users) > 70
                    else ", ".join(error_users) or "-",
                    inline=False
                )
            if already_exists_count > 0:
                embed.set_field_at(
                    2,
                    name=f"{theme.warnIcon} Already Exists ({already_exists_count}/{total_users})",
                    value="Existing user list cannot be displayed due to exceeding 70 users" if len(already_exists_users) > 70
                    else ", ".join([n for _, n in already_exists_users]) or "-",
                    inline=False
                )
            if added_count > 0 or error_count > 0 or already_exists_count > 0:
                await progress.edit(embed)
            
            # Cooperative preemption: yield to higher-priority work between members
            process_queue_cog = self.bot.get_cog('ProcessQueue')

            index = 0
            while index < len(fids_to_process):
                # Check for higher-priority work
                if process_queue_cog and process_queue_cog.should_preempt():
                    logger.info(f"AllianceMemberOps: Preempting member add for {alliance_name} - higher priority work waiting")
                    self._checkpoint_member_add_state(
                        process_queue_cog, process_id, alliance_id, alliance_name,
                        ids, invoker_id, invoker_name, added_users, error_users,
                    )
                    raise PreemptedException()

                fid = fids_to_process[index]
                try:
                    # Update progress
                    qs = self._get_queue_size()
                    queue_info = f"\n{theme.listIcon} **Operations in queue:** {qs}" if qs > 0 else ""
                    current_progress = already_exists_count + index + 1
                    embed.description = f"Processing {total_users} members...\n{rate_text}{queue_info}\n\n**Progress:** `{current_progress}/{total_users}`"
                    await progress.edit(embed)
                    
                    # Fetch player data using login handler
                    result = await self.login_handler.fetch_player_data(fid)
                    
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(f"\nAPI Response for ID {fid}:\n")
                        log_file.write(f"Status: {result['status']}\n")
                        if result.get('api_used'):
                            log_file.write(f"API Used: {result['api_used']}\n")
                    
                    if result['status'] == 'rate_limited':
                        # Handle rate limiting with countdown
                        wait_time = result.get('wait_time', 60)
                        countdown_start = time.time()
                        remaining_time = wait_time
                        
                        with open(log_file_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"Rate limit reached - Total wait time: {wait_time:.1f} seconds\n")
                        
                        # Update display with countdown
                        while remaining_time > 0:
                            qs = self._get_queue_size()
                            queue_info = f"\n{theme.listIcon} **Operations in queue:** {qs}" if qs > 0 else ""
                            embed.description = f"{theme.warnIcon} Rate limit reached. Waiting {remaining_time:.0f} seconds...{queue_info}"
                            embed.color = discord.Color.orange()
                            await progress.edit(embed)
                            
                            # Wait for up to 5 seconds before updating
                            await asyncio.sleep(min(5, remaining_time))
                            elapsed = time.time() - countdown_start
                            remaining_time = max(0, wait_time - elapsed)
                        
                        embed.color = discord.Color.blue()
                        continue  # Retry this request
                    
                    if result['status'] == 'success':
                        data = result['data']
                        with open(log_file_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"API Response Data: {str(data)}\n")
                        
                        nickname = data.get('nickname')
                        furnace_lv = data.get('stove_lv', 0)
                        stove_lv_content = data.get('stove_lv_content', None)
                        kid = data.get('kid', None)

                        if nickname:
                            try: # Since we pre-filtered, this ID should not exist in database
                                self.c_users.execute("""
                                    INSERT INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, alliance)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (fid, nickname, furnace_lv, kid, stove_lv_content, alliance_id))
                                self.conn_users.commit()
                                
                                with open(self.log_file, 'a', encoding='utf-8') as f:
                                    f.write(f"[{timestamp}] Successfully added member - ID: {fid}, Nickname: {nickname}, Level: {furnace_lv}\n")
                                
                                added_count += 1
                                added_users.append((fid, nickname))
                                
                                embed.set_field_at(
                                    0,
                                    name=f"{theme.verifiedIcon} Successfully Added ({added_count}/{total_users})",
                                    value="User list cannot be displayed due to exceeding 70 users" if len(added_users) > 70 
                                    else ", ".join([n for _, n in added_users]) or "-",
                                    inline=False
                                )
                                await progress.edit(embed)
                                
                            except sqlite3.IntegrityError as e:
                                # This shouldn't happen since we pre-filtered, but handle it just in case
                                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                                    log_file.write(f"ERROR: Member already exists (race condition?) - ID {fid}: {str(e)}\n")
                                already_exists_count += 1
                                already_exists_users.append((fid, nickname))
                                
                                embed.set_field_at(
                                    2,
                                    name=f"{theme.warnIcon} Already Exists ({already_exists_count}/{total_users})",
                                    value="Existing user list cannot be displayed due to exceeding 70 users" if len(already_exists_users) > 70 
                                    else ", ".join([n for _, n in already_exists_users]) or "-",
                                    inline=False
                                )
                                await progress.edit(embed)
                                
                            except Exception as e:
                                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                                    log_file.write(f"ERROR: Database error for ID {fid}: {str(e)}\n")
                                error_count += 1
                                error_users.append(fid)
                                
                                embed.set_field_at(
                                    1,
                                    name=f"{theme.deniedIcon} Failed ({error_count}/{total_users})",
                                    value="Error list cannot be displayed due to exceeding 70 users" if len(error_users) > 70 
                                    else ", ".join(error_users) or "-",
                                    inline=False
                                )
                                await progress.edit(embed)
                        else:
                            # No nickname in API response
                            error_count += 1
                            error_users.append(fid)
                    else:
                        # Handle other error statuses
                        error_msg = result.get('error_message', 'Unknown error')
                        with open(log_file_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"ERROR: {error_msg} for ID {fid}\n")
                        error_count += 1
                        if fid not in error_users:
                            error_users.append(fid)
                        embed.set_field_at(
                            1,
                            name=f"{theme.deniedIcon} Failed ({error_count}/{total_users})",
                            value="Error list cannot be displayed due to exceeding 70 users" if len(error_users) > 70
                            else ", ".join(error_users) or "-",
                            inline=False
                        )
                        await progress.edit(embed)
                    
                    index += 1

                except PreemptedException:
                    raise
                except Exception as e:
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(f"ERROR: Request failed for ID {fid}: {str(e)}\n")
                    error_count += 1
                    error_users.append(fid)
                    await progress.edit(embed)
                    index += 1

            embed.set_field_at(0, name=f"{theme.verifiedIcon} Successfully Added ({added_count}/{total_users})",
                value="User list cannot be displayed due to exceeding 70 users" if len(added_users) > 70 
                else ", ".join([nickname for _, nickname in added_users]) or "-",
                inline=False
            )
            
            embed.set_field_at(1, name=f"{theme.deniedIcon} Failed ({error_count}/{total_users})",
                value="Error list cannot be displayed due to exceeding 70 users" if len(error_users) > 70 
                else ", ".join(error_users) or "-",
                inline=False
            )
            
            embed.set_field_at(2, name=f"{theme.warnIcon} Already Exists ({already_exists_count}/{total_users})",
                value="Existing user list cannot be displayed due to exceeding 70 users" if len(already_exists_users) > 70 
                else ", ".join([nickname for _, nickname in already_exists_users]) or "-",
                inline=False
            )

            await progress.edit(embed)

            try:
                with sqlite3.connect('db/settings.sqlite') as settings_db:
                    cursor = settings_db.cursor()
                    cursor.execute("""
                        SELECT channel_id 
                        FROM alliance_logs 
                        WHERE alliance_id = ?
                    """, (alliance_id,))
                    alliance_log_result = cursor.fetchone()
                    
                    if alliance_log_result and alliance_log_result[0]:
                        added_ids = [str(fid) for fid, _ in added_users]
                        failed_ids = [str(fid) for fid in error_users]
                        existing_ids = [str(fid) for fid, _ in already_exists_users]
                        description = (
                            f"**Alliance:** {alliance_name}\n"
                            f"**Administrator:** {invoker_name} (`{invoker_id}`)\n"
                            f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                            f"**Results:**\n"
                            f"{theme.verifiedIcon} Successfully Added: {added_count}\n"
                            f"{theme.deniedIcon} Failed: {error_count}\n"
                            f"{theme.warnIcon} Already Exists: {already_exists_count}\n"
                        )
                        if added_ids:
                            description += f"\n**Added IDs:**\n```\n{', '.join(added_ids)}\n```"
                        if failed_ids:
                            description += f"\n**Failed IDs:**\n```\n{', '.join(failed_ids)}\n```"
                        if existing_ids:
                            description += f"\n**Already Existing IDs:**\n```\n{', '.join(existing_ids)}\n```"
                        log_embed = discord.Embed(
                            title=f"{theme.userIcon} Members Added to Alliance",
                            description=description,
                            color=theme.emColor3
                        )

                        try:
                            alliance_channel_id = int(alliance_log_result[0])
                            alliance_log_channel = self.bot.get_channel(alliance_channel_id)
                            if alliance_log_channel:
                                await alliance_log_channel.send(embed=log_embed)
                        except Exception as e:
                            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                                log_file.write(f"ERROR: Alliance Log Sending Error: {str(e)}\n")

            except Exception as e:
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(f"ERROR: Log record error: {str(e)}\n")

            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                log_file.write(f"\nFinal Results:\n")
                log_file.write(f"Successfully Added: {added_count}\n")
                log_file.write(f"Failed: {error_count}\n")
                log_file.write(f"Already Exists: {already_exists_count}\n")
                log_file.write(f"API Mode: {self.login_handler.get_mode_text()}\n")
                log_file.write(f"API1 Requests: {len(self.login_handler.api1_requests)}\n")
                log_file.write(f"API2 Requests: {len(self.login_handler.api2_requests)}\n")
                log_file.write(f"{'='*50}\n")

        except PreemptedException:
            raise
        except Exception as e:
            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                log_file.write(f"CRITICAL ERROR: {str(e)}\n")
                log_file.write(f"{'='*50}\n")

        # Calculate total processing time
        end_time = datetime.now()
        start_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        processing_time = (end_time - start_time).total_seconds()

        qs = self._get_queue_size()
        queue_info = f"\n{theme.listIcon} **Operations still in queue:** {qs}" if qs > 0 else ""

        embed.title = f"{theme.verifiedIcon} User Addition Completed"
        embed.description = (
            f"Process completed for {total_users} members.\n"
            f"**Processing Time:** {processing_time:.1f} seconds{queue_info}\n\n"
        )
        embed.color = discord.Color.green()
        await progress.edit(embed)

        await self._notify_invoker_if_headless(progress.message, invoker_id, embed)

    def _checkpoint_member_add_state(self, process_queue_cog, process_id: Optional[int],
                                     alliance_id: str, alliance_name: str, ids: str,
                                     invoker_id: Optional[int], invoker_name: str,
                                     added_users: list, error_users: list):
        if process_queue_cog is None or process_id is None:
            return
        try:
            process_queue_cog.update_details(process_id, {
                'alliance_id': str(alliance_id),
                'alliance_name': alliance_name,
                'ids': ids,
                'invoker_id': invoker_id,
                'invoker_name': invoker_name,
                'resumed_state': {
                    'added_fids': [fid for (fid, _) in added_users],
                    'error_fids': list(error_users),
                },
            })
        except Exception as e:
            logger.warning(f"member_add: failed to checkpoint state for process {process_id} ({e})")

    async def _notify_invoker_if_headless(self, message: Optional[discord.Message],
                                          invoker_id: Optional[int], embed: discord.Embed):
        if message is not None or not invoker_id:
            return
        try:
            user = self.bot.get_user(invoker_id) or await self.bot.fetch_user(invoker_id)
            if user is None:
                return
            note = discord.Embed(
                description=(
                    f"{theme.warnIcon} Your earlier member-add operation resumed after a bot restart. "
                    f"The original progress message is no longer reachable, so here is the final result:"
                ),
                color=theme.emColor2,
            )
            await user.send(embed=note)
            await user.send(embed=embed)
        except Exception as e:
            logger.warning(f"member_add: could not DM invoker {invoker_id} with headless result ({e})")

    async def is_admin(self, user_id):
        try:
            with sqlite3.connect('db/settings.sqlite') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM admin WHERE id = ?", (user_id,))
                result = cursor.fetchone()
                is_admin = result is not None
                return is_admin
        except Exception as e:
            logger.error(f"Error in admin check: {e}")
            print(f"Error in admin check: {e}")
            return False

    @commands.Cog.listener()
    async def on_ready(self):
        process_queue_cog = self.bot.get_cog('ProcessQueue')
        if process_queue_cog:
            process_queue_cog.register_handler('member_add', self.handle_member_add_process)
            logger.info("AllianceMemberOps: Registered member_add handler with ProcessQueue")
        else:
            logger.error("AllianceMemberOps: ProcessQueue cog not found, member_add operations will not work")

    def cog_unload(self):
        self.conn_users.close()
        self.conn_alliance.close()

    async def get_admin_alliances(self, user_id: int, guild_id: int):
        """Get alliances for admin from centralized PermissionManager"""
        return PermissionManager.get_admin_alliances(user_id, guild_id)

    async def handle_button_interaction(self, interaction: discord.Interaction):
        custom_id = interaction.data["custom_id"]
        
        if custom_id == "main_menu":
            await self.back_to_alliance_management(interaction)
    
    async def process_member_export(self, interaction: discord.Interaction, alliance_id, alliance_name: str, selected_columns: list, export_format: str, prefiltered_members: list | None = None):
        """Process the member export with selected columns and format.
        When prefiltered_members is provided, uses it instead of querying DB."""
        try:
            # Update the message to show processing
            processing_embed = discord.Embed(
                title="⏳ Processing Export",
                description="Generating your export file...",
                color=theme.emColor1
            )
            await interaction.response.edit_message(embed=processing_embed, view=None)

            # Build the SQL query based on selected columns
            db_columns = [col[0] for col in selected_columns]
            headers = [col[1] for col in selected_columns]

            if prefiltered_members is not None:
                filtered_columns = [col for col in selected_columns if col[0] != 'alliance_name']
                db_columns = [col[0] for col in filtered_columns]
                headers = [col[1] for col in filtered_columns]
                members = [
                    tuple(m[col] for col in db_columns)
                    for m in prefiltered_members
                ]
            elif alliance_id == "all":
                # Need to join with alliance table to get alliance names
                with sqlite3.connect('db/users.sqlite') as users_db:
                    # Attach the alliance database to get alliance names
                    cursor = users_db.cursor()
                    cursor.execute("ATTACH DATABASE 'db/alliance.sqlite' AS alliance_db")
                    
                    # Build query columns
                    query_columns = []
                    for db_col, _ in selected_columns:
                        if db_col == 'alliance_name':
                            query_columns.append('a.name AS alliance_name')
                        else:
                            query_columns.append(f'u.{db_col}')
                    
                    # Query with join
                    query = f"""
                        SELECT {', '.join(query_columns)}
                        FROM users u
                        JOIN alliance_db.alliance_list a ON u.alliance = a.alliance_id
                        ORDER BY a.name, u.furnace_lv DESC, u.nickname
                    """
                    cursor.execute(query)
                    members = cursor.fetchall()
                    cursor.execute("DETACH DATABASE alliance_db")
            else:
                # Single alliance export
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    # Filter out alliance_name if it's in the columns (not applicable for single alliance)
                    filtered_columns = [col for col in selected_columns if col[0] != 'alliance_name']
                    db_columns = [col[0] for col in filtered_columns]
                    headers = [col[1] for col in filtered_columns]
                    
                    query = f"SELECT {', '.join(db_columns)} FROM users WHERE alliance = ? ORDER BY furnace_lv DESC, nickname"
                    cursor.execute(query, (alliance_id,))
                    members = cursor.fetchall()
            
            if not members:
                error_embed = discord.Embed(
                    title=f"{theme.deniedIcon} No Members Found",
                    description="No members found in this alliance to export.",
                    color=theme.emColor2
                )
                await interaction.edit_original_response(embed=error_embed)
                return
            
            # Create the export file in memory
            output = io.StringIO()
            delimiter = '\t' if export_format == 'tsv' else ','
            writer = csv.writer(output, delimiter=delimiter)
            
            # Write headers
            writer.writerow(headers)
            
            # Process and write member data
            for member in members:
                row = []
                # Use the appropriate columns list based on whether it's a single or all export
                columns_to_use = selected_columns if alliance_id == "all" else filtered_columns
                for i, (db_col, header) in enumerate(columns_to_use):
                    value = member[i]
                    
                    # Special formatting for FC Level
                    if db_col == 'furnace_lv' and value is not None:
                        value = self.level_mapping.get(value, str(value))
                    
                    # Handle None values
                    if value is None:
                        value = ''
                    
                    row.append(value)
                
                writer.writerow(row)
            
            # Get the CSV/TSV content
            output.seek(0)
            file_content = output.getvalue()
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{alliance_name.replace(' ', '_')}_members_{timestamp}.{export_format}"
            
            # Create Discord file
            file = discord.File(io.BytesIO(file_content.encode('utf-8')), filename=filename)
            
            # Create summary embed
            summary_embed = discord.Embed(
                title=f"{theme.chartIcon} Export Ready",
                description=(
                    f"**Alliance:** {alliance_name}\n"
                    f"**Total Members:** {len(members)}\n"
                    f"**Format:** {export_format.upper()}\n"
                    f"**Columns Included:** {', '.join(headers)}\n\n"
                    "Attempting to send the file via DM..."
                ),
                color=theme.emColor3
            )
            
            # Try to DM the user
            try:
                dm_embed = discord.Embed(
                    title=f"{theme.chartIcon} Alliance Member Export",
                    description=(
                        f"**Alliance:** {alliance_name}\n"
                        f"**Export Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"**Total Members:** {len(members)}\n"
                        f"**Format:** {export_format.upper()}\n"
                        f"**Columns:** {', '.join(headers)}\n"
                    ),
                    color=theme.emColor1
                )
                
                # Add statistics
                if 'furnace_lv' in db_columns:
                    fc_index = db_columns.index('furnace_lv')
                    fc_levels = [m[fc_index] for m in members if m[fc_index] is not None]
                    if fc_levels:
                        max_fc = max(fc_levels)
                        avg_fc = sum(fc_levels) / len(fc_levels)
                        dm_embed.add_field(
                            name=f"{theme.chartIcon} Statistics",
                            value=(
                                f"**Highest FC:** {self.level_mapping.get(max_fc, str(max_fc))}\n"
                                f"**Average FC:** {self.level_mapping.get(int(avg_fc), str(int(avg_fc)))}"
                            ),
                            inline=False
                        )
                
                # Send DM with file
                await interaction.user.send(embed=dm_embed, file=file)
                
                # Update summary embed with success
                summary_embed.description += f"\n\n{theme.verifiedIcon} **File successfully sent via DM!**"
                summary_embed.color = discord.Color.green()
                
            except discord.Forbidden:
                # DM failed, provide alternative
                summary_embed.description += (
                    f"\n\n{theme.deniedIcon} **Could not send DM** (DMs may be disabled)\n"
                    "The file will be posted here instead."
                )
                summary_embed.color = discord.Color.orange()
                
                # Since DM failed, edit the original message with the file
                await interaction.edit_original_response(embed=summary_embed)
                # Send file as a follow-up
                await interaction.followup.send(file=file, ephemeral=True)
                return
            
            await interaction.edit_original_response(embed=summary_embed)
            
        except Exception as e:
            logger.error(f"Error in process_member_export: {e}")
            print(f"Error in process_member_export: {e}")
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon} Export Failed",
                description=f"An error occurred during the export process: {str(e)}",
                color=theme.emColor2
            )
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=error_embed)
            else:
                await interaction.response.send_message(embed=error_embed, ephemeral=True)

    async def back_to_alliance_management(self, interaction: discord.Interaction):
        """Navigate back to the Alliances menu."""
        try:
            main_menu_cog = self.bot.get_cog("MainMenu")
            if main_menu_cog:
                await main_menu_cog.show_alliance_management(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while returning to menu.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Menu navigation error in member operations: {e}")
            print(f"Menu navigation error in member operations: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while returning to menu.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while returning to menu.",
                    ephemeral=True
                )

class AddMemberModal(discord.ui.Modal):
    def __init__(self, alliance_id):
        super().__init__(title="Add Member")
        self.alliance_id = alliance_id
        self.add_item(discord.ui.TextInput(
            label="Enter IDs or paste CSV/TSV data",
            placeholder="12345,67890, or newline-separated IDs, or paste your CSV/TSV export",
            style=discord.TextStyle.paragraph
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            ids = self.children[0].value
            await interaction.client.get_cog("AllianceMemberOperations").add_user(
                interaction, 
                self.alliance_id, 
                ids
            )
        except Exception as e:
            logger.error(f"Modal submit error: {e}")
            print(f"ERROR: Modal submit error - {str(e)}")
            await interaction.response.send_message(
                "An error occurred. Please try again.",
                ephemeral=True
            )

class AllianceSelectView(discord.ui.View):
    def __init__(self, alliances_with_counts, cog=None, page=0, context="transfer"):
        super().__init__(timeout=7200)
        self.alliances = alliances_with_counts
        self.cog = cog
        self.page = page
        self.max_page = (len(alliances_with_counts) - 1) // 25 if alliances_with_counts else 0
        self.current_select = None
        self.callback = None
        self.member_dict = {}
        self.selected_alliance_id = None
        self.context = context  # "transfer", "furnace_history", or "nickname_history"
        self.update_select_menu()

    def update_select_menu(self):
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)

        start_idx = self.page * 25
        end_idx = min(start_idx + 25, len(self.alliances))
        current_alliances = self.alliances[start_idx:end_idx]

        options = []
        for alliance_data in current_alliances:
            # Handle both 3-tuple and 4-tuple formats
            if len(alliance_data) == 4:
                alliance_id, name, count, is_assigned = alliance_data
                label = f"{name[:45]} {'✓ Assigned' if is_assigned else ''}"[:50]
                description = f"ID: {alliance_id} | Members: {count}{' | Already Assigned' if is_assigned else ''}"[:100]
            else:
                alliance_id, name, count = alliance_data
                label = f"{name[:50]}"
                description = f"ID: {alliance_id} | Members: {count}"

            options.append(
                discord.SelectOption(
                    label=label,
                    value=str(alliance_id),
                    description=description,
                    emoji=theme.verifiedIcon if len(alliance_data) == 4 and alliance_data[3] else theme.allianceIcon
                )
            )

        select = discord.ui.Select(
            placeholder=f"{theme.allianceIcon} Select an alliance... (Page {self.page + 1}/{self.max_page + 1})",
            options=options
        )
        
        async def select_callback(interaction: discord.Interaction):
            self.current_select = select
            if self.callback:
                await self.callback(interaction)
        
        select.callback = select_callback
        self.add_item(select)
        self.current_select = select

        if hasattr(self, 'prev_button'):
            self.prev_button.disabled = self.page == 0
        if hasattr(self, 'next_button'):
            self.next_button.disabled = self.page == self.max_page

    @discord.ui.button(label="", emoji=f"{theme.prevIcon}", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(label="", emoji=f"{theme.nextIcon}", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(label="Find by Player ID", emoji=theme.searchIcon, style=discord.ButtonStyle.secondary)
    async def fid_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if self.current_select and self.current_select.values:
                self.selected_alliance_id = self.current_select.values[0]
            
            modal = IDSearchModal(
                selected_alliance_id=self.selected_alliance_id,
                alliances=self.alliances,
                callback=self.callback,
                context=self.context,
                cog=self.cog
            )
            await interaction.response.send_modal(modal)
        except Exception as e:
            logger.error(f"ID button error: {e}")
            print(f"ID button error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error has occurred. Please try again.",
                ephemeral=True
            )

class IDSearchModal(discord.ui.Modal):
    def __init__(self, selected_alliance_id=None, alliances=None, callback=None, context="transfer", cog=None):
        super().__init__(title="Search Members with ID")
        self.selected_alliance_id = selected_alliance_id
        self.alliances = alliances
        self.callback = callback
        self.context = context
        self.cog = cog

        self.add_item(discord.ui.TextInput(
            label="Member ID",
            placeholder="Example: 12345",
            min_length=1,
            max_length=20,
            required=True
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            fid = self.children[0].value.strip()
            
            # Validate ID input
            if not fid:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Please enter a valid ID.",
                    ephemeral=True
                )
                return
            
            # Check if we're in a history context
            if self.context in ["furnace_history", "nickname_history"]:
                # Get the AllianceHistory cog
                changes_cog = self.cog.bot.get_cog("AllianceHistory") if self.cog else interaction.client.get_cog("AllianceHistory")
                if changes_cog:
                    await interaction.response.defer()
                    if self.context == "furnace_history":
                        await changes_cog.show_furnace_history(interaction, int(fid))
                    else:
                        await changes_cog.show_nickname_history(interaction, int(fid))
                else:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} History feature is not available.",
                        ephemeral=True
                    )
                return

            # Get member information
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, furnace_lv, alliance
                    FROM users
                    WHERE fid = ?
                """, (fid,))
                user_result = cursor.fetchone()

                if not user_result:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} No member with this ID was found.",
                        ephemeral=True
                    )
                    return

                fid, nickname, furnace_lv, current_alliance_id = user_result

                with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                    cursor = alliance_db.cursor()
                    cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (current_alliance_id,))
                    current_alliance_name = cursor.fetchone()[0]

                # Handle remove context
                if self.context == "remove":
                    embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Member Found - Delete Process",
                        description=(
                            f"**Member Information:**\n"
                            f"{theme.userIcon} **Name:** {nickname}\n"
                            f"{theme.fidIcon} **ID:** {fid}\n"
                            f"{theme.levelIcon} **Level:** {self.cog.level_mapping.get(furnace_lv, str(furnace_lv))}\n"
                            f"{theme.allianceIcon} **Current Alliance:** {current_alliance_name}\n\n"
                            f"{theme.warnIcon} **Are you sure you want to delete this member?**"
                        ),
                        color=theme.emColor2
                    )

                    view = discord.ui.View()
                    confirm_button = discord.ui.Button(
                        label=f"{theme.verifiedIcon} Confirm Delete",
                        style=discord.ButtonStyle.danger
                    )
                    cancel_button = discord.ui.Button(
                        label=f"{theme.deniedIcon} Cancel",
                        style=discord.ButtonStyle.secondary
                    )

                    async def confirm_callback(confirm_interaction: discord.Interaction):
                        try:
                            with sqlite3.connect('db/users.sqlite') as users_db:
                                cursor = users_db.cursor()
                                cursor.execute("DELETE FROM users WHERE fid = ?", (fid,))
                                users_db.commit()

                            success_embed = discord.Embed(
                                title=f"{theme.verifiedIcon} Member Deleted",
                                description=(
                                    f"{theme.userIcon} **Member:** {nickname}\n"
                                    f"{theme.fidIcon} **ID:** {fid}\n"
                                    f"{theme.allianceIcon} **Alliance:** {current_alliance_name}"
                                ),
                                color=theme.emColor3
                            )

                            await confirm_interaction.response.edit_message(
                                embed=success_embed,
                                view=None
                            )

                            logger.info(f"Member deleted via ID search: {nickname} (ID: {fid}) from {current_alliance_name}")

                        except Exception as e:
                            logger.error(f"Delete error: {e}")
                            print(f"Delete error: {e}")
                            error_embed = discord.Embed(
                                title=f"{theme.deniedIcon} Error",
                                description="An error occurred during the delete operation.",
                                color=theme.emColor2
                            )
                            await confirm_interaction.response.edit_message(
                                embed=error_embed,
                                view=None
                            )

                    async def cancel_callback(cancel_interaction: discord.Interaction):
                        cancel_embed = discord.Embed(
                            title=f"{theme.deniedIcon} Deletion Cancelled",
                            description="Member was not deleted.",
                            color=theme.emColor4
                        )
                        await cancel_interaction.response.edit_message(
                            embed=cancel_embed,
                            view=None
                        )

                    confirm_button.callback = confirm_callback
                    cancel_button.callback = cancel_callback
                    view.add_item(confirm_button)
                    view.add_item(cancel_button)

                    await interaction.response.send_message(
                        embed=embed,
                        view=view,
                        ephemeral=True
                    )
                    return

                # Handle giftcode context - validate permission and invoke callback with alliance
                if self.context == "giftcode":
                    # Check if user has permission to manage this alliance
                    has_permission = any(aid == current_alliance_id for aid, _, _ in self.alliances)
                    if not has_permission:
                        await interaction.response.send_message(
                            f"{theme.deniedIcon} You don't have permission to manage the alliance this member belongs to.",
                            ephemeral=True
                        )
                        return

                    # Invoke callback with the alliance ID
                    if self.callback:
                        await self.callback(interaction, alliance_id=current_alliance_id)
                    return

                # Transfer logic
                embed = discord.Embed(
                    title=f"{theme.verifiedIcon} Member Found - Transfer Process",
                    description=(
                        f"**Member Information:**\n"
                        f"{theme.userIcon} **Name:** {nickname}\n"
                        f"{theme.fidIcon} **ID:** {fid}\n"
                        f"{theme.levelIcon} **Level:** {self.cog.level_mapping.get(furnace_lv, str(furnace_lv))}\n"
                        f"{theme.allianceIcon} **Current Alliance:** {current_alliance_name}\n\n"
                        "**Transfer Process**\n"
                        "Please select the alliance you want to transfer the member to:"
                    ),
                    color=theme.emColor1
                )

                select = discord.ui.Select(
                    placeholder=f"{theme.pinIcon} Choose the target alliance...",
                    options=[
                        discord.SelectOption(
                            label=f"{name[:50]}",
                            value=str(alliance_id),
                            description=f"ID: {alliance_id}",
                            emoji=theme.allianceIcon
                        ) for alliance_id, name, _ in self.alliances
                        if alliance_id != current_alliance_id
                    ]
                )

                view = discord.ui.View()
                view.add_item(select)

                async def select_callback(select_interaction: discord.Interaction):
                    target_alliance_id = int(select.values[0])

                    try:
                        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                            cursor = alliance_db.cursor()
                            cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (target_alliance_id,))
                            target_alliance_name = cursor.fetchone()[0]


                        with sqlite3.connect('db/users.sqlite') as users_db:
                            cursor = users_db.cursor()
                            cursor.execute(
                                "UPDATE users SET alliance = ? WHERE fid = ?",
                                (target_alliance_id, fid)
                            )
                            users_db.commit()


                        success_embed = discord.Embed(
                            title=f"{theme.verifiedIcon} Transfer Successful",
                            description=(
                                f"{theme.userIcon} **Member:** {nickname}\n"
                                f"{theme.fidIcon} **ID:** {fid}\n"
                                f"{theme.allianceOldIcon} **Source:** {current_alliance_name}\n"
                                f"{theme.allianceIcon} **Target:** {target_alliance_name}"
                            ),
                            color=theme.emColor3
                        )

                        await select_interaction.response.edit_message(
                            embed=success_embed,
                            view=None
                        )

                    except Exception as e:
                        logger.error(f"Transfer error: {e}")
                        print(f"Transfer error: {e}")
                        error_embed = discord.Embed(
                            title=f"{theme.deniedIcon} Error",
                            description="An error occurred during the transfer operation.",
                            color=theme.emColor2
                        )
                        await select_interaction.response.edit_message(
                            embed=error_embed,
                            view=None
                        )

                select.callback = select_callback
                await interaction.response.send_message(
                    embed=embed,
                    view=view,
                    ephemeral=True
                )

        except Exception as e:
            logger.error(f"Error in IDSearchModal on_submit: {e.__class__.__name__}: {e}")
            print(f"Error details: {str(e.__class__.__name__)}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error has occurred. Please try again.",
                    ephemeral=True
                )

class AllianceSelectViewWithAll(discord.ui.View):
    def __init__(self, alliances_with_counts, cog):
        super().__init__(timeout=300)
        self.alliances = alliances_with_counts
        self.cog = cog
        self.current_select = None
        self.callback = None
        
        # Calculate total members across all alliances
        total_members = sum(count for _, _, count in alliances_with_counts)
        
        # Create select menu with ALL option
        options = [
            discord.SelectOption(
                label="ALL ALLIANCES",
                value="all",
                description=f"Export all {total_members} members from {len(alliances_with_counts)} alliances",
                emoji=theme.stateIcon
            )
        ]
        
        # Add individual alliance options
        for alliance_id, name, count in alliances_with_counts[:24]:  # Discord limit is 25 options
            options.append(
                discord.SelectOption(
                    label=f"{name[:50]}",
                    value=str(alliance_id),
                    description=f"ID: {alliance_id} | Members: {count}",
                    emoji=theme.allianceIcon
                )
            )
        
        select = discord.ui.Select(
            placeholder=f"{theme.allianceIcon} Select an alliance or ALL...",
            options=options
        )
        
        async def select_callback(interaction: discord.Interaction):
            self.current_select = select
            if self.callback:
                await self.callback(interaction)
        
        select.callback = select_callback
        self.add_item(select)
        self.current_select = select

class ExportColumnSelectView(discord.ui.View):
    def __init__(self, alliance_id, alliance_name, cog, include_alliance=False,
                 prefiltered_members=None):
        super().__init__(timeout=300)
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.cog = cog
        self.include_alliance = include_alliance
        self.prefiltered_members = prefiltered_members
        
        # Track selected columns (all selected by default)
        self.selected_columns = {
            'id': True,
            'name': True,
            'fc_level': True,
            'state': True
        }
        
        # Add alliance column if needed
        if include_alliance:
            self.selected_columns['alliance'] = True
            alliance_btn = discord.ui.Button(
                label=f"{theme.verifiedIcon} Alliance", 
                style=discord.ButtonStyle.primary, 
                custom_id="toggle_alliance", 
                row=0
            )
            alliance_btn.callback = self.toggle_alliance_button
            self.add_item(alliance_btn)
        
        # Add other column buttons
        id_btn = discord.ui.Button(label=f"{theme.verifiedIcon} ID", style=discord.ButtonStyle.primary, custom_id="toggle_id", row=0)
        id_btn.callback = self.toggle_id_button
        self.add_item(id_btn)
        
        name_btn = discord.ui.Button(label=f"{theme.verifiedIcon} Name", style=discord.ButtonStyle.primary, custom_id="toggle_name", row=0)
        name_btn.callback = self.toggle_name_button
        self.add_item(name_btn)
        
        fc_btn = discord.ui.Button(label=f"{theme.verifiedIcon} FC Level", style=discord.ButtonStyle.primary, custom_id="toggle_fc", row=0 if not include_alliance else 1)
        fc_btn.callback = self.toggle_fc_button
        self.add_item(fc_btn)
        
        state_btn = discord.ui.Button(label=f"{theme.verifiedIcon} State", style=discord.ButtonStyle.primary, custom_id="toggle_state", row=0 if not include_alliance else 1)
        state_btn.callback = self.toggle_state_button
        self.add_item(state_btn)
        
        next_btn = discord.ui.Button(label="Next", emoji=theme.forwardIcon, style=discord.ButtonStyle.success, custom_id="next_step", row=1 if not include_alliance else 2)
        next_btn.callback = self.next_button
        self.add_item(next_btn)
        
        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="cancel", row=1 if not include_alliance else 2)
        cancel_btn.callback = self.cancel_button
        self.add_item(cancel_btn)
        
        self.update_buttons()
    
    def update_buttons(self):
        # Update button styles based on selection state
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if item.custom_id == 'toggle_alliance' and self.include_alliance:
                    item.style = discord.ButtonStyle.primary if self.selected_columns.get('alliance', False) else discord.ButtonStyle.secondary
                    item.label = f"{theme.verifiedIcon} Alliance" if self.selected_columns.get('alliance', False) else f"{theme.deniedIcon} Alliance"
                elif item.custom_id == 'toggle_id':
                    item.style = discord.ButtonStyle.primary if self.selected_columns['id'] else discord.ButtonStyle.secondary
                    item.label = f"{theme.verifiedIcon} ID" if self.selected_columns['id'] else f"{theme.deniedIcon} ID"
                elif item.custom_id == 'toggle_name':
                    item.style = discord.ButtonStyle.primary if self.selected_columns['name'] else discord.ButtonStyle.secondary
                    item.label = f"{theme.verifiedIcon} Name" if self.selected_columns['name'] else f"{theme.deniedIcon} Name"
                elif item.custom_id == 'toggle_fc':
                    item.style = discord.ButtonStyle.primary if self.selected_columns['fc_level'] else discord.ButtonStyle.secondary
                    item.label = f"{theme.verifiedIcon} FC Level" if self.selected_columns['fc_level'] else f"{theme.deniedIcon} FC Level"
                elif item.custom_id == 'toggle_state':
                    item.style = discord.ButtonStyle.primary if self.selected_columns['state'] else discord.ButtonStyle.secondary
                    item.label = f"{theme.verifiedIcon} State" if self.selected_columns['state'] else f"{theme.deniedIcon} State"

    async def toggle_alliance_button(self, interaction: discord.Interaction):
        if self.include_alliance:
            self.selected_columns['alliance'] = not self.selected_columns.get('alliance', True)
            self.update_buttons()
            
            if not any(self.selected_columns.values()):
                self.selected_columns['alliance'] = True
                self.update_buttons()
                await interaction.response.edit_message(
                    content=f"{theme.warnIcon} At least one column must be selected!",
                    view=self
                )
            else:
                await interaction.response.edit_message(view=self)
    
    async def toggle_id_button(self, interaction: discord.Interaction):
        self.selected_columns['id'] = not self.selected_columns['id']
        self.update_buttons()
        
        if not any(self.selected_columns.values()):
            self.selected_columns['id'] = True
            self.update_buttons()
            await interaction.response.edit_message(
                content=f"{theme.warnIcon} At least one column must be selected!",
                view=self
            )
        else:
            await interaction.response.edit_message(view=self)
    
    async def toggle_name_button(self, interaction: discord.Interaction):
        self.selected_columns['name'] = not self.selected_columns['name']
        self.update_buttons()
        
        if not any(self.selected_columns.values()):
            self.selected_columns['name'] = True
            self.update_buttons()
            await interaction.response.edit_message(
                content=f"{theme.warnIcon} At least one column must be selected!",
                view=self
            )
        else:
            await interaction.response.edit_message(view=self)
    
    async def toggle_fc_button(self, interaction: discord.Interaction):
        self.selected_columns['fc_level'] = not self.selected_columns['fc_level']
        self.update_buttons()
        
        if not any(self.selected_columns.values()):
            self.selected_columns['fc_level'] = True
            self.update_buttons()
            await interaction.response.edit_message(
                content=f"{theme.warnIcon} At least one column must be selected!",
                view=self
            )
        else:
            await interaction.response.edit_message(view=self)
    
    async def toggle_state_button(self, interaction: discord.Interaction):
        self.selected_columns['state'] = not self.selected_columns['state']
        self.update_buttons()
        
        if not any(self.selected_columns.values()):
            self.selected_columns['state'] = True
            self.update_buttons()
            await interaction.response.edit_message(
                content=f"{theme.warnIcon} At least one column must be selected!",
                view=self
            )
        else:
            await interaction.response.edit_message(view=self)
    
    async def next_button(self, interaction: discord.Interaction):
        # Build selected columns list
        columns = []
        if self.include_alliance and self.selected_columns.get('alliance', False):
            columns.append(('alliance_name', 'Alliance'))
        if self.selected_columns['id']:
            columns.append(('fid', 'ID'))
        if self.selected_columns['name']:
            columns.append(('nickname', 'Name'))
        if self.selected_columns['fc_level']:
            columns.append(('furnace_lv', 'FC Level'))
        if self.selected_columns['state']:
            columns.append(('kid', 'State'))
        
        # Show format selection
        format_embed = discord.Embed(
            title=f"{theme.exportIcon} Select Export Format",
            description=(
                f"**Alliance:** {self.alliance_name}\n"
                f"**Selected Columns:** {', '.join([col[1] for col in columns])}\n\n"
                "Please select the export format:"
            ),
            color=theme.emColor1
        )
        
        format_view = ExportFormatSelectView(
            self.alliance_id, self.alliance_name, columns, self.cog,
            prefiltered_members=self.prefiltered_members,
        )
        await interaction.response.edit_message(embed=format_embed, view=format_view, content=None)
    
    async def cancel_button(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=f"{theme.deniedIcon} Export cancelled.",
            embed=None,
            view=None
        )

class ExportFormatSelectView(discord.ui.View):
    def __init__(self, alliance_id, alliance_name, selected_columns, cog,
                 prefiltered_members=None):
        super().__init__(timeout=300)
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.selected_columns = selected_columns
        self.cog = cog
        self.prefiltered_members = prefiltered_members

    @discord.ui.button(label="CSV (Comma-separated)", emoji=theme.averageIcon, style=discord.ButtonStyle.primary, custom_id="csv")
    async def csv_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_member_export(
            interaction,
            self.alliance_id,
            self.alliance_name,
            self.selected_columns,
            'csv',
            prefiltered_members=self.prefiltered_members,
        )

    @discord.ui.button(label="TSV (Tab-separated)", emoji=theme.listIcon, style=discord.ButtonStyle.primary, custom_id="tsv")
    async def tsv_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_member_export(
            interaction,
            self.alliance_id,
            self.alliance_name,
            self.selected_columns,
            'tsv',
            prefiltered_members=self.prefiltered_members,
        )
    
    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, custom_id="back")
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        column_embed = discord.Embed(
            title=f"{theme.chartIcon} Select Export Columns",
            description=(
                f"**Alliance:** {self.alliance_name}\n\n"
                "Click the buttons to toggle columns on/off.\n"
                "All columns are selected by default.\n\n"
                "**Available Columns:**\n"
                "• **ID** - Member ID\n"
                "• **Name** - Member's nickname\n"
                "• **FC Level** - Furnace level\n"
                "• **State** - State ID"
            ),
            color=theme.emColor1
        )
        
        # Check if it's an all-alliance export by checking the alliance_id
        include_alliance = self.alliance_id == "all"
        if include_alliance:
            column_embed.description = (
                f"**Export Type:** ALL ALLIANCES\n\n"
                "Click the buttons to toggle columns on/off.\n"
                "All columns are selected by default.\n\n"
                "**Available Columns:**\n"
                "• **Alliance** - Alliance name\n"
                "• **ID** - Member ID\n"
                "• **Name** - Member's nickname\n"
                "• **FC Level** - Furnace level\n"
                "• **State** - State ID"
            )
        
        column_view = ExportColumnSelectView(
            self.alliance_id, self.alliance_name, self.cog, include_alliance,
            prefiltered_members=self.prefiltered_members,
        )
        await interaction.response.edit_message(embed=column_embed, view=column_view)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="cancel")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content=f"{theme.deniedIcon} Export cancelled.",
            embed=None,
            view=None
        )

def _parse_member_ids(text: str) -> list:
    """Extract IDs from raw user input. Supports CSV/TSV (with optional 'id'/'fid'
    header), plus comma- or newline-separated lists. Returns numeric strings."""
    if not text:
        return []

    ids = []
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if not lines:
        return ids

    if any(d in lines[0] for d in [',', '\t']):
        delimiter = '\t' if '\t' in lines[0] else ','
        try:
            reader = csv.reader(io.StringIO(text), delimiter=delimiter)
            rows = list(reader)
            if rows:
                headers = [h.strip().lower() for h in rows[0]]
                id_col = next((i for i, h in enumerate(headers) if h in ('id', 'fid')), None)
                if id_col is not None:
                    for row in rows[1:]:
                        if len(row) > id_col:
                            fid = ''.join(c for c in row[id_col] if c.isdigit())
                            if fid:
                                ids.append(fid)
                    return ids
                if rows[0] and rows[0][0].strip().isdigit():
                    for row in rows:
                        if row and row[0].strip():
                            fid = ''.join(c for c in row[0] if c.isdigit())
                            if fid:
                                ids.append(fid)
                    return ids
        except Exception:
            pass

    if '\n' in text:
        for line in text.split('\n'):
            fid = line.strip()
            if fid:
                ids.append(fid)
    else:
        for fid in text.split(','):
            fid = fid.strip()
            if fid:
                ids.append(fid)
    return ids


class IDMultiSelectModal(discord.ui.Modal):
    """Bulk-add IDs into an existing MemberSelectView's pending selection.

    Accepts the same input formats as Add Members (newline / comma / CSV / TSV).
    IDs not present in the parent view's member list are reported back as ignored.
    """

    def __init__(self, parent_view: "MemberSelectView"):
        super().__init__(title="Select Members by ID")
        self.parent_view = parent_view
        self.add_item(discord.ui.TextInput(
            label="Enter IDs (one per line, comma, or CSV/TSV)",
            placeholder="12345\n67890\n... or paste an exported member list",
            style=discord.TextStyle.paragraph,
            required=True,
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            requested = _parse_member_ids(self.children[0].value or "")

            available = {fid for fid, _, _ in self.parent_view.members}
            added = []
            already = []
            unknown = []
            for fid_str in requested:
                try:
                    fid = int(fid_str)
                except ValueError:
                    unknown.append(fid_str)
                    continue
                if fid not in available:
                    unknown.append(fid_str)
                elif fid in self.parent_view.pending_selections:
                    already.append(fid)
                else:
                    self.parent_view.pending_selections.add(fid)
                    added.append(fid)

            self.parent_view.update_select_menu()
            self.parent_view.update_action_buttons()
            await interaction.response.edit_message(
                embed=self.parent_view._build_main_embed(),
                view=self.parent_view,
            )

            if unknown or already:
                lines = [f"{theme.verifiedIcon} Added **{len(added)}** ID(s) to selection."]
                if already:
                    lines.append(f"{theme.warnIcon} **{len(already)}** were already selected.")
                if unknown:
                    sample = ", ".join(unknown[:10])
                    extra = f" (+{len(unknown) - 10} more)" if len(unknown) > 10 else ""
                    lines.append(
                        f"{theme.deniedIcon} **{len(unknown)}** not in this alliance: "
                        f"{sample}{extra}"
                    )
                await interaction.followup.send("\n".join(lines), ephemeral=True)

        except Exception as e:
            logger.error(f"IDMultiSelectModal error: {e}")
            print(f"IDMultiSelectModal error: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while processing IDs.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} An error occurred while processing IDs.",
                        ephemeral=True,
                    )
            except Exception:
                pass


class MemberSelectView(discord.ui.View):
    def __init__(self, members, source_alliance_name, cog, page=0, is_remove_operation=False, alliance_id=None, alliances=None):
        super().__init__(timeout=7200)
        self.members = members
        self.source_alliance_name = source_alliance_name
        self.cog = cog
        self.page = page
        self.max_page = (len(members) - 1) // 25
        self.current_select = None
        self.callback = None
        self.member_dict = {str(fid): nickname for fid, nickname, _ in members}
        self.selected_alliance_id = alliance_id
        self.alliances = alliances
        self.is_remove_operation = is_remove_operation
        self.context = "remove" if is_remove_operation else "transfer"
        self.pending_selections = set()  # Track selected FIDs across pages

        # Remove "Delete All" button if not in remove operation mode
        if not is_remove_operation:
            self.remove_item(self._delete_all_button)

        self.update_select_menu()
        self.update_action_buttons()

    def update_select_menu(self):
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)

        start_idx = self.page * 25
        end_idx = min(start_idx + 25, len(self.members))
        current_members = self.members[start_idx:end_idx]

        options = []

        # Build member options
        for fid, nickname, furnace_lv in current_members:
            # Mark as default if already selected
            is_selected = int(fid) in self.pending_selections
            options.append(discord.SelectOption(
                label=f"{nickname[:50]}",
                value=str(fid),
                description=f"ID: {fid} | FC: {self.cog.level_mapping.get(furnace_lv, str(furnace_lv))}",
                emoji=theme.verifiedIcon if is_selected else theme.userIcon,
                default=is_selected
            ))

        # Determine placeholder based on context (remove vs transfer)
        if self.is_remove_operation:
            placeholder_text = f"{theme.membersIcon} Select members to remove (Page {self.page + 1}/{self.max_page + 1})"
        else:
            placeholder_text = f"{theme.membersIcon} Select members to transfer (Page {self.page + 1}/{self.max_page + 1})"

        # Multi-select dropdown
        max_vals = min(len(options), 25)
        select = discord.ui.Select(
            placeholder=placeholder_text,
            options=options,
            max_values=max_vals,
            min_values=0
        )

        async def select_callback(interaction: discord.Interaction):
            try:
                self.current_select = select

                # Get FIDs on current page
                current_page_fids = {int(fid) for fid, _, _ in current_members}

                # Remove old selections from this page
                self.pending_selections -= current_page_fids

                # Add new selections
                for val in select.values:
                    self.pending_selections.add(int(val))

                # Update UI
                self.update_select_menu()
                self.update_action_buttons()
                await self.update_main_embed(interaction)

            except Exception as e:
                logger.error(f"Select callback error: {e}")
                print(f"Select callback error: {e}")
                error_embed = discord.Embed(
                    title=f"{theme.deniedIcon} Error",
                    description="An error occurred while selecting members. Please try again.",
                    color=theme.emColor2
                )
                try:
                    await interaction.response.edit_message(embed=error_embed, view=self)
                except Exception:
                    await interaction.followup.send(embed=error_embed, ephemeral=True)

        select.callback = select_callback
        self.add_item(select)
        self.current_select = select

        # Update navigation button states
        if hasattr(self, '_prev_button'):
            self._prev_button.disabled = self.page == 0
        if hasattr(self, '_next_button'):
            self._next_button.disabled = self.page == self.max_page

    def _build_main_embed(self) -> discord.Embed:
        """Build the member-selection embed reflecting current selection state."""
        max_fl = max(member[2] for member in self.members)
        avg_fl = sum(member[2] for member in self.members) / len(self.members)

        selection_text = ""
        if self.pending_selections:
            selection_text = f"\n\n**{theme.pinIcon} Selected: {len(self.pending_selections)} member(s)**"

        return discord.Embed(
            title=f"{theme.membersIcon} {self.source_alliance_name} - Member Selection",
            description=(
                "```ml\n"
                "Alliance Statistics\n"
                "══════════════════════════\n"
                f"{theme.chartIcon} Total Members    : {len(self.members)}\n"
                f"{theme.levelIcon} Highest Level    : {self.cog.level_mapping.get(max_fl, str(max_fl))}\n"
                f"{theme.chartIcon} Average Level    : {self.cog.level_mapping.get(int(avg_fl), str(int(avg_fl)))}\n"
                "══════════════════════════\n"
                "```"
                f"{selection_text}\n"
                f"Select members using the dropdown below:"
            ),
            color=theme.emColor2 if self.is_remove_operation else discord.Color.blue()
        )

    async def update_main_embed(self, interaction: discord.Interaction):
        """Update the main embed (used by dropdown / pagination callbacks)."""
        embed = self._build_main_embed()
        if not interaction.response.is_done():
            await interaction.response.defer()
        await interaction.edit_original_response(embed=embed, view=self)

    def update_action_buttons(self):
        """Update the state of action buttons based on selections"""
        has_selections = len(self.pending_selections) > 0

        if hasattr(self, '_process_button'):
            self._process_button.disabled = not has_selections
        if hasattr(self, '_clear_button'):
            self._clear_button.disabled = not has_selections

    @discord.ui.button(label="", emoji=f"{theme.prevIcon}", style=discord.ButtonStyle.secondary, row=1)
    async def _prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_select_menu()
        await self.update_main_embed(interaction)

    @discord.ui.button(label="", emoji=f"{theme.nextIcon}", style=discord.ButtonStyle.secondary, row=1)
    async def _next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_select_menu()
        await self.update_main_embed(interaction)

    @discord.ui.button(label="Select IDs", emoji=theme.searchIcon, style=discord.ButtonStyle.secondary, row=1)
    async def fid_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.send_modal(IDMultiSelectModal(self))
        except Exception as e:
            logger.error(f"ID button error: {e}")
            print(f"ID button error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error has occurred. Please try again.",
                ephemeral=True
            )

    @discord.ui.button(label="Process Selected", emoji=theme.verifiedIcon, style=discord.ButtonStyle.success, row=2, disabled=True)
    async def _process_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Process selected members (delete or transfer)"""
        if not self.pending_selections:
            await interaction.response.send_message("No members selected", ephemeral=True)
            return

        if self.callback:
            await self.callback(interaction, list(self.pending_selections))

    @discord.ui.button(label="Clear Selection", emoji=theme.trashIcon, style=discord.ButtonStyle.secondary, row=2, disabled=True)
    async def _clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clear all selected members"""
        self.pending_selections.clear()
        self.update_select_menu()
        self.update_action_buttons()
        await self.update_main_embed(interaction)

    @discord.ui.button(label="Delete All", emoji=theme.warnIcon, style=discord.ButtonStyle.danger, row=2)
    async def _delete_all_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Delete all members in the alliance (only shown for remove operations)"""
        if not self.is_remove_operation:
            return

        await interaction.response.send_message(
            f"{theme.warnIcon} This will delete ALL {len(self.members)} members from **{self.source_alliance_name}**. Are you sure?",
            view=DeleteAllConfirmView(self),
            ephemeral=True
        )

class DeleteAllConfirmView(discord.ui.View):
    def __init__(self, parent_view):
        super().__init__(timeout=60)
        self.parent_view = parent_view

    @discord.ui.button(label=f"{theme.verifiedIcon} Confirm Delete All", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Call the parent's delete all callback
        if self.parent_view.callback:
            all_fids = [fid for fid, _, _ in self.parent_view.members]
            await self.parent_view.callback(interaction, all_fids, delete_all=True)

    @discord.ui.button(label=f"{theme.deniedIcon} Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cancel_embed = discord.Embed(
            title=f"{theme.deniedIcon} Cancelled",
            description="Delete all operation cancelled.",
            color=theme.emColor4
        )
        await interaction.response.edit_message(embed=cancel_embed, view=None)

async def setup(bot):
    await bot.add_cog(AllianceMemberOperations(bot))