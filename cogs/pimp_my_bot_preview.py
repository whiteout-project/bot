"""
Theme Preview - Generates realistic preview of theme appearance.
Shows multiple preview pages that mimic real bot embeds.
"""
import discord

from .pimp_my_bot import (
    theme, DEFAULT_EMOJI, check_interaction_user, build_divider
)

class ThemePreviewView(discord.ui.View):
    """View for showing theme preview with multiple real-world embed examples."""

    PAGE_TITLES = [
        "Settings Menu",
        "Alliance Changes",
        "Gift Code Status",
        "Member Info",
        "Player Lookup (/w)"
    ]

    def __init__(self, cog, session, parent_view, from_menu: bool = False):
        super().__init__(timeout=7200)
        self.cog = cog
        self.session = session
        self.parent_view = parent_view
        self.from_menu = from_menu
        self.current_page = 0
        self.total_pages = len(self.PAGE_TITLES)

        self._build_components()

    def _build_components(self):
        """Build buttons based on current page state."""
        self.clear_items()

        # Row 0: Navigation
        prev_btn = discord.ui.Button(
            label="",
            emoji=theme.prevIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="prev",
            disabled=self.current_page == 0,
            row=0
        )
        prev_btn.callback = self.go_previous
        self.add_item(prev_btn)

        # Page indicator (disabled button showing current page)
        page_btn = discord.ui.Button(
            label=f"{self.current_page + 1}/{self.total_pages}",
            style=discord.ButtonStyle.secondary,
            custom_id="page_indicator",
            disabled=True,
            row=0
        )
        self.add_item(page_btn)

        next_btn = discord.ui.Button(
            label="",
            emoji=theme.nextIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="next",
            disabled=self.current_page >= self.total_pages - 1,
            row=0
        )
        next_btn.callback = self.go_next
        self.add_item(next_btn)

        # Row 1: Back button only
        back_label = "Back to Menu" if self.from_menu else "Back to Hub"
        back_btn = discord.ui.Button(
            label=back_label,
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="back",
            row=1
        )
        back_btn.callback = self.back_to_parent
        self.add_item(back_btn)

    def _get_theme_data(self):
        """Load and return theme data."""
        self.session.load_theme_data()
        # Return empty dict if theme data is None
        return self.session.theme_data if self.session.theme_data is not None else {}

    def _get_dividers(self, data):
        """Build dividers from theme data.

        Returns:
            Tuple of (upper_divider, lower_divider, middle_divider)
        """
        divider1 = build_divider(
            data.get('dividerStart1', '━'),
            data.get('dividerPattern1', '━'),
            data.get('dividerEnd1', '━'),
            int(data.get('dividerLength1', 20) or 20)
        )
        divider2 = build_divider(
            data.get('dividerStart2', '━'),
            data.get('dividerPattern2', '━'),
            data.get('dividerEnd2', '━'),
            int(data.get('dividerLength2', 20) or 20)
        )
        divider3 = build_divider(
            data.get('dividerStart3', '━'),
            data.get('dividerPattern3', '━'),
            data.get('dividerEnd3', '━'),
            int(data.get('dividerLength3', 20) or 20)
        )

        # Apply code block wrapping if enabled
        if data.get('dividerCodeBlock1'):
            divider1 = f"`{divider1}`"
        if data.get('dividerCodeBlock2'):
            divider2 = f"`{divider2}`"
        if data.get('dividerCodeBlock3'):
            divider3 = f"`{divider3}`"

        return divider1, divider2, divider3

    def _get_color(self, data, color_key='emColorString1') -> int:
        """Get color as integer from theme data."""
        color_str = data.get(color_key, '#3498DB')
        try:
            return int(color_str.lstrip('#'), 16)
        except ValueError:
            return 0x3498DB

    def _get_icon(self, data, icon_key: str) -> str:
        """Get icon value with fallback for empty/missing values."""
        # Handle None data gracefully
        if data is None:
            return DEFAULT_EMOJI

        value = data.get(icon_key)
        # Return default if None, empty string, or whitespace-only
        if value is None or not str(value).strip():
            return DEFAULT_EMOJI
        return value

    def build_preview_embed(self) -> discord.Embed:
        """Build the current page's preview embed."""
        builders = [
            self.build_settings_menu_preview,
            self.build_changes_preview,
            self.build_gift_status_preview,
            self.build_member_info_preview,
            self.build_player_lookup_preview,
        ]
        return builders[self.current_page]()

    def build_settings_menu_preview(self) -> discord.Embed:
        """Settings menu preview - demonstrates menu/category icons."""
        data = self._get_theme_data()
        divider1, divider2, divider3 = self._get_dividers(data)
        color = self._get_color(data)

        # Get icons with fallback for empty values
        settings_icon = self._get_icon(data, 'settingsIcon')
        castle_icon = self._get_icon(data, 'castleBattleIcon')
        members_icon = self._get_icon(data, 'membersIcon')
        robot_icon = self._get_icon(data, 'robotIcon')
        gift_icon = self._get_icon(data, 'giftIcon')
        list_icon = self._get_icon(data, 'listIcon')
        support_icon = self._get_icon(data, 'supportIcon')
        palette_icon = self._get_icon(data, 'paletteIcon')

        embed = discord.Embed(
            title=f"{settings_icon} Settings Menu",
            description=(
                f"Please select a category:\n\n"
                f"**Menu Categories**\n"
                f"{divider1}\n"
                f"{castle_icon} **Alliance Operations**\n"
                f"└ Manage alliances and settings\n\n"
                f"{members_icon} **Member Operations**\n"
                f"└ Add, remove, transfer members\n\n"
                f"{robot_icon} **Bot Operations**\n"
                f"└ Configure bot behavior\n\n"
                f"{gift_icon} **Gift Code Operations**\n"
                f"└ Redeem and manage gift codes\n\n"
                f"{list_icon} **Alliance History**\n"
                f"└ View alliance changes and history\n\n"
                f"{support_icon} **Support Operations**\n"
                f"└ Get help and support\n\n"
                f"{palette_icon} **Theme Settings**\n"
                f"└ Customize bot appearance\n"
                f"{divider2}"
            ),
            color=color
        )
        embed.set_footer(text=f"Preview: {self.PAGE_TITLES[self.current_page]} • Theme: {self.session.theme_name}")
        return embed

    def build_changes_preview(self) -> discord.Embed:
        """Changes preview - demonstrates Old/New icon pairs for tracking changes."""
        data = self._get_theme_data()
        divider1, divider2, divider3 = self._get_dividers(data)
        color = self._get_color(data)

        # Get icons with fallback for empty values
        level_icon = self._get_icon(data, 'levelIcon')
        stove_old_icon = self._get_icon(data, 'stoveOldIcon')
        stove_icon = self._get_icon(data, 'stoveIcon')
        avatar_old_icon = self._get_icon(data, 'avatarOldIcon')
        avatar_icon = self._get_icon(data, 'avatarIcon')
        state_old_icon = self._get_icon(data, 'stateOldIcon')
        state_icon = self._get_icon(data, 'stateIcon')
        time_icon = self._get_icon(data, 'timeIcon')
        chart_icon = self._get_icon(data, 'chartIcon')
        user_icon = self._get_icon(data, 'userIcon')

        embed = discord.Embed(
            title=f"{level_icon} Alliance Changes",
            description=(
                f"**Recent Member Changes**\n"
                f"{divider1}\n"
            ),
            color=color
        )

        # Furnace level change example
        embed.add_field(
            name=f"{user_icon} FrostWarrior",
            value=f"{stove_old_icon} `FC 5` ➜ {stove_icon} `FC 6`\n{time_icon} Just now",
            inline=False
        )

        # Nickname change example
        embed.add_field(
            name=f"{user_icon} IceQueen",
            value=f"{avatar_old_icon} `OldNickname` ➜ {avatar_icon} `IceQueen`\n{time_icon} 5 min ago",
            inline=False
        )

        # State transfer example
        embed.add_field(
            name=f"{user_icon} Wanderer",
            value=f"{state_old_icon} State `123` ➜ {state_icon} State `456`\n{time_icon} 1 hour ago",
            inline=False
        )

        embed.add_field(
            name=f"{divider2}",
            value=f"{chart_icon} **Total Changes:** 3",
            inline=False
        )

        embed.set_footer(text=f"Preview: {self.PAGE_TITLES[self.current_page]} • Theme: {self.session.theme_name}")
        return embed

    def build_gift_status_preview(self) -> discord.Embed:
        """Gift code status preview - demonstrates status/result icons."""
        data = self._get_theme_data()
        divider1, divider2, divider3 = self._get_dividers(data)
        color = self._get_color(data, 'emColorString3')  # Success color

        # Get icons with fallback for empty values
        gift_icon = self._get_icon(data, 'giftIcon')
        verified_icon = self._get_icon(data, 'verifiedIcon')
        denied_icon = self._get_icon(data, 'deniedIcon')
        warn_icon = self._get_icon(data, 'warnIcon')
        total_icon = self._get_icon(data, 'totalIcon')
        gift_check_icon = self._get_icon(data, 'giftCheckIcon')
        alliance_icon = self._get_icon(data, 'allianceIcon')
        time_icon = self._get_icon(data, 'timeIcon')

        embed = discord.Embed(
            title=f"{gift_check_icon} Gift Code Redemption Complete",
            description=(
                f"{alliance_icon} **Alliance:** Really Cool Alliance\n"
                f"{gift_icon} **Code:** `WINTERGIFT2024`\n"
                f"{divider1}\n"
                f"{verified_icon} **Success:** 45 members\n"
                f"{denied_icon} **Already Claimed:** 3 members\n"
                f"{warn_icon} **Invalid/Error:** 2 members\n"
                f"{divider2}\n"
                f"{total_icon} **Total Processed:** 50 members\n"
                f"{time_icon} **Duration:** 2m 34s\n"
            ),
            color=color
        )
        embed.set_footer(text=f"Preview: {self.PAGE_TITLES[self.current_page]} • Theme: {self.session.theme_name}")
        return embed

    def build_member_info_preview(self) -> discord.Embed:
        """Member info preview - demonstrates profile/info icons with thumbnail.

        This preview includes a thumbnail image to show how divider2 looks
        when displayed alongside an image (shorter width due to image presence).
        """
        data = self._get_theme_data()
        divider1, divider2, divider3 = self._get_dividers(data)
        color = self._get_color(data)

        # Get icons with fallback for empty values
        avatar_icon = self._get_icon(data, 'avatarIcon')
        fid_icon = self._get_icon(data, 'fidIcon')
        alliance_icon = self._get_icon(data, 'allianceIcon')
        stove_icon = self._get_icon(data, 'stoveIcon')
        state_icon = self._get_icon(data, 'stateIcon')
        verified_icon = self._get_icon(data, 'verifiedIcon')
        time_icon = self._get_icon(data, 'timeIcon')
        crown_icon = self._get_icon(data, 'crownIcon')
        home_icon = self._get_icon(data, 'homeIcon')

        embed = discord.Embed(
            title=f"{avatar_icon} Player Profile",
            description=(
                f"{divider1}\n"
                f"{fid_icon} **ID:** `123456789`\n"
                f"{avatar_icon} **Nickname:** FrostWarrior\n"
                f"{alliance_icon} **Alliance:** Really Cool Alliance\n"
                f"{stove_icon} **Furnace:** FC 8-3\n"
                f"{state_icon} **State:** 999\n"
                f"{crown_icon} **Rank:** R4\n"
                f"{divider2}\n"
                f"{verified_icon} **Status:** Verified Member\n"
                f"{home_icon} **Joined:** 2024-01-15\n"
                f"{time_icon} **Last Active:** Just now\n"
            ),
            color=color
        )
        # Using a generic placeholder avatar from Discord's CDN
        embed.set_thumbnail(url="https://cdn.discordapp.com/embed/avatars/0.png")
        embed.set_footer(text=f"Preview: {self.PAGE_TITLES[self.current_page]} • Theme: {self.session.theme_name}")
        return embed

    def build_player_lookup_preview(self) -> discord.Embed:
        """Player lookup (/w command) preview - demonstrates full image + thumbnail layout.

        The /w command shows player info with both a large avatar image and a
        thumbnail for furnace level content. This preview helps users see how
        their dividers look with both image types present.
        """
        data = self._get_theme_data()
        divider1, divider2, divider3 = self._get_dividers(data)
        color = self._get_color(data)

        # Get icons with fallback for empty values
        user_icon = self._get_icon(data, 'userIcon')
        fid_icon = self._get_icon(data, 'fidIcon')
        level_icon = self._get_icon(data, 'levelIcon')
        globe_icon = self._get_icon(data, 'globeIcon')
        alliance_icon = self._get_icon(data, 'allianceIcon')
        verified_icon = self._get_icon(data, 'verifiedIcon')

        embed = discord.Embed(
            title=f"{user_icon} FrostWarrior",
            description=(
                f"{divider1}\n"
                f"**{fid_icon} ID:** `123456789`\n"
                f"**{level_icon} Furnace Level:** `FC 8-3`\n"
                f"**{globe_icon} State:** `999`\n"
                f"{divider3}\n"
                f"**{alliance_icon} Alliance:** `Example Alliance`\n"
                f"{divider2}\n"
            ),
            color=color
        )
        embed.set_footer(text=f"Registered on the List {verified_icon}")

        # Using Discord's default avatar as placeholder for the large image
        embed.set_image(url="https://cdn.discordapp.com/embed/avatars/1.png")
        # Thumbnail would be the furnace level image
        embed.set_thumbnail(url="https://cdn.discordapp.com/embed/avatars/2.png")

        return embed

    async def go_previous(self, interaction: discord.Interaction):
        """Go to previous preview page."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        if self.current_page > 0:
            self.current_page -= 1
            self._build_components()
            embed = self.build_preview_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    async def go_next(self, interaction: discord.Interaction):
        """Go to next preview page."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self._build_components()
            embed = self.build_preview_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    async def back_to_parent(self, interaction: discord.Interaction):
        """Return to parent view (menu or hub)."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        if self.from_menu:
            # Returning to ThemeMenuView
            embed = self.parent_view.build_embed()
            await interaction.response.edit_message(embed=embed, view=self.parent_view)
        else:
            # Returning to ThemeEditorHub
            embed = self.parent_view.build_hub_embed()
            await interaction.response.edit_message(embed=embed, view=self.parent_view)