"""
Theme Editor - Central UI for creating and editing themes.
Provides categorized icon editing with a central hub for navigation.
"""
import discord
import sqlite3
import re
import logging

from .pimp_my_bot import (
    theme, THEME_DB_PATH, DEFAULT_EMOJI, ICON_CATEGORIES, check_interaction_user, build_divider,
    ThemeMenuView, ICON_NAMES, DEFAULT_ICON_VALUES
)

logger = logging.getLogger(__name__)


def format_emoji_for_display(value: str) -> str:
    """Format an emoji value for display in select option descriptions.

    Custom emojis like <:name:123> are shown as ':name:' since Discord
    doesn't render emojis in SelectOption descriptions.
    """
    # Match custom emoji format: <:name:id> or <a:name:id> (animated)
    match = re.match(r'<a?:(\w+):\d+>', value)
    if match:
        return f":{match.group(1)}:"
    return value

# Valid column names for SQL updates (security whitelist)
VALID_ICON_COLUMNS = set(ICON_NAMES)
VALID_DIVIDER_COLUMNS = {
    'dividerStart1', 'dividerPattern1', 'dividerEnd1', 'dividerLength1',
    'dividerStart2', 'dividerPattern2', 'dividerEnd2', 'dividerLength2',
    'dividerStart3', 'dividerPattern3', 'dividerEnd3', 'dividerLength3'
}
VALID_COLOR_COLUMNS = {
    'emColorString1', 'emColorString2', 'emColorString3', 'emColorString4',
    'headerColor1', 'headerColor2'
}

def is_valid_column(column_name: str) -> bool:
    """Check if column name is in the whitelist of valid columns."""
    return column_name in VALID_ICON_COLUMNS or column_name in VALID_DIVIDER_COLUMNS or column_name in VALID_COLOR_COLUMNS

def reload_theme_if_active(theme_name: str, guild_id: int = None) -> None:
    """Reload theme if it's currently in use (global active or server override)."""
    with sqlite3.connect(THEME_DB_PATH) as conn:
        cursor = conn.cursor()

        # Check if this theme is the global active theme
        cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (theme_name,))
        result = cursor.fetchone()
        is_global_active = result and result[0] == 1

        # Check if this theme is the server's override theme
        is_server_theme = False
        if guild_id:
            cursor.execute("SELECT theme_name FROM server_themes WHERE guild_id=?", (guild_id,))
            server_result = cursor.fetchone()
            is_server_theme = server_result and server_result[0] == theme_name

        # Reload if either condition is true
        if is_global_active or is_server_theme:
            theme.load_for_guild(guild_id)

class ThemeWizardSession:
    """Stores wizard session data during theme editing."""

    def __init__(self, cog, theme_name: str, user_id: int, is_new: bool = False, guild_id: int = None):
        self.cog = cog
        self.theme_name = theme_name
        self.user_id = user_id
        self.is_new = is_new
        self.guild_id = guild_id
        self.theme_data = {}
        self.original_message = None

    def load_theme_data(self):
        """Load current theme data from database."""
        with sqlite3.connect(THEME_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (self.theme_name,))
            row = cursor.fetchone()
            if row:
                self.theme_data = dict(row)
        return self.theme_data

    def get_icon_value(self, icon_name: str) -> str:
        """Get the current value for an icon."""
        return self.theme_data.get(icon_name) or DEFAULT_EMOJI

    def get_description(self) -> str:
        """Get theme description."""
        return self.theme_data.get('themeDescription', '') or ''

    def get_created_at(self) -> str:
        """Get theme creation date."""
        return self.theme_data.get('createdAt', '') or ''

class ThemeBasicsModal(discord.ui.Modal):
    """Modal for entering theme name and description when creating a new theme."""

    def __init__(self, cog, user_id: int):
        super().__init__(title="Create New Theme")
        self.cog = cog
        self.user_id = user_id

        self.theme_name_input = discord.ui.TextInput(
            label="Theme Name",
            placeholder="Enter a unique theme name",
            required=True,
            min_length=1,
            max_length=50
        )
        self.add_item(self.theme_name_input)

        self.description_input = discord.ui.TextInput(
            label="Description (optional)",
            placeholder="Describe your theme...",
            required=False,
            max_length=200,
            style=discord.TextStyle.paragraph
        )
        self.add_item(self.description_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who initiated this can use it.",
                ephemeral=True
            )
            return

        theme_name = self.theme_name_input.value.strip()
        description = self.description_input.value.strip()

        # Check if theme exists
        with sqlite3.connect(THEME_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (theme_name,))
            if cursor.fetchone()[0] > 0:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} A theme with name **{theme_name}** already exists.",
                    ephemeral=True
                )
                return

        # Create the theme
        await interaction.response.defer()

        try:
            guild_id = interaction.guild_id if interaction.guild else None
            result = await self.cog.create_theme_with_metadata(
                theme_name, interaction.user.id, description, guild_id
            )

            if result["success"]:
                # Open the editor hub - embed description shows success message
                session = ThemeWizardSession(self.cog, theme_name, interaction.user.id, is_new=True, guild_id=guild_id)
                session.load_theme_data()

                hub_view = ThemeEditorHub(self.cog, session)
                embed = hub_view.build_hub_embed()
                embed.description = (
                    f"{theme.verifiedIcon} Theme **{theme_name}** created!\n"
                    f"You can now customize the emojis, dividers, and colors used by your theme below.\n"
                    f"{theme.upperDivider}\n"
                    f"{embed.description or ''}"
                )

                await interaction.followup.send(embed=embed, view=hub_view)
            else:
                await interaction.followup.send(result["error"], ephemeral=True)
        except Exception as e:
            print(f"ThemeBasicsModal on_submit error: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error creating theme: {e}",
                ephemeral=True
            )

class ThemeEditorHub(discord.ui.View):
    """
    Main hub view for theme editing.
    Shows all icons grouped by category and provides navigation buttons.
    """

    def __init__(self, cog, session: ThemeWizardSession):
        super().__init__(timeout=7200)  # 2 hour timeout
        self.cog = cog
        self.session = session
        self._build_buttons()

    def _build_buttons(self):
        """Build the category select and action buttons."""
        self.clear_items()

        # Category emoji config for select options
        category_emojis = {
            "Status": theme.verifiedIcon,
            "Navigation": theme.forwardIcon,
            "Actions": theme.targetIcon,
            "Display": theme.chartIcon,
            "Operations": theme.giftIcon,
            "Alliance History": theme.allianceIcon,
            "Notifications": theme.bellIcon,
            "Events": theme.calendarIcon,
            "Minister": theme.ministerIcon,
            "Bot Management": theme.robotIcon,
        }

        # Row 0: Category select dropdown
        categories = list(ICON_CATEGORIES.keys())
        options = []
        for cat_name in categories:
            emoji = category_emojis.get(cat_name)
            icon_count = len(ICON_CATEGORIES.get(cat_name, []))
            options.append(discord.SelectOption(
                label=cat_name,
                value=cat_name,
                description=f"{icon_count} icons",
                emoji=emoji
            ))

        category_select = discord.ui.Select(
            placeholder="Select icon category to edit...",
            options=options,
            custom_id="category_select",
            row=0
        )
        category_select.callback = self._category_select_callback
        self.add_item(category_select)

        # Row 1: Dividers, Colors
        dividers_btn = discord.ui.Button(
            label="Dividers",
            emoji="➖",
            style=discord.ButtonStyle.secondary,
            custom_id="cat_dividers",
            row=1
        )
        dividers_btn.callback = self.open_dividers
        self.add_item(dividers_btn)

        colors_btn = discord.ui.Button(
            label="Colors",
            emoji="🎨",
            style=discord.ButtonStyle.secondary,
            custom_id="cat_colors",
            row=1
        )
        colors_btn.callback = self.open_colors
        self.add_item(colors_btn)

        reset_icons_btn = discord.ui.Button(
            label="Reset Icons",
            emoji=theme.retryIcon,
            style=discord.ButtonStyle.danger,
            custom_id="reset_icons",
            row=1
        )
        reset_icons_btn.callback = self.reset_all_icons
        self.add_item(reset_icons_btn)

        # Row 2: Preview, Save & Exit, Back
        preview_btn = discord.ui.Button(
            label="Preview",
            emoji=theme.eyesIcon,
            style=discord.ButtonStyle.primary,
            custom_id="preview",
            row=2
        )
        preview_btn.callback = self.show_preview
        self.add_item(preview_btn)

        save_btn = discord.ui.Button(
            label="Save & Exit",
            emoji=theme.saveIcon,
            style=discord.ButtonStyle.success,
            custom_id="save_exit",
            row=2
        )
        save_btn.callback = self.save_and_exit
        self.add_item(save_btn)

        back_btn = discord.ui.Button(
            label="Back",
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="back",
            row=2
        )
        back_btn.callback = self.cancel_editing
        self.add_item(back_btn)

    async def _category_select_callback(self, interaction: discord.Interaction):
        """Handle category selection from dropdown."""
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the theme creator can edit.",
                ephemeral=True
            )
            return

        category_name = interaction.data['values'][0]
        view = IconCategoryView(self.cog, self.session, category_name, self)
        embed = view.build_category_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    def _make_category_callback(self, category_name: str):
        """Create a callback for a category button."""
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.session.user_id:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Only the theme creator can edit.",
                    ephemeral=True
                )
                return

            view = IconCategoryView(self.cog, self.session, category_name, self)
            embed = view.build_category_embed()
            await interaction.response.edit_message(embed=embed, view=view)

        return callback

    def build_hub_embed(self, confirmation: str = None) -> discord.Embed:
        """Build the main hub embed showing all icons by category (emoji only)."""
        self.session.load_theme_data()

        description = self.session.get_description()
        created_at = self.session.get_created_at()

        embed = discord.Embed(
            title=f"{theme.paletteIcon} Theme Editor: {self.session.theme_name}",
            color=theme.emColor1
        )

        # Add description and created date to embed description
        desc_parts = []
        if self.session.theme_name != "default":
            desc_parts.append(f"*New themes inherit icons from the default theme.*")
        if description:
            desc_parts.append(f"📝 {description}")
        if created_at:
            desc_parts.append(f"📅 Created: {created_at}")
        if desc_parts:
            embed.description = "\n".join(desc_parts)

        # Add confirmation footer if provided
        if confirmation:
            embed.set_footer(text=f"✓ {confirmation}")

        # Add each category as a field - emoji only, no names
        for cat_name, icon_list in ICON_CATEGORIES.items():
            # Filter out any None values just in case
            icons = [self.session.get_icon_value(icon_name) for icon_name in icon_list]
            icons_str = [str(i) for i in icons if i]

            field_value = " ".join(icons_str)

            embed.add_field(
                name=f"{cat_name} ({len(icon_list)})",
                value=field_value or "No icons",
                inline=True
            )

        # Add dividers field - show preview of each divider with code block status
        div1 = build_divider(
            self.session.theme_data.get('dividerStart1') or '━',
            self.session.theme_data.get('dividerPattern1') or '━',
            self.session.theme_data.get('dividerEnd1') or '━',
            int(self.session.theme_data.get('dividerLength1') or 20)
        )
        div2 = build_divider(
            self.session.theme_data.get('dividerStart2') or '━',
            self.session.theme_data.get('dividerPattern2') or '━',
            self.session.theme_data.get('dividerEnd2') or '━',
            int(self.session.theme_data.get('dividerLength2') or 20)
        )
        div3 = build_divider(
            self.session.theme_data.get('dividerStart3') or '━',
            self.session.theme_data.get('dividerPattern3') or '━',
            self.session.theme_data.get('dividerEnd3') or '━',
            int(self.session.theme_data.get('dividerLength3') or 20)
        )

        # Apply code block wrapping if enabled (show actual rendering)
        if self.session.theme_data.get('dividerCodeBlock1'):
            div1 = f"`{div1}`"
        if self.session.theme_data.get('dividerCodeBlock2'):
            div2 = f"`{div2}`"
        if self.session.theme_data.get('dividerCodeBlock3'):
            div3 = f"`{div3}`"

        embed.add_field(
            name="Dividers",
            value=(
                f"**1:** {div1}\n"
                f"**2:** {div2}\n"
                f"**3:** {div3}"
            ),
            inline=False
        )

        # Add colors field with color square previews
        def hex_to_square(hex_color: str) -> str:
            """Convert hex color to closest colored square emoji."""
            try:
                hex_color = hex_color.lstrip('#')
                r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
            except (ValueError, IndexError):
                return "⬜"

            # Map to closest Discord square emoji based on dominant color
            if r > 180 and g < 100 and b < 100:
                return "🟥"  # Red
            elif r > 180 and g > 100 and g < 180 and b < 100:
                return "🟧"  # Orange
            elif r > 180 and g > 180 and b < 100:
                return "🟨"  # Yellow
            elif r < 100 and g > 150 and b < 100:
                return "🟩"  # Green
            elif r < 100 and g < 150 and b > 150:
                return "🟦"  # Blue
            elif r > 100 and g < 100 and b > 150:
                return "🟪"  # Purple
            elif r < 80 and g < 80 and b < 80:
                return "⬛"  # Black
            elif r > 200 and g > 200 and b > 200:
                return "⬜"  # White
            elif r > 100 and g > 50 and g < 100 and b < 80:
                return "🟫"  # Brown
            else:
                return "🟦"  # Default to blue

        c1 = self.session.theme_data.get('emColorString1') or '#0000FF'
        c2 = self.session.theme_data.get('emColorString2') or '#FF0000'
        c3 = self.session.theme_data.get('emColorString3') or '#00FF00'
        c4 = self.session.theme_data.get('emColorString4') or '#FFFF00'
        h1 = self.session.theme_data.get('headerColor1') or '#1F77B4'
        h2 = self.session.theme_data.get('headerColor2') or '#28A745'

        embed.add_field(
            name="Colors",
            value=(
                f"**Embed:** {hex_to_square(c1)}{c1} {hex_to_square(c2)}{c2} {hex_to_square(c3)}{c3} {hex_to_square(c4)}{c4}\n"
                f"**Header:** {hex_to_square(h1)}{h1} {hex_to_square(h2)}{h2}"
            ),
            inline=False
        )

        return embed

    async def open_dividers(self, interaction: discord.Interaction):
        """Open divider editor."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        view = DividerEditorView(self.cog, self.session, self)
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    async def open_colors(self, interaction: discord.Interaction):
        """Open color editor."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        view = ColorEditorView(self.cog, self.session, self)
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    async def reset_all_icons(self, interaction: discord.Interaction):
        """Reset all icons to default values."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        await interaction.response.defer()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                # Build SET clause from DEFAULT_ICON_VALUES
                set_clauses = [f"{col}=?" for col in DEFAULT_ICON_VALUES.keys()]
                values = list(DEFAULT_ICON_VALUES.values()) + [self.session.theme_name]

                cursor.execute(f"""
                    UPDATE pimpsettings SET {', '.join(set_clauses)}
                    WHERE themeName=?
                """, values)
                conn.commit()

            self.session.load_theme_data()
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            embed = self.build_hub_embed(confirmation="All icons reset to defaults")
            await interaction.edit_original_response(embed=embed, view=self)

        except Exception as e:
            await interaction.followup.send(
                f"{theme.deniedIcon} Error resetting icons: {e}",
                ephemeral=True
            )

    async def show_preview(self, interaction: discord.Interaction):
        """Show theme preview."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        # Import preview function from preview module
        from .pimp_my_bot_preview import ThemePreviewView

        view = ThemePreviewView(self.cog, self.session, self)
        embed = view.build_preview_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    async def save_and_exit(self, interaction: discord.Interaction):
        """Save and return to theme menu."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        view = ThemeMenuView(self.cog, self.session.user_id, self.session.guild_id)
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    async def cancel_editing(self, interaction: discord.Interaction):
        """Return to theme menu."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        view = ThemeMenuView(self.cog, self.session.user_id, self.session.guild_id)
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)

class IconCategoryView(discord.ui.View):
    """View for editing icons within a specific category."""

    def __init__(self, cog, session: ThemeWizardSession, category_name: str, hub_view: ThemeEditorHub):
        super().__init__(timeout=7200)
        self.cog = cog
        self.session = session
        self.category_name = category_name
        self.hub_view = hub_view
        self._build_components()

    def _build_components(self):
        """Build the select menu and back button."""
        self.clear_items()

        # Icon select dropdown
        icons = ICON_CATEGORIES.get(self.category_name, [])
        if icons:
            options = []
            for icon_name in icons[:25]:  # Discord limit
                value = self.session.get_icon_value(icon_name)
                short_name = icon_name.replace('Icon', '')

                # Format for display (custom emojis show as :name: since descriptions don't render them)
                display_value = format_emoji_for_display(value)
                option = discord.SelectOption(
                    label=short_name,
                    value=icon_name,
                    description=f"Current: {display_value[:50]}" if len(display_value) > 50 else f"Current: {display_value}"
                )
                options.append(option)

            select = discord.ui.Select(
                placeholder="Select an icon to edit...",
                options=options,
                custom_id="icon_select",
                row=0
            )
            select.callback = self.icon_selected
            self.add_item(select)

        # Back button
        back_btn = discord.ui.Button(
            label="Back to Hub",
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="back_to_hub",
            row=1
        )
        back_btn.callback = self.back_to_hub
        self.add_item(back_btn)

    def build_category_embed(self) -> discord.Embed:
        """Build embed showing icons in this category."""
        self.session.load_theme_data()

        icons = ICON_CATEGORIES.get(self.category_name, [])

        embed = discord.Embed(
            title=f"{theme.settingsIcon} Editing: {self.category_name} Icons",
            description=f"Select an icon from the dropdown to edit it.\n{theme.lowerDivider}",
            color=theme.emColor1
        )

        # Show each icon with its current value
        icon_lines = []
        for icon_name in icons:
            value = self.session.get_icon_value(icon_name)
            short_name = icon_name.replace('Icon', '')
            icon_lines.append(f"{value} **{short_name}**")

        # Split into columns if many icons
        if len(icon_lines) > 10:
            mid = len(icon_lines) // 2
            embed.add_field(name="Icons", value="\n".join(icon_lines[:mid]), inline=True)
            embed.add_field(name="\u200b", value="\n".join(icon_lines[mid:]), inline=True)
        else:
            embed.add_field(name="Icons", value="\n".join(icon_lines), inline=False)

        return embed

    async def icon_selected(self, interaction: discord.Interaction):
        """Handle icon selection."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        icon_name = interaction.data['values'][0]
        current_value = self.session.get_icon_value(icon_name)

        # Show edit choice view
        view = IconEditChoiceView(self.cog, self.session, icon_name, current_value, self)
        embed = discord.Embed(
            title=f"{theme.editListIcon} Edit: {icon_name}",
            description=(
                f"**Current value:** {current_value}\n\n"
                f"Choose how to update this icon:\n"
                f"• Click **Enter URL** to provide an image URL\n"
                f"• Or send an emoji/image in chat (within 5 minutes)"
            ),
            color=theme.emColor2
        )

        # Register emoji edit session so on_message listener can pick up emoji input
        session_key = f"{interaction.user.id}_{interaction.channel.id}"
        self.cog.emoji_edit_sessions[session_key] = {
            'emoji_name': icon_name,
            'themename': self.session.theme_name,
            'choice_view': view,
            'category_view': self,
            'timeout': 300,
            'original_message': interaction.message,
            'session': self.session
        }

        await interaction.response.edit_message(embed=embed, view=view)

    async def back_to_hub(self, interaction: discord.Interaction):
        """Return to the main hub."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        embed = self.hub_view.build_hub_embed()
        await interaction.response.edit_message(embed=embed, view=self.hub_view)

class IconEditChoiceView(discord.ui.View):
    """View for choosing how to edit an icon (URL or chat input)."""

    def __init__(self, cog, session: ThemeWizardSession, icon_name: str, current_value: str,
                 parent_view: IconCategoryView):
        super().__init__(timeout=7200)
        self.cog = cog
        self.session = session
        self.icon_name = icon_name
        self.current_value = current_value
        self.parent_view = parent_view

        # URL button
        url_btn = discord.ui.Button(
            label="Enter URL",
            emoji=theme.linkIcon,
            style=discord.ButtonStyle.primary,
            custom_id="enter_url"
        )
        url_btn.callback = self.enter_url
        self.add_item(url_btn)

        # Back button
        back_btn = discord.ui.Button(
            label="Back",
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="back"
        )
        back_btn.callback = self.go_back
        self.add_item(back_btn)

    async def enter_url(self, interaction: discord.Interaction):
        """Open modal to enter URL."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        modal = IconUrlModal(self.cog, self.session, self.icon_name, self.current_value, self.parent_view)
        await interaction.response.send_modal(modal)

    async def go_back(self, interaction: discord.Interaction):
        """Go back to category view."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        embed = self.parent_view.build_category_embed()
        await interaction.response.edit_message(embed=embed, view=self.parent_view)

class IconUrlModal(discord.ui.Modal):
    """Modal for entering an icon URL."""

    def __init__(self, cog, session: ThemeWizardSession, icon_name: str, current_value: str,
                 parent_view: IconCategoryView):
        super().__init__(title=f"Edit {icon_name}")
        self.cog = cog
        self.session = session
        self.icon_name = icon_name
        self.parent_view = parent_view

        default_url = current_value if current_value.startswith('http') else ''

        self.url_input = discord.ui.TextInput(
            label="Image URL or Emoji",
            placeholder="https://... or paste an emoji",
            default=default_url,
            required=True,
            style=discord.TextStyle.long
        )
        self.add_item(self.url_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        new_value = self.url_input.value.strip()

        # Validate column name before SQL execution
        if not is_valid_column(self.icon_name):
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid icon name.",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        # Update in database
        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"UPDATE pimpsettings SET {self.icon_name}=? WHERE themeName=?",
                    (new_value, self.session.theme_name)
                )
                conn.commit()

            # Reload theme if active
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            # Reload session data
            self.session.load_theme_data()

            # Return to category view with rebuilt dropdown to show updated values
            self.parent_view._build_components()
            embed = self.parent_view.build_category_embed()
            await interaction.edit_original_response(embed=embed, view=self.parent_view)

        except Exception as e:
            logger.error(f"Icon update error: {e}")
            print(f"Icon update error: {e}")

            error_embed = discord.Embed(
                title=f"{theme.deniedIcon} Update Failed",
                description=f"Error updating icon: {e}",
                color=0xFF0000
            )
            await interaction.edit_original_response(
                embed=error_embed,
                view=self.parent_view,
                content=None
            )

class DividerEditorView(discord.ui.View):
    """View for editing divider settings."""

    def __init__(self, cog, session: ThemeWizardSession, hub_view: ThemeEditorHub):
        super().__init__(timeout=7200)
        self.cog = cog
        self.session = session
        self.hub_view = hub_view
        self._build_buttons()

    def _build_buttons(self):
        """Build all buttons for divider editing."""
        self.clear_items()

        # Edit Upper Divider
        div1_btn = discord.ui.Button(
            label="Edit Upper",
            style=discord.ButtonStyle.primary,
            custom_id="edit_div1",
            row=0
        )
        div1_btn.callback = self.edit_divider1
        self.add_item(div1_btn)

        # Edit Middle Divider
        div3_btn = discord.ui.Button(
            label="Edit Middle",
            style=discord.ButtonStyle.primary,
            custom_id="edit_div3",
            row=0
        )
        div3_btn.callback = self.edit_divider3
        self.add_item(div3_btn)

        # Edit Lower Divider
        div2_btn = discord.ui.Button(
            label="Edit Lower",
            style=discord.ButtonStyle.primary,
            custom_id="edit_div2",
            row=0
        )
        div2_btn.callback = self.edit_divider2
        self.add_item(div2_btn)

        # Code block toggles - show current state
        cb1 = self.session.theme_data.get('dividerCodeBlock1', 0)
        cb2 = self.session.theme_data.get('dividerCodeBlock2', 0)
        cb3 = self.session.theme_data.get('dividerCodeBlock3', 0)

        toggle1_btn = discord.ui.Button(
            label=f"Upper: {'`[ code ]`' if cb1 else '[ plain ]'}",
            style=discord.ButtonStyle.success if cb1 else discord.ButtonStyle.secondary,
            custom_id="toggle_cb1",
            row=1
        )
        toggle1_btn.callback = self.toggle_codeblock1
        self.add_item(toggle1_btn)

        toggle3_btn = discord.ui.Button(
            label=f"Middle: {'`[ code ]`' if cb3 else '[ plain ]'}",
            style=discord.ButtonStyle.success if cb3 else discord.ButtonStyle.secondary,
            custom_id="toggle_cb3",
            row=1
        )
        toggle3_btn.callback = self.toggle_codeblock3
        self.add_item(toggle3_btn)

        toggle2_btn = discord.ui.Button(
            label=f"Lower: {'`[ code ]`' if cb2 else '[ plain ]'}",
            style=discord.ButtonStyle.success if cb2 else discord.ButtonStyle.secondary,
            custom_id="toggle_cb2",
            row=1
        )
        toggle2_btn.callback = self.toggle_codeblock2
        self.add_item(toggle2_btn)

        # Reset button
        reset_btn = discord.ui.Button(
            label="Reset Dividers",
            emoji=theme.retryIcon,
            style=discord.ButtonStyle.danger,
            custom_id="reset_dividers",
            row=2
        )
        reset_btn.callback = self.reset_dividers
        self.add_item(reset_btn)

        # Back button
        back_btn = discord.ui.Button(
            label="Back to Hub",
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="back",
            row=2
        )
        back_btn.callback = self.back_to_hub
        self.add_item(back_btn)

    def build_embed(self, confirmation: str = None) -> discord.Embed:
        """Build divider editor embed."""
        self.session.load_theme_data()

        # Build divider previews (showing actual rendering with/without code blocks)
        div1 = self._build_divider_preview(1)
        div2 = self._build_divider_preview(2)
        div3 = self._build_divider_preview(3)

        # Code block status indicators
        cb1 = "[ code ]" if self.session.theme_data.get('dividerCodeBlock1') else "[ plain ]"
        cb2 = "[ code ]" if self.session.theme_data.get('dividerCodeBlock2') else "[ plain ]"
        cb3 = "[ code ]" if self.session.theme_data.get('dividerCodeBlock3') else "[ plain ]"

        embed = discord.Embed(
            title=f"{theme.settingsIcon} Edit Dividers",
            description=(
                "Configure the divider patterns used throughout the bot.\n\n"
                "**Upper:** First divider in embeds with multiple sections\n"
                "**Middle:** Used for single dividers or middle sections\n"
                "**Lower:** Last divider in embeds\n\n"
                "Use the toggle buttons to wrap dividers in code blocks, "
                "which prevent Discord markdown (like `~~` strikethrough) from being interpreted."
            ),
            color=theme.emColor1
        )

        # Add confirmation footer if provided
        if confirmation:
            embed.set_footer(text=f"✓ {confirmation}")

        embed.add_field(
            name="Upper Divider",
            value=(
                f"**Start:** `{self.session.theme_data.get('dividerStart1', '━')}`\n"
                f"**Pattern:** `{self.session.theme_data.get('dividerPattern1', '━')}`\n"
                f"**End:** `{self.session.theme_data.get('dividerEnd1', '━')}`\n"
                f"**Length:** {self.session.theme_data.get('dividerLength1', 20)}\n"
                f"**Mode:** {cb1}\n"
                f"**Preview:**\n{div1}"
            ),
            inline=True
        )

        embed.add_field(
            name="Middle Divider",
            value=(
                f"**Start:** `{self.session.theme_data.get('dividerStart3', '━')}`\n"
                f"**Pattern:** `{self.session.theme_data.get('dividerPattern3', '━')}`\n"
                f"**End:** `{self.session.theme_data.get('dividerEnd3', '━')}`\n"
                f"**Length:** {self.session.theme_data.get('dividerLength3', 20)}\n"
                f"**Mode:** {cb3}\n"
                f"**Preview:**\n{div3}"
            ),
            inline=True
        )

        embed.add_field(
            name="Lower Divider",
            value=(
                f"**Start:** `{self.session.theme_data.get('dividerStart2', '━')}`\n"
                f"**Pattern:** `{self.session.theme_data.get('dividerPattern2', '━')}`\n"
                f"**End:** `{self.session.theme_data.get('dividerEnd2', '━')}`\n"
                f"**Length:** {self.session.theme_data.get('dividerLength2', 20)}\n"
                f"**Mode:** {cb2}\n"
                f"**Preview:**\n{div2}"
            ),
            inline=True
        )

        return embed

    def _build_divider_preview(self, num: int) -> str:
        """Build a preview of a divider showing actual rendering."""
        start = self.session.theme_data.get(f'dividerStart{num}', '━')
        pattern = self.session.theme_data.get(f'dividerPattern{num}', '━')
        end = self.session.theme_data.get(f'dividerEnd{num}', '━')
        length = int(self.session.theme_data.get(f'dividerLength{num}', 20) or 20)
        use_code_block = self.session.theme_data.get(f'dividerCodeBlock{num}', 0)

        # Build preview with max 20 chars for display
        preview_len = min(length, 20)
        divider = build_divider(start, pattern, end, preview_len)

        # Show exactly how it will render (with or without code block)
        if use_code_block:
            return f"`{divider}`"
        return divider

    async def edit_divider1(self, interaction: discord.Interaction):
        """Open modal to edit divider 1 (Upper)."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        modal = DividerModal(self.cog, self.session, 1, self)
        await interaction.response.send_modal(modal)

    async def edit_divider2(self, interaction: discord.Interaction):
        """Open modal to edit divider 2 (Lower)."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        modal = DividerModal(self.cog, self.session, 2, self)
        await interaction.response.send_modal(modal)

    async def edit_divider3(self, interaction: discord.Interaction):
        """Open modal to edit divider 3 (Middle)."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        modal = DividerModal(self.cog, self.session, 3, self)
        await interaction.response.send_modal(modal)

    async def _toggle_codeblock(self, interaction: discord.Interaction, divider_num: int):
        """Toggle code block setting for a divider."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        await interaction.response.defer()

        # Get current value and toggle it
        current = self.session.theme_data.get(f'dividerCodeBlock{divider_num}', 0)
        new_value = 0 if current else 1

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"UPDATE pimpsettings SET dividerCodeBlock{divider_num}=? WHERE themeName=?",
                    (new_value, self.session.theme_name)
                )
                conn.commit()

            self.session.load_theme_data()
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            # Rebuild buttons to reflect new state
            self._build_buttons()
            embed = self.build_embed()
            await interaction.edit_original_response(embed=embed, view=self)

        except Exception as e:
            logger.error(f"Error toggling code block: {e}")
            print(f"Error toggling code block: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error toggling code block: {e}",
                ephemeral=True
            )

    async def toggle_codeblock1(self, interaction: discord.Interaction):
        """Toggle code block for upper divider."""
        await self._toggle_codeblock(interaction, 1)

    async def toggle_codeblock2(self, interaction: discord.Interaction):
        """Toggle code block for lower divider."""
        await self._toggle_codeblock(interaction, 2)

    async def toggle_codeblock3(self, interaction: discord.Interaction):
        """Toggle code block for middle divider."""
        await self._toggle_codeblock(interaction, 3)

    async def reset_dividers(self, interaction: discord.Interaction):
        """Reset all dividers to default values."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        await interaction.response.defer()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE pimpsettings SET
                        dividerStart1='━', dividerPattern1='━', dividerEnd1='━', dividerLength1=20, dividerCodeBlock1=0,
                        dividerStart2='━', dividerPattern2='━', dividerEnd2='━', dividerLength2=20, dividerCodeBlock2=0,
                        dividerStart3='━', dividerPattern3='━', dividerEnd3='━', dividerLength3=20, dividerCodeBlock3=0
                    WHERE themeName=?
                """, (self.session.theme_name,))
                conn.commit()

            self.session.load_theme_data()
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            self._build_buttons()
            embed = self.build_embed(confirmation="All dividers reset to defaults")
            await interaction.edit_original_response(embed=embed, view=self)

        except Exception as e:
            logger.error(f"Error resetting dividers: {e}")
            print(f"Error resetting dividers: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error resetting dividers: {e}",
                ephemeral=True
            )

    async def back_to_hub(self, interaction: discord.Interaction):
        """Return to hub."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        embed = self.hub_view.build_hub_embed()
        await interaction.response.edit_message(embed=embed, view=self.hub_view)

class DividerModal(discord.ui.Modal):
    """Modal for editing a divider's settings."""

    def __init__(self, cog, session: ThemeWizardSession, divider_num: int, parent_view: DividerEditorView):
        divider_names = {1: "Upper Divider", 2: "Lower Divider", 3: "Middle Divider"}
        divider_name = divider_names.get(divider_num, "Divider")
        super().__init__(title=f"Edit {divider_name}")
        self.cog = cog
        self.session = session
        self.divider_num = divider_num
        self.parent_view = parent_view

        # Pre-fill with current values
        current_start = session.theme_data.get(f'dividerStart{divider_num}', '━')
        current_pattern = session.theme_data.get(f'dividerPattern{divider_num}', '━')
        current_end = session.theme_data.get(f'dividerEnd{divider_num}', '━')
        current_length = str(session.theme_data.get(f'dividerLength{divider_num}', 20))

        self.start_input = discord.ui.TextInput(
            label="Start Character(s)",
            placeholder="e.g., ─ or [",
            default=current_start,
            required=True,
            max_length=50
        )
        self.add_item(self.start_input)

        self.pattern_input = discord.ui.TextInput(
            label="Pattern (repeats to fill)",
            placeholder="Characters or emoji to repeat",
            default=current_pattern,
            required=True,
            max_length=50
        )
        self.add_item(self.pattern_input)

        self.end_input = discord.ui.TextInput(
            label="End Character(s)",
            placeholder="e.g., ─ or ]",
            default=current_end,
            required=True,
            max_length=50
        )
        self.add_item(self.end_input)

        self.length_input = discord.ui.TextInput(
            label="Total Length (characters)",
            placeholder="1-99",
            default=current_length,
            required=True,
            max_length=2
        )
        self.add_item(self.length_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        try:
            length = int(self.length_input.value)
            if not 1 <= length <= 99:
                raise ValueError("Length must be 1-99")
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Length must be a number between 1 and 99.",
                ephemeral=True
            )
            return

        # Validate divider number
        if self.divider_num not in (1, 2, 3):
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid divider number.",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"""UPDATE pimpsettings SET
                        dividerStart{self.divider_num}=?,
                        dividerPattern{self.divider_num}=?,
                        dividerEnd{self.divider_num}=?,
                        dividerLength{self.divider_num}=?
                    WHERE themeName=?""",
                    (
                        self.start_input.value,
                        self.pattern_input.value,
                        self.end_input.value,
                        length,
                        self.session.theme_name
                    )
                )
                conn.commit()

            self.session.load_theme_data()
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            embed = self.parent_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=self.parent_view)

        except Exception as e:
            logger.error(f"Error updating divider: {e}")
            print(f"Error updating divider: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error updating divider: {e}",
                ephemeral=True
            )

class ColorEditorView(discord.ui.View):
    """View for editing theme colors."""

    # Preset color swatches for quick selection
    COLOR_PRESETS = [
        ("#3498DB", "Blue"),
        ("#E74C3C", "Red"),
        ("#2ECC71", "Green"),
        ("#F1C40F", "Gold"),
        ("#9B59B6", "Purple"),
        ("#E91E63", "Pink"),
        ("#1ABC9C", "Teal"),
        ("#FF5722", "Orange"),
        ("#607D8B", "Gray"),
        ("#FFFFFF", "White"),
    ]

    def __init__(self, cog, session: ThemeWizardSession, hub_view: ThemeEditorHub):
        super().__init__(timeout=7200)
        self.cog = cog
        self.session = session
        self.hub_view = hub_view
        self.selected_fields = []  # Track which color fields are selected for swatch application (multi-select)
        self._pending_confirmation = None  # For confirmation messages in footer
        self._last_edited_field = "emColorString1"  # Track last edited color for preview
        self._build_buttons()

    def _build_buttons(self):
        """Build color edit buttons."""
        self.clear_items()

        color_fields = [
            ("emColorString1", "Embed 1"),
            ("emColorString2", "Embed 2"),
            ("emColorString3", "Embed 3"),
            ("emColorString4", "Embed 4"),
            ("headerColor1", "Header 1"),
            ("headerColor2", "Header 2"),
        ]

        # Row 0-1: Color buttons for custom hex input
        for idx, (field, label) in enumerate(color_fields):
            btn = discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.secondary,
                custom_id=f"edit_{field}",
                row=idx // 3
            )
            btn.callback = self._make_color_callback(field, label)
            self.add_item(btn)

        # Row 2: Target field selector for swatches (FIRST - select where to apply)
        field_options = [
            discord.SelectOption(
                label="Embed Color 1",
                value="emColorString1",
                default="emColorString1" in self.selected_fields
            ),
            discord.SelectOption(
                label="Embed Color 2",
                value="emColorString2",
                default="emColorString2" in self.selected_fields
            ),
            discord.SelectOption(
                label="Embed Color 3",
                value="emColorString3",
                default="emColorString3" in self.selected_fields
            ),
            discord.SelectOption(
                label="Embed Color 4",
                value="emColorString4",
                default="emColorString4" in self.selected_fields
            ),
            discord.SelectOption(
                label="Header Color 1",
                value="headerColor1",
                default="headerColor1" in self.selected_fields
            ),
            discord.SelectOption(
                label="Header Color 2",
                value="headerColor2",
                default="headerColor2" in self.selected_fields
            ),
        ]
        field_select = discord.ui.Select(
            placeholder="First select where to apply color...",
            options=field_options,
            custom_id="field_select",
            min_values=1,
            max_values=6,  # Allow selecting multiple fields
            row=2
        )
        field_select.callback = self.field_selected
        self.add_item(field_select)

        # Row 3: Color swatches dropdown (SECOND - select the color to apply)
        swatch_options = [
            discord.SelectOption(label=name, value=hex_color, description=hex_color)
            for hex_color, name in self.COLOR_PRESETS
        ]
        swatch_select = discord.ui.Select(
            placeholder="Then select the color to apply...",
            options=swatch_options,
            custom_id="swatch_select",
            row=3
        )
        swatch_select.callback = self.swatch_selected
        self.add_item(swatch_select)

        # Row 4: Reset and Back buttons
        reset_btn = discord.ui.Button(
            label="Reset Colors",
            emoji=theme.retryIcon,
            style=discord.ButtonStyle.danger,
            custom_id="reset_colors",
            row=4
        )
        reset_btn.callback = self.reset_colors
        self.add_item(reset_btn)

        back_btn = discord.ui.Button(
            label="Back to Hub",
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            custom_id="back",
            row=4
        )
        back_btn.callback = self.back_to_hub
        self.add_item(back_btn)

    async def field_selected(self, interaction: discord.Interaction):
        """Handle field selection for swatch application."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        self.selected_fields = interaction.data['values']
        # Rebuild buttons to update the default selections in dropdown
        self._build_buttons()
        # Update embed to show selected fields in footer
        embed = self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    async def swatch_selected(self, interaction: discord.Interaction):
        """Apply selected swatch color to the target field(s)."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        if not self.selected_fields:
            await interaction.response.send_message(
                f"{theme.warnIcon} First select which color field(s) to apply the swatch to using the dropdown above.",
                ephemeral=True
            )
            return

        # Validate all column names
        for field in self.selected_fields:
            if not is_valid_column(field):
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Invalid color field.",
                    ephemeral=True
                )
                return

        color = interaction.data['values'][0]
        await interaction.response.defer()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                # Update all selected fields with the same color
                for field in self.selected_fields:
                    cursor.execute(
                        f"UPDATE pimpsettings SET {field}=? WHERE themeName=?",
                        (color, self.session.theme_name)
                    )
                conn.commit()

            self.session.load_theme_data()
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            # Update last edited field for preview (use first selected if multiple)
            if self.selected_fields:
                self._last_edited_field = self.selected_fields[0]

            embed = self.build_embed()
            await interaction.edit_original_response(embed=embed, view=self)

        except Exception as e:
            await interaction.followup.send(
                f"{theme.deniedIcon} Error applying color: {e}",
                ephemeral=True
            )

    def _make_color_callback(self, field: str, label: str):
        """Create callback for color button."""
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.session.user_id:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Only the theme creator can edit.",
                    ephemeral=True
                )
                return

            modal = ColorModal(self.cog, self.session, field, label, self)
            await interaction.response.send_modal(modal)

        return callback

    def _color_preview_url(self, hex_color: str) -> str:
        """Generate a color preview URL from colorhexa.com."""
        color_code = hex_color.lstrip('#')
        return f"https://www.colorhexa.com/{color_code}.png"

    def _color_link(self, hex_color: str) -> str:
        """Generate a clickable color link with preview."""
        color_code = hex_color.lstrip('#')
        url = f"https://www.colorhexa.com/{color_code}"
        return f"[{hex_color}]({url})"

    def build_embed(self, confirmation: str = None) -> discord.Embed:
        """Build color editor embed with visual previews."""
        self.session.load_theme_data()

        # Get colors
        c1 = self.session.theme_data.get('emColorString1', '#3498DB')
        c2 = self.session.theme_data.get('emColorString2', '#E74C3C')
        c3 = self.session.theme_data.get('emColorString3', '#2ECC71')
        c4 = self.session.theme_data.get('emColorString4', '#F1C40F')
        h1 = self.session.theme_data.get('headerColor1', '#1F77B4')
        h2 = self.session.theme_data.get('headerColor2', '#28A745')

        # Use the first embed color as the embed's actual color for preview
        try:
            embed_color = int(c1.lstrip('#'), 16)
        except ValueError:
            embed_color = theme.emColor1

        # Store confirmation for footer
        self._pending_confirmation = confirmation

        embed = discord.Embed(
            title=f"{theme.paletteIcon} Edit Colors",
            description=(
                "Configure the colors in embeds and headers.\n"
                "- Use the buttons for custom hex input\n"
                "- Use the drop-downs for quick swatches\n"
                "- Preview always shows the last color edited\n"
            ),
            color=embed_color
        )

        # Embed colors with clickable links
        embed.add_field(
            name="Embed Colors",
            value=(
                f"**1:** {self._color_link(c1)}\n"
                f"**2:** {self._color_link(c2)}\n"
                f"**3:** {self._color_link(c3)}\n"
                f"**4:** {self._color_link(c4)}"
            ),
            inline=True
        )

        # Header colors with clickable links
        embed.add_field(
            name="Header Colors",
            value=(
                f"**1:** {self._color_link(h1)}\n"
                f"**2:** {self._color_link(h2)}"
            ),
            inline=True
        )

        # Set thumbnail to show the last edited color as visual preview
        color_map = {
            "emColorString1": c1, "emColorString2": c2,
            "emColorString3": c3, "emColorString4": c4,
            "headerColor1": h1, "headerColor2": h2,
        }
        preview_color = color_map.get(self._last_edited_field, c1)
        embed.set_thumbnail(url=self._color_preview_url(preview_color))

        # Add footer - confirmation takes priority, then swatch targets
        if self._pending_confirmation:
            embed.set_footer(text=f"✓ {self._pending_confirmation}")
            self._pending_confirmation = None  # Clear after use
        elif self.selected_fields:
            field_names = {
                "emColorString1": "Embed 1",
                "emColorString2": "Embed 2",
                "emColorString3": "Embed 3",
                "emColorString4": "Embed 4",
                "headerColor1": "Header 1",
                "headerColor2": "Header 2",
            }
            selected_names = [field_names.get(f, f) for f in self.selected_fields]
            embed.set_footer(text=f"Swatch targets: {', '.join(selected_names)}")

        return embed

    async def reset_colors(self, interaction: discord.Interaction):
        """Reset all colors to default values."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        await interaction.response.defer()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE pimpsettings SET
                        emColorString1='#3498DB', emColorString2='#E74C3C',
                        emColorString3='#2ECC71', emColorString4='#F1C40F',
                        headerColor1='#1F77B4', headerColor2='#28A745'
                    WHERE themeName=?
                """, (self.session.theme_name,))
                conn.commit()

            self.session.load_theme_data()
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            embed = self.build_embed(confirmation="All colors reset to defaults")
            await interaction.edit_original_response(embed=embed, view=self)

        except Exception as e:
            await interaction.followup.send(
                f"{theme.deniedIcon} Error resetting colors: {e}",
                ephemeral=True
            )

    async def back_to_hub(self, interaction: discord.Interaction):
        """Return to hub."""
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        embed = self.hub_view.build_hub_embed()
        await interaction.response.edit_message(embed=embed, view=self.hub_view)

class ColorModal(discord.ui.Modal):
    """Modal for editing a color value."""

    def __init__(self, cog, session: ThemeWizardSession, field: str, label: str,
                 parent_view: ColorEditorView):
        super().__init__(title=f"Edit {label}")
        self.cog = cog
        self.session = session
        self.field = field
        self.parent_view = parent_view

        current = session.theme_data.get(field, '#FFFFFF')

        self.color_input = discord.ui.TextInput(
            label="Hex Color (e.g., #FF0080)",
            placeholder="#RRGGBB",
            default=current,
            required=True,
            max_length=7,
            min_length=7
        )
        self.add_item(self.color_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.session.user_id):
            return

        color = self.color_input.value.strip()

        # Validate hex color
        if not color.startswith('#') or len(color) != 7:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid color format. Use #RRGGBB (e.g., #FF0080)",
                ephemeral=True
            )
            return

        try:
            int(color[1:], 16)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid hex color. Use #RRGGBB format.",
                ephemeral=True
            )
            return

        # Validate column name
        if not is_valid_column(self.field):
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid color field.",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"UPDATE pimpsettings SET {self.field}=? WHERE themeName=?",
                    (color, self.session.theme_name)
                )
                conn.commit()

            self.session.load_theme_data()
            reload_theme_if_active(self.session.theme_name, self.session.guild_id)

            # Update last edited field for preview
            self.parent_view._last_edited_field = self.field

            embed = self.parent_view.build_embed()
            await interaction.edit_original_response(embed=embed, view=self.parent_view)

        except Exception as e:
            await interaction.followup.send(
                f"{theme.deniedIcon} Error updating color: {e}",
                ephemeral=True
            )