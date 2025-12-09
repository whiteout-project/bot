import discord
from discord.ext import commands
import sqlite3
from cogs import prettification_is_my_purpose as pimp

class PageModal(discord.ui.Modal):
    def __init__(self, view, original_user_id):
        super().__init__(title="Go to Page")
        self.view = view
        self.original_user_id = original_user_id
        self.page_input = discord.ui.TextInput(
            label="Enter page number",
            placeholder=f"1 to {len(self.view.pages)}",
            required=True,
            min_length=1,
            max_length=3
        )
        self.add_item(self.page_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        try:
            page_num = int(self.page_input.value) - 1  # 0-based
            if 0 <= page_num < len(self.view.pages):
                self.view.current_page = page_num
                self.view.update_buttons()
                await interaction.response.edit_message(embeds=self.view.pages[self.view.current_page], view=self.view)
            else:
                await interaction.response.send_message("Invalid page number.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)

class CreateThemeModal(discord.ui.Modal):
    def __init__(self, cog, original_user_id):
        super().__init__(title="Create New Theme")
        self.cog = cog
        self.original_user_id = original_user_id
        
        self.theme_name_input = discord.ui.TextInput(
            label="Theme Name",
            placeholder="Enter a unique theme name",
            required=True,
            min_length=1,
            max_length=50
        )
        self.add_item(self.theme_name_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        try:
            await interaction.response.defer()
            
            new_theme_name = self.theme_name_input.value.strip()
            
            # Check if theme already exists
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (new_theme_name,))
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    await interaction.followup.send(f"{pimp.deniedIcon} A theme with the name **{new_theme_name}** already exists!", ephemeral=True)
                    return
                
                # Get default theme structure
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                data_columns = [col[1] for col in columns_info if col[1] not in ['id', 'themeName', 'themeCreator', 'is_active']]
                
                default_emojis = [
                    '⚔️', '👤', '🔥', '🌎', '⚔️', '👤', '🔥', '🌏',
                    '📜', '🆔', '🕰️', '🏠', '1️⃣', '2️⃣', '3️⃣', '4️⃣',
                    '5️⃣', '🔟', '🆕', '📍', '💾', '🎁', '🛍️', '⚠️', 
                    '🤖', '⚔️', '💗', '🟰', '🛡️', '🎯', '🔄', '👥', 
                    '📈', '🔢', '🔊', '🆘', '🏭', '📢', '🏛️', '🔬', 
                    '⚔️', '🤪', '🐻', '📅', '📝', '⚙️', '⚙️', '⏳', 
                    '🔇', '⚪', '⏰', '⏰', '🔍', '🐉', '🏞️', '⚒️', 
                    '🏰', '✅', '🗑️', '➕', '🔄', '✅', '❓', '↔️', 
                    '✖️', '➗', '❌', '➖', '➡️', '⬅️', '🔄', '🟰', 
                    'ℹ️', '⚠️', '➕'
                ]
                divider_values = ['━', '━', '━', 16, '━', '━', '━', 16]
                color_values = ['#0000FF', '#FF0000', '#00FF00', '#FFFF00', '#1F77B4', '#28A745']
             
                url_values = [
                    "https://cdn-icons-png.freepik.com/512/12388/12388244.png", 
                    "https://cdn-icons-png.freepik.com/512/9932/9932935.png", "https://cdn-icons-png.freepik.com/512/9933/9933057.png",
                    "https://cdn-icons-png.freepik.com/512/9933/9933179.png", "https://cdn-icons-png.freepik.com/512/9933/9933300.png",
                    "https://cdn-icons-png.freepik.com/512/9933/9933423.png", "https://cdn-icons-png.freepik.com/512/9933/9933543.png",
                    "https://cdn-icons-png.freepik.com/512/9933/9933662.png", "https://cdn-icons-png.freepik.com/512/9933/9933786.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933890.png", "https://cdn-icons-png.freepik.com/512/9932/9932941.png", 
                    "https://cdn-icons-png.freepik.com/512/9932/9932961.png", "https://cdn-icons-png.freepik.com/512/9932/9932971.png", 
                    "https://cdn-icons-png.freepik.com/512/9932/9932980.png", "https://cdn-icons-png.freepik.com/512/9932/9932990.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933000.png", "https://cdn-icons-png.freepik.com/512/9933/9933014.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933025.png", "https://cdn-icons-png.freepik.com/512/9933/9933036.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933047.png", "https://cdn-icons-png.freepik.com/512/9933/9933068.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933080.png", "https://cdn-icons-png.freepik.com/512/9933/9933089.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933103.png", "https://cdn-icons-png.freepik.com/512/9933/9933114.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933126.png", "https://cdn-icons-png.freepik.com/512/9933/9933137.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933147.png", "https://cdn-icons-png.freepik.com/512/9933/9933157.png", 
                    "https://cdn-icons-png.freepik.com/512/9933/9933168.png", "https://cdn-icons-png.freepik.com/512/9933/9933190.png"
                ]
                
                # Insert new theme
                placeholders = ', '.join(['?' for _ in range(len(data_columns))])
                columns_str = ', '.join(data_columns)
                query = f"INSERT INTO pimpsettings (themeName, themeCreator, {columns_str}, is_active) VALUES (?, ?, {placeholders}, 0)"
                cursor.execute(query, [new_theme_name, interaction.user.id] + default_emojis + divider_values + color_values + url_values)
                pimpSettings_db.commit()
            
            # Load the theme preview (fetch_theme_info will handle the interaction response)
            await self.cog.fetch_theme_info(interaction, new_theme_name, is_new_theme=True)
            
        except Exception as e:
            print(f"Create theme error: {e}")
            await interaction.followup.send(f"{pimp.deniedIcon} Error creating theme: {e}", ephemeral=True)

class CreateThemeView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=300)
        self.cog = cog
        self.original_user_id = original_user_id
        
        create_button = discord.ui.Button(
            label="Create New Theme",
            style=discord.ButtonStyle.primary,
            emoji=pimp.addIcon
        )
        create_button.callback = self.create_callback
        self.add_item(create_button)
    
    async def create_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        modal = CreateThemeModal(self.cog, self.original_user_id)
        await interaction.response.send_modal(modal)

class DeleteThemeView(discord.ui.View):
    def __init__(self, cog, original_user_id, themename=None):
        super().__init__(timeout=300)
        self.cog = cog
        self.original_user_id = original_user_id
        self.selected_theme = themename  # Pre-select if provided from command

        with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
            cursor = pimpSettings_db.cursor()
            cursor.execute("SELECT themeName FROM pimpsettings WHERE themeName != 'default'")
            themeNames = cursor.fetchall()
            themeNames = [themeName[0] for themeName in themeNames]
       
        select_options = [discord.SelectOption(label=themeName, value=themeName, default=(themeName == themename)) for themeName in themeNames]        
        delete_select = discord.ui.Select(
            placeholder="Select a theme to delete",
            options=select_options,
            custom_id="delete_select"
        )
        delete_select.callback = self.select_callback
        self.add_item(delete_select)
        
        # Add Delete button
        delete_button = discord.ui.Button(
            label="Delete Theme",
            style=discord.ButtonStyle.secondary,
            emoji=pimp.deleteIcon
        )
        delete_button.callback = self.delete_callback
        self.add_item(delete_button)

    async def select_callback(self, interaction: discord.Interaction):
        """Handle theme selection from dropdown."""
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        # Get selected theme from dropdown
        selected_theme = interaction.data.get('values', [None])[0] if interaction.data else None
        
        if selected_theme:
            self.selected_theme = selected_theme
            await interaction.response.defer()

    async def delete_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        if not self.selected_theme:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Please select a theme from the dropdown first.",
                ephemeral=True
            )
            return
        
        try:
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                
                # Check if theme is active
                cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (self.selected_theme,))
                result = cursor.fetchone()
                
                if result and result[0] == 1:
                    await interaction.response.send_message(
                        f"{pimp.deniedIcon} Cannot delete the active theme. Please activate a different theme first.",
                        ephemeral=True
                    )
                    return
                
                # Delete the theme
                cursor.execute("DELETE FROM pimpsettings WHERE themeName=?", (self.selected_theme,))
                pimpSettings_db.commit()
            
            deleted_theme_name = self.selected_theme
            
            # Refresh the select menu with updated theme list
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("SELECT themeName FROM pimpsettings WHERE themeName != 'default'")
                themeNames = cursor.fetchall()
                themeNames = [themeName[0] for themeName in themeNames]
            
            # Clear existing items and rebuild
            self.clear_items()
            
            if themeNames:
                # Rebuild select menu with remaining themes
                select_options = [discord.SelectOption(label=themeName, value=themeName) for themeName in themeNames]        
                delete_select = discord.ui.Select(
                    placeholder="Select a theme to delete",
                    options=select_options,
                    custom_id="delete_select"
                )
                delete_select.callback = self.select_callback
                self.add_item(delete_select)
                
                # Re-add Delete button
                delete_button = discord.ui.Button(
                    label="Delete Theme",
                    style=discord.ButtonStyle.secondary,
                    emoji=pimp.deleteIcon
                )
                delete_button.callback = self.delete_callback
                self.add_item(delete_button)
                
                # Reset selected theme
                self.selected_theme = None
                
                await interaction.response.send_message(
                    f"{pimp.verifiedIcon} Theme **{deleted_theme_name}** has been deleted successfully!",
                    ephemeral=True
                )
                
                # Update the original message with refreshed view
                await interaction.message.edit(view=self)
            else:
                # No more themes to delete
                await interaction.response.send_message(
                    f"{pimp.verifiedIcon} Theme **{deleted_theme_name}** has been deleted successfully!\n\nNo more custom themes available to delete.",
                    ephemeral=True
                )
                
                # Disable the view since there are no themes left
                await interaction.message.edit(view=None)
            
        except Exception as e:
            print(f"Delete theme error: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Error deleting theme: {e}",
                ephemeral=True
            )

class MultiFieldEditModal(discord.ui.Modal):
    """Modal for editing divider patterns or other multi-value fields."""
    def __init__(self, view, field_name, themename, original_user_id):
        super().__init__(title=f"Edit {field_name}")
        self.view = view
        self.field_name = field_name
        self.themename = themename
        self.cog = view.cog
        self.original_user_id = original_user_id
        self.inputs = []
        
        # Get current values
        icons = self.cog._get_theme_data(themename)
        current_value = str(icons.get(field_name, ""))
        
        # Create appropriate inputs based on field type
        if field_name.startswith('emColor') or field_name.startswith('attendanceReport'):
            # Color field - single input
            input_field = discord.ui.TextInput(
                label="Color Value (hex color code)",
                placeholder="#FFFFFF",
                default=current_value,
                required=True,
                max_length=7
            )
            self.inputs.append(input_field)
            self.add_item(input_field)
        elif ',' in current_value:
            # Multi-value field - split by comma and create multiple inputs
            values = [v.strip() for v in current_value.split(',')]
            
            # Discord modals support max 5 inputs
            for i, val in enumerate(values[:5]):
                import re
                # Check if it's an emoji or text
                emoji_pattern = r'<a?:(\w+):(\d+)>'
                emoji_match = re.search(emoji_pattern, val)
                
                if emoji_match:
                    label = f"Emoji {i+1} (URL or emoji)"
                    placeholder = "Enter emoji or image URL"
                elif val.startswith('http'):
                    label = f"Link {i+1}"
                    placeholder = "Enter URL"
                else:
                    label = f"Value {i+1}"
                    placeholder = "Enter text or emoji"
                
                input_field = discord.ui.TextInput(
                    label=label,
                    placeholder=placeholder,
                    default=val,
                    required=False,
                    max_length=200
                )
                self.inputs.append(input_field)
                self.add_item(input_field)
        else:
            # Single value field
            input_field = discord.ui.TextInput(
                label="Value",
                placeholder="Enter value",
                default=current_value,
                required=True,
                style=discord.TextStyle.long
            )
            self.inputs.append(input_field)
            self.add_item(input_field)
    
    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        try:
            await interaction.response.defer()
            
            # Collect values from all inputs
            if len(self.inputs) == 1:
                # Single input field
                new_value = self.inputs[0].value.strip()
            else:
                # Multiple inputs - reconstruct comma-separated value
                values = []
                for input_field in self.inputs:
                    val = input_field.value.strip()
                    if val:  # Only include non-empty values
                        values.append(val)
                new_value = ','.join(values)
            
            # Get old value to check if it changed
            icons = self.cog._get_theme_data(self.themename)
            old_value = str(icons.get(self.field_name, ""))
            
            # Normalize both for comparison (remove extra spaces)
            normalized_old = ','.join([v.strip() for v in old_value.split(',')])
            normalized_new = ','.join([v.strip() for v in new_value.split(',')])
            
            # Don't update if value hasn't changed
            if normalized_new == normalized_old:
                await interaction.followup.send(f"{pimp.deniedIcon} Value hasn't changed. No update needed.", ephemeral=True)
                return
            
            # Update database directly for non-emoji fields
            import sqlite3
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                
                # Find column by name
                column_name = None
                for col in columns_info:
                    if col[1] == self.field_name:
                        column_name = col[1]
                        break
                
                if column_name:
                    query = f"UPDATE pimpsettings SET {column_name}=? WHERE themeName=?"
                    cursor.execute(query, (new_value, self.themename))
                    pimpSettings_db.commit()
            
            # Reload if active theme
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (self.themename,))
                result = cursor.fetchone()
                is_active = result[0] if result else 0
                
                if is_active == 1:
                    import importlib
                    from cogs import prettification_is_my_purpose
                    importlib.reload(prettification_is_my_purpose)
            
            # Rebuild embeds and update view using helper method
            new_icons = self.cog._get_theme_data(self.themename)
            if self.themename == "default":
                new_lines = self.cog._build_default_theme_lines()
            else:
                new_lines = [f"{name} = {value} = \\{value}" for name, value in new_icons.items()]
            
            new_embeds = self.cog._build_embeds_from_lines(new_lines, self.themename)
            
            self.view.pages = [new_embeds[i:i+10] for i in range(0, len(new_embeds), 10)]
            self.view.all_emoji_names = [line.split(" = ")[0] for line in new_lines]
            self.view.update_buttons()
            
            await interaction.edit_original_response(embeds=self.view.pages[self.view.current_page], view=self.view)
            await interaction.followup.send(f"{pimp.verifiedIcon} Field **{self.field_name}** updated successfully!", ephemeral=True)
        
        except Exception as e:
            print(f"Multi-field edit error: {e}")
            await interaction.followup.send(f"{pimp.deniedIcon} Error: {e}", ephemeral=True)

class EditEmojiChoiceView(discord.ui.View):
    def __init__(self, pagination_view, emoji_name, current_url, themename, original_user_id):
        super().__init__(timeout=60)
        self.pagination_view = pagination_view
        self.emoji_name = emoji_name
        self.current_url = current_url
        self.themename = themename
        self.cog = pagination_view.cog
        self.original_user_id = original_user_id
        self.waiting_for_message = False
        
        url_button = discord.ui.Button(
            label="Enter URL",
            style=discord.ButtonStyle.primary,
            emoji=pimp.exportIcon
        )
        url_button.callback = self.url_callback
        self.add_item(url_button)
    
    async def url_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        modal = EditEmojiModal(self.pagination_view, self.emoji_name, self.current_url, self.themename, self.original_user_id)
        await interaction.response.send_modal(modal)
    
    async def on_timeout(self):
        # Disable buttons when timeout
        for item in self.children:
            item.disabled = True

class EditEmojiModal(discord.ui.Modal):
    def __init__(self, view, emoji_name, current_value, themename, original_user_id):
        super().__init__(title=f"Edit {emoji_name}")
        self.view = view
        self.emoji_name = emoji_name
        self.themename = themename
        self.cog = view.cog
        self.original_user_id = original_user_id
        
        self.url_input = discord.ui.TextInput(
            label="Image URL",
            placeholder="Enter direct image URL (png, jpg, gif)",
            default=current_value if current_value.startswith("http") else "",
            required=True,
            style=discord.TextStyle.long
        )
        self.add_item(self.url_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        try:
            await interaction.response.defer()
            
            new_url = self.url_input.value.strip()
            
            # Verify it's a valid URL
            if not new_url.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
                await interaction.followup.send(f"{pimp.deniedIcon} URL must be a direct image link (.png, .jpg, .gif, .webp)", ephemeral=True)
                return
            
            # Use the helper method to process the update
            success = await self.cog._process_emoji_update(
                interaction.channel,
                self.emoji_name,
                new_url,
                self.themename,
                self.view,
                None
            )
            
            if success:
                await interaction.edit_original_response(embeds=self.view.pages[self.view.current_page], view=self.view)
                await interaction.followup.send(f"{pimp.verifiedIcon} Emoji **{self.emoji_name}** updated successfully!", ephemeral=True)
            else:
                await interaction.followup.send(f"{pimp.deniedIcon} Failed to update emoji. Check if the URL is accessible.", ephemeral=True)
        
        except Exception as e:
            print(f"Edit emoji modal error: {e}")
            await interaction.followup.send(f"{pimp.deniedIcon} Error: {e}", ephemeral=True)

class PaginationView(discord.ui.View):
    def __init__(self, pages, current_page, all_emoji_names, themename, cog, original_user_id):
        super().__init__(timeout=300)
        self.pages = pages
        self.current_page = current_page
        self.all_emoji_names = all_emoji_names
        self.themename = themename
        self.cog = cog
        self.original_user_id = original_user_id
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()
        prev_button = discord.ui.Button(label="", style=discord.ButtonStyle.secondary, custom_id="prev", emoji=f"{pimp.importIcon}")
        prev_button.callback = self.prev_callback
        if self.current_page == 0:
            prev_button.disabled = True
        self.add_item(prev_button)

        page_button = discord.ui.Button(label=f"{self.current_page + 1} of {len(self.pages)}", style=discord.ButtonStyle.secondary, custom_id="pages", emoji=f"{pimp.listIcon}")
        page_button.callback = self.page_callback
        self.add_item(page_button)

        next_button = discord.ui.Button(label="", style=discord.ButtonStyle.secondary, custom_id="next", emoji=f"{pimp.exportIcon}")
        next_button.callback = self.next_callback
        if self.current_page == len(self.pages) - 1:
            next_button.disabled = True
        self.add_item(next_button)
        
        # Only add select menu for non-default themes
        if self.themename != "default":
            # Add select menu for emojis on current page
            start_idx = self.current_page * 10
            end_idx = start_idx + 10
            page_emojis = self.all_emoji_names[start_idx:end_idx]
            
            if page_emojis:
                # Get the actual emoji values from the theme data (fresh data every time)
                icons = self.cog._get_theme_data(self.themename)
                
                select_options = []
                for emoji_name in page_emojis:
                    emoji_value = str(icons.get(emoji_name, ""))
                    
                    # Try to extract a valid emoji for display
                    import re
                    import unicodedata
                    
                    display_emoji = None
                    
                    # Match both static <:name:id> and animated <a:name:id> emojis
                    emoji_pattern = r'<a?:([\w]+):(\d+)>'
                    emoji_match = re.search(emoji_pattern, emoji_value)
                    
                    if emoji_match:
                        # Use just the first matched custom emoji
                        try:
                            display_emoji = emoji_match.group(0)
                        except:
                            pass
                    
                    # If no custom emoji or invalid, try unicode emoji
                    if not display_emoji and emoji_value:
                        # Check for unicode emoji characters
                        for char in emoji_value[:5]:  # Only check first few chars
                            try:
                                cat = unicodedata.category(char)
                                # Check if it's an emoji/symbol and not a special character like comma, dot, etc
                                if (cat in ['So', 'Sm'] or ord(char) > 0x1F000) and char not in [',', '.', '•', '━', '-', '=', '#']:
                                    display_emoji = char
                                    break
                            except:
                                continue
                    
                    # Fallback: don't add emoji to option if we couldn't find a valid one
                    if display_emoji:
                        select_options.append(discord.SelectOption(
                            label=emoji_name,
                            value=emoji_name,
                            emoji=display_emoji
                        ))
                    else:
                        # No emoji parameter - just label
                        select_options.append(discord.SelectOption(
                            label=emoji_name,
                            value=emoji_name
                        ))
                
                emoji_select = discord.ui.Select(
                    placeholder="Select an emoji to edit",
                    options=select_options,
                    custom_id="emoji_select"
                )
                emoji_select.callback = self.emoji_select_callback
                self.add_item(emoji_select)

    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embeds=self.pages[self.current_page], view=self)

    async def page_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        modal = PageModal(self, self.original_user_id)
        await interaction.response.send_modal(modal)

    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embeds=self.pages[self.current_page], view=self)
    
    async def emoji_select_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Only the user who initiated this command can use this.",
                ephemeral=True
            )
            return
        
        selected_emoji = interaction.data['values'][0]
        
        # Check if this is a special field (divider, color, etc) or a regular emoji field
        if selected_emoji.startswith(('divider', 'emColor', 'attendanceReport')):
            # Use multi-field modal for dividers and colors
            modal = MultiFieldEditModal(self, selected_emoji, self.themename, self.original_user_id)
            await interaction.response.send_modal(modal)
            return
        
        # Regular emoji field - use existing flow
        # Get current value of the emoji
        icons = self.cog._get_theme_data(self.themename)
        current_value = str(icons.get(selected_emoji, ""))
        
        # Extract the URL from the current value if it's a custom emoji
        import re
        # Match both static <:name:id> and animated <a:name:id> emojis
        emoji_pattern = r'<a?:[\w]+:(\d+)>'
        emoji_match = re.search(emoji_pattern, current_value)
        
        if emoji_match:
            # It's a custom emoji, get the URL
            emoji_id = emoji_match.group(1)
            # Check if it's animated
            is_animated = current_value.startswith("<a:")
            emoji_ext = "gif" if is_animated else "png"
            current_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{emoji_ext}"
        else:
            # It's a URL or unicode emoji
            current_url = current_value if current_value.startswith("http") else ""
        
        # Show edit options
        edit_view = EditEmojiChoiceView(self, selected_emoji, current_url, self.themename, self.original_user_id)
        
        # Set up emoji edit session for message listening
        session_key = f"{interaction.user.id}_{interaction.channel.id}"
        self.cog.emoji_edit_sessions[session_key] = {
            'emoji_name': selected_emoji,
            'themename': self.themename,
            'pagination_view': self,
            'timeout': 60,
            'original_message': interaction.message
        }
        
        embed = discord.Embed(
            title=f"{pimp.settingsIcon} Edit {selected_emoji}",
            description=(
                f"**Current Value:** {current_value}\n\n"
                f"**Choose how to update this emoji:**\n"
                f"{pimp.exportIcon} Click the button below to enter a URL\n"
                f"{pimp.importIcon} Or send in chat within 60 seconds:\n"
                f"  • An emoji (custom or unicode)\n"
                f"  • An image attachment\n"
                f"  • A direct image URL"
            ),
            color=pimp.emColor2
        )
        
        await interaction.response.send_message(embed=embed, view=edit_view, ephemeral=True)

class PIMP(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self.emoji_edit_sessions = {}  # Store active emoji edit sessions

    async def _process_emoji_update(self, channel, emoji_name, new_url, themename, pagination_view, context_message=None, original_interaction_message=None):
        """Helper method to process emoji updates from either modal or message."""
        global pimp
        import aiohttp
        import base64
        import re
        import io
        try:
            from PIL import Image
            PIL_AVAILABLE = True
        except ImportError:
            PIL_AVAILABLE = False
        
        try:
            # Check if this is a unicode emoji (not a URL)
            is_unicode_emoji = new_url and not new_url.startswith('http')
            
            if is_unicode_emoji:
                # For unicode emojis, skip the upload process and just update the database
                new_emoji_str = new_url
            else:
                # Get app credentials for URL-based emojis
                app_id = self.bot.application_id
                bot_token = self.bot.http.token
                
                async with aiohttp.ClientSession() as session:
                    # Download image
                    async with session.get(new_url) as resp:
                        if resp.status != 200:
                            error_msg = f"Failed to download image. Status: {resp.status}"
                            print(error_msg)
                            if context_message:
                                await context_message.add_reaction(f"{pimp.deniedIcon}")
                                await context_message.reply(f"{pimp.deniedIcon} {error_msg}")
                            return False
                        image_data = await resp.read()
                        print(f"Downloaded image: {len(image_data)} bytes")
                        
                        # Check if image is too large (Discord limit is 2MB for app emojis)
                        max_size = 2048 * 1024  # 2MB in bytes
                        if len(image_data) > max_size:
                            if not PIL_AVAILABLE:
                                error_msg = "Image is too large (max 2MB) and PIL is not available for resizing."
                                print(error_msg)
                                if context_message:
                                    await context_message.add_reaction(f"{pimp.deniedIcon}")
                                    await context_message.reply(f"{pimp.deniedIcon} {error_msg}")
                                return False
                            
                            print(f"Image too large ({len(image_data)} bytes), resizing...")
                            try:
                                # Open image with PIL
                                img = Image.open(io.BytesIO(image_data))
                                
                                # Calculate new size (keep aspect ratio, max 256x256 for emojis)
                                max_dimension = 256
                                ratio = min(max_dimension / img.width, max_dimension / img.height)
                                new_size = (int(img.width * ratio), int(img.height * ratio))
                                
                                # Resize
                                img = img.resize(new_size, Image.Resampling.LANCZOS)
                                
                                # Convert to bytes
                                img_byte_arr = io.BytesIO()
                                img_format = 'PNG' if not new_url.lower().endswith('.gif') else 'GIF'
                                img.save(img_byte_arr, format=img_format, optimize=True)
                                image_data = img_byte_arr.getvalue()
                                
                                print(f"Resized image: {len(image_data)} bytes")
                                
                                if context_message:
                                    await context_message.reply(f"{pimp.processingIcon} Image was too large, automatically resized to {new_size[0]}x{new_size[1]}")
                            except Exception as resize_error:
                                print(f"Failed to resize image: {resize_error}")
                                if context_message:
                                    await context_message.add_reaction(f"{pimp.deniedIcon}")
                                    await context_message.reply(f"{pimp.deniedIcon} Image is too large (max 2MB) and could not be resized.")
                                return False
                    
                    # Get old emoji data
                    icons = self._get_theme_data(themename)
                    old_value = icons.get(emoji_name, "")
                    
                    emoji_pattern = r'<a?:(\w+):(\d+)>'
                    old_emoji_match = re.search(emoji_pattern, str(old_value))
                    
                    # Generate new emoji name with theme prefix to avoid duplicates across themes
                    base_name = emoji_name.replace(" ", "")
                    # Sanitize theme name (remove spaces, special chars)
                    safe_theme = themename.replace(" ", "")
                    new_emoji_name = f"{safe_theme}_{base_name}"
                    
                    # Delete old emoji if exists
                    if old_emoji_match:
                        old_emoji_id = old_emoji_match.group(2)
                        delete_url = f"https://discord.com/api/v10/applications/{app_id}/emojis/{old_emoji_id}"
                        headers = {"Authorization": f"Bot {bot_token}"}
                        
                        try:
                            async with session.delete(delete_url, headers=headers) as resp:
                                if resp.status == 204:
                                    print(f"Deleted old emoji: {old_emoji_id}")
                        except Exception as e:
                            print(f"Failed to delete old emoji: {e}")
                    
                    # Determine mime type
                    if new_url.lower().endswith('.gif'):
                        mime_type = "image/gif"
                    elif new_url.lower().endswith(('.jpg', '.jpeg')):
                        mime_type = "image/jpeg"
                    elif new_url.lower().endswith('.webp'):
                        mime_type = "image/webp"
                    else:
                        mime_type = "image/png"
                    
                    # Upload new emoji
                    image_base64 = base64.b64encode(image_data).decode('utf-8')
                    data_uri = f"data:{mime_type};base64,{image_base64}"
                    
                    upload_url = f"https://discord.com/api/v10/applications/{app_id}/emojis"
                    headers = {
                        "Authorization": f"Bot {bot_token}",
                        "Content-Type": "application/json"
                    }
                    payload = {"name": new_emoji_name, "image": data_uri}
                    
                    new_emoji_str = ""
                    async with session.post(upload_url, headers=headers, json=payload) as resp:
                        if resp.status == 201:
                            result = await resp.json()
                            new_emoji_id = result['id']
                            is_animated = result.get('animated', False)
                            emoji_prefix = "a" if is_animated else ""
                            new_emoji_str = f"<{emoji_prefix}:{new_emoji_name}:{new_emoji_id}>"
                        else:
                            error_text = await resp.text()
                            error_msg = f"Failed to upload emoji. Status: {resp.status}, Error: {error_text}"
                            print(error_msg)
                            if context_message:
                                await context_message.add_reaction(f"{pimp.deniedIcon}")
                                await context_message.reply(f"{pimp.deniedIcon} {error_msg}")
                            return False
            
            # Update database
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                
                icon_mapping = {
                    'allianceOldIcon': 3, 'avatarOldIcon': 4, 'stoveOldIcon': 5, 
                    'stateOldIcon': 6, 'allianceIcon': 7, 'avatarIcon': 8, 'stoveIcon': 9, 'stateIcon': 10, 
                    'listIcon': 11, 'fidIcon': 12, 'timeIcon': 13, 'homeIcon': 14, 'num1Icon': 15, 
                    'num2Icon': 16, 'num3Icon': 17, 'num4Icon': 18, 'num5Icon': 19, 'num10Icon': 20, 
                    'newIcon': 21, 'pinIcon': 22, 'saveIcon': 23, 'giftIcon': 24, 'giftsIcon': 25, 
                    'alertIcon': 26, 'robotIcon': 27, 'crossIcon': 28, 'heartIcon': 29, 'total2Icon': 30, 
                    'shieldIcon': 31, 'targetIcon': 32, 'redeemIcon': 33, 'membersIcon': 34, 'averageIcon': 35, 
                    'hashtagIcon': 36, 'messageIcon': 37, 'supportIcon': 38, 'foundryIcon': 39, 'announceIcon': 40, 
                    'ministerIcon': 41, 'researchIcon': 42, 'trainingIcon': 43, 'crazyJoeIcon': 44, 'bearTrapIcon': 45, 
                    'calendarIcon': 46, 'editListIcon': 47, 'settingsIcon': 48, 'settings2Icon': 49, 'hourglassIcon': 50, 
                    'messageNoIcon': 51, 'blankListIcon': 52, 'alarmGiftIcon': 53, 'alarmClockIcon': 54, 'magnifyingIcon': 55, 
                    'frostdragonIcon': 56, 'canyonClashIcon': 57, 'constructionIcon': 58, 'castleBattleIcon': 59, 'checkGiftCodeIcon': 60, 
                    'deleteGiftCodeIcon': 61, 'addGiftCodeIcon': 62, 'processingIcon': 63, 'verifiedIcon': 64, 'questionIcon': 65, 
                    'transferIcon': 66, 'multiplyIcon': 67, 'divideIcon': 68, 'deniedIcon': 69, 'deleteIcon': 70, 
                    'exportIcon': 71, 'importIcon': 72, 'retryIcon': 73, 'totalIcon': 74, 'infoIcon': 75, 
                    'warnIcon': 76, 'addIcon': 77
                }
                
                if emoji_name in icon_mapping:
                    col_index = icon_mapping[emoji_name]
                    column_name = columns_info[col_index][1]
                    query = f"UPDATE pimpsettings SET {column_name}=? WHERE themeName=?"
                    cursor.execute(query, (new_emoji_str, themename))
                    pimpSettings_db.commit()
            
            # Check if active theme and reload
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (themename,))
                result = cursor.fetchone()
                is_active = result[0] if result else 0
                
                if is_active == 1:
                    import importlib
                    from cogs import prettification_is_my_purpose
                    importlib.reload(prettification_is_my_purpose)
                    pimp = prettification_is_my_purpose
            
            # Update the pagination view using helper method
            new_icons = self._get_theme_data(themename)
            if themename == "default":
                new_lines = self._build_default_theme_lines()
            else:
                new_lines = [f"{name} = {value} = \\{value}" for name, value in new_icons.items()]
            
            # Rebuild embeds using helper method
            new_embeds = self._build_embeds_from_lines(new_lines, themename)
            
            pagination_view.pages = [new_embeds[i:i+10] for i in range(0, len(new_embeds), 10)]
            pagination_view.all_emoji_names = [line.split(" = ")[0] for line in new_lines]
            pagination_view.update_buttons()
            
            # Update the original pagination message if available
            if original_interaction_message:
                try:
                    await original_interaction_message.edit(embeds=pagination_view.pages[pagination_view.current_page], view=pagination_view)
                except Exception as e:
                    print(f"Failed to update original message: {e}")
            
            if context_message:
                await context_message.add_reaction(f"{pimp.verifiedIcon}")
                await context_message.reply(f"{pimp.verifiedIcon} Emoji **{emoji_name}** updated successfully!")
            
            return True
            
        except Exception as e:
            import traceback
            print(f"Edit emoji error: {e}")
            traceback.print_exc()
            if context_message:
                await context_message.add_reaction(f"{pimp.deniedIcon}")
                await context_message.reply(f"{pimp.deniedIcon} Error updating emoji: {str(e)}")
            return False

    @discord.app_commands.command(name='pimp', description='Shows Theme Icons (Must Select Theme Name).')
    @discord.app_commands.describe(
        themename="Select a theme to display",
        export="Export theme as JSON file",
        import_file="Import theme from JSON file (will upload emojis to bot)",
        activate="Activate this theme (sets it as the active theme)",
        create="Create a new theme from default template",
        delete="Delete an existing theme (cannot delete active theme)"
    )
    @discord.app_commands.choices(
        export=[discord.app_commands.Choice(name="True", value="true")],
        activate=[discord.app_commands.Choice(name="True", value="true")],
        create=[discord.app_commands.Choice(name="True", value="true")],
        delete=[discord.app_commands.Choice(name="True", value="true")]
    )
    async def pimp(
        self, 
        interaction: discord.Interaction, 
        themename: str, 
        export: str = None,
        import_file: discord.Attachment = None,
        activate: str = None,
        create: str = None,
        delete: str = None
    ):
        # Check if user is initial admin
        try:
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (interaction.user.id,))
                result = cursor.fetchone()
                
                if not result or result[0] != 1:
                    await interaction.response.send_message(
                        f"{pimp.deniedIcon} Only global administrators can use this command.",
                        ephemeral=True
                    )
                    return
        except Exception as e:
            print(f"Admin check error: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} Error checking permissions.",
                ephemeral=True
            )
            return
        
        if create and create.lower() == "true":
            await self.show_create_theme(interaction)
        elif delete and delete.lower() == "true":
            await self.show_delete_theme(interaction, themename)
        elif import_file:
            await self.import_theme(interaction, import_file)
        elif activate and activate.lower() == "true":
            await self.activate_theme(interaction, themename)
        elif export and export.lower() == "true":
            await self.export_theme(interaction, themename)
        elif themename:
            await self.fetch_theme_info(interaction, themename)
        else:
            await interaction.response.send_message(f"{pimp.deniedIcon} Please select a theme or use the create option.", ephemeral=True)

    @pimp.autocomplete('themename')
    async def autocomplete_themename(self, interaction: discord.Interaction, current: str):
        try:
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("SELECT themename FROM pimpsettings")
                themes = [row[0] for row in cursor.fetchall()]

            choices = [
                discord.app_commands.Choice(name=theme, value=theme) 
                for theme in themes
            ]

            if current:
                filtered_choices = [choice for choice in choices if current.lower() in choice.name.lower()][:25]
            else:
                filtered_choices = choices[:25]

            return filtered_choices
        
        except Exception as e:
            print(f"Autocomplete could not be loaded: {e}")
            return []


    def _build_embeds_from_lines(self, lines, themename):
        """Helper method to build embeds from theme lines. Reduces code duplication."""
        import re
        
        embeds = []
        for line in lines:
            parts = line.split(" = ")
            name = parts[0]
            
            if len(parts) >= 2:
                # For default theme, use the CDN URL directly
                if themename == "default" and len(parts) == 2:
                    value = parts[1]
                    if value.startswith("http"):
                        # Handle URL values
                        description = f"[{name} - Link]({value})"
                        embed = discord.Embed(title=name, description=description, color=pimp.emColor3)
                        embed.set_thumbnail(url=value)
                    elif value.startswith('#'):
                        # Handle color values (like #FF5733)
                        color_url = f"https://www.colorhexa.com/{value.strip('#')}.png"
                        description = f"[{value}]({color_url})"
                        embed = discord.Embed(title=name, description=description, color=pimp.emColor3)
                        embed.set_thumbnail(url=color_url)
                    else:
                        # Handle divider and color values that aren't URLs
                        embed = discord.Embed(title=name, description=f"`{value}`", color=pimp.emColor3)
                # For custom themes with emoji format
                elif len(parts) == 3:
                    emoji_display = parts[2].strip('\\')
                    
                    # Check if it contains custom Discord emojis and extract links
                    emoji_pattern = r'<a?:(\w+):(\d+)>'
                    emoji_matches = re.findall(emoji_pattern, emoji_display)
                    
                    if emoji_matches:
                        # Build description with actual emojis and links
                        description = f"{emoji_display}\n\n**Emoji Link(s):**\n"
                        for emoji_name, emoji_id in emoji_matches:
                            # Check if it's animated by looking at the original string
                            is_animated = f"<a:{emoji_name}:{emoji_id}>" in emoji_display
                            emoji_ext = "gif" if is_animated else "png"
                            emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{emoji_ext}"
                            description += f"• [{emoji_name}]({emoji_url})\n"
                        
                        embed = discord.Embed(title=name, description=description, color=pimp.emColor3)
                        
                        # Set thumbnail to first emoji
                        if len(emoji_matches) >= 1:
                            is_animated = f"<a:{emoji_matches[0][0]}:{emoji_matches[0][1]}>" in emoji_display
                            emoji_ext = "gif" if is_animated else "png"
                            first_emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_matches[0][1]}.{emoji_ext}"
                            embed.set_thumbnail(url=first_emoji_url)
                        
                        # Set image to second emoji if available
                        if len(emoji_matches) >= 2:
                            is_animated = f"<a:{emoji_matches[1][0]}:{emoji_matches[1][1]}>" in emoji_display
                            emoji_ext = "gif" if is_animated else "png"
                            second_emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_matches[1][1]}.{emoji_ext}"
                            embed.set_image(url=second_emoji_url)

                    elif emoji_display.startswith("http"):
                        description = f"[{name} - Link]({emoji_display})"
                        embed = discord.Embed(title=name, description=description, color=pimp.emColor3)
                        embed.set_thumbnail(url=emoji_display)
                    
                    # Handle color values (like #FF5733)
                    elif emoji_display.startswith('#'):
                        color_url = f"https://www.colorhexa.com/{emoji_display.strip('#')}.png"
                        description = f"[{emoji_display}]({color_url})"
                        embed = discord.Embed(title=name, description=description, color=pimp.emColor3)
                        embed.set_thumbnail(url=color_url)

                    else:
                        # Handle non-emoji divider values (like ━━━)
                        embed = discord.Embed(title=name, description=f"{emoji_display}", color=pimp.emColor3)
               
                # For other cases (dividers, etc)
                else:
                    if len(parts) > 1 and parts[1].startswith('http'):
                        description = f"[{name} - Link]({parts[1]})"
                        embed = discord.Embed(title=name, description=description, color=pimp.emColor3)
                        embed.set_thumbnail(url=parts[1])
                    elif len(parts) > 1 and parts[1].startswith('#'):
                        color_url = f"https://www.colorhexa.com/{parts[1].strip('#')}.png"
                        description = f"[{parts[1]}]({color_url})"
                        embed = discord.Embed(title=name, description=description, color=pimp.emColor3)
                        embed.set_thumbnail(url=color_url)
                    elif len(parts) > 1: 
                        value = parts[1] if len(parts) > 1 else ""
                        embed = discord.Embed(title=name, description=f"{value}", color=pimp.emColor3)
                
                embeds.append(embed)
        
        return embeds
    
    def _get_theme_data(self, themename: str):
        """Fetch and parse theme data from database."""
        elseEmoji = "👻"
        
        with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
            cursor = pimpSettings_db.cursor()
            cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (themename,))
            theme = cursor.fetchone()
            
            if not theme:
                return None
            
            # Define icon names mapping to database indices
            icon_mapping = {
                'allianceOldIcon': 3, 'avatarOldIcon': 4, 'stoveOldIcon': 5, 
                'stateOldIcon': 6, 'allianceIcon': 7, 'avatarIcon': 8, 'stoveIcon': 9, 'stateIcon': 10, 
                'listIcon': 11, 'fidIcon': 12, 'timeIcon': 13, 'homeIcon': 14, 'num1Icon': 15, 
                'num2Icon': 16, 'num3Icon': 17, 'num4Icon': 18, 'num5Icon': 19, 'num10Icon': 20, 
                'newIcon': 21, 'pinIcon': 22, 'saveIcon': 23, 'giftIcon': 24, 'giftsIcon': 25, 
                'alertIcon': 26, 'robotIcon': 27, 'crossIcon': 28, 'heartIcon': 29, 'total2Icon': 30, 
                'shieldIcon': 31, 'targetIcon': 32, 'redeemIcon': 33, 'membersIcon': 34, 'averageIcon': 35, 
                'hashtagIcon': 36, 'messageIcon': 37, 'supportIcon': 38, 'foundryIcon': 39, 'announceIcon': 40, 
                'ministerIcon': 41, 'researchIcon': 42, 'trainingIcon': 43, 'crazyJoeIcon': 44, 'bearTrapIcon': 45, 
                'calendarIcon': 46, 'editListIcon': 47, 'settingsIcon': 48, 'settings2Icon': 49, 'hourglassIcon': 50, 
                'messageNoIcon': 51, 'blankListIcon': 52, 'alarmGiftIcon': 53, 'alarmClockIcon': 54, 'magnifyingIcon': 55, 
                'frostdragonIcon': 56, 'canyonClashIcon': 57, 'constructionIcon': 58, 'castleBattleIcon': 59, 'checkGiftCodeIcon': 60, 
                'deleteGiftCodeIcon': 61, 'addGiftCodeIcon': 62, 'processingIcon': 63, 'verifiedIcon': 64, 'questionIcon': 65, 
                'transferIcon': 66, 'multiplyIcon': 67, 'divideIcon': 68, 'deniedIcon': 69, 'deleteIcon': 70, 
                'exportIcon': 71, 'importIcon': 72, 'retryIcon': 73, 'totalIcon': 74, 'infoIcon': 75, 
                'warnIcon': 76, 'addIcon': 77
            }
            
            # Extract icons using mapping
            icons = {name: theme[idx] if theme else elseEmoji for name, idx in icon_mapping.items()}
            
            # Extract divider and color data
            icons.update({
                'dividerEmojiStart1': theme[78] if theme else "━",
                'dividerEmojiPattern1': theme[79] if theme else "━",
                'dividerEmojiEnd1': theme[80] if theme else "━",
                'dividerLength1': theme[81] if theme else 9,
                'dividerEmojiStart2': theme[82] if theme else "━",
                'dividerEmojiPattern2': theme[83] if theme else "━",
                'dividerEmojiEnd2': theme[84] if theme else "━",
                'dividerLength2': theme[85] if theme else 9,
                'emColorString1': theme[86] if theme else "#FFFFFF",
                'emColorString2': theme[87] if theme else "#FFFFFF",
                'emColorString3': theme[88] if theme else "#FFFFFF",
                'emColorString4': theme[89] if theme else "#FFFFFF",
                'headerColor1': theme[90] if theme else "#FFFFFF",
                'headerColor2': theme[91] if theme else "#FFFFFF",
                'furnaceLevel0Icon': theme[92] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel1Icon': theme[93] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel2Icon': theme[94] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel3Icon': theme[95] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel4Icon': theme[96] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel5Icon': theme[97] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel6Icon': theme[98] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel7Icon': theme[99] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel8Icon': theme[100] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel9Icon': theme[101] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel10Icon': theme[102] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel11Icon': theme[103] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel12Icon': theme[104] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel13Icon': theme[105] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel14Icon': theme[106] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel15Icon': theme[107] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel16Icon': theme[108] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel17Icon': theme[109] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel18Icon': theme[110] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel19Icon': theme[111] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel20Icon': theme[112] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel21Icon': theme[113] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel22Icon': theme[114] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel23Icon': theme[115] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel24Icon': theme[116] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel25Icon': theme[117] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel26Icon': theme[118] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel27Icon': theme[119] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel28Icon': theme[120] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel29Icon': theme[121] if theme else pimp.furnaceLevelImageDefaultURL,
                'furnaceLevel30Icon': theme[122] if theme else pimp.furnaceLevelImageDefaultURL
            })
            
            return icons

    def _build_default_theme_lines(self):
        """Build default theme lines with Twemoji CDN URLs."""
        
        default_icons = {
            'allianceOldIcon': '2694', 'avatarOldIcon': '1f464', 'stoveOldIcon': '1f525', 'stateOldIcon': '1f30e',
            'allianceIcon': '2694', 'avatarIcon': '1f464', 'stoveIcon': '1f525', 'stateIcon': '1f30f',
            'listIcon': '1f4dc', 'fidIcon': '1f194', 'timeIcon': '1f570', 'homeIcon': '1f3e0',
            'num1Icon': '31-20e3', 'num2Icon': '32-20e3', 'num3Icon': '33-20e3', 'num4Icon': '34-20e3', 'num5Icon': '35-20e3', 
            'num10Icon': '1f51f', 'newIcon': '1f195',
            'pinIcon': '1f4cd', 'saveIcon': '1f4be', 'giftIcon': '1f381', 'giftsIcon': '1f6cd', 
            'alertIcon': '26a0', 'robotIcon': '1f916', 'crossIcon': '2694', 'heartIcon': '1f497', 
            'total2Icon': '1f7f0', 'shieldIcon': '1f6e1', 'targetIcon': '1f3af', 'redeemIcon': '1f503', 
            'membersIcon': '1f465', 'averageIcon': '1f4c8', 'hashtagIcon': '1f522', 'messageIcon': '1f50a', 
            'supportIcon': '1f198', 'foundryIcon': '1f3ed', 'announceIcon': '1f4e2', 'ministerIcon': '1f3db', 
            'researchIcon': '1f52c', 'trainingIcon': '2694', 'crazyJoeIcon': '1f92a ', 'bearTrapIcon': '1f43b', 
            'calendarIcon': '1f4c5', 'editListIcon': '1f4dd', 'settingsIcon': '2699', 'settings2Icon': '2699', 
            'hourglassIcon': '23f3', 'messageNoIcon': '1f507', 'blankListIcon': '26AA', 'alarmGiftIcon': '2795', 
            'alarmClockIcon': '23f0', 'magnifyingIcon': '1f50d', 'frostdragonIcon': '1f409', 'canyonClashIcon': '1f3de', 
            'constructionIcon': '2692', 'castleBattleIcon': '1f3f0', 'checkGiftCodeIcon': '2705', 'deleteGiftCodeIcon': '1f5d1', 
            'addGiftCodeIcon': '2795', 'processingIcon': '1f504', 'verifiedIcon': '2705', 'questionIcon': '2753', 
            'transferIcon': '2194', 'multiplyIcon': '2716', 'divideIcon': '2797', 'deniedIcon': '274c', 
            'deleteIcon': '2796', 'exportIcon': '27a1', 'importIcon': '2b05', 'retryIcon': '1f501', 
            'totalIcon': '1f7f0', 'infoIcon': '2139', 'warnIcon': '26a0', 'addIcon': '2795'
        }
        
        cdn_base = "https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72/"
        lines = []
        for name, code in default_icons.items():
            cdn_url = f"{cdn_base}{code}.png"
            lines.append(f"{name} = {cdn_url}")
        
        # Add non-icon fields
        lines.extend([
            "dividerEmojiStart1 = ━", "dividerEmojiPattern1 = ━", "dividerEmojiEnd1 = ━", 
            "dividerLength1 = 9", "dividerEmojiStart2 = ━", "dividerEmojiPattern2 = ━", 
            "dividerEmojiEnd2 = ━", "dividerLength2 = 9",
            "emColorString1 = #0000FF", "emColorString2 = #FF0000", "emColorString3 = #00FF00", "emColorString4 = #FFFF00",
            "headerColor1 = #FFFFFF", "headerColor2 = #FFFFFF",
            "furnaceLevel0Icon = https://cdn-icons-png.freepik.com/512/12388/12388244.png", "furnaceLevel1Icon = https://cdn-icons-png.freepik.com/512/9932/9932935.png",
            "furnaceLevel2Icon = https://cdn-icons-png.freepik.com/512/9933/9933057.png", "furnaceLevel3Icon = https://cdn-icons-png.freepik.com/512/9933/9933179.png",
            "furnaceLevel4Icon = https://cdn-icons-png.freepik.com/512/9933/9933300.png", "furnaceLevel5Icon = https://cdn-icons-png.freepik.com/512/9933/9933423.png",
            "furnaceLevel6Icon = https://cdn-icons-png.freepik.com/512/9933/9933543.png", "furnaceLevel7Icon = https://cdn-icons-png.freepik.com/512/9933/9933662.png",
            "furnaceLevel8Icon = https://cdn-icons-png.freepik.com/512/9933/9933786.png", "furnaceLevel9Icon = https://cdn-icons-png.freepik.com/512/9933/9933890.png",
            "furnaceLevel10Icon = https://cdn-icons-png.freepik.com/512/9932/9932941.png", "furnaceLevel11Icon = https://cdn-icons-png.freepik.com/512/9932/9932961.png",
            "furnaceLevel12Icon = https://cdn-icons-png.freepik.com/512/9932/9932971.png", "furnaceLevel13Icon = https://cdn-icons-png.freepik.com/512/9932/9932980.png",
            "furnaceLevel14Icon = https://cdn-icons-png.freepik.com/512/9932/9932990.png", "furnaceLevel15Icon = https://cdn-icons-png.freepik.com/512/9933/9933000.png",
            "furnaceLevel16Icon = https://cdn-icons-png.freepik.com/512/9933/9933014.png", "furnaceLevel17Icon = https://cdn-icons-png.freepik.com/512/9933/9933025.png",
            "furnaceLevel18Icon = https://cdn-icons-png.freepik.com/512/9933/9933036.png", "furnaceLevel19Icon = https://cdn-icons-png.freepik.com/512/9933/9933047.png",
            "furnaceLevel20Icon = https://cdn-icons-png.freepik.com/512/9933/9933068.png", "furnaceLevel21Icon = https://cdn-icons-png.freepik.com/512/9933/9933080.png",
            "furnaceLevel22Icon = https://cdn-icons-png.freepik.com/512/9933/9933089.png", "furnaceLevel23Icon = https://cdn-icons-png.freepik.com/512/9933/9933103.png",
            "furnaceLevel24Icon = https://cdn-icons-png.freepik.com/512/9933/9933114.png", "furnaceLevel25Icon = https://cdn-icons-png.freepik.com/512/9933/9933126.png",
            "furnaceLevel26Icon = https://cdn-icons-png.freepik.com/512/9933/9933137.png", "furnaceLevel27Icon = https://cdn-icons-png.freepik.com/512/9933/9933147.png",
            "furnaceLevel28Icon = https://cdn-icons-png.freepik.com/512/9933/9933157.png", "furnaceLevel29Icon = https://cdn-icons-png.freepik.com/512/9933/9933168.png",
            "furnaceLevel30Icon = https://cdn-icons-png.freepik.com/512/9933/9933190.png"
        ])
        return lines

    async def show_create_theme(self, interaction: discord.Interaction):
        """Show the create theme interface."""
        try:
            view = CreateThemeView(self, interaction.user.id)
            
            embed = discord.Embed(
                title=f"{pimp.addIcon} Create New Theme",
                description=(
                    "Click the button below to create a new theme.\n\n"
                    f"{pimp.infoIcon} You'll be able to:\n"
                    "- Name your theme\n"
                    "- Start with default emojis\n"
                    "- Edit each emoji individually"
                ),
                color=pimp.emColor1
            )
            
            await interaction.response.send_message(embed=embed, view=view)
            
        except Exception as e:
            print(f"Show create theme error: {e}")
            await interaction.response.send_message(f"{pimp.deniedIcon} Error: {e}", ephemeral=True)
    
    async def show_delete_theme(self, interaction: discord.Interaction, themename: str | None = None):
        """Show the delete theme interface."""
        try:
            view = DeleteThemeView(self, interaction.user.id, themename)

            description = (
                f"{pimp.divider1}\n"
                f"### {pimp.warnIcon} Warning: {pimp.warnIcon}\n"
                "You cannot delete the active theme.\n"
                "Make sure to activate a different theme before deleting.\n\n"
                f"{pimp.divider2}\n\n"
            )
            
            if themename:
                description += f"Theme **{themename}** is pre-selected.\nClick the **Delete Theme** button to confirm deletion."
            else:
                description += "Select a theme from the dropdown and click the **Delete Theme** button to confirm."
            
            description += f"\n\n{pimp.divider1}\n"
            
            embed = discord.Embed(
                title=f"{pimp.deleteIcon} Delete Theme",
                description=description,
                color=pimp.emColor2
            )
            await interaction.response.send_message(embed=embed, view=view)
            
        except Exception as e:
            print(f"Show delete theme error: {e}")
            await interaction.response.send_message(f"{pimp.deniedIcon} Error: {e}", ephemeral=True)
    

    @commands.Cog.listener()
    async def on_message(self, message):
        """Listen for emoji messages when user is editing."""
        if message.author.bot:
            return
        
        # Check if user has an active edit session
        session_key = f"{message.author.id}_{message.channel.id}"
        if session_key not in self.emoji_edit_sessions:
            return
        
        session = self.emoji_edit_sessions[session_key]
        
        # Extract emoji from message (both custom Discord emojis, unicode, and image attachments)
        import re
        import unicodedata
        
        emoji_pattern = r'<a?:(\w+):(\d+)>'
        emoji_match = re.search(emoji_pattern, message.content)
        
        emoji_url = None
        is_valid = False
        
        # Check for image attachments first
        if message.attachments:
            for attachment in message.attachments:
                # Check if it's an image
                if attachment.content_type and attachment.content_type.startswith('image/'):
                    emoji_url = attachment.url
                    is_valid = True
                    break
                # Fallback: check file extension
                elif any(attachment.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']):
                    emoji_url = attachment.url
                    is_valid = True
                    break
        
        if not is_valid and emoji_match:
            # Custom Discord emoji
            emoji_name = emoji_match.group(1)
            emoji_id = emoji_match.group(2)
            # Check if animated by looking at the actual match in content
            is_animated = message.content[message.content.find('<'):message.content.find('>')].startswith("<a:")
            emoji_ext = "gif" if is_animated else "png"
            emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{emoji_ext}"
            is_valid = True
        elif not is_valid:
            # Try to find unicode emoji or image URL
            content = message.content.strip()
            if content.startswith('http') and any(ext in content.lower() for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']):
                # Direct image URL
                emoji_url = content
                is_valid = True
            elif content and len(content) <= 10:  # Unicode emoji should be short
                # Check if it contains actual emoji characters
                for char in content:
                    try:
                        if unicodedata.category(char) in ['So', 'Sm'] or ord(char) > 0x1F000:
                            # It's a symbol or emoji
                            emoji_url = content
                            is_valid = True
                            break
                    except:
                        pass
        
        # If no valid emoji/URL/attachment found, ignore the message
        if not is_valid:
            return
        
        # Remove the session
        del self.emoji_edit_sessions[session_key]
        
        # Process the emoji update
        try:
            await message.add_reaction(f"{pimp.hourglassIcon}")
            
            # Simulate the modal submission by calling the update logic directly
            result = await self._process_emoji_update(
                message.channel,
                session['emoji_name'],
                emoji_url if emoji_url else message.content.strip(),
                session['themename'],
                session['pagination_view'],
                message,
                session.get('original_message')
            )
            
            if not result:
                await message.reply(f"{pimp.deniedIcon} Failed to update emoji. Check the console for details.")
            
        except Exception as e:
            print(f"Error processing emoji from message: {e}")
            import traceback
            traceback.print_exc()
            await message.add_reaction(f"{pimp.deniedIcon}")
            await message.reply(f"{pimp.deniedIcon} Error: {str(e)}")

    async def activate_theme(self, interaction: discord.Interaction, themename: str):
        """Activate a theme and reload the prettification module."""
        global pimp
        try:
            await interaction.response.defer(thinking=True)
            
            # Check if theme exists
            icons = self._get_theme_data(themename)
            if not icons:
                await interaction.followup.send(f"{pimp.deniedIcon} Theme '{themename}' not found.")
                return
            
            # Update database: set all themes to inactive, then activate the selected one
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                
                # Set all themes to inactive
                cursor.execute("UPDATE pimpsettings SET is_active=0")
                
                # Activate the selected theme
                cursor.execute("UPDATE pimpsettings SET is_active=1 WHERE themeName=?", (themename,))
                
                pimpSettings_db.commit()
            
            # Reload the prettification module to apply changes immediately
            import importlib
            from cogs import prettification_is_my_purpose
            importlib.reload(prettification_is_my_purpose)
            
            # Update the global pimp reference
            pimp = prettification_is_my_purpose
            
            success_embed = discord.Embed(
                title=f"{pimp.verifiedIcon} Theme Activated",
                description=(
                    f"**Theme:** {themename}\n"
                    f"**Status:** Active and loaded\n\n"
                    f"The bot is now using this theme across all commands."
                ),
                color=pimp.emColor3
            )
            
            await interaction.followup.send(embed=success_embed)
            
        except Exception as e:
            print(f"Activate error: {e}")
            await interaction.followup.send(f"{pimp.deniedIcon} An error occurred while activating theme: {e}")

    async def export_theme(self, interaction: discord.Interaction, themename: str):
        """Export theme data as JSON file."""
        try:
            await interaction.response.defer(thinking=True)
            
            icons = self._get_theme_data(themename)
            
            if not icons:
                await interaction.followup.send(f"Theme '{themename}' not found.")
                return
            
            import re
            import json
            import io
            
            export_data = {
                "themeName": themename,
                "icons": {},
                "dividers": {},
                "colors": {},
                "urls": {}
            }
            
            emoji_pattern = r'<:(\w+):(\d+)>'
            
            # Process all icons
            for key, value in icons.items():
                if key.startswith('divider'):
                    # Handle divider fields
                    if key == 'dividerLength1' or key == 'dividerLength2':
                        # Store lengths as plain integers
                        export_data["dividers"][key] = int(value)
                    else:
                        emoji_matches = re.findall(emoji_pattern, str(value))
                        if emoji_matches:
                            export_data["dividers"][key] = {
                                "raw": str(value),
                                "emojis": [{"name": name, "id": eid} for name, eid in emoji_matches]
                            }
                        else:
                            export_data["dividers"][key] = {"raw": str(value)}
                        
                elif key.startswith('emColor') or key.startswith('headerColor'):
                    # Handle color fields
                    export_data["colors"][key] = str(value)
                elif key.startswith('furnaceLevel'):
                    # Handle furnace level icon URLs
                    export_data["urls"][key] = str(value)
                else:
                    # Handle icon fields
                    emoji_matches = re.findall(emoji_pattern, str(value))
                    if emoji_matches:
                        export_data["icons"][key] = {
                            "emoji": str(value),
                            "name": emoji_matches[0][0],
                            "id": emoji_matches[0][1],
                            "url": f"https://cdn.discordapp.com/emojis/{emoji_matches[0][1]}.png"
                        }
                    elif str(value).startswith("http"):
                        export_data["icons"][key] = {
                            "url": str(value)
                        }
                    else:
                        export_data["icons"][key] = {
                            "value": str(value)
                        }
            
            # Create JSON file
            json_data = json.dumps(export_data, indent=2, ensure_ascii=False)
            json_file = io.BytesIO(json_data.encode('utf-8'))
            json_file.seek(0)
            
            file = discord.File(json_file, filename=f"{themename}_export.json")
            
            embed = discord.Embed(
                title=f"{pimp.exportIcon} Theme Exported",
                description=f"**Theme:** {themename}\n**Icons:** {len(export_data['icons'])}\n**Dividers:** {len(export_data['dividers'])}\n**Colors:** {len(export_data['colors'])}",
                color=pimp.emColor1
            )
            
            await interaction.followup.send(embed=embed, file=file)
            
        except Exception as e:
            print(f"Export error: {e}")
            await interaction.followup.send(f"An error occurred while exporting theme: {e}")

    async def import_theme(self, interaction: discord.Interaction, import_file: discord.Attachment):
        """Import theme from JSON file and upload emojis to bot."""
        try:
            await interaction.response.defer(thinking=True)
            
            if not import_file.filename.endswith('.json'):
                await interaction.followup.send(f"{pimp.deniedIcon} Please provide a valid JSON file.")
                return
            
            import json
            import re
            import aiohttp
            
            # Download and parse JSON
            json_data = await import_file.read()
            theme_data = json.loads(json_data.decode('utf-8'))
            
            themename = theme_data.get("themeName", "imported_theme")
            icons_data = theme_data.get("icons", {})
            dividers_data = theme_data.get("dividers", {})
            colors_data = theme_data.get("colors", {})
            urls_data = theme_data.get("urls", {})
            
            status_embed = discord.Embed(
                title=f"{pimp.processingIcon} Importing Theme",
                description=f"**Theme:** {themename}\n**Status:** Uploading emojis to bot...",
                color=pimp.emColor2
            )
            status_msg = await interaction.followup.send(embed=status_embed)
            
            # Upload emojis to bot (application emojis, not guild emojis)
            uploaded_emojis = {}
            emoji_pattern = r'<:(\w+):(\d+)>'
            
            import base64
            
            # Collect all emojis that need to be uploaded
            emojis_to_upload = {}
            
            for key, data in icons_data.items():
                if isinstance(data, dict) and "id" in data:
                    emoji_id = data["id"]
                    emoji_name = data["name"]
                    emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.png"
                    emojis_to_upload[emoji_name] = emoji_url
            
            for key, data in dividers_data.items():
                if isinstance(data, dict) and "emojis" in data:
                    for emoji_info in data["emojis"]:
                        emoji_id = emoji_info["id"]
                        emoji_name = emoji_info["name"]
                        emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.png"
                        emojis_to_upload[emoji_name] = emoji_url
            
            # Upload emojis to bot's application emojis using Discord API directly
            app_id = self.bot.application_id
            bot_token = self.bot.http.token
            
            async with aiohttp.ClientSession() as session:
                # First, fetch all existing bot emojis
                api_url = f"https://discord.com/api/v10/applications/{app_id}/emojis"
                headers = {
                    "Authorization": f"Bot {bot_token}",
                    "Content-Type": "application/json"
                }
                
                existing_emojis = {}
                try:
                    async with session.get(api_url, headers=headers) as resp:
                        if resp.status == 200:
                            emoji_list = await resp.json()
                            # Build dict of existing emoji names to IDs
                            for emoji_data in emoji_list.get('items', []):
                                existing_emojis[emoji_data['name']] = emoji_data['id']
                except Exception as e:
                    print(f"Failed to fetch existing emojis: {e}")
                
                # Upload only emojis that don't already exist
                for emoji_name, emoji_url in emojis_to_upload.items():
                    try:
                        # Check if emoji already exists
                        if emoji_name in existing_emojis:
                            emoji_id = existing_emojis[emoji_name]
                            uploaded_emojis[emoji_name] = f"<:{emoji_name}:{emoji_id}>"
                            continue
                        
                        # Download emoji image
                        async with session.get(emoji_url) as resp:
                            if resp.status == 200:
                                image_data = await resp.read()
                                
                                # Determine mime type from URL
                                if emoji_url.endswith('.gif'):
                                    mime_type = 'image/gif'
                                elif emoji_url.endswith('.jpg') or emoji_url.endswith('.jpeg'):
                                    mime_type = 'image/jpeg'
                                else:
                                    mime_type = 'image/png'
                                
                                # Convert to base64 data URI
                                image_base64 = base64.b64encode(image_data).decode('utf-8')
                                data_uri = f"data:{mime_type};base64,{image_base64}"
                                
                                # Upload via Discord REST API (same as Node.js script)
                                payload = {
                                    "name": emoji_name,
                                    "image": data_uri
                                }
                                
                                async with session.post(api_url, headers=headers, json=payload) as upload_resp:
                                    if upload_resp.status in [200, 201]:
                                        emoji_data = await upload_resp.json()
                                        emoji_id = emoji_data['id']
                                        uploaded_emojis[emoji_name] = f"<:{emoji_name}:{emoji_id}>"
                                    else:
                                        error_text = await upload_resp.text()
                                        print(f"Failed to upload emoji {emoji_name}: {upload_resp.status} - {error_text}")
                    except Exception as e:
                        print(f"Failed to upload emoji {emoji_name}: {e}")
            
            # Update status
            status_embed.description = f"**Theme:** {themename}\n**Status:** Saving to database..."
            await status_msg.edit(embed=status_embed)
            
            # Build icon values with new emoji IDs
            icon_values = []
            
            # Map icon positions (same as _get_theme_data)
            icon_positions = [
                'allianceOldIcon', 'avatarOldIcon', 'stoveOldIcon', 
                'stateOldIcon', 'allianceIcon', 'avatarIcon', 'stoveIcon', 'stateIcon', 
                'listIcon', 'fidIcon', 'timeIcon', 'homeIcon', 'num1Icon', 
                'num2Icon', 'num3Icon', 'num4Icon', 'num5Icon', 'num10Icon', 
                'newIcon', 'pinIcon', 'saveIcon', 'giftIcon', 'giftsIcon', 
                'alertIcon', 'robotIcon', 'crossIcon', 'heartIcon', 'total2Icon', 
                'shieldIcon', 'targetIcon', 'redeemIcon', 'membersIcon', 'averageIcon', 
                'hashtagIcon', 'messageIcon', 'supportIcon', 'foundryIcon', 'announceIcon', 
                'ministerIcon', 'researchIcon', 'trainingIcon', 'crazyJoeIcon', 'bearTrapIcon', 
                'calendarIcon', 'editListIcon', 'settingsIcon', 'settings2Icon', 'hourglassIcon', 
                'messageNoIcon', 'blankListIcon', 'alarmGiftIcon', 'alarmClockIcon', 'magnifyingIcon', 
                'frostdragonIcon', 'canyonClashIcon', 'constructionIcon', 'castleBattleIcon', 'checkGiftCodeIcon', 
                'deleteGiftCodeIcon', 'addGiftCodeIcon', 'processingIcon', 'verifiedIcon', 'questionIcon', 
                'transferIcon', 'multiplyIcon', 'divideIcon', 'deniedIcon', 'deleteIcon', 
                'exportIcon', 'importIcon', 'retryIcon', 'totalIcon', 'infoIcon', 
                'warnIcon', 'addIcon'
            ]
            
            # Build icon values list - ensure all values are strings
            for key in icon_positions:
                if key in icons_data:
                    data = icons_data[key]
                    if isinstance(data, dict) and "name" in data:
                        # Replace with new uploaded emoji
                        if data["name"] in uploaded_emojis:
                            icon_values.append(str(uploaded_emojis[data["name"]]))
                        else:
                            icon_values.append(str(data.get("emoji", "👻")))
                    else:
                        icon_values.append(str(data.get("value", "👻")))
                else:
                    icon_values.append("👻")
            # Process dividers - ensure returns are strings
            def process_divider(key):
                if key in dividers_data:
                    data = dividers_data[key]
                    if isinstance(data, dict):
                        if "emojis" in data:
                            # Replace emoji IDs with uploaded ones
                            raw = str(data["raw"])
                            for emoji_info in data["emojis"]:
                                old_name = emoji_info["name"]
                                if old_name in uploaded_emojis:
                                    old_pattern = f"<:{old_name}:{emoji_info['id']}>"
                                    raw = raw.replace(old_pattern, uploaded_emojis[old_name])
                            return raw
                        return str(data.get("raw", "━"))
                return "━"
            
            # Get divider lengths (stored as plain integers)
            divider_length1 = dividers_data.get('dividerLength1', 16)
            divider_length2 = dividers_data.get('dividerLength2', 16)
            # Ensure they're integers
            if not isinstance(divider_length1, int):
                divider_length1 = int(divider_length1) if str(divider_length1).isdigit() else 16
            if not isinstance(divider_length2, int):
                divider_length2 = int(divider_length2) if str(divider_length2).isdigit() else 16
            
            divider_values = [
                process_divider('dividerEmojiStart1'),
                process_divider('dividerEmojiPattern1'),
                process_divider('dividerEmojiEnd1'),
                divider_length1,
                process_divider('dividerEmojiStart2'),
                process_divider('dividerEmojiPattern2'),
                process_divider('dividerEmojiEnd2'),
                divider_length2,
            ]
            
            # Process colors - ensure they're strings
            color_values = [
                str(colors_data.get('emColorString1', '#0000FF')),
                str(colors_data.get('emColorString2', '#FF0000')),
                str(colors_data.get('emColorString3', '#00FF00')),
                str(colors_data.get('emColorString4', '#FFFF00')),
                str(colors_data.get('headerColor1', '#1F77B4')),
                str(colors_data.get('headerColor2', '#28A745')),
            ]

            # Process urls - ensure they're strings
            urls_values = [
                str(urls_data.get('furnaceLevel0Icon', 'None')),
                str(urls_data.get('furnaceLevel1Icon', 'None')),
                str(urls_data.get('furnaceLevel2Icon', 'None')),
                str(urls_data.get('furnaceLevel3Icon', 'None')),
                str(urls_data.get('furnaceLevel4Icon', 'None')),
                str(urls_data.get('furnaceLevel5Icon', 'None')),
                str(urls_data.get('furnaceLevel6Icon', 'None')),
                str(urls_data.get('furnaceLevel7Icon', 'None')),
                str(urls_data.get('furnaceLevel8Icon', 'None')),
                str(urls_data.get('furnaceLevel9Icon', 'None')),
                str(urls_data.get('furnaceLevel10Icon', 'None')),
                str(urls_data.get('furnaceLevel11Icon', 'None')),
                str(urls_data.get('furnaceLevel12Icon', 'None')),
                str(urls_data.get('furnaceLevel13Icon', 'None')),
                str(urls_data.get('furnaceLevel14Icon', 'None')),
                str(urls_data.get('furnaceLevel15Icon', 'None')),
                str(urls_data.get('furnaceLevel16Icon', 'None')),
                str(urls_data.get('furnaceLevel17Icon', 'None')),
                str(urls_data.get('furnaceLevel18Icon', 'None')),
                str(urls_data.get('furnaceLevel19Icon', 'None')),
                str(urls_data.get('furnaceLevel20Icon', 'None')),
                str(urls_data.get('furnaceLevel21Icon', 'None')),
                str(urls_data.get('furnaceLevel22Icon', 'None')),
                str(urls_data.get('furnaceLevel23Icon', 'None')),
                str(urls_data.get('furnaceLevel24Icon', 'None')),
                str(urls_data.get('furnaceLevel25Icon', 'None')),
                str(urls_data.get('furnaceLevel26Icon', 'None')),
                str(urls_data.get('furnaceLevel27Icon', 'None')),
                str(urls_data.get('furnaceLevel28Icon', 'None')),
                str(urls_data.get('furnaceLevel29Icon', 'None')),
                str(urls_data.get('furnaceLevel30Icon', 'None')),
            ]
            
            # Save to database
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                
                # Get actual column names from the table
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                # Skip id (0), themeName (1), and is_active (last), get columns 2 onwards for data
                data_columns = [col[1] for col in columns_info if col[1] not in ['id', 'themeName', 'themeCreator', 'is_active']]
                
                # Check if theme exists
                cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (themename,))
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    # Update existing theme (don't update is_active)
                    update_columns = [f"{col}=?" for col in data_columns]
                    query = f"UPDATE pimpsettings SET {', '.join(update_columns)} WHERE themeName=?"
                    cursor.execute(query, icon_values + divider_values + color_values + urls_values + [themename])
                else:
                    # Insert new theme with is_active = 0 (inactive by default)
                    placeholders = ', '.join(['?' for _ in range(len(data_columns))])
                    columns_str = ', '.join(data_columns)
                    query = f"INSERT INTO pimpsettings (themeName, themeCreator, {columns_str}, is_active) VALUES (?, ?, {placeholders}, 0)"
                    cursor.execute(query, [themename, interaction.user.id] + icon_values + divider_values + color_values + urls_values)
                
                pimpSettings_db.commit()
            
            # Final success message
            success_embed = discord.Embed(
                title=f"{pimp.verifiedIcon} Theme Imported Successfully",
                description=(
                    f"**Theme:** {themename}\n"
                    f"**Emojis Uploaded:** {len(uploaded_emojis)}\n"
                    f"**Status:** Ready to use\n\n"
                    f"Use `/pimp themename:{themename}` to view the imported theme."
                ),
                color=pimp.emColor3
            )
            
            await status_msg.edit(embed=success_embed)
            
        except Exception as e:
            print(f"Import error: {e}")
            await interaction.followup.send(f"{pimp.deniedIcon} An error occurred while importing theme: {e}")

    async def fetch_theme_info(self, interaction: discord.Interaction, themename: str, is_new_theme: bool = False):
        try:
            # Only defer if interaction hasn't been responded to yet
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)
            
            icons = self._get_theme_data(themename)
            
            if not icons:
                await interaction.followup.send(f"Theme '{themename}' not found.")
                return

            # Build lines based on theme type
            if themename == "default":
                lines = self._build_default_theme_lines()
            else:
                # Build lines for custom theme
                lines = [f"{name} = {value} = \\{value}" for name, value in icons.items()]

            # Build embeds using helper method
            embeds = self._build_embeds_from_lines(lines, themename)

            pages = [embeds[i:i+10] for i in range(0, len(embeds), 10)]
            current_page = 0
            
            # Get all field names (include dividers and colors now)
            all_emoji_names = [line.split(" = ")[0] for line in lines]

            view = PaginationView(pages, current_page, all_emoji_names, themename, self, interaction.user.id)
            
            # Add success message for newly created themes
            if is_new_theme:
                await interaction.followup.send(
                    f"{pimp.verifiedIcon} **Theme '{themename}' created successfully!**\n\nHere's your new theme preview:",
                    embeds=pages[current_page],
                    view=view
                )
            else:
                await interaction.followup.send(embeds=pages[current_page], view=view)
            
        except Exception as e:
            print(f"An error occurred: {e}")
            await interaction.followup.send("An error occurred while fetching theme info.")

    async def show_pimp_cog_menu(self, interaction: discord.Interaction):
        """Show the PIMP cog menu (You can add buttons or other interactive elements here)."""
        try:
            embed = discord.Embed(
                title=f"{pimp.robotIcon} PIMP - Theme Icon Display",
                description=(
                    f"Use the `/pimp` command to display theme icons.{chr(10)}{chr(10)}"
                    f"**Available Command:**{chr(10)}"
                    f"{pimp.pinIcon} `/pimp themename:<theme>` - Shows all icons for the selected theme{chr(10)}{chr(10)}"
                    f"**Note:** Theme names are autocompleted from available themes."
                ),
                color=pimp.emColor1
            )
            
            await interaction.response.edit_message(embed=embed, view=None)
            
        except Exception as e:
            print(f"Error in show_pimp_cog_menu: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} An error occurred while showing the PIMP menu.",
                    ephemeral=True
                )

async def setup(bot):
    await bot.add_cog(PIMP(bot))