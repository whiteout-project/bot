import discord
from discord.ext import commands
import sqlite3
import json
import os
from typing import Optional, Dict, List

# Import event type configuration
import sys
sys.path.insert(0, os.path.dirname(__file__))
from bear_event_types import EVENT_CONFIG, get_event_types, get_event_icon, get_event_config

class BearTrapTemplates(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = 'db/beartime.sqlite'
        os.makedirs('db', exist_ok=True)

        self.conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        self.cursor = self.conn.cursor()

        # Enable WAL mode
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.commit()

        # Create templates table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS notification_templates (
                template_id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_name TEXT NOT NULL,
                event_type TEXT,
                description TEXT,
                notification_type INTEGER,
                default_times TEXT,
                embed_title TEXT,
                embed_description TEXT,
                embed_color TEXT,
                embed_image_url TEXT,
                embed_thumbnail_url TEXT,
                repeat_config TEXT,
                is_global INTEGER DEFAULT 1,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

        # Populate pre-built templates if none exist
        self.cursor.execute("SELECT COUNT(*) FROM notification_templates WHERE is_global = 1")
        if self.cursor.fetchone()[0] == 0:
            self._populate_default_templates()

    def _populate_default_templates(self):
        """Populate database with pre-built templates for all event types"""
        templates = []

        for event_name, config in EVENT_CONFIG.items():
            emoji = config.get("emoji", "📅")
            description = config.get("description", "")
            image_url = config.get("image_url", "")
            thumbnail_url = config.get("thumbnail_url", "")
            notification_type = config.get("default_notification_type", 1)
            embed_desc = description

            # Create embed title
            embed_title = f"%i %n"

            # Repeat configuration based on event schedule type
            repeat_config = {}
            schedule_type = config.get("schedule_type")

            if schedule_type == "daily":
                repeat_config = {"type": "interval", "minutes": 1440}  # Daily
            elif schedule_type == "global_weekly":
                repeat_config = {"type": "fixed_days", "days": [4]}  # Friday
            elif schedule_type == "global_biweekly":
                if event_name == "Crazy Joe":
                    repeat_config = {"type": "custom"}  # Will be set by wizard
                else:
                    repeat_config = {"type": "interval", "minutes": 20160}  # 2 weeks
            elif schedule_type in ["global_monthly", "global_4weekly", "global_4weekly_alt"]:
                repeat_config = {"type": "interval", "minutes": 40320}  # 4 weeks
            else:
                repeat_config = {"type": "custom"}

            templates.append({
                "template_name": event_name,
                "event_type": event_name,
                "description": "",  # Will be generated dynamically when displaying
                "notification_type": notification_type,
                "embed_title": embed_title,
                "embed_description": embed_desc,
                "embed_color": "3447003",  # Discord blue
                "embed_image_url": image_url,
                "embed_thumbnail_url": thumbnail_url,
                "repeat_config": json.dumps(repeat_config),
                "is_global": 1,
                "created_by": None
            })

        # Insert all templates
        for template in templates:
            self.cursor.execute("""
                INSERT INTO notification_templates
                (template_name, event_type, description, notification_type, embed_title,
                 embed_description, embed_color, embed_image_url, embed_thumbnail_url,
                 repeat_config, is_global, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                template["template_name"],
                template["event_type"],
                template["description"],
                template["notification_type"],
                template["embed_title"],
                template["embed_description"],
                template["embed_color"],
                template["embed_image_url"],
                template["embed_thumbnail_url"],
                template["repeat_config"],
                template["is_global"],
                template["created_by"]
            ))

        self.conn.commit()

    async def check_admin(self, interaction: discord.Interaction) -> bool:
        """Check if user is an admin"""
        conn = sqlite3.connect('db/settings.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM admin WHERE id = ?", (interaction.user.id,))
        is_admin = cursor.fetchone() is not None
        conn.close()

        if not is_admin:
            await interaction.response.send_message(
                "❌ You don't have permission to use this command!",
                ephemeral=True
            )
        return is_admin

    def get_template(self, template_id: int) -> Optional[Dict]:
        """Get a template by ID"""
        self.cursor.execute("""
            SELECT template_id, template_name, event_type, description, notification_type,
                   default_times, embed_title, embed_description, embed_color,
                   embed_image_url, embed_thumbnail_url, repeat_config, is_global, created_by
            FROM notification_templates
            WHERE template_id = ?
        """, (template_id,))

        row = self.cursor.fetchone()
        if not row:
            return None

        return {
            "template_id": row[0],
            "template_name": row[1],
            "event_type": row[2],
            "description": row[3],
            "notification_type": row[4],
            "default_times": row[5],
            "embed_title": row[6],
            "embed_description": row[7],
            "embed_color": row[8],
            "embed_image_url": row[9],
            "embed_thumbnail_url": row[10],
            "repeat_config": row[11],
            "is_global": row[12],
            "created_by": row[13]
        }

    def update_template(self, template_id: int, embed_title: str, embed_description: str,
                       embed_image_url: str, embed_thumbnail_url: str, user_id: int = None):
        """Update a template's embed settings"""
        self.cursor.execute("""
            UPDATE notification_templates
            SET embed_title = ?, embed_description = ?, embed_image_url = ?, embed_thumbnail_url = ?,
                is_global = 0, created_by = COALESCE(created_by, ?)
            WHERE template_id = ?
        """, (embed_title, embed_description, embed_image_url, embed_thumbnail_url, user_id, template_id))
        self.conn.commit()

    def get_templates_by_event_type(self, event_type: Optional[str] = None) -> List[Dict]:
        """Get all templates, optionally filtered by event type"""
        if event_type:
            self.cursor.execute("""
                SELECT template_id, template_name, event_type, description, notification_type,
                       embed_title, embed_description, is_global, created_by
                FROM notification_templates
                WHERE event_type = ?
                ORDER BY is_global DESC, template_name ASC
            """, (event_type,))
        else:
            self.cursor.execute("""
                SELECT template_id, template_name, event_type, description, notification_type,
                       embed_title, embed_description, is_global, created_by
                FROM notification_templates
                ORDER BY is_global DESC, event_type ASC, template_name ASC
            """)

        results = []
        for row in self.cursor.fetchall():
            results.append({
                "template_id": row[0],
                "template_name": row[1],
                "event_type": row[2],
                "description": row[3],
                "notification_type": row[4],
                "embed_title": row[5],
                "embed_description": row[6],
                "is_global": row[7],
                "created_by": row[8]
            })
        return results

    async def show_templates(self, interaction: discord.Interaction):
        """Show templates browser directly"""
        if not await self.check_admin(interaction):
            return
        templates = self.get_templates_by_event_type()
        if not templates:
            await interaction.response.send_message(
                "❌ No templates found.",
                ephemeral=True
            )
            return
        view = TemplateBrowseView(self, templates)
        await view.show_page(interaction, 0, ephemeral=True)

class TemplateBrowseView(discord.ui.View):
    def __init__(self, cog: BearTrapTemplates, templates: List[Dict], event_filter: Optional[str] = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.templates = templates
        self.event_filter = event_filter
        self.current_page = 0
        self.page_size = 10
        self.total_pages = (len(templates) + self.page_size - 1) // self.page_size

    async def show_page(self, interaction: discord.Interaction, page: int, ephemeral: bool = False):
        """Display a page of templates"""
        self.current_page = page
        start = page * self.page_size
        end = min(start + self.page_size, len(self.templates))
        page_templates = self.templates[start:end]
        title = "📚 Available Templates"
        if self.event_filter:
            icon = get_event_icon(self.event_filter)
            title = f"{icon} {self.event_filter} Templates"
        embed = discord.Embed(
            title=title,
            description=f"Templates define the default notification settings used by the Setup Wizard. Edit them to customize how the event notifications appear when you create them using the wizard.\n\nShowing {start + 1}-{end} of {len(self.templates)} templates",
            color=discord.Color.blue()
        )
        for template in page_templates:
            icon = get_event_icon(template["event_type"])

            # Check if template has been customized
            is_customized = not template["is_global"] or template.get("created_by")

            # Simple display: just event name with customization status
            value = "✏️ Customized" if is_customized else "*Default template*"

            embed.add_field(
                name=f"{icon} {template['template_name']}",
                value=value,
                inline=True
            )
        self.clear_items()
        if page_templates:
            self.add_item(TemplateSelectDropdown(self.cog, page_templates))
        if self.total_pages > 1:
            prev_button = discord.ui.Button(
                label="Previous",
                emoji="⬅️",
                style=discord.ButtonStyle.secondary,
                disabled=(page == 0),
                row=1
            )
            prev_button.callback = lambda i: self.show_page(i, page - 1, ephemeral)
            self.add_item(prev_button)
            page_indicator = discord.ui.Button(
                label=f"Page {page + 1}/{self.total_pages}",
                style=discord.ButtonStyle.secondary,
                disabled=True,
                row=1
            )
            self.add_item(page_indicator)
            next_button = discord.ui.Button(
                label="Next",
                emoji="➡️",
                style=discord.ButtonStyle.secondary,
                disabled=(page >= self.total_pages - 1),
                row=1
            )
            next_button.callback = lambda i: self.show_page(i, page + 1, ephemeral)
            self.add_item(next_button)

        # Send message based on ephemeral flag
        if ephemeral:
            await interaction.response.send_message(embed=embed, view=self, ephemeral=True)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

class TemplateSelectDropdown(discord.ui.Select):
    def __init__(self, cog: BearTrapTemplates, templates: List[Dict]):
        self.cog = cog
        self.templates = templates

        options = []
        for template in templates[:25]:  # Discord limit
            icon = get_event_icon(template["event_type"])
            options.append(discord.SelectOption(
                label=template["template_name"],
                value=str(template["template_id"]),
                emoji=icon,
                description=template["description"][:100] if template["description"] else "No description"
            ))

        super().__init__(
            placeholder="Select a template to preview...",
            options=options,
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        template_id = int(self.values[0])
        template = self.cog.get_template(template_id)

        if not template:
            await interaction.response.send_message(
                "❌ Template not found.",
                ephemeral=True
            )
            return

        view = TemplatePreviewView(self.cog, template, self.templates)
        await view.show_preview(interaction)

class TemplateEditModal(discord.ui.Modal, title="Edit Template"):
    def __init__(self, cog: BearTrapTemplates, template: Dict):
        super().__init__()
        self.cog = cog
        self.template = template

        # Add input fields with current values
        self.title_input = discord.ui.TextInput(
            label="Embed Title",
            placeholder="Enter notification title...",
            default=template.get("embed_title", ""),
            max_length=256,
            required=True
        )
        self.add_item(self.title_input)

        self.description_input = discord.ui.TextInput(
            label="Embed Description",
            placeholder="Enter notification description...",
            default=template.get("embed_description", ""),
            style=discord.TextStyle.paragraph,
            max_length=4000,
            required=False
        )
        self.add_item(self.description_input)

        self.image_url_input = discord.ui.TextInput(
            label="Image URL",
            placeholder="https://example.com/image.png",
            default=template.get("embed_image_url", ""),
            max_length=512,
            required=False
        )
        self.add_item(self.image_url_input)

        self.thumbnail_url_input = discord.ui.TextInput(
            label="Thumbnail URL",
            placeholder="https://example.com/thumbnail.png",
            default=template.get("embed_thumbnail_url", ""),
            max_length=512,
            required=False
        )
        self.add_item(self.thumbnail_url_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Handle template update"""
        try:
            self.cog.update_template(
                self.template["template_id"],
                self.title_input.value,
                self.description_input.value,
                self.image_url_input.value or None,
                self.thumbnail_url_input.value or None,
                interaction.user.id
            )

            # Refresh the template data
            updated_template = self.cog.get_template(self.template["template_id"])
            if updated_template:
                # Show updated preview
                view = TemplatePreviewView(self.cog, updated_template)
                await view.show_preview(interaction)
            else:
                await interaction.response.send_message(
                    "❌ Failed to refresh template preview",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error updating template: {e}")
            await interaction.response.send_message(
                f"❌ Failed to update template: {str(e)}",
                ephemeral=True
            )

class TemplatePreviewView(discord.ui.View):
    def __init__(self, cog: BearTrapTemplates, template: Dict, all_templates: List[Dict] = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.template = template
        self.all_templates = all_templates or []

    async def show_preview(self, interaction: discord.Interaction):
        """Show preview of template"""
        template = self.template

        # Create preview embed based on template
        icon = get_event_icon(template["event_type"])

        info_embed = discord.Embed(
            title=f"{icon} Template Preview: {template['template_name']}",
            description=template["description"],
            color=discord.Color.green()
        )

        info_embed.add_field(
            name="Event Type",
            value=template["event_type"],
            inline=True
        )

        info_embed.add_field(
            name="Notification Type",
            value=f"Type {template['notification_type']}",
            inline=True
        )

        if template["repeat_config"]:
            try:
                repeat_data = json.loads(template["repeat_config"])
                repeat_type = repeat_data.get("type", "custom")
                if repeat_type == "interval":
                    minutes = repeat_data.get("minutes", 0)
                    if minutes >= 1440:
                        days = minutes // 1440
                        info_embed.add_field(
                            name="Repeat",
                            value=f"Every {days} day(s)",
                            inline=True
                        )
                    else:
                        info_embed.add_field(
                            name="Repeat",
                            value=f"Every {minutes} minutes",
                            inline=True
                        )
                elif repeat_type == "fixed_days":
                    days = repeat_data.get("days", [])
                    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    day_str = ", ".join([day_names[d] for d in days])
                    info_embed.add_field(
                        name="Repeat",
                        value=day_str,
                        inline=True
                    )
                else:
                    info_embed.add_field(
                        name="Repeat",
                        value="Custom",
                        inline=True
                    )
            except:
                pass

        info_embed.add_field(
            name="How to Use",
            value="This template will be automatically applied by the **Setup Wizard**.",
            inline=False
        )

        # Create notification preview embed with sample placeholder values
        event_config = get_event_config(template["event_type"])
        sample_emoji = event_config.get("emoji", "📅") if event_config else "📅"
        sample_time = "14:00"
        sample_date = "Nov 29"
        sample_countdown = "10 minutes"

        # Replace placeholder variables with sample values for preview
        preview_title = template["embed_title"] or "Notification"
        preview_title = preview_title.replace("%i", sample_emoji)
        preview_title = preview_title.replace("%n", template["event_type"])
        preview_title = preview_title.replace("%e", sample_time)
        preview_title = preview_title.replace("%d", sample_date)
        preview_title = preview_title.replace("%t", sample_countdown)

        preview_desc = template["embed_description"] or "No description"
        preview_desc = preview_desc.replace("%i", sample_emoji)
        preview_desc = preview_desc.replace("%n", template["event_type"])
        preview_desc = preview_desc.replace("%e", sample_time)
        preview_desc = preview_desc.replace("%d", sample_date)
        preview_desc = preview_desc.replace("%t", sample_countdown)

        notification_embed = discord.Embed(
            title=preview_title,
            description=preview_desc,
            color=int(template["embed_color"]) if template["embed_color"] else discord.Color.blue().value
        )

        if template["embed_image_url"]:
            notification_embed.set_image(url=template["embed_image_url"])

        if template["embed_thumbnail_url"]:
            notification_embed.set_thumbnail(url=template["embed_thumbnail_url"])

        # Add edit button
        edit_button = discord.ui.Button(
            label="Edit Template",
            emoji="✏️",
            style=discord.ButtonStyle.primary
        )
        edit_button.callback = self.edit_template
        self.add_item(edit_button)

        # Add back button
        back_button = discord.ui.Button(
            label="Back",
            emoji="◀️",
            style=discord.ButtonStyle.primary
        )
        back_button.callback = self.back_to_browse
        self.add_item(back_button)

        await interaction.response.edit_message(embeds=[info_embed, notification_embed], view=self)

    async def edit_template(self, interaction: discord.Interaction):
        """Show template edit modal"""
        modal = TemplateEditModal(self.cog, self.template)
        await interaction.response.send_modal(modal)

    async def back_to_browse(self, interaction: discord.Interaction):
        """Return to template browser"""
        templates = self.all_templates if self.all_templates else self.cog.get_templates_by_event_type()
        view = TemplateBrowseView(self.cog, templates)
        await view.show_page(interaction, 0)

async def setup(bot):
    await bot.add_cog(BearTrapTemplates(bot))