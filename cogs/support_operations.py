import discord
from discord.ext import commands
from .pimp_my_bot import theme

class SupportOperations(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def show_support_menu(self, interaction: discord.Interaction):
        support_menu_embed = discord.Embed(
            title=f"{theme.targetIcon} Support Operations",
            description=(
                f"Please select an operation:\n\n"
                f"**Available Operations**\n"
                f"{theme.upperDivider}\n"
                f"{theme.editListIcon} **Request Support**\n"
                f"└ Get help and support\n\n"
                f"{theme.infoIcon} **About Project**\n"
                f"└ Project information\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        view = SupportView(self)
        
        try:
            await interaction.response.edit_message(embed=support_menu_embed, view=view)
        except discord.errors.InteractionResponded:
            await interaction.message.edit(embed=support_menu_embed, view=view)

    async def show_support_info(self, interaction: discord.Interaction):
        support_embed = discord.Embed(
            title=f"{theme.robotIcon} Bot Support Information",
            description=(
                "If you need help with the bot or are experiencing any issues, "
                "please feel free to ask on our [Discord](https://discord.gg/apYByj6K2m)\n\n"
                "**Additional resources:**\n"
                "**GitHub Repository:** [Whiteout Project](https://github.com/whiteout-project/bot)\n"
                "**Issues & Bug Reports:** [GitHub Issues](https://github.com/whiteout-project/bot/issues)\n\n"
                "This bot is open source and maintained by the WOSLand community. "
                "You can report bugs, request features, or contribute to the project "
                "through our Discord or GitHub repository.\n\n"
                "For technical support, please make sure to provide "
                "detailed information about your problem."
            ),
            color=theme.emColor1
        )
        
        try:
            await interaction.response.send_message(embed=support_embed, ephemeral=True)
            try:
                await interaction.user.send(embed=support_embed)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Could not send DM because your DMs are closed!",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error sending support info: {e}")

class SupportView(discord.ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    @discord.ui.button(
        label="Request Support",
        emoji=f"{theme.editListIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="request_support"
    )
    async def support_request_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_support_info(interaction)

    @discord.ui.button(
        label="About Project",
        emoji=f"{theme.infoIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="about_project"
    )
    async def about_project_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        about_embed = discord.Embed(
            title=f"{theme.infoIcon} About Whiteout Project",
            description=(
                f"**Open Source Bot**\n"
                f"{theme.upperDivider}\n"
                f"This is an open source Discord bot for Whiteout Survival.\n"
                f"The project is community-driven and freely available for everyone.\n"
                f"**Repository:** [GitHub](https://github.com/whiteout-project/bot)\n"
                f"**Community:** [Discord](https://discord.gg/apYByj6K2m)\n\n"
                f"**Features**\n"
                f"{theme.middleDivider}\n"
                f"• Alliance member management\n"
                f"• Gift code operations\n"
                f"• Automated member tracking\n"
                f"• Bear trap notifications\n"
                f"• ID channel verification\n"
                f"• and more...\n\n"
                f"**Contributing**\n"
                f"{theme.middleDivider}\n"
                f"Contributions are welcome! Please check our GitHub repository "
                f"to report issues, suggest features, or submit pull requests."
            ),
            color=discord.Color.green()
        )

        about_embed.set_footer(text=f"Made with {theme.heartIcon} by the WOSLand Bot Team.")
        
        try:
            await interaction.response.send_message(embed=about_embed, ephemeral=True)
            try:
                await interaction.user.send(embed=about_embed)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Could not send DM because your DMs are closed!",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error sending project info: {e}")

    @discord.ui.button(
        label="Main Menu",
        emoji=f"{theme.homeIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu"
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        alliance_cog = self.cog.bot.get_cog("Alliance")
        if alliance_cog:
            try:
                await interaction.message.edit(content=None, embed=None, view=None)
                await alliance_cog.show_main_menu(interaction)
            except discord.errors.InteractionResponded:
                await interaction.message.edit(content=None, embed=None, view=None)
                await alliance_cog.show_main_menu(interaction)

async def setup(bot):
    await bot.add_cog(SupportOperations(bot))