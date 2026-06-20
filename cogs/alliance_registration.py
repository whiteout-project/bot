"""Alliance registration flow. /register links an in-game ID to a Discord account (multi-FID, cross-server). /unregister detaches one of your own."""
import discord
from discord.ext import commands
import sqlite3
import asyncio
import logging
from datetime import datetime, timezone
from .pimp_my_bot import theme
from .login_handler import LoginHandler

logger = logging.getLogger('alliance')


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


class AllianceRegistration(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        self.conn_alliance = sqlite3.connect("db/alliance.sqlite", timeout=30.0, check_same_thread=False)
        self.c_alliance = self.conn_alliance.cursor()

        self.conn_users = sqlite3.connect("db/users.sqlite", timeout=30.0, check_same_thread=False)
        self.c_users = self.conn_users.cursor()

    async def cog_unload(self):
        self.conn_alliance.close()
        self.conn_users.close()

    # ── Registration state helpers ─────────────────────────────────────────

    def _get_user_row(self, fid: int):
        """Return (fid, discord_id, discord_server_id, alliance, nickname) or None."""
        self.c_users.execute(
            "SELECT fid, discord_id, discord_server_id, alliance, nickname "
            "FROM users WHERE fid = ?",
            (fid,),
        )
        return self.c_users.fetchone()

    def _linked_fids_for(self, discord_id: int) -> list:
        """All FIDs owned by this Discord user across all servers.
        Returns list of (fid, nickname, alliance, discord_server_id)."""
        self.c_users.execute(
            "SELECT fid, nickname, alliance, discord_server_id "
            "FROM users WHERE discord_id = ? "
            "ORDER BY nickname COLLATE NOCASE",
            (discord_id,),
        )
        return self.c_users.fetchall()

    def set_registration_enabled(self, enabled: bool) -> None:
        """Persist the global self-registration toggle to settings.sqlite."""
        try:
            with sqlite3.connect("db/settings.sqlite") as conn:
                cursor = conn.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS register_settings (enabled BOOLEAN)")
                cursor.execute("SELECT COUNT(*) FROM register_settings")
                exists = cursor.fetchone()[0] > 0
                if exists:
                    cursor.execute(
                        "UPDATE register_settings SET enabled = ? WHERE rowid = 1",
                        (enabled,),
                    )
                else:
                    cursor.execute(
                        "INSERT INTO register_settings VALUES (?)", (enabled,)
                    )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating register settings: {e}")
            print(f"Error updating register settings: {e}")

    def is_registration_enabled(self) -> bool:
        """Check if registration is enabled in the settings database."""
        try:
            conn = sqlite3.connect("db/settings.sqlite")
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='register_settings'")
            table_exists = cursor.fetchone()

            if not table_exists:
                conn.close()
                return False

            cursor.execute("SELECT enabled FROM register_settings WHERE rowid = 1")
            result = cursor.fetchone()

            conn.close()

            return bool(result[0]) if result else False

        except Exception as e:
            logger.error(f"Error checking registration status: {e}")
            print(f"Error checking registration status: {e}")
            return False

    async def alliance_autocomplete(self, interaction: discord.Interaction, current: str):
        def _read():
            with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
                return conn.execute("SELECT alliance_id, name FROM alliance_list").fetchall()
        alliances = await asyncio.to_thread(_read)

        return [
            discord.app_commands.Choice(name=name, value=alliance_id)
            for alliance_id, name in alliances if current.lower() in name.lower()
        ][:25]

    async def fetch_user(self, fid: int):
        result = await LoginHandler().fetch_player_data(str(fid))

        if result['status'] == 'success':
            return {"msg": "success", "data": result['data']}
        elif result['status'] == 'rate_limited':
            raise Exception("RATE_LIMITED")
        elif result['status'] == 'not_found':
            return {"msg": "role not exist"}
        else:
            raise Exception(result.get('error_message', 'Failed to fetch user data'))

    # ── DB writes ──────────────────────────────────────────────────────────

    def _insert_new_user(self, fid: int, user_data: dict, alliance: int,
                         discord_id: int, server_id: int):
        self.c_users.execute(
            "INSERT INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, "
            "alliance, discord_id, discord_server_id, discord_id_updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (fid, user_data["nickname"], user_data["stove_lv"], user_data["kid"],
             user_data.get("stove_lv_content"), alliance, discord_id, server_id, _now_iso()),
        )
        self.conn_users.commit()

    def _attach_discord_to_existing(self, fid: int, discord_id: int, server_id: int):
        self.c_users.execute(
            "UPDATE users SET discord_id = ?, discord_server_id = ?, "
            "discord_id_updated_at = ? WHERE fid = ?",
            (discord_id, server_id, _now_iso(), fid),
        )
        self.conn_users.commit()

    def _move_registration_to_server(self, fid: int, new_server_id: int):
        self.c_users.execute(
            "UPDATE users SET discord_server_id = ?, discord_id_updated_at = ? "
            "WHERE fid = ?",
            (new_server_id, _now_iso(), fid),
        )
        self.conn_users.commit()

    def _detach_discord(self, fid: int):
        self.c_users.execute(
            "UPDATE users SET discord_id = NULL, discord_server_id = NULL, "
            "discord_id_updated_at = ? WHERE fid = ?",
            (_now_iso(), fid),
        )
        self.conn_users.commit()

    # ── /register ──────────────────────────────────────────────────────────

    @discord.app_commands.command(
        name="register",
        description="Link your in-game ID to your Discord account. Multiple IDs supported.",
    )
    @discord.app_commands.describe(
        fid="Your In-Game ID",
        alliance="Your Alliance Name"
    )
    @discord.app_commands.rename(fid="id")
    @discord.app_commands.autocomplete(alliance=alliance_autocomplete)
    async def register(self, interaction: discord.Interaction, fid: int, alliance: int):
        if not self.is_registration_enabled():
            await interaction.response.send_message(
                f"{theme.deniedIcon} Registration is currently disabled.",
                ephemeral=True
            )
            return

        caller_id = interaction.user.id
        current_server_id = interaction.guild_id if interaction.guild else None

        existing = self._get_user_row(fid)

        if existing and existing[1] is not None and existing[1] != caller_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} This ID is already registered to another Discord user. "
                f"Contact an admin if this needs to be fixed.",
                ephemeral=True,
            )
            return

        if existing and existing[1] == caller_id:
            existing_server_id = existing[2]
            if existing_server_id == current_server_id:
                await interaction.response.send_message(
                    f"{theme.verifiedIcon} ID `{fid}` is already registered to you here. "
                    f"Nothing to change.",
                    ephemeral=True,
                )
                return
            view = _MoveServerView(
                cog=self, fid=fid, caller_id=caller_id,
                old_server_id=existing_server_id, new_server_id=current_server_id,
            )
            old_name = self._server_name_or_id(existing_server_id, interaction)
            new_name = self._server_name_or_id(current_server_id, interaction)
            await interaction.response.send_message(
                f"{theme.infoIcon} ID `{fid}` is currently registered on **{old_name}**.\n"
                f"Move the registration here ({new_name})?",
                view=view, ephemeral=True,
            )
            return

        if existing:
            self._attach_discord_to_existing(fid, caller_id, current_server_id)
            await self._send_register_success(interaction, fid, caller_id, action="linked")
            return

        # API lookup can exceed the 3s ack window — defer before the slow call.
        await interaction.response.defer(ephemeral=True)
        try:
            api_response = await self.fetch_user(fid)
            if api_response.get("msg") != "success":
                error_msg = api_response.get("msg", "Unknown error")
                if "role not exist" in error_msg.lower():
                    display_msg = f"{theme.deniedIcon} Invalid ID. Please try again."
                else:
                    display_msg = f"{theme.deniedIcon} Invalid ID: {error_msg}"
                await interaction.followup.send(display_msg, ephemeral=True)
                return
            if "data" not in api_response:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Invalid response from server. Please try again later.",
                    ephemeral=True,
                )
                return
            user_data = api_response["data"]
        except Exception as e:
            if str(e) == "RATE_LIMITED":
                await interaction.followup.send(
                    "⏳ Rate limit reached. Please wait a minute before trying again.",
                    ephemeral=True,
                )
            else:
                logger.error(f"Error fetching user data for ID {fid}: {e}")
                print(f"Error fetching user data for ID {fid}: {e}")
                await interaction.followup.send(
                    f"{theme.deniedIcon} Failed to fetch user data. Please try again later.",
                    ephemeral=True,
                )
            return

        self._insert_new_user(fid, user_data, alliance, caller_id, current_server_id)
        await self._send_register_success(interaction, fid, caller_id, action="registered")

    async def _send_register_success(self, interaction: discord.Interaction,
                                     fid: int, caller_id: int, action: str):
        all_linked = self._linked_fids_for(caller_id)
        if len(all_linked) > 1:
            lines = "\n".join(
                f"  {theme.fidIcon} `{f}` — {n or '(unnamed)'}"
                for f, n, _, _ in all_linked
            )
            extra = f"\n\nYou now have **{len(all_linked)} characters** linked:\n{lines}"
        else:
            extra = ""
        verb = "Linked" if action == "linked" else "Registered"
        msg = f"{theme.verifiedIcon} {verb} ID `{fid}` to your Discord account.{extra}"
        # Called from both the deferred (new-user) and non-deferred (attach) paths.
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    def _server_name_or_id(self, server_id, interaction):
        if server_id is None:
            return "(no server)"
        guild = self.bot.get_guild(server_id) if self.bot else None
        if guild and guild.name:
            return guild.name
        if interaction.guild and interaction.guild.id == server_id:
            return interaction.guild.name
        return f"server `{server_id}`"

    # ── /unregister ────────────────────────────────────────────────────────

    async def unregister_autocomplete(self, interaction: discord.Interaction, current: str):
        rows = self._linked_fids_for(interaction.user.id)
        cur = (current or "").lower()
        choices = []
        for fid, nickname, alliance, _ in rows:
            label = f"{nickname or '(unnamed)'} ({fid})"
            if cur and cur not in str(fid) and cur not in (nickname or "").lower():
                continue
            choices.append(discord.app_commands.Choice(name=label[:100], value=fid))
            if len(choices) >= 25:
                break
        return choices

    @discord.app_commands.command(
        name="unregister",
        description="Unlink one of your in-game IDs from your Discord account.",
    )
    @discord.app_commands.describe(fid="The in-game ID to unlink")
    @discord.app_commands.rename(fid="id")
    @discord.app_commands.autocomplete(fid=unregister_autocomplete)
    async def unregister(self, interaction: discord.Interaction, fid: int):
        caller_id = interaction.user.id
        existing = self._get_user_row(fid)

        if not existing:
            await interaction.response.send_message(
                f"{theme.deniedIcon} ID `{fid}` is not in the database.",
                ephemeral=True,
            )
            return

        linked_discord_id = existing[1]
        if linked_discord_id is None:
            await interaction.response.send_message(
                f"{theme.deniedIcon} ID `{fid}` is not linked to any Discord user.",
                ephemeral=True,
            )
            return

        if linked_discord_id != caller_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} ID `{fid}` is linked to a different Discord user. "
                f"Only the owner can unregister it (or an admin via the admin tools).",
                ephemeral=True,
            )
            return

        self._detach_discord(fid)
        remaining = self._linked_fids_for(caller_id)
        if remaining:
            lines = "\n".join(
                f"  {theme.fidIcon} `{f}` — {n or '(unnamed)'}"
                for f, n, _, _ in remaining
            )
            extra = f"\n\nYou still have **{len(remaining)} character(s)** linked:\n{lines}"
        else:
            extra = "\n\nYou no longer have any characters linked."
        await interaction.response.send_message(
            f"{theme.verifiedIcon} Unlinked ID `{fid}` from your Discord account.{extra}",
            ephemeral=True,
        )


class _MoveServerView(discord.ui.View):
    def __init__(self, cog: AllianceRegistration, fid: int, caller_id: int,
                 old_server_id, new_server_id):
        super().__init__(timeout=120)
        self.cog = cog
        self.fid = fid
        self.caller_id = caller_id
        self.old_server_id = old_server_id
        self.new_server_id = new_server_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.caller_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} This prompt is for someone else.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Move here", style=discord.ButtonStyle.success,
                       emoji=f"{theme.verifiedIcon}")
    async def confirm(self, interaction: discord.Interaction, _button: discord.ui.Button):
        self.cog._move_registration_to_server(self.fid, self.new_server_id)
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=f"{theme.verifiedIcon} ID `{self.fid}` is now registered here.",
            view=self,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary,
                       emoji=f"{theme.backIcon}")
    async def cancel(self, interaction: discord.Interaction, _button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=f"{theme.infoIcon} No change made. Registration remains on the original server.",
            view=self,
        )


async def setup(bot):
    await bot.add_cog(AllianceRegistration(bot))
