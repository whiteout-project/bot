"""Player Sync menu: set up the player-data API, test it, and run or schedule syncs."""
import asyncio
import io
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import discord

from . import alliance_api_sync as sync
from .bot_level_mapping import format_furnace_level
from .permission_handler import PermissionManager
from .pimp_my_bot import MenuView, check_interaction_user, report_failed_click, safe_edit_message, theme

logger = logging.getLogger('alliance')

FIELD_LABELS = {"name": "Name", "level": "Furnace level", "kingdom": "State", "power": "Power",
                "avatar": "Avatar picture"}
FIELD_EXAMPLES = {"name": "player.nick_name", "level": "player.furnace_level",
                  "kingdom": "player.state", "power": "player.power", "avatar": "player.avatar_url"}
QUEUE_TEXT = {
    'queued': "The sync is queued and starts shortly. You can follow it in Bot Health → Running Now.",
    'busy': "A sync is already running or waiting to start.",
    'unavailable': "The sync couldn't start because the bot's task queue isn't running. The bot log has the details.",
}
KEY_REJECTED = "The API didn't accept your key. Check it under API Key."
STATUS_HINTS = {
    401: KEY_REJECTED,
    403: KEY_REJECTED,
    402: "Your API plan doesn't cover this request. Check your account with the provider.",
    404: "The API doesn't know this player ID. If it should, check the player path under Connection.",
    429: "The API asked the bot to slow down. Wait a minute and try again.",
}
KEY_CLEARED = "Since the API changed, the old key was removed. Add the key for the new API under API Key."


def _done(text):
    return theme.verifiedIcon, text


def queue_result(status):
    icon = {'queued': theme.verifiedIcon, 'busy': theme.infoIcon}.get(status, theme.deniedIcon)
    return icon, QUEUE_TEXT[status]


async def _is_global_admin(interaction: discord.Interaction) -> bool:
    _, is_global = PermissionManager.is_admin(interaction.user.id)
    if not is_global:
        await interaction.response.send_message(
            f"{theme.deniedIcon} Only global administrators can set up Player Sync.", ephemeral=True)
    return is_global


async def open_player_sync(cog, interaction: discord.Interaction):
    if await _is_global_admin(interaction):
        await show_player_sync(cog, interaction)


async def show_player_sync(cog, interaction: discord.Interaction, last_result=None):
    hub = PlayerSyncView(cog, interaction.user.id, last_result)
    await safe_edit_message(interaction, embed=await hub.build_embed(), view=hub, content=None)


def _clip(value) -> str:
    return str(value).replace("`", "'")[:60]


def test_report(answer, profile) -> str:
    if answer.status == 0:
        return "The API didn't answer. Check the base URL, or try again in a bit."
    if answer.status != 200:
        return f"The API answered with error {answer.status}. {STATUS_HINTS.get(answer.status, '')}".strip()
    if answer.payload is None:
        return "The API answered, but not with player data. Check the base URL and player path."
    lines = ["It worked. Here's what the bot read:"]
    for field, label in FIELD_LABELS.items():
        path = profile["fields"][field]
        if not path:
            lines.append(f"• {label}: skipped")
            continue
        raw = sync.extract(answer.payload, path)
        value = sync.convert(field, raw)
        if value is None:
            shown = "nothing found at this path"
        elif field == "level":
            shown = format_furnace_level(value)
        else:
            shown = _clip(value)
        lines.append(f"• {label}: `{_clip(raw)}` → {shown}")
    return "\n".join(lines)


def _whole_number(text):
    text = (text or "").strip().replace(",", "")
    return int(text) if text.isdigit() else None


async def _store_profile(profile):
    found = sync.problems(profile)
    if found:
        return " ".join(found)
    await asyncio.to_thread(sync.save_profile, profile)
    return None


async def _save_profile(change):
    profile = await asyncio.to_thread(sync.get_profile)
    change(profile)
    return await _store_profile(profile)


def _outcome(error, done_text):
    return (error, None) if error else (None, _done(done_text))


def _host(url) -> str:
    return urlparse(url or "").netloc.lower()


async def _clear_key_if_site_changed(old_url, new_url) -> bool:
    old_host = _host(old_url)
    if not old_host or old_host == _host(new_url) or not await asyncio.to_thread(sync.get_key):
        return False
    await asyncio.to_thread(sync.save_key, "")
    logger.info("Player Sync: API key cleared because the API changed")
    return True


class _SyncView(MenuView):
    def __init__(self, cog, user_id):
        super().__init__(user_id)
        self.cog = cog

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _is_global_admin(interaction) and await super().interaction_check(interaction)

    def _add_button(self, label, emoji, style, row, callback, disabled=False):
        button = discord.ui.Button(label=label, emoji=f"{emoji}", style=style, row=row, disabled=disabled)
        button.callback = callback
        self.add_item(button)

    async def _to_hub(self, interaction, last_result=None):
        await show_player_sync(self.cog, interaction, last_result)


def _hub_state():
    state = sync.load_settings("profile", "enabled", "interval", "key", "last_run", "budget")
    return {
        **state,
        "missing": sync.missing_for_sync(state["profile"]),
        "key": sync.mask_key(state["key"]),
        "used": sync.used_today(state["budget"], sync.utc_today()),
    }


def _finished_at(last_run) -> str:
    try:
        return f"<t:{int(datetime.fromisoformat(last_run.get('finished_at', '')).timestamp())}:R>"
    except (TypeError, ValueError):
        return "Unknown time"


def _count(number, word) -> str:
    return f"{number} {word}" if number == 1 else f"{number} {word}s"


def _every(hours) -> str:
    return "every hour" if hours == 1 else f"every {hours} hours"


def _and_list(items) -> str:
    return items[0] if len(items) == 1 else f"{', '.join(items[:-1])} and {items[-1]}"


def _last_run_line(last_run) -> str:
    if not last_run:
        return "Hasn't run yet"
    scope = last_run.get('scope', 'All alliances')
    scope = "all alliances" if scope == "All alliances" else scope
    line = (f"{_finished_at(last_run)} for {scope}. Checked {_count(last_run.get('checked', 0), 'member')}, "
            f"found {_count(last_run.get('changes', 0), 'change')}.")
    stopped = last_run.get("stopped")
    return f"{line} {stopped}" if stopped else line


def _turned_on_text(last_queued, interval_hours) -> str:
    if sync.is_due(datetime.now(timezone.utc), last_queued, interval_hours):
        return "Automatic sync is on. The first sync starts within 5 minutes."
    due = last_queued + timedelta(hours=interval_hours)
    return f"Automatic sync is on. The next sync runs <t:{int(due.timestamp())}:R>."


class PlayerSyncView(_SyncView):
    def __init__(self, cog, user_id, last_result=None):
        super().__init__(cog, user_id)
        self.last_result = last_result

    async def build_embed(self):
        state = await asyncio.to_thread(_hub_state)
        job = self.cog.sync_job()
        ready = not state["missing"]
        on = state["enabled"]

        self.clear_items()
        primary, secondary = discord.ButtonStyle.primary, discord.ButtonStyle.secondary
        self._add_button(f"Auto Sync: {'On' if on else 'Off'}", theme.retryIcon,
                         discord.ButtonStyle.success if on else secondary, 0, self._on_toggle,
                         not ready and not on)
        self._add_button("Sync Now", theme.retryIcon, primary, 0, self._on_sync_now, not ready)
        self._add_button("Sync Alliance", theme.allianceIcon, primary, 0, self._on_sync_alliance, not ready)
        self._add_button("Interval", theme.timeIcon, primary, 0, self._on_interval)
        self._add_button("Connection", theme.globeIcon, primary, 1, self._on_connection)
        self._add_button("API Key", theme.shieldIcon, primary, 1, self._on_key)
        self._add_button("Fields", theme.editListIcon, primary, 1, self._on_fields)
        self._add_button("Limits", theme.chartIcon, primary, 1, self._on_limits)
        self._add_button("Test", theme.magnifyingIcon, primary, 2, self._on_test, not ready)
        self._add_button("Import", theme.importIcon, secondary, 2, self._on_import)
        self._add_button("Export", theme.exportIcon, secondary, 2, self._on_export)
        self._add_button("Back", theme.backIcon, secondary, 2, self._on_back)

        profile = state["profile"]
        setup = "Ready" if ready else f"Still needs the {_and_list(state['missing'])}"
        auto = f"On, {_every(state['interval'])}" if on else "Off"
        key = state['key'] if state['key'] != "not set" else "Not added yet"
        lines = [
            "Keep your members' names, furnace levels, states, power and pictures up to date. "
            "Connect the bot to a player-data API you use, add your API key, and the bot "
            "takes care of the rest.\n",
            f"{theme.upperDivider}",
            f"{theme.settingsIcon} **Setup:** {setup}",
            f"{theme.retryIcon} **Automatic sync:** {auto}",
            f"{theme.shieldIcon} **API key:** {key}",
            f"{theme.chartIcon} **Requests today:** `{state['used']}` of `{profile['per_day']}`",
            f"{theme.timeIcon} **Last run:** {_last_run_line(state['last_run'])}",
        ]
        if state["last_run"].get("error"):
            lines.append(f"{theme.deniedIcon} **Last error:** {state['last_run']['error']}")
        if job:
            lines.append(f"{theme.processingIcon} **{'Syncing right now' if job.get('status') == 'active' else 'Waiting to start'}**")
        lines += [
            f"{theme.lowerDivider}",
            f"{theme.retryIcon} **Auto Sync** / **Sync Now** / **Sync Alliance**",
            "└ Sync on a schedule, or right now for everyone or one alliance.",
            f"{theme.timeIcon} **Interval**",
            "└ How often automatic syncs run.",
            f"{theme.globeIcon} **Connection**",
            "└ Where the bot asks the API for a player.",
            f"{theme.shieldIcon} **API Key**",
            "└ Your key for the API.",
            f"{theme.editListIcon} **Fields**",
            "└ Which part of the API's answer to use for each value.",
            f"{theme.chartIcon} **Limits**",
            "└ How many requests the API allows, so the bot stays within them.",
            f"{theme.magnifyingIcon} **Test**",
            "└ Try one player to check that everything works.",
            f"{theme.importIcon} **Import** / {theme.exportIcon} **Export**",
            "└ Share your setup as a file. Your key stays private.",
            f"\n{theme.warnIcon} Bot backups include your API key, so keep them private.",
        ]
        if self.last_result:
            icon, text = self.last_result
            lines.append(f"\n{icon} {text}")
        return discord.Embed(title=f"{theme.retryIcon} Player Sync", description="\n".join(lines),
                             color=theme.emColor1)

    async def refresh(self, interaction, last_result=None):
        self.last_result = last_result
        await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)

    async def _on_toggle(self, interaction):
        on = not await asyncio.to_thread(sync.is_enabled)
        await asyncio.to_thread(sync.set_enabled, on)
        logger.info(f"Player Sync: admin turned automatic sync {'on' if on else 'off'}")
        if not on:
            await self.refresh(interaction, _done("Automatic sync is off. A sync that's already running will still finish."))
            return
        last_queued = await asyncio.to_thread(sync.get_last_queued)
        hours = await asyncio.to_thread(sync.get_interval_hours)
        await self.refresh(interaction, _done(_turned_on_text(last_queued, hours)))

    async def _on_sync_now(self, interaction):
        await self.refresh(interaction, queue_result(self.cog.queue_sync()))

    async def _on_sync_alliance(self, interaction):
        view = AlliancePickView(self.cog, self.original_user_id)
        await safe_edit_message(interaction, embed=await view.build_embed(), view=view, content=None)

    async def _on_interval(self, interaction):
        hours = await asyncio.to_thread(sync.get_interval_hours)
        await interaction.response.send_modal(IntervalModal(self.cog, self.original_user_id, hours))

    async def _on_connection(self, interaction):
        profile = await asyncio.to_thread(sync.get_profile)
        await interaction.response.send_modal(ConnectionModal(self.cog, self.original_user_id, profile))

    async def _on_key(self, interaction):
        profile = await asyncio.to_thread(sync.get_profile)
        await interaction.response.send_modal(KeyModal(self.cog, self.original_user_id, profile))

    async def _on_fields(self, interaction):
        view = FieldPickView(self.cog, self.original_user_id)
        await safe_edit_message(interaction, embed=await view.build_embed(), view=view, content=None)

    async def _on_limits(self, interaction):
        profile = await asyncio.to_thread(sync.get_profile)
        await interaction.response.send_modal(LimitsModal(self.cog, self.original_user_id, profile))

    async def _on_test(self, interaction):
        await interaction.response.send_modal(TestModal(self.cog, self.original_user_id))

    async def _on_import(self, interaction):
        await interaction.response.send_modal(ImportModal(self.cog, self.original_user_id))

    async def _on_export(self, interaction):
        profile = await asyncio.to_thread(sync.get_profile)
        file = discord.File(io.BytesIO(sync.export_profile(profile).encode("utf-8")), filename="api-profile.json")
        embed = discord.Embed(title=f"{theme.exportIcon} Player Sync Setup",
                              description="Here's your setup file. It's safe to share, since your API key stays out of it.",
                              color=theme.emColor1)
        await interaction.response.send_message(embed=embed, file=file, ephemeral=True)

    async def _on_back(self, interaction):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu:
            await main_menu.show_alliance_management(interaction)


class AlliancePickView(_SyncView):
    async def build_embed(self):
        alliances = (await asyncio.to_thread(PermissionManager.list_alliances))[:25]
        self.clear_items()
        if alliances:
            select = discord.ui.Select(placeholder="Pick an alliance to sync now", row=0, options=[
                discord.SelectOption(label=name[:100], value=str(aid)) for aid, name in alliances])
            select.callback = self._on_select
            self.add_item(select)
        self._add_button("Back", theme.backIcon, discord.ButtonStyle.secondary, 1, self._to_hub)
        text = "Choose the alliance to sync right now." if alliances else "Add an alliance first, then you can sync it here."
        return discord.Embed(title=f"{theme.allianceIcon} Sync One Alliance", description=text,
                             color=theme.emColor1)

    async def _on_select(self, interaction):
        alliance_id = int(interaction.data["values"][0])
        await self._to_hub(interaction, queue_result(self.cog.queue_sync(alliance_id)))


class FieldPickView(_SyncView):
    async def build_embed(self):
        profile = await asyncio.to_thread(sync.get_profile)
        self.clear_items()
        select = discord.ui.Select(placeholder="Choose a field to edit", row=0, options=[
            discord.SelectOption(label=label, value=field,
                                 description=(profile["fields"][field] or "Skipped")[:100])
            for field, label in FIELD_LABELS.items()])
        select.callback = self._on_select
        self.add_item(select)
        self._add_button("Back", theme.backIcon, discord.ButtonStyle.secondary, 1, self._to_hub)
        lines = ["Tell the bot where to find each value in the API's answer, with dots between the steps. "
                 "In `{\"player\": {\"nick_name\": \"Ann\"}}` the name is at `player.nick_name`. "
                 "Use a number to pick from a list, like `heroes.0.name`. Leave a field empty to skip it, "
                 "then press **Test** to check your paths.\n"]
        lines += [f"**{label}:** `{profile['fields'][field]}`" if profile["fields"][field]
                  else f"**{label}:** skipped" for field, label in FIELD_LABELS.items()]
        return discord.Embed(title=f"{theme.editListIcon} Field Paths", description="\n".join(lines),
                             color=theme.emColor1)

    async def _on_select(self, interaction):
        field = interaction.data["values"][0]
        profile = await asyncio.to_thread(sync.get_profile)
        await interaction.response.send_modal(
            FieldPathModal(self.cog, self.original_user_id, field, profile["fields"][field]))


class _SyncModal(discord.ui.Modal):
    def __init__(self, cog, user_id, title):
        super().__init__(title=title)
        self.cog = cog
        self.user_id = user_id

    async def _allowed(self, interaction) -> bool:
        return await _is_global_admin(interaction) and await check_interaction_user(interaction, self.user_id)

    async def on_submit(self, interaction: discord.Interaction):
        if not await self._allowed(interaction):
            return
        error, done = await self.apply()
        if error:
            await interaction.response.send_message(f"{theme.deniedIcon} {error}", ephemeral=True)
            return
        await show_player_sync(self.cog, interaction, done)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await report_failed_click(interaction, type(self).__name__, error)


def _text_input(label, default=None, placeholder=None, max_length=200, style=discord.TextStyle.short):
    return discord.ui.TextInput(label=label, default=default or None, placeholder=placeholder,
                                required=False, max_length=max_length, style=style)


class ConnectionModal(_SyncModal):
    def __init__(self, cog, user_id, profile):
        super().__init__(cog, user_id, "Connection")
        self.base_url = _text_input("Base URL", profile["base_url"], "https://api.example.com")
        self.player_path = _text_input("Player path ({fid} = player ID)", profile["player_path"],
                                       "/v1/players/{fid}")
        self.add_item(self.base_url)
        self.add_item(self.player_path)

    async def apply(self):
        profile = await asyncio.to_thread(sync.get_profile)
        old_url = profile["base_url"]
        profile.update(base_url=self.base_url.value.strip(), player_path=self.player_path.value.strip())
        error = await _store_profile(profile)
        if error:
            return error, None
        if await _clear_key_if_site_changed(old_url, self.base_url.value.strip()):
            return None, (theme.infoIcon, f"Connection saved. {KEY_CLEARED}")
        return None, _done("Connection saved.")


class KeyModal(_SyncModal):
    def __init__(self, cog, user_id, profile):
        super().__init__(cog, user_id, "API Key")
        self.header = _text_input("Header name", profile["auth_header"], "Authorization or X-Api-Key", 60)
        self.key = _text_input("Key (include Bearer if the API needs it)", None,
                               "Leave empty to keep your current key", 500)
        self.add_item(self.header)
        self.add_item(self.key)

    async def apply(self):
        error = await _save_profile(lambda p: p.update(auth_header=self.header.value.strip()))
        if error:
            return error, None
        if self.key.value.strip():
            await asyncio.to_thread(sync.save_key, self.key.value)
            logger.info("Player Sync: admin changed the API key")
            return None, _done("API key saved.")
        return None, _done("Header saved. Your key stays the same.")


class LimitsModal(_SyncModal):
    def __init__(self, cog, user_id, profile):
        super().__init__(cog, user_id, "Request Limits")
        self.per_minute = _text_input("Requests per minute", str(profile["per_minute"]), "30", 4)
        self.per_day = _text_input("Requests per day", str(profile["per_day"]), "1000", 7)
        self.add_item(self.per_minute)
        self.add_item(self.per_day)

    async def apply(self):
        per_minute, per_day = _whole_number(self.per_minute.value), _whole_number(self.per_day.value)
        error = await _save_profile(lambda p: p.update(per_minute=per_minute, per_day=per_day))
        return _outcome(error, f"Saved. The bot will make up to {per_minute} requests a minute and {per_day} a day.")


class IntervalModal(_SyncModal):
    def __init__(self, cog, user_id, hours):
        super().__init__(cog, user_id, "Sync Interval")
        self.hours = _text_input("Hours between syncs (at least 1)", str(hours), "24", 3)
        self.add_item(self.hours)

    async def apply(self):
        hours = _whole_number(self.hours.value)
        try:
            await asyncio.to_thread(sync.set_interval_hours, hours if hours is not None else -1)
        except ValueError:
            return (f"Enter a whole number of hours from {sync.INTERVAL_MIN_HOURS} "
                    f"to {sync.INTERVAL_MAX_HOURS}."), None
        return None, _done(f"Automatic syncs now run {_every(hours)}.")


class FieldPathModal(_SyncModal):
    def __init__(self, cog, user_id, field, current):
        super().__init__(cog, user_id, f"{FIELD_LABELS[field]} Path")
        self.field = field
        self.path = _text_input(f"{FIELD_LABELS[field]} path (leave empty to skip)", current,
                                FIELD_EXAMPLES[field])
        self.add_item(self.path)

    async def apply(self):
        path = self.path.value.strip()
        done = (f"{FIELD_LABELS[self.field]} path saved." if path
                else f"{FIELD_LABELS[self.field]} will be skipped.")
        return _outcome(await _save_profile(lambda p: p["fields"].update({self.field: path})), done)


class ImportModal(_SyncModal):
    def __init__(self, cog, user_id):
        super().__init__(cog, user_id, "Import Setup")
        self.text = _text_input("Paste the whole setup file", None, '{"base_url": ...}', 4000,
                                discord.TextStyle.paragraph)
        self.add_item(self.text)

    async def apply(self):
        profile, error = sync.import_profile(self.text.value)
        if error:
            return error, None
        old_url = (await asyncio.to_thread(sync.get_profile))["base_url"]
        await asyncio.to_thread(sync.save_profile, profile)
        logger.info("Player Sync: admin imported a setup")
        if await _clear_key_if_site_changed(old_url, profile["base_url"]):
            return None, (theme.infoIcon, f"Setup imported. {KEY_CLEARED}")
        return None, _done("Setup imported. Add your key under API Key, then press Test.")


class TestModal(_SyncModal):
    def __init__(self, cog, user_id):
        super().__init__(cog, user_id, "Test One Player")
        self.fid = _text_input("Player ID", None, "A member's in-game ID", 12)
        self.add_item(self.fid)

    async def on_submit(self, interaction: discord.Interaction):
        if not await self._allowed(interaction):
            return
        fid = _whole_number(self.fid.value)
        if fid is None:
            await interaction.response.send_message(f"{theme.deniedIcon} Player IDs are numbers only. Please try again.",
                                                    ephemeral=True)
            return
        await interaction.response.defer()
        profile, key = await asyncio.to_thread(sync.load_connection)
        await asyncio.to_thread(sync.count_request, sync.utc_today())
        answer = await sync.fetch_player(self.cog.session(), profile, key, fid)
        icon = theme.verifiedIcon if answer.status == 200 and answer.payload is not None else theme.deniedIcon
        await show_player_sync(self.cog, interaction, (icon, f"Test with player `{fid}`:\n{test_report(answer, profile)}"))
