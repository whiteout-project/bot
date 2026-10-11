"""Player Sync: keeps member names, levels, kingdoms, power and pictures current from a player-data API."""
import asyncio
import json
import logging
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import aiohttp
import discord
from discord.ext import commands, tasks

from .alliance_member_edit import apply_member_edit, enqueue_catchups, parse_power
from .bot_level_mapping import format_furnace_level, parse_furnace_level, parse_state
from .gift_redemption import rate_limit_delay
from .gift_state_resolver import _put_setting, _settings
from .permission_handler import PermissionManager
from .pimp_my_bot import theme
from .process_queue import PLAYER_SYNC, PreemptedException

logger = logging.getLogger('alliance')


# --- Profile: which API to call and where each player field sits in its JSON ----------

FIELDS = {
    "name": "nickname",
    "level": "furnace_lv",
    "kingdom": "kid",
    "power": "power",
    "avatar": "avatar_url",
}
PER_MINUTE_MAX = 600
PER_DAY_MAX = 1_000_000
LIMIT_LABELS = {"per_minute": "Requests per minute", "per_day": "Requests per day"}
NOT_A_PROFILE = "That doesn't look like a Player Sync setup file. Paste the whole file, from the first { to the last }."

DEFAULT_PROFILE = {
    "base_url": "",
    "player_path": "",
    "auth_header": "Authorization",
    "per_minute": 30,
    "per_day": 1000,
    "fields": {name: "" for name in FIELDS},
}


def normalise(raw) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    profile = {key: raw.get(key, default) for key, default in DEFAULT_PROFILE.items() if key != "fields"}
    fields = raw.get("fields") if isinstance(raw.get("fields"), dict) else {}
    profile["fields"] = {name: fields.get(name) or "" for name in FIELDS}
    return profile


def problems(profile: dict) -> list[str]:
    found = []
    base_url = profile.get("base_url")
    if not isinstance(base_url, str) or (base_url and not base_url.startswith(("http://", "https://"))):
        found.append("The base URL must start with http:// or https://.")
    player_path = profile.get("player_path")
    if not isinstance(player_path, str) or (player_path and "{fid}" not in player_path):
        found.append("Put {fid} in the player path where the player ID goes.")
    for key, maximum in (("per_minute", PER_MINUTE_MAX), ("per_day", PER_DAY_MAX)):
        value = profile.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
            found.append(f"{LIMIT_LABELS[key]} must be a whole number from 1 to {maximum}.")
    header = profile.get("auth_header")
    if not isinstance(header, str) or not header.strip() or any(c in header for c in ": \t\r\n"):
        found.append("The header name needs to be a single word, without spaces or colons.")
    fields = profile.get("fields")
    if not isinstance(fields, dict) or any(not isinstance(fields.get(name, ""), str) for name in FIELDS):
        found.append("Each field path must be text, like player.nick_name.")
    return found


def missing_for_sync(profile: dict) -> list[str]:
    missing = []
    if not profile["base_url"]:
        missing.append("base URL")
    if not profile["player_path"]:
        missing.append("player path")
    if not any(profile["fields"].values()):
        missing.append("at least one field")
    return missing


def extract(payload, path: str):
    if not path:
        return None
    node = payload
    for part in path.split("."):
        if isinstance(node, dict):
            node = node.get(part)
        elif isinstance(node, list) and part.isdigit() and int(part) < len(node):
            node = node[int(part)]
        else:
            return None
        if node is None:
            return None
    return node


def _whole(value) -> str:
    return str(int(value)) if isinstance(value, float) and value.is_integer() else str(value)


def convert(field: str, value):
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    if field == "name":
        return str(value).strip() or None
    if field == "level":
        return parse_furnace_level(_whole(value))
    if field == "kingdom":
        return parse_state(_whole(value))
    if field == "power":
        return parse_power(_whole(value))
    url = str(value).strip()
    return url if url.startswith(("http://", "https://")) else None


def read_player(payload, profile: dict) -> dict:
    values = {}
    for name, column in FIELDS.items():
        value = convert(name, extract(payload, profile["fields"][name]))
        if value is not None:
            values[column] = value
    return values


def export_profile(profile: dict) -> str:
    return json.dumps(normalise(profile), indent=2)


def import_profile(text: str):
    try:
        raw = json.loads(text)
    except ValueError:
        return None, NOT_A_PROFILE
    if not isinstance(raw, dict):
        return None, NOT_A_PROFILE
    profile = normalise(raw)
    found = problems(profile)
    if found:
        return None, " ".join(found)
    return profile, None


REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)


@dataclass
class FetchResult:
    status: int
    payload: object = None
    retry_after: float | None = None


def player_url(profile: dict, fid) -> str:
    path = profile["player_path"].lstrip("/").replace("{fid}", str(int(fid)))
    return f"{profile['base_url'].rstrip('/')}/{path}"


async def fetch_player(session: aiohttp.ClientSession, profile: dict, key: str, fid) -> FetchResult:
    headers = {"Accept": "application/json"}
    if key:
        headers[profile["auth_header"]] = key
    try:
        async with session.get(player_url(profile, fid), headers=headers, timeout=REQUEST_TIMEOUT) as response:
            if response.status == 429:
                return FetchResult(429, retry_after=rate_limit_delay(response))
            if response.status != 200:
                return FetchResult(response.status)
            try:
                return FetchResult(200, await response.json(content_type=None))
            except ValueError:
                return FetchResult(200)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return FetchResult(0)


# --- Storage: settings, the daily request count and when members last synced ----------

USERS_DB = 'db/users.sqlite'
INTERVAL_DEFAULT_HOURS = 24
INTERVAL_MIN_HOURS = 1
INTERVAL_MAX_HOURS = 720


def _json_dict(value) -> dict:
    try:
        value = json.loads(value or "")
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def _interval_hours(value) -> int:
    try:
        return int(value or INTERVAL_DEFAULT_HOURS)
    except ValueError:
        return INTERVAL_DEFAULT_HOURS


def _datetime(value):
    try:
        return datetime.fromisoformat(value) if value else None
    except ValueError:
        return None


SETTINGS = {
    "profile": ('api_sync_profile', lambda value: normalise(_json_dict(value))),
    "key": ('api_sync_key', lambda value: value or ""),
    "enabled": ('api_sync_enabled', lambda value: value == '1'),
    "interval": ('api_sync_interval_hours', _interval_hours),
    "last_run": ('api_sync_last_run', _json_dict),
    "last_queued": ('api_sync_last_queued', _datetime),
    "budget": ('api_sync_budget', _json_dict),
}


def load_settings(*names) -> dict:
    stored = _settings(*(SETTINGS[name][0] for name in names))
    return {name: SETTINGS[name][1](stored.get(SETTINGS[name][0])) for name in names}


def _load(name):
    return load_settings(name)[name]


def load_connection():
    settings = load_settings("profile", "key")
    return settings["profile"], settings["key"]


def get_profile() -> dict:
    return _load("profile")


def save_profile(profile: dict) -> None:
    _put_setting('api_sync_profile', json.dumps(normalise(profile)))


def get_key() -> str:
    return _load("key")


def save_key(value: str) -> None:
    _put_setting('api_sync_key', value.strip())


def mask_key(key: str) -> str:
    if not key:
        return "not set"
    return f"••••{key[-4:]}" if len(key) > 8 else "set"


def is_enabled() -> bool:
    return _load("enabled")


def set_enabled(on: bool) -> None:
    _put_setting('api_sync_enabled', '1' if on else '0')


def get_interval_hours() -> int:
    return _load("interval")


def set_interval_hours(hours: int) -> None:
    if not INTERVAL_MIN_HOURS <= hours <= INTERVAL_MAX_HOURS:
        raise ValueError(f"must be {INTERVAL_MIN_HOURS}-{INTERVAL_MAX_HOURS} hours")
    _put_setting('api_sync_interval_hours', hours)


def save_last_run(last_run: dict) -> None:
    _put_setting('api_sync_last_run', json.dumps(last_run))


def get_last_queued():
    return _load("last_queued")


def set_last_queued(when: datetime) -> None:
    _put_setting('api_sync_last_queued', when.isoformat())


def used_today(budget: dict, today: str) -> int:
    return int(budget.get('used', 0)) if budget.get('date') == today else 0


def requests_today(today: str) -> int:
    return used_today(_load("budget"), today)


def save_request_count(today: str, used: int) -> None:
    _put_setting('api_sync_budget', json.dumps({'date': today, 'used': used}))


def count_request(today: str) -> None:
    save_request_count(today, requests_today(today) + 1)


def members_to_sync(alliance_id=None, before=None) -> list:
    where, params = "alliance IS NOT NULL AND alliance != ''", ()
    if alliance_id is not None:
        where, params = "alliance = ?", (str(alliance_id),)
    if before is not None:
        where, params = f"{where} AND (api_synced_at IS NULL OR api_synced_at < ?)", params + (before,)
    with closing(sqlite3.connect(USERS_DB, timeout=30.0)) as conn:
        return conn.execute(
            f"SELECT fid, alliance FROM users WHERE {where} "
            f"ORDER BY api_synced_at IS NOT NULL, api_synced_at, fid", params).fetchall()


def mark_synced(fid, when: str) -> None:
    with closing(sqlite3.connect(USERS_DB, timeout=30.0)) as conn:
        conn.execute("UPDATE users SET api_synced_at = ? WHERE fid = ?", (when, fid))
        conn.commit()


def member_snapshot(fid):
    with closing(sqlite3.connect(USERS_DB, timeout=30.0)) as conn:
        row = conn.execute("SELECT nickname, furnace_lv, kid, power, avatar_url FROM users WHERE fid = ?",
                           (fid,)).fetchone()
    return dict(zip(("nickname", "furnace_lv", "kid", "power", "avatar_url"), row)) if row else None


def save_avatar(fid, url: str) -> None:
    with closing(sqlite3.connect(USERS_DB, timeout=30.0)) as conn:
        conn.execute("UPDATE users SET avatar_url = ? WHERE fid = ?", (url, fid))
        conn.commit()


def log_channel_id(alliance_id):
    with closing(sqlite3.connect('db/settings.sqlite', timeout=30.0)) as conn:
        row = conn.execute("SELECT channel_id FROM alliance_logs WHERE alliance_id = ?",
                           (int(alliance_id),)).fetchone()
    return int(row[0]) if row and row[0] else None


# --- One run: fetch each member, write what changed, collect change lines -------------

STOPPED = "Stopped by an admin."
SERVER_ATTEMPTS = 3
RATE_LIMIT_RETRIES = 5
RATE_LIMIT_MAX_WAIT = 300.0
KEY_REJECTED = "The API didn't accept your key. Check it under API Key."
KEY_ERRORS = {
    401: KEY_REJECTED,
    403: KEY_REJECTED,
    402: "Your API plan doesn't cover this request. Check your account with the provider.",
}
NOT_JSON = "The API answered, but not with player data. Check the base URL and player path."
BUDGET_USED = "Today's request limit was reached. The next sync picks up where this one left off."
RATE_LIMITED = "The API asked the bot to wait too long. The next sync picks up where this one left off."
NAME_LENGTH = 40
_CHANGE_LABELS = {"name": ("Name", "nickname"), "level": ("Furnace", "furnace_lv"),
                  "state": ("State", "kid"), "power": ("Power", "power")}


@dataclass
class RunResult:
    checked: int = 0
    requests: int = 0
    changes: dict = field(default_factory=dict)
    kingdom_changed: list = field(default_factory=list)
    error: str | None = None
    stopped: str | None = None
    preempted: bool = False


def utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class _Pacer:
    def __init__(self, profile, sleep, clock, today):
        self.gap = 60.0 / profile["per_minute"]
        self.per_day = profile["per_day"]
        self.sleep, self.clock, self.today = sleep, clock, today
        self.last = None
        self.day, self.used = None, 0

    async def take(self) -> bool:
        day = self.today()
        if day != self.day:
            self.day, self.used = day, await asyncio.to_thread(requests_today, day)
        if self.used >= self.per_day:
            return False
        if self.last is not None:
            wait = self.gap - (self.clock() - self.last)
            if wait > 0:
                await self.sleep(wait)
        self.last = self.clock()
        self.used += 1
        await asyncio.to_thread(save_request_count, day, self.used)
        return True


def _should_stop(queue, result) -> bool:
    if queue and queue.stop_requested():
        result.stopped = STOPPED
    elif queue and queue.should_preempt():
        result.preempted = True
    return bool(result.stopped or result.preempted)


async def _fetch(fetch, fid, pacer, sleep, result, queue):
    """The member's answer after retries, or None when the run has to end."""
    server_failures = rate_limited = 0
    while True:
        if not await pacer.take():
            result.stopped = BUDGET_USED
            return None
        result.requests += 1
        answer = await fetch(fid)
        if answer.status == 429 and rate_limited < RATE_LIMIT_RETRIES:
            rate_limited += 1
            wait = 60.0 if answer.retry_after is None else answer.retry_after
            if wait > RATE_LIMIT_MAX_WAIT:
                logger.warning(f"Player Sync: API asked for a {wait:.0f}s wait, run stopped")
                result.stopped = RATE_LIMITED
                return None
            if _should_stop(queue, result):
                return None
            logger.warning(f"Player Sync: rate limited by the API, waiting {wait:.0f}s")
            await sleep(wait)
            continue
        if (answer.status == 0 or answer.status >= 500) and server_failures < SERVER_ATTEMPTS - 1:
            server_failures += 1
            await sleep(2.0 ** server_failures)
            continue
        return answer


def _show(column, value):
    if value is None:
        return "-"
    if column == "furnace_lv":
        return format_furnace_level(value)
    if column == "power":
        return f"{value:,}"
    return str(value)[:NAME_LENGTH]


def _apply(fid, alliance, values, result):
    before = member_snapshot(fid)
    if before is None:
        return
    avatar = values.pop("avatar_url", None)
    if avatar and avatar != before["avatar_url"]:
        save_avatar(fid, avatar)
    changed = apply_member_edit(fid, nickname=values.get("nickname"), furnace_lv=values.get("furnace_lv"),
                                kid=values.get("kid"), power=values.get("power"))
    if "state" in changed:
        result.kingdom_changed.append(fid)
    name = str(before["nickname"] or fid)[:NAME_LENGTH]
    for label in changed:
        title, column = _CHANGE_LABELS[label]
        result.changes.setdefault(alliance, []).append(
            f"`{fid}` {name}: {title} {_show(column, before[column])} → {_show(column, values[column])}")


def _apply_and_mark(fid, alliance, values, result):
    _apply(fid, alliance, values, result)
    mark_synced(fid, utc_now())


async def run_sync(fetch, profile, *, alliance_id=None, queue=None, started_at=None,
                   sleep=asyncio.sleep, clock=time.monotonic, today=utc_today) -> RunResult:
    result = RunResult()
    missing = missing_for_sync(profile)
    if missing:
        result.error = f"Player Sync still needs the {', '.join(missing)} before it can run."
        return result
    pacer = _Pacer(profile, sleep, clock, today)
    for fid, alliance in await asyncio.to_thread(members_to_sync, alliance_id, started_at):
        if _should_stop(queue, result):
            break
        try:
            answer = await _fetch(fetch, fid, pacer, sleep, result, queue)
            if answer is None:
                break
            if answer.status in KEY_ERRORS:
                result.error = KEY_ERRORS[answer.status]
                logger.error(f"Player Sync: API answered HTTP {answer.status}, run stopped")
                break
            if answer.status == 200 and answer.payload is None:
                result.error = NOT_JSON
                logger.error("Player Sync: API answered without JSON, run stopped")
                break
            if answer.status == 0 or answer.status >= 429:
                logger.warning(f"Player Sync: no usable answer for {fid} (HTTP {answer.status}), skipped")
                continue
            if answer.status == 200:
                await asyncio.to_thread(_apply_and_mark, fid, alliance, read_player(answer.payload, profile), result)
            else:
                logger.info(f"Player Sync: API has no data for {fid} (HTTP {answer.status})")
                await asyncio.to_thread(mark_synced, fid, utc_now())
            result.checked += 1
        except Exception as e:
            logger.warning(f"Player Sync: {fid} failed, skipped: {e}")
    return result


def summary(result: RunResult, scope: str) -> dict:
    return {"finished_at": utc_now(), "scope": scope, "checked": result.checked,
            "changes": sum(len(lines) for lines in result.changes.values()),
            "requests": result.requests, "error": result.error, "stopped": result.stopped}


# --- Cog: schedule, queue handler and change-log posts --------------------------------

CHANGE_LINES_PER_POST = 20


def is_due(now, last_queued, interval_hours) -> bool:
    return last_queued is None or now - last_queued >= timedelta(hours=interval_hours)


def change_embeds(changes: dict, names: dict) -> list:
    embeds = []
    for alliance, lines in changes.items():
        shown = lines[:CHANGE_LINES_PER_POST]
        if len(lines) > len(shown):
            shown = shown + [f"and {len(lines) - len(shown)} more"]
        name = names.get(int(alliance), f"Alliance {alliance}")
        embeds.append((alliance, discord.Embed(title=f"{theme.retryIcon} Player Sync: {name}",
                                               description="\n".join(shown), color=theme.emColor1)))
    return embeds


def _started_at(process, queue) -> str:
    details = process.get('details') or {}
    if details.get('started_at'):
        return details['started_at']
    started_at = utc_now()
    if queue:
        queue.update_details(process['id'], {**details, 'started_at': started_at})
    return started_at


class AllianceApiSync(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._session = None

    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def show_player_sync(self, interaction: discord.Interaction):
        from . import alliance_api_menu
        await alliance_api_menu.open_player_sync(self, interaction)

    async def cog_load(self):
        if self.bot.is_ready():
            self._start()

    @commands.Cog.listener()
    async def on_ready(self):
        self._start()

    def _start(self):
        if not self.schedule_loop.is_running():
            self.schedule_loop.start()
        queue = self.bot.get_cog('ProcessQueue')
        if queue:
            self.register_queue_handlers(queue)
        else:
            logger.error("Player Sync: ProcessQueue cog not found, syncs can't run")

    async def cog_unload(self):
        self.schedule_loop.cancel()
        if self._session and not self._session.closed:
            await self._session.close()

    def register_queue_handlers(self, queue):
        queue.register_handler('api_sync', self.handle_api_sync)

    def sync_job(self):
        queue = self.bot.get_cog('ProcessQueue')
        if not queue:
            return None
        jobs = queue.get_queued_processes_by_action('api_sync', statuses=('queued', 'active'))
        return jobs[0] if jobs else None

    def queue_sync(self, alliance_id=None) -> str:
        queue = self.bot.get_cog('ProcessQueue')
        if not queue:
            logger.warning("Player Sync: ProcessQueue cog not found, sync not queued")
            return 'unavailable'
        if self.sync_job():
            return 'busy'
        queue.enqueue('api_sync', PLAYER_SYNC, alliance_id=alliance_id, details={'alliance_id': alliance_id})
        set_last_queued(datetime.now(timezone.utc))
        return 'queued'

    @tasks.loop(minutes=5)
    async def schedule_loop(self):
        try:
            state = await asyncio.to_thread(load_settings, "enabled", "profile", "last_queued", "interval")
            if not state["enabled"] or missing_for_sync(state["profile"]):
                return
            if is_due(datetime.now(timezone.utc), state["last_queued"], state["interval"]):
                self.queue_sync()
        except Exception as e:
            logger.warning(f"Player Sync: schedule check failed: {e}")

    async def handle_api_sync(self, process):
        alliance_id = (process.get('details') or {}).get('alliance_id')
        queue = self.bot.get_cog('ProcessQueue')
        started_at = _started_at(process, queue)
        profile, key = await asyncio.to_thread(load_connection)
        session = self.session()
        result = await run_sync(lambda fid: fetch_player(session, profile, key, fid), profile,
                                alliance_id=alliance_id, queue=queue, started_at=started_at)
        names = {}
        if alliance_id or result.changes:
            names = dict(await asyncio.to_thread(PermissionManager.list_alliances))
        scope = names.get(alliance_id, f"Alliance {alliance_id}") if alliance_id else "All alliances"
        await asyncio.to_thread(save_last_run, summary(result, scope))
        if result.kingdom_changed:
            enqueue_catchups(self.bot, result.kingdom_changed)
        await self.post_changes(result.changes, names)
        logger.info(f"Player Sync: {scope}, {result.checked} checked, {result.requests} requests, "
                    f"stopped={result.stopped}, error={result.error}")
        if result.preempted:
            raise PreemptedException()

    async def post_changes(self, changes: dict, names: dict):
        for alliance, embed in change_embeds(changes, names):
            channel_id = await asyncio.to_thread(log_channel_id, alliance)
            channel = self.bot.get_channel(channel_id) if channel_id else None
            if channel is None:
                continue
            try:
                await channel.send(embed=embed)
            except discord.HTTPException as e:
                logger.warning(f"Player Sync: couldn't post changes for alliance {alliance}: {e}")


async def setup(bot):
    await bot.add_cog(AllianceApiSync(bot))
