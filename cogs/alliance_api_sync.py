"""Player Sync: keeps member names, levels, kingdoms, power and pictures current from a player-data API."""
import asyncio
import json
import logging
import sqlite3
import threading
import time
from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import aiohttp
import discord
from discord.ext import commands, tasks

from .alliance_member_edit import apply_member_edit, enqueue_catchups, new_member_name, parse_power
from .bot_level_mapping import format_furnace_level, parse_furnace_level, parse_state
from .gift_redemption import apply_removals, auto_remove_on, maybe_remove_transferred_member, rate_limit_delay
from . import gift_state_resolver
from .gift_state_resolver import _put_setting, _settings, left_alliance_state
from .permission_handler import PermissionManager
from .pimp_my_bot import count_text, theme
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
PLAYER_ID_PLACEHOLDERS = ("{id}", "{fid}")
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
    if not isinstance(player_path, str) or (player_path and not any(p in player_path for p in PLAYER_ID_PLACEHOLDERS)):
        found.append("Put {id} in the player path where the player ID goes.")
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


# Some APIs hold a request up to 90 seconds while they refresh stale player data.
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=100)


@dataclass
class FetchResult:
    status: int
    payload: object = None
    retry_after: float | None = None


def player_url(profile: dict, fid) -> str:
    path = profile["player_path"].lstrip("/")
    for placeholder in PLAYER_ID_PLACEHOLDERS:
        path = path.replace(placeholder, str(int(fid)))
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
SETTINGS_DB = 'db/settings.sqlite'
INTERVAL_DEFAULT_HOURS = 24
INTERVAL_MIN_HOURS = 1
INTERVAL_MAX_HOURS = 720
KEEP_MINUTES_DEFAULT = 5
KEEP_MINUTES_MIN = 1
KEEP_MINUTES_MAX = 1440


def _json_dict(value) -> dict:
    try:
        value = json.loads(value or "")
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def _datetime(value):
    try:
        return datetime.fromisoformat(value) if value else None
    except ValueError:
        return None


def _int_or(default):
    def parse(value) -> int:
        try:
            return int(value or default)
        except ValueError:
            return default
    return parse


def _int_within(low, high, unit):
    def check(value: int) -> int:
        if not low <= value <= high:
            raise ValueError(f"must be {low}-{high} {unit}")
        return value
    return check


def _flag(on: bool) -> str:
    return '1' if on else '0'


# name: (settings key, read the stored text, value to store)
SETTINGS = {
    "profile": ('api_sync_profile', lambda value: normalise(_json_dict(value)),
                lambda profile: json.dumps(normalise(profile))),
    "key": ('api_sync_key', lambda value: value or "", str.strip),
    "enabled": ('api_sync_enabled', lambda value: value == '1', _flag),
    "interval": ('api_sync_interval_hours', _int_or(INTERVAL_DEFAULT_HOURS),
                 _int_within(INTERVAL_MIN_HOURS, INTERVAL_MAX_HOURS, "hours")),
    "last_run": ('api_sync_last_run', _json_dict, json.dumps),
    "last_queued": ('api_sync_last_queued', _datetime, datetime.isoformat),
    "budget": ('api_sync_budget', _json_dict, json.dumps),
    "autodelete": ('api_sync_autodelete', lambda value: value != '0', _flag),
    "keep_minutes": ('api_sync_keep_minutes', _int_or(KEEP_MINUTES_DEFAULT),
                     _int_within(KEEP_MINUTES_MIN, KEEP_MINUTES_MAX, "minutes")),
}


def load_settings(*names) -> dict:
    stored = _settings(*(SETTINGS[name][0] for name in names))
    return {name: SETTINGS[name][1](stored.get(SETTINGS[name][0])) for name in names}


def load_setting(name):
    return load_settings(name)[name]


def save_setting(name, value) -> None:
    key, _, to_text = SETTINGS[name]
    _put_setting(key, to_text(value))


def load_connection():
    settings = load_settings("profile", "key")
    return settings["profile"], settings["key"]


def mask_key(key: str) -> str:
    if not key:
        return "not set"
    return f"••••{key[-4:]}" if len(key) > 8 else "set"


def syncs_names() -> bool:
    """True when Auto Sync is on and keeps names up to date."""
    settings = load_settings("enabled", "profile")
    profile = settings["profile"]
    return settings["enabled"] and not missing_for_sync(profile) and bool(profile["fields"]["name"])


def used_today(budget: dict, today: str) -> int:
    return int(budget.get('used', 0)) if budget.get('date') == today else 0


_BUDGET_LOCK = threading.Lock()


def take_request(today: str, per_day=None) -> bool:
    """Counts one request on one settings connection; False, uncounted, once `per_day` are used today."""
    key = SETTINGS["budget"][0]
    with _BUDGET_LOCK, closing(sqlite3.connect(SETTINGS_DB, timeout=30.0)) as conn:
        row = conn.execute("SELECT setting_value FROM bot_global_settings WHERE setting_key = ?", (key,)).fetchone()
        used = used_today(_json_dict(row[0] if row else None), today)
        if per_day is not None and used >= per_day:
            return False
        conn.execute("INSERT OR REPLACE INTO bot_global_settings (setting_key, setting_value) VALUES (?, ?)",
                     (key, json.dumps({'date': today, 'used': used + 1})))
        conn.commit()
        return True


def budget_spent() -> bool:
    """True when the API is set up and today's request limit is used up; a failed read counts as not."""
    try:
        settings = load_settings("profile", "budget")
        profile = settings["profile"]
        return api_available(profile) and used_today(settings["budget"], utc_today()) >= profile["per_day"]
    except Exception as e:
        logger.warning(f"Player Sync: could not read the API request count: {e}")
        return False


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


@contextmanager
def _users(conn=None):
    """`conn`, or a users.sqlite connection of its own that commits and closes."""
    if conn is not None:
        yield conn
        return
    with closing(sqlite3.connect(USERS_DB, timeout=30.0)) as own:
        yield own
        own.commit()


def mark_synced(fid, when: str, conn=None) -> None:
    with _users(conn) as users:
        users.execute("UPDATE users SET api_synced_at = ? WHERE fid = ?", (when, fid))


_SNAPSHOT = ("nickname", "furnace_lv", "kid", "power", "avatar_url")


def member_snapshot(fid, conn=None):
    with _users(conn) as users:
        row = users.execute(f"SELECT {', '.join(_SNAPSHOT)} FROM users WHERE fid = ?", (fid,)).fetchone()
    return dict(zip(_SNAPSHOT, row)) if row else None


def save_avatar(fid, url: str, conn=None) -> None:
    with _users(conn) as users:
        users.execute("UPDATE users SET avatar_url = ? WHERE fid = ?", (url, fid))


ALLIANCE_DB = 'db/alliance.sqlite'


def sync_channels() -> dict:
    with closing(sqlite3.connect(ALLIANCE_DB, timeout=30.0)) as conn:
        rows = conn.execute("SELECT alliance_id, channel_id FROM alliancesettings "
                            "WHERE channel_id IS NOT NULL AND channel_id != 0").fetchall()
    return {str(alliance_id): int(channel_id) for alliance_id, channel_id in rows}


def set_sync_channel(alliance_id, channel_id: int) -> None:
    with closing(sqlite3.connect(ALLIANCE_DB, timeout=30.0)) as conn:
        conn.execute("INSERT INTO alliancesettings (alliance_id, channel_id) VALUES (?, ?) "
                     "ON CONFLICT(alliance_id) DO UPDATE SET channel_id = excluded.channel_id",
                     (int(alliance_id), int(channel_id)))
        conn.commit()


def clear_sync_channel(alliance_id) -> None:
    with closing(sqlite3.connect(ALLIANCE_DB, timeout=30.0)) as conn:
        conn.execute("UPDATE alliancesettings SET channel_id = NULL WHERE alliance_id = ?", (int(alliance_id),))
        conn.commit()


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
class Transfer:
    alliance: str
    fid: int
    name: str
    kid: int
    removed: bool = False
    pending: bool = False   # new state not written yet: Auto-remove decides first


@dataclass
class RunResult:
    checked: int = 0
    requests: int = 0
    changes: dict = field(default_factory=dict)
    kingdom_changed: list = field(default_factory=list)
    transfers: list = field(default_factory=list)
    error: str | None = None
    stopped: str | None = None
    preempted: bool = False


def utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class _LookupGate:
    """Spaces API requests by the profile's per-minute limit, across syncs and every lookup."""
    def __init__(self, sleep=asyncio.sleep, clock=time.monotonic):
        self.sleep, self.clock = sleep, clock
        self.last = None
        self._lock, self._loop = None, None

    async def wait(self, per_minute):
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self._lock, self._loop = asyncio.Lock(), loop
        async with self._lock:
            if self.last is not None:
                wait = 60.0 / per_minute - (self.clock() - self.last)
                if wait > 0:
                    await self.sleep(wait)
            self.last = self.clock()


_LOOKUP_GATE = _LookupGate()


async def _take_slot(profile, today=utc_today) -> bool:
    """Counts one request and waits its turn; False when today's limit is used up."""
    if not await asyncio.to_thread(take_request, today(), profile["per_day"]):
        return False
    await _LOOKUP_GATE.wait(profile["per_minute"])
    return True


def _should_stop(queue, result) -> bool:
    if queue and queue.stop_requested():
        result.stopped = STOPPED
    elif queue and queue.should_preempt():
        result.preempted = True
    return bool(result.stopped or result.preempted)


async def _fetch(fetch, fid, take_slot, sleep, result, queue):
    """The member's answer after retries, or None when the run has to end."""
    server_failures = rate_limited = 0
    while True:
        if not await take_slot():
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


def _change_line(fid, name, title, column, old, new):
    if old is None:
        return f"`{fid}` {name}: {title} set to {_show(column, new)}"
    return f"`{fid}` {name}: {title} {_show(column, old)} → {_show(column, new)}"


def _transfer_to(alliance, before, values):
    kid = values.get("kid")
    if kid is None or kid == before["kid"] or not left_alliance_state(alliance, kid):
        return None
    return kid


def _write_member(conn, fid, alliance, before, values, result):
    avatar = values.pop("avatar_url", None)
    if avatar and avatar != before["avatar_url"]:
        save_avatar(fid, avatar, conn)
    transfer_kid = _transfer_to(alliance, before, values)
    pending = transfer_kid is not None and auto_remove_on(alliance)
    changed = apply_member_edit(fid, nickname=values.get("nickname"), furnace_lv=values.get("furnace_lv"),
                                kid=None if pending else values.get("kid"), power=values.get("power"), conn=conn)
    name = str(before["nickname"] or fid)[:NAME_LENGTH]
    if transfer_kid is not None:
        result.transfers.append(Transfer(str(alliance), fid, name, transfer_kid, pending=pending))
    if "state" in changed:
        result.kingdom_changed.append(fid)
        if transfer_kid is not None:
            changed.remove("state")
    for label in changed:
        title, column = _CHANGE_LABELS[label]
        result.changes.setdefault(alliance, []).append(
            _change_line(fid, name, title, column, before[column], values[column]))


def _apply(fid, alliance, values, result):
    with closing(sqlite3.connect(USERS_DB, timeout=30.0)) as conn:
        before = member_snapshot(fid, conn)
        if before is not None:
            _write_member(conn, fid, alliance, before, values, result)
        mark_synced(fid, utc_now(), conn)
        conn.commit()


async def _sync_member(fetch, profile, fid, alliance, take_slot, sleep, result, queue) -> bool:
    """False when the run has to end."""
    answer = await _fetch(fetch, fid, take_slot, sleep, result, queue)
    if answer is None:
        return False
    if answer.status in KEY_ERRORS:
        result.error = KEY_ERRORS[answer.status]
        logger.error(f"Player Sync: API answered HTTP {answer.status}, run stopped")
        return False
    if answer.status == 200 and answer.payload is None:
        result.error = NOT_JSON
        logger.error("Player Sync: API answered without JSON, run stopped")
        return False
    if answer.status == 0 or answer.status >= 429:
        logger.warning(f"Player Sync: no usable answer for {fid} (HTTP {answer.status}), skipped")
        return True
    if answer.status == 200:
        await asyncio.to_thread(_apply, fid, alliance, read_player(answer.payload, profile), result)
    else:
        logger.info(f"Player Sync: API has no data for {fid} (HTTP {answer.status})")
        await asyncio.to_thread(mark_synced, fid, utc_now())
    result.checked += 1
    return True


async def run_sync(fetch, profile, *, alliance_id=None, queue=None, started_at=None, reporter=None,
                   sleep=asyncio.sleep, today=utc_today) -> RunResult:
    result = RunResult()
    missing = missing_for_sync(profile)
    if missing:
        result.error = f"Player Sync still needs the {', '.join(missing)} before it can run."
        return result
    members = await asyncio.to_thread(members_to_sync, alliance_id, started_at)
    if reporter:
        await reporter.start(Counter(str(alliance) for _, alliance in members))
    for fid, alliance in members:
        if _should_stop(queue, result):
            break
        try:
            if not await _sync_member(fetch, profile, fid, alliance, lambda: _take_slot(profile, today),
                                      sleep, result, queue):
                break
        except Exception as e:
            logger.warning(f"Player Sync: {fid} failed, skipped: {e}")
        if reporter:
            await reporter.advance(str(alliance))
    return result


def _write_state(fid, kid) -> bool:
    return "state" in apply_member_edit(fid, kid=kid)


async def remove_transfers(cog, result: RunResult) -> set:
    """Hands transfers to gift redemption's Auto-remove Transfers path; returns the IDs removed.
    A held-back removal keeps the old state, flagged; a pending transfer that isn't a candidate
    after all gets its new state now."""
    candidates = []
    for transfer in result.transfers:
        if not await maybe_remove_transferred_member(cog, transfer.fid, candidates) and transfer.pending:
            if await asyncio.to_thread(_write_state, transfer.fid, transfer.kid):
                result.kingdom_changed.append(transfer.fid)
    if not candidates:
        return set()
    removed = {fid for _, fid, _ in await apply_removals(cog, candidates)}
    for transfer in result.transfers:
        transfer.removed = transfer.fid in removed
    return removed


# --- One-off lookups for other cogs: one player, counted against the daily budget -----

def api_available(profile=None) -> bool:
    return not missing_for_sync(profile if profile is not None else load_setting("profile"))


async def api_available_async() -> bool:
    """Whether the API is set up; a failed check counts as not."""
    try:
        return await asyncio.to_thread(api_available)
    except Exception as e:
        logger.warning(f"Player Sync: could not check whether the API is set up: {e}")
        return False


NOT_SET_UP = "not_set_up"
BUDGET_SPENT = "budget_spent"
FAILED = "failed"


@dataclass(frozen=True)
class Lookup:
    """`values` when the API knew the player, else `reason`: None for unknown (404), or why it wasn't asked or failed."""
    values: dict | None = None
    reason: str | None = None

    @property
    def failed(self) -> bool:
        return self.reason == FAILED

    @property
    def kid(self):
        return (self.values or {}).get("kid")


async def lookup_player(session, fid) -> Lookup:
    try:
        profile, key = await asyncio.to_thread(load_connection)
        if not api_available(profile):
            return Lookup(reason=NOT_SET_UP)
        if not await _take_slot(profile):
            return Lookup(reason=BUDGET_SPENT)
        answer = await fetch_player(session, profile, key, fid)
        if answer.status == 200 and answer.payload is not None:
            return Lookup(read_player(answer.payload, profile))
        if answer.status == 404:
            return Lookup()
        logger.warning(f"Player Sync: lookup of {fid} gave no usable answer (HTTP {answer.status})")
    except Exception as e:
        logger.warning(f"Player Sync: lookup of {fid} failed: {e}")
    return Lookup(reason=FAILED)


async def lookup_via_bot(bot, fid) -> Lookup:
    """One player's lookup, NOT_SET_UP when Player Sync isn't loaded. Never raises."""
    cog = bot.get_cog("AllianceApiSync")
    return await cog.lookup(fid) if cog is not None else Lookup(reason=NOT_SET_UP)


class LookupBatch:
    """Lookups for a batch of adds: after the first failure the rest skip the API and `note` is logged."""
    def __init__(self, note):
        self.note, self.failed = note, False

    def record(self, lookup: Lookup):
        if lookup.failed and not self.failed:
            self.failed = True
            logger.warning(self.note)


@dataclass
class NewMember:
    nickname: str
    furnace_lv: int
    kid: int | None
    power: int | None = None
    avatar_url: str | None = None
    from_api: bool = False
    verified: bool = False


async def probe_state(bot, fid, alliance_id):
    """(kid, verified) from one probe in the alliance's state; (None, False) without Gift Codes."""
    gift_cog = bot.get_cog("GiftOperations")
    if gift_cog is None:
        return None, False
    return await gift_state_resolver.verify_add_state(gift_cog, fid, alliance_id)


async def look_up_new_member(bot, fid, alliance_id, name=None, level=None, *, probe=True, batch=None) -> NewMember:
    """A new member's name, level and state from the API, else the typed values (a typed name wins);
    without an API state, `probe` asks the game once whether they're in the alliance's state."""
    skip_api = batch is not None and batch.failed
    lookup = Lookup(reason=FAILED) if skip_api else await lookup_via_bot(bot, fid)
    if batch is not None:
        batch.record(lookup)
    api = lookup.values or {}
    member = NewMember(new_member_name((name or "").strip() or api.get("nickname"), fid),
                       api.get("furnace_lv") or level or 0, api.get("kid"),
                       api.get("power"), api.get("avatar_url"), lookup.values is not None)
    if member.kid is None and probe:
        member.kid, member.verified = await probe_state(bot, fid, alliance_id)
    return member


def _store_api_values(member: NewMember, fid):
    with closing(sqlite3.connect(USERS_DB, timeout=30.0)) as conn:
        if member.avatar_url:
            save_avatar(fid, member.avatar_url, conn)
        apply_member_edit(fid, power=member.power, conn=conn)
        mark_synced(fid, utc_now(), conn)
        conn.commit()


async def store_new_member(member: NewMember, fid) -> None:
    """Writes the API's power and picture for a member just inserted, and marks them synced."""
    if not member.from_api:
        return
    try:
        await asyncio.to_thread(_store_api_values, member, fid)
    except Exception as e:
        logger.warning(f"Player Sync: could not store the API values for new member {fid}: {e}")


def summary(result: RunResult, scope: str) -> dict:
    return {"finished_at": utc_now(), "scope": scope, "checked": result.checked,
            "changes": sum(len(lines) for lines in result.changes.values()) + len(result.transfers),
            "requests": result.requests, "error": result.error, "stopped": result.stopped}


# --- Sync Log: one message per alliance, progress while the run goes, then the results --

CHANGE_LINES_PER_POST = 20
PROGRESS_EDIT_SECONDS = 15.0
NO_CHANGES = "Everyone checked is already up to date."


def _sync_log_embed(name, description, color) -> discord.Embed:
    return discord.Embed(title=f"{theme.retryIcon} Player Sync: {name}", description=description, color=color)


def progress_embed(name, done, total) -> discord.Embed:
    return _sync_log_embed(name, f"Syncing {name}…\n`{done}` of {count_text(total, 'member')} checked so far.",
                           theme.emColor1)


def transfer_note(transfer: Transfer) -> str:
    note = f"`{transfer.fid}` {transfer.name} moved to State {transfer.kid}"
    return f"{note} and was removed from the alliance" if transfer.removed else note


def alliance_lines(result: RunResult, alliance) -> list:
    notes = [transfer_note(t) for t in result.transfers if t.alliance == alliance]
    return notes + result.changes.get(alliance, [])


def change_lines(lines) -> list:
    shown = lines[:CHANGE_LINES_PER_POST]
    if len(lines) > len(shown):
        shown = shown + [f"and {len(lines) - len(shown)} more"]
    return shown


def paused_embed(name, done, total, lines) -> discord.Embed:
    text = [f"Paused after `{done}` of {count_text(total, 'member')} so the bot can finish other work first. "
            f"The sync continues on its own and posts a new message."]
    if lines:
        text += [""] + change_lines(lines)
    return _sync_log_embed(name, "\n".join(text), theme.emColor1)


def results_embed(name, done, total, lines, result: RunResult) -> discord.Embed:
    text = [f"Checked `{done}` of {count_text(total, 'member')}."]
    if lines:
        text += [""] + change_lines(lines)
    elif done and not result.error:
        text += ["", NO_CHANGES]
    reason = result.error or result.stopped
    if reason:
        text.append(f"\n{theme.warnIcon} {reason}")
    return _sync_log_embed(name, "\n".join(text), theme.emColor2 if result.error else theme.emColor3)


def removal_note(keep_minutes) -> str:
    return f"This message removes itself after {count_text(keep_minutes, 'minute')}."


@dataclass
class _Post:
    message: object
    name: str
    total: int
    edited_at: float
    done: int = 0


class SyncLogReporter:
    """Keeps one message per alliance with a Sync Log channel up to date during a run."""

    def __init__(self, get_channel, channels: dict, names: dict, keep_minutes=None, clock=time.monotonic):
        self.get_channel, self.channels, self.names = get_channel, channels, names
        self.keep_minutes, self.clock = keep_minutes, clock
        self.posts = {}

    async def start(self, totals: dict):
        for alliance, total in totals.items():
            channel_id = self.channels.get(alliance)
            if channel_id:
                await self._post(alliance, channel_id, total)

    async def _post(self, alliance, channel_id, total):
        name = self.names.get(int(alliance), f"Alliance {alliance}")
        try:
            channel = self.get_channel(channel_id)
            if channel is None:
                logger.warning(f"Player Sync: Sync Log channel {channel_id} for {name} wasn't found, "
                               f"no message posted")
                return
            message = await channel.send(embed=progress_embed(name, 0, total))
        except Exception as e:
            logger.warning(f"Player Sync: couldn't post in the Sync Log channel for {name}: {e}")
            return
        self.posts[alliance] = _Post(message, name, total, self.clock())

    async def advance(self, alliance):
        post = self.posts.get(alliance)
        if post is None:
            return
        post.done += 1
        if self.clock() - post.edited_at >= PROGRESS_EDIT_SECONDS:
            post.edited_at = self.clock()
            await self._edit(alliance, progress_embed(post.name, post.done, post.total))

    async def finish(self, result: RunResult):
        for alliance, post in list(self.posts.items()):
            lines = alliance_lines(result, alliance)
            if result.preempted:
                embed = paused_embed(post.name, post.done, post.total, lines)
            else:
                embed = results_embed(post.name, post.done, post.total, lines, result)
            if self.keep_minutes:
                embed.set_footer(text=removal_note(self.keep_minutes))
            if await self._edit(alliance, embed) and self.keep_minutes:
                await self._delete_later(post)

    async def _delete_later(self, post):
        try:
            await post.message.delete(delay=self.keep_minutes * 60)
        except Exception as e:
            logger.warning(f"Player Sync: couldn't schedule removal of the Sync Log message for {post.name}: {e}")

    async def _edit(self, alliance, embed) -> bool:
        post = self.posts[alliance]
        try:
            await post.message.edit(embed=embed)
            return True
        except Exception as e:
            logger.warning(f"Player Sync: couldn't update the Sync Log message for {post.name}: {e}")
            del self.posts[alliance]
            return False


# --- Cog: schedule and queue handler ---------------------------------------------------

def is_due(now, last_queued, interval_hours) -> bool:
    return last_queued is None or now - last_queued >= timedelta(hours=interval_hours)


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
        self.logger = logger
        self._session = None

    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def lookup(self, fid) -> Lookup:
        return await lookup_player(self.session(), fid)

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
        save_setting("last_queued", datetime.now(timezone.utc))
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
        names = dict(await asyncio.to_thread(PermissionManager.list_alliances))
        reporter = await asyncio.to_thread(self._sync_log_reporter, names)
        session = self.session()
        result = await run_sync(lambda fid: fetch_player(session, profile, key, fid), profile,
                                alliance_id=alliance_id, queue=queue, started_at=started_at, reporter=reporter)
        scope = names.get(alliance_id, f"Alliance {alliance_id}") if alliance_id else "All alliances"
        await asyncio.to_thread(save_setting, "last_run", summary(result, scope))
        removed = await self._remove_transfers(result)
        catchups = [fid for fid in result.kingdom_changed if fid not in removed]
        if catchups:
            enqueue_catchups(self.bot, catchups)
        try:
            await reporter.finish(result)
        except Exception as e:
            logger.warning(f"Player Sync: couldn't finish the Sync Log messages: {e}")
        logger.info(f"Player Sync: {scope}, {result.checked} checked, {result.requests} requests, "
                    f"stopped={result.stopped}, error={result.error}")
        if result.preempted:
            raise PreemptedException()

    async def _remove_transfers(self, result) -> set:
        try:
            return await remove_transfers(self, result)
        except Exception as e:
            logger.error(f"Player Sync: couldn't auto-remove transferred members: {e}")
            return set()

    def _sync_log_reporter(self, names) -> SyncLogReporter:
        settings = load_settings("autodelete", "keep_minutes")
        keep = settings["keep_minutes"] if settings["autodelete"] else None
        return SyncLogReporter(self._channel, sync_channels(), names, keep)

    def _channel(self, channel_id):
        return self.bot.get_channel(channel_id)


async def setup(bot):
    await bot.add_cog(AllianceApiSync(bot))
