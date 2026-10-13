"""Player Sync: profile, storage, a sync run and the cog."""
import asyncio
import json
import sqlite3
import types
from contextlib import closing
from datetime import datetime, timedelta, timezone

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import cogs.alliance_api_sync as sync
import cogs.process_queue as pq
from cogs.alliance_api_sync import FetchResult, normalise
from cogs.bot_health import describe_job
from harness import kid_and_flag


@pytest.fixture(autouse=True)
def instant_lookup_gate(monkeypatch):
    async def no_wait(seconds):
        pass
    monkeypatch.setattr(sync, "_LOOKUP_GATE", sync._LookupGate(sleep=no_wait))


# --- Profile: validation, import/export, JSON paths, value reading, fetch -------------

def _profile(**overrides):
    profile = sync.normalise({
        "base_url": "https://api.example.com",
        "player_path": "/v1/players/{id}",
        "fields": {"name": "player.nick_name", "level": "player.furnace_level",
                   "kingdom": "player.kid", "power": "player.power", "avatar": "player.avatar_url"},
    })
    profile.update(overrides)
    return profile


PAYLOAD = {"player": {"nick_name": " Alpha ", "furnace_level": 45, "kid": 172,
                      "power": 123456789, "avatar_url": "https://img.example.com/a.png",
                      "heroes": [{"name": "First"}, {"name": "Second"}]}}


def test_defaults_are_off_and_empty():
    profile = sync.normalise({})
    assert profile["base_url"] == "" and profile["player_path"] == ""
    assert profile["auth_header"] == "Authorization"
    assert set(profile["fields"]) == set(sync.FIELDS)
    assert all(path == "" for path in profile["fields"].values())


def test_normalise_drops_unknown_keys_including_keys_that_look_like_secrets():
    profile = sync.normalise({"base_url": "https://x", "api_key": "secret", "key": "secret", "token": "t"})
    assert "api_key" not in profile and "key" not in profile and "token" not in profile


def test_valid_profile_has_no_problems():
    assert sync.problems(_profile()) == []


@pytest.mark.parametrize("overrides, fragment", [
    ({"base_url": "ftp://api.example.com"}, "http"),
    ({"player_path": "/v1/players/"}, "{id}"),
    ({"per_minute": 0}, "per minute"),
    ({"per_minute": 601}, "per minute"),
    ({"per_day": "1000"}, "per day"),
    ({"per_day": True}, "per day"),
    ({"auth_header": "X Api Key"}, "header"),
    ({"auth_header": ""}, "header"),
])
def test_problems_name_the_bad_setting(overrides, fragment):
    found = " ".join(sync.problems(_profile(**overrides)))
    assert fragment in found


def test_field_path_must_be_text():
    profile = _profile()
    profile["fields"]["power"] = 5
    assert sync.problems(profile)


def test_missing_for_sync():
    assert sync.missing_for_sync(_profile()) == []
    empty = sync.normalise({})
    assert sync.missing_for_sync(empty) == ["base URL", "player path", "at least one field"]


def test_export_import_round_trip():
    text = sync.export_profile(_profile())
    profile, error = sync.import_profile(text)
    assert error is None and profile == _profile()


def test_import_never_carries_a_key():
    raw = json.loads(sync.export_profile(_profile()))
    raw["api_key"] = "secret"
    profile, error = sync.import_profile(json.dumps(raw))
    assert error is None and "secret" not in json.dumps(profile)


@pytest.mark.parametrize("text", ["not json", "[1, 2]", '"just text"'])
def test_import_rejects_non_profiles(text):
    profile, error = sync.import_profile(text)
    assert profile is None and error


def test_import_rejects_invalid_values():
    raw = json.loads(sync.export_profile(_profile()))
    raw["player_path"] = "/v1/players/"
    profile, error = sync.import_profile(json.dumps(raw))
    assert profile is None and "{id}" in error


@pytest.mark.parametrize("path, expected", [
    ("player.nick_name", " Alpha "),
    ("player.heroes.1.name", "Second"),
    ("player.heroes.5.name", None),
    ("player.heroes.x.name", None),
    ("player.missing", None),
    ("player.nick_name.deeper", None),
    ("", None),
])
def test_extract(path, expected):
    assert sync.extract(PAYLOAD, path) == expected


def test_extract_from_top_level_list():
    assert sync.extract([{"name": "A"}], "0.name") == "A"


@pytest.mark.parametrize("field, value, expected", [
    ("name", " Alpha ", "Alpha"),
    ("name", "   ", None),
    ("name", 12345, "12345"),
    ("level", 45, 45),
    ("level", 45.0, 45),
    ("level", "FC 3", 45),
    ("level", "FC 3 - 1", 46),
    ("level", 999, None),
    ("kingdom", 172, 172),
    ("kingdom", "172", 172),
    ("kingdom", 0, None),
    ("power", 123456789, 123456789),
    ("power", "1.2M", 1200000),
    ("power", 0, None),
    ("power", True, None),
    ("avatar", "https://img.example.com/a.png", "https://img.example.com/a.png"),
    ("avatar", "a.png", None),
    ("name", {"x": 1}, None),
    ("level", [45], None),
    ("kingdom", None, None),
])
def test_convert(field, value, expected):
    assert sync.convert(field, value) == expected


def test_read_player_maps_to_users_columns():
    assert sync.read_player(PAYLOAD, _profile()) == {
        "nickname": "Alpha", "furnace_lv": 45, "kid": 172, "power": 123456789,
        "avatar_url": "https://img.example.com/a.png"}


def test_read_player_skips_unmapped_and_missing_fields():
    profile = _profile()
    profile["fields"]["power"] = ""
    profile["fields"]["kingdom"] = "player.nope"
    assert set(sync.read_player(PAYLOAD, profile)) == {"nickname", "furnace_lv", "avatar_url"}


def _serve(handler, scenario):
    async def main():
        app = web.Application()
        app.router.add_get("/v1/players/{fid}", handler)
        async with TestServer(app) as server:
            async with aiohttp.ClientSession() as session:
                base = str(server.make_url("")).rstrip("/")
                return await scenario(session, _profile(base_url=base))
    return asyncio.run(main())


@pytest.mark.parametrize("placeholder", ["{id}", "{fid}"])
def test_player_path_accepts_either_placeholder(placeholder):
    assert sync.problems(_profile(player_path=f"/v1/players/{placeholder}")) == []


@pytest.mark.parametrize("player_path", ["v1/players/{id}?include=base", "v1/players/{fid}?include=base"])
def test_player_url_joins_cleanly(player_path):
    profile = _profile(base_url="https://api.example.com/", player_path=player_path)
    assert sync.player_url(profile, 42) == "https://api.example.com/v1/players/42?include=base"


@pytest.mark.parametrize("placeholder", ["{id}", "{fid}"])
def test_player_url_fills_plain_path(placeholder):
    profile = _profile(base_url="https://api.example.com", player_path=f"/v1/players/{placeholder}")
    assert sync.player_url(profile, 7) == "https://api.example.com/v1/players/7"


def test_fetch_sends_key_under_configured_header():
    seen = {}

    async def handler(request):
        seen["auth"] = request.headers.get("X-Api-Key")
        seen["fid"] = request.match_info["fid"]
        return web.json_response(PAYLOAD)

    async def scenario(session, profile):
        profile["auth_header"] = "X-Api-Key"
        return await sync.fetch_player(session, profile, "abc123", 42)

    result = _serve(handler, scenario)
    assert result.status == 200 and result.payload == PAYLOAD
    assert seen == {"auth": "abc123", "fid": "42"}


def test_fetch_without_key_sends_no_auth_header():
    seen = {}

    async def handler(request):
        seen["has_auth"] = "Authorization" in request.headers
        return web.json_response({})

    _serve(handler, lambda session, profile: sync.fetch_player(session, profile, "", 1))
    assert seen["has_auth"] is False


def test_fetch_429_carries_retry_after():
    async def handler(request):
        return web.Response(status=429, headers={"Retry-After": "7"})

    result = _serve(handler, lambda session, profile: sync.fetch_player(session, profile, "k", 1))
    assert result.status == 429 and result.retry_after == 7.0 and result.payload is None


def test_fetch_404_has_no_payload():
    async def handler(request):
        return web.json_response({"ok": False}, status=404)

    result = _serve(handler, lambda session, profile: sync.fetch_player(session, profile, "k", 1))
    assert result.status == 404 and result.payload is None


def test_fetch_200_that_is_not_json():
    async def handler(request):
        return web.Response(text="<html>login</html>", content_type="text/html")

    result = _serve(handler, lambda session, profile: sync.fetch_player(session, profile, "k", 1))
    assert result.status == 200 and result.payload is None


def test_fetch_connection_error_is_status_zero():
    async def scenario():
        async with aiohttp.ClientSession() as session:
            return await sync.fetch_player(session, _profile(base_url="http://127.0.0.1:9"), "k", 1)

    assert asyncio.run(scenario()).status == 0


# --- Storage: settings, daily request count and member order --------------------------

@pytest.fixture
def store_dbs(dbs):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.executemany(
            "INSERT INTO users (fid, nickname, furnace_lv, kid, alliance, power, api_synced_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(1, "Old", 40, 172, "10", 100, "2026-10-09T10:00:00+00:00"),
             (2, "Never", 40, 172, "10", None, None),
             (3, "Newer", 40, 172, "20", None, "2026-10-10T10:00:00+00:00"),
             (4, "NoAlliance", 40, 172, "", None, None)])
        conn.commit()
    return dbs


def test_defaults(store_dbs):
    assert sync.load_setting("enabled") is False
    assert sync.load_setting("interval") == 24
    assert sync.load_setting("key") == ""
    assert sync.mask_key(sync.load_setting("key")) == "not set"
    assert sync.load_settings("last_run") == {"last_run": {}}
    assert sync.load_setting("last_queued") is None
    assert sync.load_setting("profile")["base_url"] == ""


def test_profile_round_trip(store_dbs):
    profile = sync.load_setting("profile")
    profile["base_url"] = "https://api.example.com"
    sync.save_setting("profile", profile)
    assert sync.load_setting("profile")["base_url"] == "https://api.example.com"


def test_key_is_stored_apart_from_profile_and_masked(store_dbs):
    sync.save_setting("key", "Bearer abcdef1234")
    assert sync.load_setting("key") == "Bearer abcdef1234"
    assert sync.mask_key(sync.load_setting("key")) == "••••1234"
    assert "abcdef" not in str(sync.load_setting("profile"))


@pytest.mark.parametrize("key, shown", [("abc", "set"), ("12345678", "set"), ("123456789", "••••6789")])
def test_short_keys_are_not_partly_shown(store_dbs, key, shown):
    sync.save_setting("key", key)
    assert sync.mask_key(sync.load_setting("key")) == shown


def test_enable_and_interval(store_dbs):
    sync.save_setting("enabled", True)
    sync.save_setting("interval", 6)
    assert sync.load_setting("enabled") is True and sync.load_setting("interval") == 6


@pytest.mark.parametrize("hours", [0, 721, -5])
def test_interval_out_of_range(store_dbs, hours):
    with pytest.raises(ValueError):
        sync.save_setting("interval", hours)


def test_last_queued_round_trip(store_dbs):
    when = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
    sync.save_setting("last_queued", when)
    assert sync.load_setting("last_queued") == when


def test_request_count_resets_on_new_day(store_dbs):
    sync.take_request("2026-10-10")
    sync.take_request("2026-10-10")
    assert sync.used_today(sync.load_setting("budget"), "2026-10-10") == 2
    assert sync.used_today(sync.load_setting("budget"), "2026-10-11") == 0
    sync.take_request("2026-10-11")
    assert sync.used_today(sync.load_setting("budget"), "2026-10-11") == 1


def test_members_oldest_first_never_synced_first(store_dbs):
    assert sync.members_to_sync() == [(2, "10"), (1, "10"), (3, "20")]


def test_members_before_skips_those_synced_since(store_dbs):
    assert sync.members_to_sync(before="2026-10-10T00:00:00+00:00") == [(2, "10"), (1, "10")]
    assert sync.members_to_sync(10, before="2026-10-09T00:00:00+00:00") == [(2, "10")]


def test_members_of_one_alliance(store_dbs):
    assert sync.members_to_sync(20) == [(3, "20")]


def test_mark_synced_moves_member_to_the_back(store_dbs):
    sync.mark_synced(2, "2026-10-10T11:00:00+00:00")
    assert sync.members_to_sync() == [(1, "10"), (3, "20"), (2, "10")]


def test_snapshot_and_avatar(store_dbs):
    sync.save_avatar(1, "https://img.example.com/a.png")
    assert sync.member_snapshot(1) == {"nickname": "Old", "furnace_lv": 40, "kid": 172,
                                        "power": 100, "avatar_url": "https://img.example.com/a.png"}
    assert sync.member_snapshot(999) is None


def test_sync_channel_set_without_a_settings_row(store_dbs):
    sync.set_sync_channel(10, 555)
    assert sync.sync_channels().get("10") == 555 and sync.sync_channels().get("20") is None
    assert sync.sync_channels() == {"10": 555}


def test_sync_channel_keeps_the_other_settings(store_dbs):
    with closing(sqlite3.connect("db/alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliancesettings (alliance_id, channel_id, interval, redemption_channel_id) "
                     "VALUES (10, NULL, 0, 777)")
        conn.commit()
    sync.set_sync_channel(10, 555)
    sync.set_sync_channel(10, 556)
    sync.clear_sync_channel(10)
    with closing(sqlite3.connect("db/alliance.sqlite")) as conn:
        assert conn.execute("SELECT alliance_id, channel_id, interval, redemption_channel_id "
                            "FROM alliancesettings").fetchall() == [(10, None, 0, 777)]
    assert sync.sync_channels() == {}


def test_old_zero_channel_counts_as_not_set(store_dbs):
    with closing(sqlite3.connect("db/alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliancesettings (alliance_id, channel_id) VALUES (10, 0)")
        conn.commit()
    assert sync.sync_channels().get("10") is None


def test_auto_delete_defaults_on_for_five_minutes(store_dbs):
    assert sync.load_setting("autodelete") is True and sync.load_setting("keep_minutes") == 5
    sync.save_setting("autodelete", False)
    sync.save_setting("keep_minutes", 1440)
    assert sync.load_setting("autodelete") is False and sync.load_setting("keep_minutes") == 1440


@pytest.mark.parametrize("minutes", [0, 1441, -1])
def test_keep_minutes_out_of_range(store_dbs, minutes):
    with pytest.raises(ValueError):
        sync.save_setting("keep_minutes", minutes)
    assert sync.load_setting("keep_minutes") == 5


def test_load_settings_reads_several_at_once(store_dbs):
    sync.save_setting("enabled", True)
    sync.save_setting("interval", 6)
    sync.save_setting("key", "abc")
    assert sync.load_settings("enabled", "interval", "key", "last_queued") == {
        "enabled": True, "interval": 6, "key": "abc", "last_queued": None}
    assert sync.load_connection() == (sync.load_setting("profile"), "abc")


# --- Run: writes, history, pacing, budget, retries and stop conditions ----------------

PROFILE = normalise({
    "base_url": "https://api.example.com", "player_path": "/p/{fid}", "per_minute": 60, "per_day": 100,
    "fields": {"name": "name", "level": "level", "kingdom": "kid", "power": "power", "avatar": "avatar"},
})


@pytest.fixture
def run_dbs(dbs):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.executemany("INSERT INTO users (fid, nickname, furnace_lv, kid, alliance, power) VALUES (?, ?, ?, ?, ?, ?)",
                         [(1, "Alpha", 40, 172, "10", 1000), (2, "Bravo", 45, 172, "10", 2000),
                          (3, "Charlie", 45, 172, "20", 3000)])
        conn.commit()
    return dbs


class Clock:
    def __init__(self):
        self.now = 0.0
        self.slept = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def _fetcher(answers):
    calls = []

    async def fetch(fid):
        calls.append(fid)
        answer = answers[fid]
        return answer.pop(0) if isinstance(answer, list) else answer
    return fetch, calls


def _ok(**player):
    return FetchResult(200, player)


def _run(fetch, clock=None, **kwargs):
    clock = clock or Clock()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sync, "_LOOKUP_GATE", sync._LookupGate(clock.sleep, clock))
        return asyncio.run(sync.run_sync(fetch, kwargs.pop("profile", PROFILE), sleep=clock.sleep,
                                        today=lambda: "2026-10-10", **kwargs))


def _user(fid):
    return sync.member_snapshot(fid)


def _history(table):
    with closing(sqlite3.connect("db/changes.sqlite")) as conn:
        return conn.execute(f"SELECT * FROM {table}").fetchall()


def test_writes_changes_and_history(run_dbs):
    fetch, _ = _fetcher({1: _ok(name="Alpha2", level=45, kid=173, power=1500, avatar="https://i.example.com/1.png"),
                         2: _ok(name="Bravo", level=45, kid=172, power=2000),
                         3: _ok(name="Charlie")})
    result = _run(fetch)
    assert _user(1) == {"nickname": "Alpha2", "furnace_lv": 45, "kid": 173, "power": 1500,
                        "avatar_url": "https://i.example.com/1.png"}
    assert len(_history("nickname_changes")) == 1 and len(_history("furnace_changes")) == 1
    assert result.checked == 3 and result.kingdom_changed == [1]
    assert len(result.changes["10"]) == 4 and "20" not in result.changes
    assert any("Alpha" in line and "FC 3" in line for line in result.changes["10"])


def test_unmapped_and_missing_fields_are_left_alone(run_dbs):
    profile = normalise({**PROFILE, "fields": {"name": "name"}})
    fetch, _ = _fetcher({1: _ok(level=99), 2: _ok(), 3: _ok()})
    _run(fetch, profile=profile)
    assert _user(1)["furnace_lv"] == 40 and _user(1)["nickname"] == "Alpha"


def test_not_set_up_does_nothing(run_dbs):
    fetch, calls = _fetcher({})
    result = _run(fetch, profile=normalise({}))
    assert calls == [] and "still needs the" in result.error


def test_one_alliance_only(run_dbs):
    fetch, calls = _fetcher({3: _ok(name="C")})
    _run(fetch, alliance_id=20)
    assert calls == [3]


def test_404_marks_member_checked_without_changes(run_dbs):
    fetch, _ = _fetcher({1: FetchResult(404), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert result.checked == 3 and _user(1)["nickname"] == "Alpha"
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        assert conn.execute("SELECT COUNT(*) FROM users WHERE api_synced_at IS NULL").fetchone()[0] == 0


@pytest.mark.parametrize("status", [401, 402, 403])
def test_key_problems_stop_the_run(run_dbs, status):
    fetch, calls = _fetcher({1: FetchResult(status), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert calls == [1] and result.error and result.checked == 0


def test_html_instead_of_json_stops_the_run(run_dbs):
    fetch, calls = _fetcher({1: FetchResult(200), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert calls == [1] and "not with player data" in result.error


def test_429_waits_retry_after_then_retries(run_dbs):
    clock = Clock()
    fetch, calls = _fetcher({1: [FetchResult(429, retry_after=30), _ok(name="A")], 2: _ok(), 3: _ok()})
    result = _run(fetch, clock)
    assert calls == [1, 1, 2, 3] and 30 in clock.slept and result.checked == 3


def test_server_errors_retry_then_skip_without_marking(run_dbs):
    fetch, calls = _fetcher({1: [FetchResult(503), FetchResult(0), FetchResult(500)], 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert calls.count(1) == 3 and result.checked == 2
    assert sync.members_to_sync()[0][0] == 1


def test_pacing_spaces_requests(run_dbs):
    clock = Clock()
    profile = normalise({**PROFILE, "per_minute": 30})
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    _run(fetch, clock, profile=profile)
    assert clock.slept == [2.0, 2.0]


def test_budget_stops_the_run_and_next_run_continues(run_dbs):
    profile = normalise({**PROFILE, "per_day": 2})
    fetch, calls = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    result = _run(fetch, profile=profile)
    assert calls == [1, 2] and result.stopped and sync.members_to_sync()[0][0] == 3


def test_budget_continues_from_todays_count(run_dbs):
    sync.take_request("2026-10-10")
    profile = normalise({**PROFILE, "per_day": 2})
    fetch, calls = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    _run(fetch, profile=profile)
    assert calls == [1] and sync.used_today(sync.load_setting("budget"), "2026-10-10") == 2


def test_budget_restarts_on_a_new_day(run_dbs):
    days = iter(["2026-10-10", "2026-10-10", "2026-10-11"])
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    clock = Clock()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sync, "_LOOKUP_GATE", sync._LookupGate(clock.sleep, clock))
        asyncio.run(sync.run_sync(fetch, PROFILE, sleep=clock.sleep, today=lambda: next(days)))
    assert sync.used_today(sync.load_setting("budget"), "2026-10-11") == 1


def test_budget_counts_every_request_including_retries(run_dbs):
    fetch, _ = _fetcher({1: [FetchResult(503), _ok()], 2: _ok(), 3: _ok()})
    _run(fetch)
    assert sync.used_today(sync.load_setting("budget"), "2026-10-10") == 4


def test_stop_requested(run_dbs):
    class Queue:
        def stop_requested(self):
            return True

        def should_preempt(self):
            return False

    fetch, calls = _fetcher({})
    result = _run(fetch, queue=Queue())
    assert calls == [] and result.stopped


def test_preempt_returns_partial_result(run_dbs):
    class Queue:
        def __init__(self):
            self.checks = 0

        def stop_requested(self):
            return False

        def should_preempt(self):
            self.checks += 1
            return self.checks > 1

    fetch, calls = _fetcher({1: _ok(name="Alpha2"), 2: _ok(), 3: _ok()})
    result = _run(fetch, queue=Queue())
    assert calls == [1] and result.preempted and result.stopped is None and result.changes["10"]


def test_member_removed_mid_run_is_skipped(run_dbs):
    async def fetch(fid):
        if fid == 1:
            with closing(sqlite3.connect("db/users.sqlite")) as conn:
                conn.execute("DELETE FROM users WHERE fid = 2")
                conn.commit()
        return _ok(name=f"N{fid}")

    result = _run(fetch)
    assert result.checked == 3 and _user(2) is None


def test_summary(run_dbs):
    result = sync.RunResult(checked=5, requests=6, changes={"10": ["a", "b"], "20": ["c"]})
    data = sync.summary(result, "All alliances")
    assert data["checked"] == 5 and data["changes"] == 3 and data["scope"] == "All alliances"
    assert data["finished_at"]


def test_member_failure_is_skipped_and_run_continues(run_dbs, monkeypatch):
    real_apply = sync._apply

    def flaky(fid, alliance, values, result):
        if fid == 2:
            raise RuntimeError("database is locked")
        real_apply(fid, alliance, values, result)

    monkeypatch.setattr(sync, "_apply", flaky)
    fetch, _ = _fetcher({1: _ok(name="Alpha2"), 2: _ok(name="Bravo2"), 3: _ok()})
    result = _run(fetch)
    assert result.checked == 2 and _user(1)["nickname"] == "Alpha2" and _user(2)["nickname"] == "Bravo"
    assert result.changes["10"] and sync.members_to_sync()[0][0] == 2


def test_long_429_wait_stops_the_run_unmarked(run_dbs):
    clock = Clock()
    fetch, calls = _fetcher({1: FetchResult(429, retry_after=3600), 2: _ok(), 3: _ok()})
    result = _run(fetch, clock)
    assert calls == [1] and result.stopped == sync.RATE_LIMITED and result.checked == 0
    assert max(clock.slept, default=0) <= 300 and sync.members_to_sync()[0][0] == 1


def test_429_with_zero_retry_after_does_not_wait_a_minute(run_dbs):
    clock = Clock()
    fetch, calls = _fetcher({1: [FetchResult(429, retry_after=0), _ok()], 2: _ok(), 3: _ok()})
    result = _run(fetch, clock)
    assert calls == [1, 1, 2, 3] and 60.0 not in clock.slept and result.checked == 3


class _PreemptAfter:
    def __init__(self, free_checks, stop=False):
        self.checks, self.free_checks, self.stop = 0, free_checks, stop

    def _due(self):
        self.checks += 1
        return self.checks > self.free_checks

    def stop_requested(self):
        return self.stop and self._due()

    def should_preempt(self):
        return not self.stop and self._due()


def test_resume_skips_members_synced_this_run(run_dbs):
    started_at = sync.utc_now()
    fetch, calls = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    _run(fetch, queue=_PreemptAfter(1), started_at=started_at)
    assert calls == [1]
    fetch, calls = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    result = _run(fetch, started_at=started_at)
    assert calls == [2, 3] and result.checked == 2


@pytest.mark.parametrize("stop, stopped", [(False, None), (True, sync.STOPPED)])
def test_stop_or_preempt_during_429_wait(run_dbs, stop, stopped):
    clock = Clock()
    fetch, calls = _fetcher({1: [FetchResult(429, retry_after=30), _ok()], 2: _ok(), 3: _ok()})
    result = _run(fetch, clock, queue=_PreemptAfter(1, stop))
    assert calls == [1] and result.stopped == stopped and result.preempted is not stop and 30 not in clock.slept
    assert result.checked == 0 and sync.members_to_sync()[0][0] == 1


def test_change_lines_clip_long_names(run_dbs):
    long_name = "N" * 60
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute("UPDATE users SET nickname = ? WHERE fid = 1", (long_name,))
        conn.commit()
    fetch, _ = _fetcher({1: _ok(name="M" * 60), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    line, = result.changes["10"]
    assert f"`1` {'N' * 40}: Name {'N' * 40} → {'M' * 40}" == line


# --- Sync Log: the progress message and the results it turns into ----------------------

class FakeMessage:
    def __init__(self, embed):
        self.embeds = [embed]
        self.deleted_after = None

    async def edit(self, embed):
        self.embeds.append(embed)

    async def delete(self, delay=None):
        self.deleted_after = delay

    @property
    def text(self):
        return self.embeds[-1].description


class FakeChannel:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    async def send(self, embed):
        if self.fail:
            raise sync.discord.Forbidden(types.SimpleNamespace(status=403, reason="Forbidden"), "Missing Access")
        self.sent.append(FakeMessage(embed))
        return self.sent[-1]


def _reporter(channels, clock=None, keep_minutes=None, names=None):
    found = {555: FakeChannel(), 666: FakeChannel(fail=True)}
    reporter = sync.SyncLogReporter(found.get, channels, names or {10: "Wolves", 20: "Bears"},
                                    keep_minutes, clock or Clock())
    return reporter, found


def test_progress_message_for_each_alliance_with_a_channel(run_dbs):
    reporter, found = _reporter({"10": 555})
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    _run(fetch, reporter=reporter)
    message, = found[555].sent
    assert message.embeds[0].title.endswith("Player Sync: Wolves")
    assert message.embeds[0].description == "Syncing Wolves…\n`0` of 2 members checked so far."


def test_progress_edits_at_most_every_15_seconds(run_dbs):
    clock = Clock()
    reporter, found = _reporter({"10": 555}, clock)
    asyncio.run(reporter.start({"10": 4}))
    message = found[555].sent[0]
    for seconds in (5, 5, 6, 1):
        clock.now += seconds
        asyncio.run(reporter.advance("10"))
    assert [e.description.split("\n")[1] for e in message.embeds[1:]] == ["`3` of 4 members checked so far."]
    clock.now += 15
    asyncio.run(reporter.advance("20"))
    assert len(message.embeds) == 2


def test_results_replace_the_progress_message(run_dbs):
    reporter, found = _reporter({"10": 555, "20": 556})
    fetch, _ = _fetcher({1: _ok(name="Alpha2"), 2: _ok(), 3: _ok()})
    result = _run(fetch, reporter=reporter)
    asyncio.run(reporter.finish(result))
    message = found[555].sent[0]
    assert len(found[555].sent) == 1
    assert message.text.startswith("Checked `2` of 2 members.\n\n`1` Alpha: Name Alpha → Alpha2")
    assert message.deleted_after is None and not message.embeds[-1].footer.text


def test_results_without_changes_say_so(run_dbs):
    reporter, found = _reporter({"20": 555})
    asyncio.run(reporter.start({"20": 1}))
    asyncio.run(reporter.advance("20"))
    asyncio.run(reporter.finish(sync.RunResult(checked=1)))
    assert found[555].sent[0].text == f"Checked `1` of 1 member.\n\n{sync.NO_CHANGES}"


def test_results_cap_change_lines():
    lines = [f"line {i}" for i in range(25)]
    shown = sync.change_lines(lines)
    assert shown[-2] == "line 19" and shown[-1] == "and 5 more" and len(shown) == 21


@pytest.mark.parametrize("result", [sync.RunResult(stopped=sync.BUDGET_USED),
                                    sync.RunResult(error=sync.KEY_REJECTED)])
def test_results_name_the_stop_or_error_reason(result):
    reporter, found = _reporter({"10": 555})
    asyncio.run(reporter.start({"10": 2}))
    asyncio.run(reporter.finish(result))
    text = found[555].sent[0].text
    assert text.startswith("Checked `0` of 2 members.") and text.endswith(
        f"{sync.theme.warnIcon} {result.stopped or result.error}")


def test_preempt_says_paused_and_resume_posts_a_fresh_message(run_dbs):
    reporter, found = _reporter({"10": 555})
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    started_at = sync.utc_now()
    result = _run(fetch, queue=_PreemptAfter(1), started_at=started_at, reporter=reporter)
    asyncio.run(reporter.finish(result))
    assert "Paused after `1` of 2 members" in found[555].sent[0].text
    assert "posts a new message" in found[555].sent[0].text

    resumed, _ = _reporter({"10": 555})
    resumed.get_channel = found.get
    _run(fetch, started_at=started_at, reporter=resumed)
    assert len(found[555].sent) == 2 and "`0` of 1 member checked" in found[555].sent[1].text


def test_auto_delete_removes_the_results_after_keep_minutes():
    reporter, found = _reporter({"10": 555}, keep_minutes=5)
    asyncio.run(reporter.start({"10": 1}))
    asyncio.run(reporter.finish(sync.RunResult()))
    message = found[555].sent[0]
    assert message.deleted_after == 300
    assert message.embeds[-1].footer.text == "This message removes itself after 5 minutes."


def test_missing_or_forbidden_channel_is_a_warning(run_dbs, caplog):
    reporter, found = _reporter({"10": 999, "20": 666})
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    with caplog.at_level("WARNING", logger="alliance"):
        result = _run(fetch, reporter=reporter)
        asyncio.run(reporter.finish(result))
    assert result.checked == 3 and reporter.posts == {}
    assert [r.levelname for r in caplog.records] == ["WARNING", "WARNING"]


def test_failed_edit_is_a_warning_and_stops_editing(caplog):
    reporter, found = _reporter({"10": 555})
    asyncio.run(reporter.start({"10": 1}))

    async def gone(embed):
        raise sync.discord.NotFound(types.SimpleNamespace(status=404, reason="Not Found"), "Unknown Message")
    found[555].sent[0].edit = gone
    with caplog.at_level("WARNING", logger="alliance"):
        asyncio.run(reporter.finish(sync.RunResult()))
    assert reporter.posts == {} and found[555].sent[0].deleted_after is None
    assert [r.levelname for r in caplog.records] == ["WARNING"]


def test_non_discord_error_on_send_doesnt_stop_the_run(run_dbs, caplog):
    reporter, found = _reporter({"10": 555})

    async def broken(embed):
        raise aiohttp.ClientConnectionError("connection reset")
    found[555].send = broken
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    with caplog.at_level("WARNING", logger="alliance"):
        result = _run(fetch, reporter=reporter)
    assert result.checked == 3 and reporter.posts == {}
    assert [r.levelname for r in caplog.records] == ["WARNING"]


def test_non_discord_error_on_edit_doesnt_stop_the_run(run_dbs):
    clock = Clock()
    reporter, found = _reporter({"10": 555}, clock)

    async def timed_out(embed):
        raise asyncio.TimeoutError()

    async def run():
        await reporter.start({"10": 2})
        found[555].sent[0].edit = timed_out
        clock.now += 20
        await reporter.advance("10")
        await reporter.finish(sync.RunResult(checked=2))
    asyncio.run(run())
    assert reporter.posts == {}


def test_paused_message_keeps_the_changes_found_so_far(run_dbs):
    reporter, found = _reporter({"10": 555})
    fetch, _ = _fetcher({1: _ok(name="Alpha2"), 2: _ok(), 3: _ok()})
    result = _run(fetch, queue=_PreemptAfter(1), reporter=reporter)
    asyncio.run(reporter.finish(result))
    text = found[555].sent[0].text
    assert text.startswith("Paused after `1` of 2 members") and text.endswith("Name Alpha → Alpha2")


def test_paused_message_caps_change_lines():
    reporter, found = _reporter({"10": 555})
    asyncio.run(reporter.start({"10": 30}))
    asyncio.run(reporter.finish(sync.RunResult(preempted=True, changes={"10": [f"line {i}" for i in range(25)]})))
    assert found[555].sent[0].text.endswith("line 19\nand 5 more")


@pytest.mark.parametrize("done, result", [(0, sync.RunResult(stopped=sync.STOPPED)),
                                          (1, sync.RunResult(error=sync.KEY_REJECTED))])
def test_no_up_to_date_line_when_nobody_was_checked_or_it_failed(done, result):
    reporter, found = _reporter({"10": 555})

    async def run():
        await reporter.start({"10": 2})
        for _ in range(done):
            await reporter.advance("10")
        await reporter.finish(result)
    asyncio.run(run())
    assert sync.NO_CHANGES not in found[555].sent[0].text


def test_preempted_run_requeues_even_when_finish_fails(dbs, monkeypatch, caplog):
    async def fake_run(fetch, profile, **kwargs):
        return sync.RunResult(preempted=True)

    async def broken_finish(self, result):
        raise OSError("socket closed")

    monkeypatch.setattr(sync, "run_sync", fake_run)
    monkeypatch.setattr(sync.AllianceApiSync, "session", lambda self: object())
    monkeypatch.setattr(sync.SyncLogReporter, "finish", broken_finish)
    monkeypatch.setattr(sync.PermissionManager, "list_alliances", staticmethod(lambda: []))
    with caplog.at_level("WARNING", logger="alliance"), pytest.raises(pq.PreemptedException):
        asyncio.run(_cog(FakeQueue()).handle_api_sync({"id": 1, "details": {"alliance_id": None}}))
    assert any("couldn't finish" in r.message for r in caplog.records)


# --- Cog: schedule check, single queued job, handler and change-log embeds ------------

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


class FakeQueue:
    def __init__(self, jobs=()):
        self.jobs = list(jobs)
        self.enqueued = []

    def get_queued_processes_by_action(self, action, statuses=()):
        return [job for job in self.jobs if job["action"] == action]

    def enqueue(self, action, priority, alliance_id=None, details=None):
        self.enqueued.append((action, priority, alliance_id, details))
        self.jobs.append({"action": action, "details": details})
        return len(self.jobs)

    def update_details(self, process_id, details):
        self.updated = (process_id, details)


def _cog(queue):
    bot = types.SimpleNamespace(get_cog=lambda name: queue if name == "ProcessQueue" else None)
    return sync.AllianceApiSync(bot)


@pytest.mark.parametrize("last, hours, due", [
    (None, 24, True),
    (NOW - timedelta(hours=23), 24, False),
    (NOW - timedelta(hours=24), 24, True),
    (NOW - timedelta(minutes=59), 1, False),
])
def test_is_due(last, hours, due):
    assert sync.is_due(NOW, last, hours) is due


def test_player_sync_is_stoppable_and_timeout_exempt():
    assert pq.PLAYER_SYNC == 500
    assert "api_sync" in pq.STOPPABLE_ACTIONS and "api_sync" in pq.HANDLER_TIMEOUT_EXEMPT
    assert not hasattr(pq, "ALLIANCE_SYNC") and not hasattr(pq, "ALLIANCE_CONTROL")


def test_queue_sync_queues_once(dbs):
    queue = FakeQueue()
    cog = _cog(queue)
    assert cog.queue_sync(10) == "queued"
    assert queue.enqueued == [("api_sync", pq.PLAYER_SYNC, 10, {"alliance_id": 10})]
    assert sync.load_setting("last_queued") is not None


def test_queue_sync_refuses_second_job(dbs):
    queue = FakeQueue([{"action": "api_sync", "details": {}}])
    assert _cog(queue).queue_sync() == "busy" and queue.enqueued == []


def test_queue_sync_without_queue(dbs):
    assert _cog(None).queue_sync() == "unavailable"


def test_handler_saves_and_reraises_on_preempt(dbs, monkeypatch):
    async def fake_run(fetch, profile, **kwargs):
        return sync.RunResult(checked=1, changes={"10": ["`1` A: Name A → B"]}, preempted=True)

    finished = []

    async def fake_finish(self, result):
        finished.append(result.changes)

    monkeypatch.setattr(sync, "run_sync", fake_run)
    monkeypatch.setattr(sync.AllianceApiSync, "session", lambda self: object())
    monkeypatch.setattr(sync.SyncLogReporter, "finish", fake_finish)
    monkeypatch.setattr(sync.PermissionManager, "list_alliances", staticmethod(lambda: []))
    cog = _cog(FakeQueue())
    with pytest.raises(pq.PreemptedException):
        asyncio.run(cog.handle_api_sync({"id": 1, "details": {"alliance_id": None}}))
    last_run = sync.load_settings("last_run")["last_run"]
    assert last_run["checked"] == 1 and last_run["stopped"] is None and finished == [{"10": ["`1` A: Name A → B"]}]


def test_handler_gives_the_run_a_reporter_for_the_sync_log_channels(dbs, monkeypatch):
    seen = _capture_run(monkeypatch)
    sync.set_sync_channel(10, 555)
    sync.save_setting("keep_minutes", 7)
    asyncio.run(_cog(FakeQueue()).handle_api_sync({"id": 7, "details": {"alliance_id": None}}))
    reporter = seen["reporter"]
    assert reporter.channels == {"10": 555} and reporter.keep_minutes == 7


def test_handler_reporter_keeps_results_when_auto_delete_is_off(dbs, monkeypatch):
    seen = _capture_run(monkeypatch)
    sync.save_setting("autodelete", False)
    asyncio.run(_cog(FakeQueue()).handle_api_sync({"id": 7, "details": {"alliance_id": None}}))
    assert seen["reporter"].keep_minutes is None


def test_running_now_line():
    assert describe_job({"action": "api_sync", "details": {"alliance_id": None}}, {}) == "Player sync, all alliances"
    assert describe_job({"action": "api_sync", "alliance_id": 10, "details": {"alliance_id": 10}},
                        {10: "Wolves"}) == "Player sync for Wolves"


def _capture_run(monkeypatch):
    seen = {}

    async def fake_run(fetch, profile, **kwargs):
        seen.update(kwargs)
        return sync.RunResult()

    monkeypatch.setattr(sync, "run_sync", fake_run)
    monkeypatch.setattr(sync.AllianceApiSync, "session", lambda self: object())
    monkeypatch.setattr(sync.PermissionManager, "list_alliances", staticmethod(lambda: []))
    return seen


def test_handler_records_start_time_once(dbs, monkeypatch):
    seen = _capture_run(monkeypatch)
    queue = FakeQueue()
    asyncio.run(_cog(queue).handle_api_sync({"id": 7, "details": {"alliance_id": 10}}))
    started_at = seen["started_at"]
    assert started_at and queue.updated == (7, {"alliance_id": 10, "started_at": started_at})


def test_handler_reuses_start_time_on_resume(dbs, monkeypatch):
    seen = _capture_run(monkeypatch)
    queue = FakeQueue()
    process = {"id": 7, "details": {"alliance_id": None, "started_at": "2026-10-10T08:00:00+00:00"}}
    asyncio.run(_cog(queue).handle_api_sync(process))
    assert seen["started_at"] == "2026-10-10T08:00:00+00:00" and not hasattr(queue, "updated")


def test_schedule_and_missing_queue_log_warnings(dbs, monkeypatch, caplog):
    def broken(*names):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(sync, "load_settings", broken)
    with caplog.at_level("WARNING", logger="alliance"):
        asyncio.run(_cog(None).schedule_loop.coro(_cog(None)))
        _cog(None).queue_sync()
    assert [r.levelname for r in caplog.records if "Player Sync" in r.message] == ["WARNING", "WARNING"]


# --- Lookups for other cogs: one player, counted against the budget -------------------

@pytest.fixture
def lookup_dbs(run_dbs):
    sync.save_setting("profile", PROFILE)
    sync.save_setting("key", "k")
    return run_dbs


def _answering(monkeypatch, answer):
    calls = []

    async def fake_fetch(session, profile, key, fid):
        calls.append((key, fid))
        return answer
    monkeypatch.setattr(sync, "fetch_player", fake_fetch)
    return calls


def test_api_available_follows_setup(dbs):
    assert sync.api_available() is False
    sync.save_setting("profile", PROFILE)
    assert sync.api_available() is True
    assert sync.api_available(normalise({})) is False


def test_lookup_returns_values_and_counts_the_request(lookup_dbs, monkeypatch):
    calls = _answering(monkeypatch, _ok(name="Alpha2", level=45))
    values = asyncio.run(sync.lookup_player(None, 1)).values
    assert values == {"nickname": "Alpha2", "furnace_lv": 45}
    assert calls == [("k", 1)] and sync.used_today(sync.load_setting("budget"), sync.utc_today()) == 1


def test_lookup_without_setup_makes_no_request(dbs, monkeypatch):
    calls = _answering(monkeypatch, _ok(name="Alpha2"))
    assert asyncio.run(sync.lookup_player(None, 1)) == sync.Lookup(reason=sync.NOT_SET_UP)
    assert calls == []


def test_lookup_with_budget_used_makes_no_request(lookup_dbs, monkeypatch):
    calls = _answering(monkeypatch, _ok(name="Alpha2"))
    sync.save_setting("budget", {"date": sync.utc_today(), "used": PROFILE["per_day"]})
    assert asyncio.run(sync.lookup_player(None, 1)) == sync.Lookup(reason=sync.BUDGET_SPENT)
    assert calls == [] and sync.used_today(sync.load_setting("budget"), sync.utc_today()) == PROFILE["per_day"]


def test_lookup_404_is_unknown_without_a_warning(lookup_dbs, monkeypatch, caplog):
    _answering(monkeypatch, FetchResult(404))
    with caplog.at_level("WARNING", logger="alliance"):
        assert asyncio.run(sync.lookup_player(None, 1)) == sync.Lookup()
    assert not caplog.records


@pytest.mark.parametrize("answer", [FetchResult(200), FetchResult(0), FetchResult(429, retry_after=5.0),
                                    FetchResult(500), FetchResult(401)])
def test_lookup_errors_and_non_json_fail_with_a_warning(lookup_dbs, monkeypatch, caplog, answer):
    _answering(monkeypatch, answer)
    with caplog.at_level("WARNING", logger="alliance"):
        assert asyncio.run(sync.lookup_player(None, 1)) == sync.Lookup(reason=sync.FAILED)
    assert [r.levelname for r in caplog.records] == ["WARNING"]


def _cog_with_session():
    cog = _cog(None)
    cog.session = lambda: None
    return cog


def test_cog_lookup_returns_values_without_storing(lookup_dbs, monkeypatch):
    _answering(monkeypatch, _ok(name="Alpha2"))
    assert asyncio.run(_cog_with_session().lookup(1)).values == {"nickname": "Alpha2"}
    assert _user(1)["nickname"] == "Alpha"


def test_lookup_between_two_sync_slots_is_counted(lookup_dbs, monkeypatch):
    _answering(monkeypatch, _ok(name="X"))

    async def scenario():
        assert await sync._take_slot(PROFILE)
        await sync.lookup_player(None, 1)
        assert await sync._take_slot(PROFILE)

    asyncio.run(scenario())
    assert sync.used_today(sync.load_setting("budget"), sync.utc_today()) == 3


def test_lookup_never_raises_on_a_bad_id(lookup_dbs):
    assert asyncio.run(sync.lookup_player(None, "abc")).failed


def test_lookup_never_raises_when_the_database_is_locked(lookup_dbs, monkeypatch, caplog):
    def locked():
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(sync, "load_connection", locked)
    with caplog.at_level("WARNING", logger="alliance"):
        assert asyncio.run(sync.lookup_player(None, 1)).failed
    assert [r.levelname for r in caplog.records] == ["WARNING"]


def _bot(cog):
    return types.SimpleNamespace(get_cog=lambda name: cog if name == "AllianceApiSync" else None)


def test_lookup_via_bot(lookup_dbs, monkeypatch):
    _answering(monkeypatch, _ok(name="Alpha2"))
    cog = _cog_with_session()
    bot = _bot(cog)
    cog.bot = bot
    assert asyncio.run(sync.lookup_via_bot(bot, 1)).values == {"nickname": "Alpha2"}
    assert _user(1)["nickname"] == "Alpha"


def test_lookup_via_bot_without_cog_or_setup(dbs):
    assert asyncio.run(sync.lookup_via_bot(_bot(None), 1)) == sync.Lookup(reason=sync.NOT_SET_UP)
    assert asyncio.run(sync.lookup_via_bot(_bot(_cog_with_session()), 1)) == sync.Lookup(reason=sync.NOT_SET_UP)


# --- New members: API values at add time ----------------------------------------------

def _lookup_bot(monkeypatch, answer):
    calls = _answering(monkeypatch, answer)
    cog = _cog_with_session()
    cog.bot = _bot(cog)
    return cog.bot, calls


NEW_PLAYER = _ok(name="Delta", level=45, kid=173, power=5000, avatar="https://i.example.com/9.png")


def test_new_member_takes_name_level_and_state_from_the_api(lookup_dbs, monkeypatch):
    bot, calls = _lookup_bot(monkeypatch, NEW_PLAYER)
    member = asyncio.run(sync.look_up_new_member(bot, 9, 10))
    assert (member.nickname, member.furnace_lv, member.kid) == ("Delta", 45, 173)
    assert calls == [("k", 9)]


def test_new_member_keeps_a_typed_name_but_takes_the_api_level(lookup_dbs, monkeypatch):
    bot, _ = _lookup_bot(monkeypatch, NEW_PLAYER)
    member = asyncio.run(sync.look_up_new_member(bot, 9, 10, name=" Typed ", level=30))
    assert (member.nickname, member.furnace_lv, member.kid) == ("Typed", 45, 173)


@pytest.mark.parametrize("answer", [FetchResult(404), FetchResult(500)])
def test_new_member_unknown_to_the_api_falls_back(lookup_dbs, monkeypatch, answer):
    bot, _ = _lookup_bot(monkeypatch, answer)
    member = asyncio.run(sync.look_up_new_member(bot, 9, 10, level=30))
    assert (member.nickname, member.furnace_lv, member.kid, member.from_api) == ("Player 9", 30, None, False)


def test_new_member_without_setup_makes_no_request(dbs, monkeypatch):
    bot, calls = _lookup_bot(monkeypatch, NEW_PLAYER)
    member = asyncio.run(sync.look_up_new_member(bot, 9, 10, name="Typed"))
    assert (member.nickname, member.furnace_lv, member.kid) == ("Typed", 0, None)
    assert calls == []


def test_new_member_skips_the_api_once_its_batch_had_a_failure(lookup_dbs, monkeypatch):
    bot, calls = _lookup_bot(monkeypatch, NEW_PLAYER)
    batch = sync.LookupBatch("failed")
    batch.failed = True
    member = asyncio.run(sync.look_up_new_member(bot, 9, 10, batch=batch))
    assert (member.nickname, member.kid, member.from_api) == ("Player 9", None, False) and calls == []


def test_new_member_falls_back_when_the_settings_cannot_be_read(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    bot, calls = _lookup_bot(monkeypatch, NEW_PLAYER)
    member = asyncio.run(sync.look_up_new_member(bot, 9, 10))
    assert (member.nickname, member.kid) == ("Player 9", None) and calls == []


def test_rapid_lookups_are_spaced_by_the_per_minute_limit(lookup_dbs, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(sync, "_LOOKUP_GATE", sync._LookupGate(sleep=clock.sleep, clock=clock))
    calls = _answering(monkeypatch, _ok(name="X"))

    async def burst():
        await asyncio.gather(*(sync.lookup_player(None, fid) for fid in (1, 2, 3)))

    asyncio.run(burst())
    assert len(calls) == 3 and clock.slept == [60 / PROFILE["per_minute"]] * 2


def test_lookup_failures_count_errors_but_not_404s(lookup_dbs, monkeypatch):
    _answering(monkeypatch, FetchResult(404))
    assert not asyncio.run(sync.lookup_player(None, 1)).failed
    _answering(monkeypatch, FetchResult(500))
    assert asyncio.run(sync.lookup_player(None, 1)).failed


def _insert_new(fid, member):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute("INSERT INTO users (fid, nickname, furnace_lv, kid, alliance) VALUES (?, ?, ?, ?, '10')",
                     (fid, member.nickname, member.furnace_lv, member.kid))
        conn.commit()


def test_store_new_member_writes_power_picture_and_sync_time_without_history(lookup_dbs, monkeypatch):
    bot, calls = _lookup_bot(monkeypatch, NEW_PLAYER)
    member = asyncio.run(sync.look_up_new_member(bot, 9, 10, name="Typed"))
    _insert_new(9, member)
    asyncio.run(sync.store_new_member(member, 9))
    assert _user(9) == {"nickname": "Typed", "furnace_lv": 45, "kid": 173, "power": 5000,
                        "avatar_url": "https://i.example.com/9.png"}
    assert _history("nickname_changes") == [] and _history("furnace_changes") == []
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        assert conn.execute("SELECT api_synced_at FROM users WHERE fid = 9").fetchone()[0]
    assert len(calls) == 1


def test_store_new_member_without_api_values_writes_nothing(dbs):
    member = sync.NewMember("Player 9", 0, None)
    _insert_new(9, member)
    asyncio.run(sync.store_new_member(member, 9))
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        assert conn.execute("SELECT api_synced_at FROM users WHERE fid = 9").fetchone()[0] is None


# --- Transfers: a new state outside the alliance's state ---------------------------------

def _alliance(alliance_id, kid=172, multistate=0, auto_remove=0):
    with closing(sqlite3.connect("db/alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_list (alliance_id, name, kid, multistate) VALUES (?, ?, ?, ?)",
                     (alliance_id, f"A{alliance_id}", kid, multistate))
        conn.execute("INSERT INTO alliancesettings (alliance_id, auto_remove_on_transfer) VALUES (?, ?)",
                     (alliance_id, auto_remove))
        conn.commit()


def test_moving_into_the_alliance_state_is_no_transfer(run_dbs):
    _alliance(10, kid=173)
    fetch, _ = _fetcher({1: _ok(kid=173), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert result.transfers == []
    assert result.changes["10"] == ["`1` Alpha: State 172 → 173"]


def test_moving_to_another_state_is_a_transfer(run_dbs):
    _alliance(10)
    fetch, _ = _fetcher({1: _ok(kid=456), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert result.transfers == [sync.Transfer("10", 1, "Alpha", 456)]
    assert "10" not in result.changes and result.kingdom_changed == [1]
    assert sync.alliance_lines(result, "10") == ["`1` Alpha moved to State 456"]
    assert sync.summary(result, "All")["changes"] == 1


@pytest.mark.parametrize("kid, multistate", [(None, 0), (None, 1)])
def test_multistate_or_stateless_alliance_has_no_transfers(run_dbs, kid, multistate):
    _alliance(10, kid=kid, multistate=multistate)
    fetch, _ = _fetcher({1: _ok(kid=456), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert result.transfers == []
    assert result.changes["10"] == ["`1` Alpha: State 172 → 456"]


def test_transfer_notes_come_first_in_the_sync_log(run_dbs):
    _alliance(10)
    reporter, found = _reporter({"10": 555})
    fetch, _ = _fetcher({1: _ok(kid=456), 2: _ok(name="Bravo2"), 3: _ok()})
    result = _run(fetch, reporter=reporter)
    result.transfers[0].removed = True
    asyncio.run(reporter.finish(result))
    assert found[555].sent[0].text == ("Checked `2` of 2 members.\n\n"
                                       "`1` Alpha moved to State 456 and was removed from the alliance\n"
                                       "`2` Bravo: Name Bravo → Bravo2")


def _removals(monkeypatch, removed=None):
    calls = []

    async def fake_apply(cog, candidates):
        calls.append((cog, list(candidates)))
        return list(candidates) if removed is None else removed
    monkeypatch.setattr(sync, "apply_removals", fake_apply)
    return calls


def test_auto_remove_goes_through_the_gift_removal_path(run_dbs, monkeypatch):
    _alliance(10, auto_remove=1)
    calls = _removals(monkeypatch)
    transfers = [sync.Transfer("10", 1, "Alpha", 456)]
    removed = asyncio.run(sync.remove_transfers("cog", sync.RunResult(transfers=transfers)))
    assert calls == [("cog", [("10", 1, "Alpha")])]
    assert removed == {1} and transfers[0].removed


def test_auto_remove_off_removes_nobody(run_dbs, monkeypatch):
    _alliance(10)
    calls = _removals(monkeypatch)
    transfers = [sync.Transfer("10", 1, "Alpha", 456)]
    assert asyncio.run(sync.remove_transfers("cog", sync.RunResult(transfers=transfers))) == set()
    assert calls == [] and not transfers[0].removed


def test_handler_removes_transfers_and_skips_their_catchups(run_dbs, monkeypatch):
    _alliance(10, auto_remove=1)
    transfer = sync.Transfer("10", 1, "Alpha", 456)

    async def fake_run(fetch, profile, **kwargs):
        return sync.RunResult(checked=2, kingdom_changed=[1, 2], transfers=[transfer])

    queued = []
    monkeypatch.setattr(sync, "run_sync", fake_run)
    monkeypatch.setattr(sync, "enqueue_catchups", lambda bot, fids: queued.append(list(fids)))
    monkeypatch.setattr(sync.AllianceApiSync, "session", lambda self: object())
    monkeypatch.setattr(sync.PermissionManager, "list_alliances", staticmethod(lambda: []))
    calls = _removals(monkeypatch)
    cog = _cog(FakeQueue())
    asyncio.run(cog.handle_api_sync({"id": 1, "details": {"alliance_id": None}}))
    assert calls == [(cog, [("10", 1, "Alpha")])] and transfer.removed
    assert queued == [[2]]


def test_a_lookup_just_before_a_run_delays_its_first_request(run_dbs, monkeypatch):
    clock = Clock()
    gate = sync._LookupGate(clock.sleep, clock)
    monkeypatch.setattr(sync, "_LOOKUP_GATE", gate)
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})

    async def lookup_then_run():
        await gate.wait(PROFILE["per_minute"])
        await sync.run_sync(fetch, PROFILE, today=lambda: "2026-10-10")

    asyncio.run(lookup_then_run())
    assert clock.slept == [1.0, 1.0, 1.0]


def test_lookup_with_a_spent_budget_does_not_wait_for_the_gate(lookup_dbs, monkeypatch):
    clock = Clock()
    gate = sync._LookupGate(sleep=clock.sleep, clock=clock)
    gate.last = clock()
    monkeypatch.setattr(sync, "_LOOKUP_GATE", gate)
    calls = _answering(monkeypatch, _ok(name="X"))
    sync.save_setting("budget", {"date": sync.utc_today(), "used": PROFILE["per_day"]})
    assert asyncio.run(sync.lookup_player(None, 1)).reason == sync.BUDGET_SPENT
    assert clock.slept == [] and calls == []


def _real_removal_cog():
    import logging
    return types.SimpleNamespace(logger=logging.getLogger("test"),
                                 bot=types.SimpleNamespace(get_channel=lambda cid: None))


def test_auto_remove_transfer_keeps_the_old_state_until_removal_decides(run_dbs):
    _alliance(10, auto_remove=1)
    fetch, _ = _fetcher({1: _ok(kid=456), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    assert result.transfers == [sync.Transfer("10", 1, "Alpha", 456, pending=True)]
    assert _user(1)["kid"] == 172 and result.kingdom_changed == []
    assert asyncio.run(sync.remove_transfers(_real_removal_cog(), result)) == {1}
    assert _user(1) is None and result.transfers[0].removed


def test_held_back_sync_transfers_keep_their_old_state_flagged(run_dbs):
    _alliance(10, auto_remove=1)
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.executemany("INSERT INTO users (fid, nickname, kid, alliance) VALUES (?, ?, 172, '10')",
                         [(4, "Delta"), (5, "Echo")])
        conn.commit()
    fetch, _ = _fetcher({1: _ok(kid=456), 2: _ok(), 3: _ok(), 4: _ok(kid=456), 5: _ok(kid=456)})
    result = _run(fetch)
    assert asyncio.run(sync.remove_transfers(_real_removal_cog(), result)) == set()
    assert [(kid, flag is not None) for kid, flag in map(kid_and_flag, (1, 4, 5))] == [(172, True)] * 3
    assert result.kingdom_changed == [] and not any(t.removed for t in result.transfers)


def test_pending_transfer_gets_its_state_when_auto_remove_was_switched_off(run_dbs):
    _alliance(10, auto_remove=1)
    fetch, _ = _fetcher({1: _ok(kid=456), 2: _ok(), 3: _ok()})
    result = _run(fetch)
    with closing(sqlite3.connect("db/alliance.sqlite")) as conn:
        conn.execute("UPDATE alliancesettings SET auto_remove_on_transfer = 0")
        conn.commit()
    assert asyncio.run(sync.remove_transfers(_real_removal_cog(), result)) == set()
    assert _user(1)["kid"] == 456 and result.kingdom_changed == [1]


def test_first_value_reads_set_to_and_changed_value_keeps_arrow():
    assert sync._change_line(10001, "Frosty", "Power", "power", None, 12345678) == "`10001` Frosty: Power set to 12,345,678"
    assert sync._change_line(10001, "Frosty", "State", "kid", None, 172) == "`10001` Frosty: State set to 172"
    assert sync._change_line(10001, "Frosty", "Power", "power", 5, 12345678) == "`10001` Frosty: Power 5 → 12,345,678"
