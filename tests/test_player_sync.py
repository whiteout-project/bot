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
from harness import make_player_sync_dbs


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    make_player_sync_dbs(tmp_path)
    return tmp_path


# --- Profile: validation, import/export, JSON paths, value reading, fetch -------------

def _profile(**overrides):
    profile = sync.normalise({
        "base_url": "https://api.example.com",
        "player_path": "/v1/players/{fid}",
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
    ({"player_path": "/v1/players/"}, "{fid}"),
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
    assert profile is None and "{fid}" in error


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


def test_player_url_joins_cleanly():
    profile = _profile(base_url="https://api.example.com/", player_path="v1/players/{fid}?include=base")
    assert sync.player_url(profile, 42) == "https://api.example.com/v1/players/42?include=base"


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
    assert sync.is_enabled() is False
    assert sync.get_interval_hours() == 24
    assert sync.get_key() == ""
    assert sync.mask_key(sync.get_key()) == "not set"
    assert sync.load_settings("last_run") == {"last_run": {}}
    assert sync.get_last_queued() is None
    assert sync.get_profile()["base_url"] == ""


def test_profile_round_trip(store_dbs):
    profile = sync.get_profile()
    profile["base_url"] = "https://api.example.com"
    sync.save_profile(profile)
    assert sync.get_profile()["base_url"] == "https://api.example.com"


def test_key_is_stored_apart_from_profile_and_masked(store_dbs):
    sync.save_key("Bearer abcdef1234")
    assert sync.get_key() == "Bearer abcdef1234"
    assert sync.mask_key(sync.get_key()) == "••••1234"
    assert "abcdef" not in str(sync.get_profile())


@pytest.mark.parametrize("key, shown", [("abc", "set"), ("12345678", "set"), ("123456789", "••••6789")])
def test_short_keys_are_not_partly_shown(store_dbs, key, shown):
    sync.save_key(key)
    assert sync.mask_key(sync.get_key()) == shown


def test_enable_and_interval(store_dbs):
    sync.set_enabled(True)
    sync.set_interval_hours(6)
    assert sync.is_enabled() is True and sync.get_interval_hours() == 6


@pytest.mark.parametrize("hours", [0, 721, -5])
def test_interval_out_of_range(store_dbs, hours):
    with pytest.raises(ValueError):
        sync.set_interval_hours(hours)


def test_last_queued_round_trip(store_dbs):
    when = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
    sync.set_last_queued(when)
    assert sync.get_last_queued() == when


def test_request_count_resets_on_new_day(store_dbs):
    sync.count_request("2026-10-10")
    sync.count_request("2026-10-10")
    assert sync.requests_today("2026-10-10") == 2
    assert sync.requests_today("2026-10-11") == 0
    sync.count_request("2026-10-11")
    assert sync.requests_today("2026-10-11") == 1


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


def test_log_channel(store_dbs):
    with closing(sqlite3.connect("db/settings.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_logs VALUES (10, 555)")
        conn.commit()
    assert sync.log_channel_id("10") == 555
    assert sync.log_channel_id(20) is None


def test_load_settings_reads_several_at_once(store_dbs):
    sync.set_enabled(True)
    sync.set_interval_hours(6)
    sync.save_key("abc")
    assert sync.load_settings("enabled", "interval", "key", "last_queued") == {
        "enabled": True, "interval": 6, "key": "abc", "last_queued": None}
    assert sync.load_connection() == (sync.get_profile(), "abc")


def test_changes_post_to_the_alliance_log_channel(store_dbs):
    with closing(sqlite3.connect("db/settings.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_logs VALUES (10, 555)")
        conn.commit()
    sent = []

    class Channel:
        async def send(self, embed):
            sent.append(embed.title)

    bot = types.SimpleNamespace(get_channel=lambda channel_id: Channel() if channel_id == 555 else None)
    cog = sync.AllianceApiSync(bot)
    asyncio.run(cog.post_changes({"10": ["a"], "20": ["b"]}, {10: "Wolves"}))
    assert sent == [f"{sync.theme.retryIcon} Player Sync: Wolves"]


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
    return asyncio.run(sync.run_sync(fetch, kwargs.pop("profile", PROFILE), sleep=clock.sleep, clock=clock,
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
    sync.count_request("2026-10-10")
    profile = normalise({**PROFILE, "per_day": 2})
    fetch, calls = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    _run(fetch, profile=profile)
    assert calls == [1] and sync.requests_today("2026-10-10") == 2


def test_budget_is_read_once_per_day(run_dbs, monkeypatch):
    reads = []
    real_read = sync.requests_today

    def counted(day):
        reads.append(day)
        return real_read(day)

    monkeypatch.setattr(sync, "requests_today", counted)
    days = iter(["2026-10-10", "2026-10-10", "2026-10-11"])
    fetch, _ = _fetcher({1: _ok(), 2: _ok(), 3: _ok()})
    clock = Clock()
    asyncio.run(sync.run_sync(fetch, PROFILE, sleep=clock.sleep, clock=clock, today=lambda: next(days)))
    assert reads == ["2026-10-10", "2026-10-11"]
    assert real_read("2026-10-11") == 1


def test_budget_counts_every_request_including_retries(run_dbs):
    fetch, _ = _fetcher({1: [FetchResult(503), _ok()], 2: _ok(), 3: _ok()})
    _run(fetch)
    assert sync.requests_today("2026-10-10") == 4


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
    assert sync.get_last_queued() is not None


def test_queue_sync_refuses_second_job(dbs):
    queue = FakeQueue([{"action": "api_sync", "details": {}}])
    assert _cog(queue).queue_sync() == "busy" and queue.enqueued == []


def test_queue_sync_without_queue(dbs):
    assert _cog(None).queue_sync() == "unavailable"


def test_handler_saves_and_reraises_on_preempt(dbs, monkeypatch):
    async def fake_run(fetch, profile, **kwargs):
        return sync.RunResult(checked=1, changes={"10": ["`1` A: Name A → B"]}, preempted=True)

    posted = []

    async def fake_post(self, changes, names):
        posted.append(changes)

    monkeypatch.setattr(sync, "run_sync", fake_run)
    monkeypatch.setattr(sync.AllianceApiSync, "session", lambda self: object())
    monkeypatch.setattr(sync.AllianceApiSync, "post_changes", fake_post)
    monkeypatch.setattr(sync.PermissionManager, "list_alliances", staticmethod(lambda: []))
    cog = _cog(FakeQueue())
    with pytest.raises(pq.PreemptedException):
        asyncio.run(cog.handle_api_sync({"id": 1, "details": {"alliance_id": None}}))
    last_run = sync.load_settings("last_run")["last_run"]
    assert last_run["checked"] == 1 and last_run["stopped"] is None and posted == [{"10": ["`1` A: Name A → B"]}]


def test_change_embeds_cap_lines():
    lines = [f"line {i}" for i in range(25)]
    (alliance, embed), = sync.change_embeds({"10": lines}, {10: "Wolves"})
    assert alliance == "10" and "Wolves" in embed.title
    assert "line 19" in embed.description and "line 20" not in embed.description
    assert embed.description.endswith("and 5 more")


def test_running_now_line():
    assert describe_job({"action": "api_sync", "details": {"alliance_id": None}}, {}) == "Player sync, all alliances"
    assert describe_job({"action": "api_sync", "alliance_id": 10, "details": {"alliance_id": 10}},
                        {10: "Wolves"}) == "Player sync for Wolves"


def _capture_run(monkeypatch):
    seen = {}

    async def fake_run(fetch, profile, **kwargs):
        seen.update(kwargs)
        return sync.RunResult()

    async def fake_post(self, changes, names):
        pass

    monkeypatch.setattr(sync, "run_sync", fake_run)
    monkeypatch.setattr(sync.AllianceApiSync, "session", lambda self: object())
    monkeypatch.setattr(sync.AllianceApiSync, "post_changes", fake_post)
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
