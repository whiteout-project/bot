"""Discover a player's state (kid) via a gift-code probe.

The API checks fid+kid before the code, so an invalid code reveals the state with no
redemption: a wrong kid returns 40020, the correct kid returns 40014. Probes are
per-FID rate limited, so we try the alliance state, alliance-mates' states, then a
bounded window around the last-known state, and resolve different FIDs in parallel.
"""
import asyncio
import hashlib
import random
import sqlite3
import string
from contextlib import closing
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter

from .browser_headers import get_headers

PER_FID_INTERVAL = 2.2      # seconds between probes on the SAME fid (per-FID limit)
THROTTLE_BACKOFF = 4.0      # extra wait after a TOO FREQUENT (40019)
MAX_PROBE_RETRIES = 4       # retries for a single (fid,kid) when throttled/transient
PROGRESS_EVERY = 30         # log a scan-progress heartbeat every N probes (~once a minute at this pace)
CHECKPOINT_EVERY = 10       # save a range scan's resume point every N probes
TRANSFER_WINDOW = 200       # state transfers stay within ~200 IDs of the origin (same transfer group)
SCAN_CONCURRENCY = 6        # fids scanned at once - the limit is per-FID, not per-IP


class StateResolveInterrupted(Exception):
    """Raised by the caller's `wait_until_idle` to make the resolver stop and yield."""


class StateResolveUnavailable(Exception):
    """The API gave no answer for a state after every retry; the scan resumes there later."""


def _probe_code():
    """A code that cannot be a real gift code, so a probe never redeems anything."""
    return "ZZ" + "".join(random.choices(string.ascii_uppercase + string.digits, k=14))


def _sign(secret, data):
    encoded = "&".join(f"{k}={data[k]}" for k in sorted(data))
    sign = hashlib.md5(f"{encoded}{secret}".encode()).hexdigest()
    return {"sign": sign, **data}


def _make_session(cog):
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=cog.retry_config))
    session.headers.update(get_headers(cog.wos_giftcode_redemption_url))
    return session


def classify_probe(status_code, payload):
    """match | nomatch | throttle | error, from a probe's HTTP status + JSON body."""
    if status_code in (429, 500, 502, 503, 504):
        return "throttle"
    err = payload.get("err_code")
    if err == 40019:                       # TOO FREQUENT
        return "throttle"
    if err == 40020:                       # USER INFO ERROR - fid+kid didn't resolve
        return "nomatch"
    if payload.get("msg"):                 # any real verdict = fid+kid resolved
        return "match"
    return "error"


async def _probe(cog, session, fid, kid, code):
    from . import gift_redemption
    import time as _time
    payload_in = {"fid": str(fid), "cdk": code, "kid": str(kid), "time": str(int(_time.time()))}
    data = _sign(cog.wos_encrypt_key, payload_in)

    def _post():
        return session.post(cog.wos_giftcode_url, data=data, timeout=(10, 30))

    await gift_redemption.await_rate_limit(cog)  # probes spend the same per-IP budget as redemption
    try:
        response = await asyncio.to_thread(_post)
    except requests.exceptions.RequestException:
        return "error"
    gift_redemption.note_rate_limit(cog, response)
    try:
        body = response.json()
    except ValueError:
        body = {}
    return classify_probe(response.status_code, body)


def _candidate_kids(fid):
    """Ordered kid candidates for `fid`: the alliance's bound state, then the states
    alliance-mates are in (most common first). Best-effort; DB errors -> []."""
    try:
        with sqlite3.connect('db/users.sqlite', timeout=30.0) as uconn:
            row = uconn.execute("SELECT alliance FROM users WHERE fid = ?", (fid,)).fetchone()
            alliance = row[0] if row else None
            mates = []
            if alliance:
                mates = [r[0] for r in uconn.execute(
                    "SELECT kid FROM users WHERE alliance = ? AND kid IS NOT NULL "
                    "GROUP BY kid ORDER BY COUNT(*) DESC", (alliance,)).fetchall()]
    except sqlite3.Error:
        return []
    candidates = []
    try:
        if alliance is not None:
            with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as aconn:
                arow = aconn.execute(
                    "SELECT kid FROM alliance_list WHERE alliance_id = ?", (alliance,)).fetchone()
                if arow and arow[0] is not None:
                    candidates.append(arow[0])
    except sqlite3.Error:
        pass
    candidates.extend(mates)
    seen, ordered = set(), []
    for k in candidates:
        if k not in seen:
            seen.add(k); ordered.append(k)
    return ordered


def _reference_center(fid):
    """State to center a transfer-window scan on: the member's own (possibly stale) kid
    if they have one - a transfer lands within ~200 of it - else their alliance's state."""
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        row = conn.execute("SELECT kid, alliance FROM users WHERE fid = ?", (fid,)).fetchone()
    if not row:
        return None
    kid, alliance = row
    if kid is not None:
        return kid
    if alliance is not None:
        return get_alliance_kid(alliance)
    return None


def _window_kids(center, width, exclude):
    """States within `width` of `center`, nearest first (center±1, ±2, ...), clamped to >=1.
    Skips anything already in `exclude` and adds what it yields to it."""
    out = []
    for d in range(1, width + 1):
        for k in (center + d, center - d):
            if k >= 1 and k not in exclude:
                exclude.add(k)
                out.append(k)
    return out


def _likely_kids(fid, prefer, window):
    """Alliance and mate states, long-shot guesses, then the transfer window, nearest first."""
    ordered = _candidate_kids(fid)
    ordered = list(ordered) + [k for k in prefer if k not in set(ordered)]
    if window:
        center = _reference_center(fid)
        if center is not None:
            ordered = ordered + _window_kids(center, window, set(ordered))
    return ordered


async def _probe_with_retries(cog, session, fid, kid, code, pace, wait_until_idle, *, first):
    result = "error"
    for attempt in range(MAX_PROBE_RETRIES):
        if wait_until_idle is not None:
            await wait_until_idle()
        if not (first and attempt == 0):
            await asyncio.sleep(pace)
        result = await _probe(cog, session, fid, kid, code)
        if result in ("match", "nomatch"):
            return result
        await asyncio.sleep(THROTTLE_BACKOFF)
    # An outage is not a "no": moving on would skip the right state and mark the member done.
    raise StateResolveUnavailable(kid)


async def resolve_state(cog, fid, *, scan_range=None, start_at=None, window=TRANSFER_WINDOW,
                        pace=PER_FID_INTERVAL, wait_until_idle=None, on_progress=None,
                        on_checkpoint=None, prefer=()):
    """Find `fid`'s true state, or None: likely states first, then `scan_range` from `start_at`
    (a resume skips the likely phase). Hours per member on a wide range, so never inline.
    `wait_until_idle` runs before every probe and raises StateResolveInterrupted to stop;
    `on_checkpoint(next_kid)` records where a range scan should resume."""
    likely = await asyncio.to_thread(_likely_kids, fid, prefer, window)
    if start_at is not None and scan_range:
        # Resuming: the sweep reaches anything from start_at on; earlier likely states may be new.
        likely = [k for k in likely if not start_at <= k <= scan_range[1]]
    tried = set(likely)
    sweep = []
    if scan_range:
        lo, hi = scan_range
        sweep = [k for k in range(max(lo, start_at or lo), hi + 1) if k not in tried]
    ordered = likely + sweep
    if not ordered:
        return None

    session = _make_session(cog)
    code = _probe_code()
    cog.logger.info(f"GiftOps: resolving state for FID {fid} - up to {len(ordered)} probes "
                    f"(~{int(len(ordered) * (pace + 1)) // 60} min)")
    probes, next_beat, next_kid = 0, PROGRESS_EVERY, None
    try:
        for index, kid in enumerate(ordered):
            if index >= len(likely):
                next_kid = kid
                if on_checkpoint is not None and (index == len(likely) or probes % CHECKPOINT_EVERY == 0):
                    await on_checkpoint(kid)
            result = await _probe_with_retries(cog, session, fid, kid, code, pace, wait_until_idle,
                                               first=probes == 0)
            probes += 1
            if result == "match":
                next_kid = None
                cog.logger.info(f"GiftOps: resolved state for FID {fid} -> {kid} after {probes} probe(s)")
                return kid
            if probes >= next_beat:
                cog.logger.info(f"GiftOps: FID {fid} state scan - {probes}/{len(ordered)} states checked")
                next_beat += PROGRESS_EVERY
                if on_progress is not None:
                    await on_progress(probes, len(ordered))
        next_kid = None
        cog.logger.info(f"GiftOps: no state found for FID {fid} after {probes} probe(s)")
        return None
    finally:
        session.close()
        if next_kid is not None and on_checkpoint is not None:
            await on_checkpoint(next_kid)  # interrupted mid-range: resume on this unanswered state


# --- Alliance -> state binding (majority vote over members' known kid) --------------

BIND_THRESHOLD = 0.6        # a state must hold this share of known-kid members to bind
BIND_MIN_KNOWN = 3          # and at least this many members must have a known kid


def _state_distribution(alliance_id):
    """[(kid, count), ...] most-common first, for members with a known kid."""
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        return conn.execute(
            "SELECT kid, COUNT(*) FROM users WHERE alliance = ? AND kid IS NOT NULL "
            "GROUP BY kid ORDER BY COUNT(*) DESC", (str(alliance_id),)).fetchall()


def compute_alliance_binding(alliance_id, *, threshold=BIND_THRESHOLD, min_known=BIND_MIN_KNOWN):
    """Majority state among an alliance's members with a known kid.
    Returns (kid, share, known_count), or None when too few knowns / no clear winner."""
    rows = _state_distribution(alliance_id)
    if not rows:
        return None
    known = sum(c for _, c in rows)
    top_kid, top_count = rows[0]
    share = top_count / known
    # A unanimous small alliance is still a confident bind even below min_known.
    if (known < min_known and share < 1.0) or share < threshold:
        return None
    return (top_kid, share, known)


def apply_alliance_binding(alliance_id, kid):
    """Write the alliance's bound state to alliance_list.kid."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        conn.execute("UPDATE alliance_list SET kid = ? WHERE alliance_id = ?", (kid, alliance_id))
        conn.commit()


def get_alliance_kid(alliance_id):
    """The alliance's currently-bound state, or None."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        row = conn.execute(
            "SELECT kid FROM alliance_list WHERE alliance_id = ?", (alliance_id,)).fetchone()
    return row[0] if row and row[0] is not None else None


def is_multistate(alliance_id):
    """True if the alliance is flagged multistate (members span many states - never bound)."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        row = conn.execute(
            "SELECT multistate FROM alliance_list WHERE alliance_id = ?", (alliance_id,)).fetchone()
    return bool(row and row[0])


def set_multistate(alliance_id, on):
    """Flag/unflag an alliance as multistate. Flagging clears any home state and lock."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        if on:
            conn.execute(
                "UPDATE alliance_list SET multistate = 1, kid = NULL, state_locked = 0 WHERE alliance_id = ?",
                (alliance_id,))
        else:
            conn.execute("UPDATE alliance_list SET multistate = 0 WHERE alliance_id = ?", (alliance_id,))
        conn.commit()


def is_state_locked(alliance_id):
    """True if the alliance is explicitly state-locked (rejects out-of-state adds)."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        row = conn.execute(
            "SELECT COALESCE(state_locked, 0) FROM alliance_list WHERE alliance_id = ?",
            (alliance_id,)).fetchone()
    return bool(row and row[0])


def set_state_locked(alliance_id, on):
    """Turn the deliberate state-lock on/off. Locking requires a home state already set."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        conn.execute("UPDATE alliance_list SET state_locked = ? WHERE alliance_id = ?",
                     (1 if on else 0, alliance_id))
        conn.commit()


def survey_alliance_bindings(*, threshold=BIND_THRESHOLD, min_known=BIND_MIN_KNOWN):
    """Per alliance: proposed binding + confidence, without writing anything.
    Returns [{alliance_id, name, current_kid, multistate, proposed_kid, share, known}]."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        alliances = conn.execute(
            "SELECT alliance_id, name, kid, COALESCE(multistate, 0), COALESCE(state_locked, 0) "
            "FROM alliance_list").fetchall()
    report = []
    for alliance_id, name, current_kid, multistate, state_locked in alliances:
        binding = None if multistate else compute_alliance_binding(
            alliance_id, threshold=threshold, min_known=min_known)
        report.append({
            "alliance_id": alliance_id, "name": name, "current_kid": current_kid,
            "multistate": bool(multistate), "state_locked": bool(state_locked),
            "proposed_kid": binding[0] if binding else None,
            "share": binding[1] if binding else None,
            "known": binding[2] if binding else 0,
        })
    return report


def bind_all_alliances(*, threshold=BIND_THRESHOLD, min_known=BIND_MIN_KNOWN, only_unbound=True):
    """Apply the majority-vote binding to every alliance with a confident winner.
    Skips multistate alliances. only_unbound=True skips already-bound ones. Returns applied list."""
    applied = []
    for row in survey_alliance_bindings(threshold=threshold, min_known=min_known):
        if row["multistate"] or row["proposed_kid"] is None:
            continue
        if only_unbound and row["current_kid"] is not None:
            continue
        apply_alliance_binding(row["alliance_id"], row["proposed_kid"])
        applied.append(row)
    return applied


MULTISTATE_MIN_STATES = 2       # genuine multi-state = at least this many states...
MULTISTATE_MIN_PER_STATE = 2    # ...each holding at least this many members


def looks_multistate(alliance_id, *, threshold=BIND_THRESHOLD,
                     min_states=MULTISTATE_MIN_STATES, min_per=MULTISTATE_MIN_PER_STATE):
    """True if the alliance genuinely spans states: no majority reaches `threshold` AND
    at least `min_states` states each hold `min_per`+ members. A few stray migrants in an
    otherwise single-state alliance do NOT trip this (that alliance still binds)."""
    dist = _state_distribution(alliance_id)
    if not dist:
        return False
    known = sum(c for _, c in dist)
    if dist[0][1] / known >= threshold:      # a clear majority -> bind, not multistate
        return False
    strong = [k for k, c in dist if c >= min_per]
    return len(strong) >= min_states


def auto_flag_multistate():
    """Flag currently-unbound alliances that genuinely span states (so they migrate to
    multistate instead of mis-binding). Existing members keep redeeming via their own kid.
    Returns the flagged rows."""
    flagged = []
    for row in survey_alliance_bindings():
        if row["multistate"] or row["current_kid"] is not None:
            continue
        if looks_multistate(row["alliance_id"]):
            set_multistate(row["alliance_id"], True)
            flagged.append(row)
    return flagged


# --- Member state backfill ----------------------------------------------------------

def set_user_kid(fid, kid, *, conn=None):
    """Set a member's state and clear any wrong-state flag and scan progress.
    Pass `conn` to join the caller's transaction (not committed here)."""
    sql = ("UPDATE users SET kid = ?, state_mismatch_at = NULL, kingdom_scan_next = NULL, "
           "kingdom_scan_done_at = NULL WHERE fid = ?")
    if conn is not None:
        conn.execute(sql, (kid, fid))
        return
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as own:
        own.execute(sql, (kid, fid))
        own.commit()


# --- Wrong-state flag (set when redemption gets a 40020 for a member) ----------------

def flag_state_mismatch(fid):
    """Mark that the state on file for `fid` was rejected by the game."""
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        conn.execute("UPDATE users SET state_mismatch_at = ? WHERE fid = ?",
                     (datetime.now().isoformat(timespec='seconds'), fid))
        conn.commit()


def flag_state_mismatch_many(fids):
    """Mark the state on file for every fid as rejected by the game, in one transaction."""
    flagged_at = datetime.now().isoformat(timespec='seconds')
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        conn.executemany("UPDATE users SET state_mismatch_at = ? WHERE fid = ?",
                         [(flagged_at, fid) for fid in fids])
        conn.commit()


def clear_state_mismatch(fid):
    """Drop the wrong-state flag without touching the stored state."""
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        conn.execute("UPDATE users SET state_mismatch_at = NULL WHERE fid = ?", (fid,))
        conn.commit()


def fids_with_state_mismatch():
    """[(fid, nickname, kid, alliance, flagged_at), ...] for members the game rejected."""
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        return conn.execute(
            "SELECT fid, nickname, kid, alliance, state_mismatch_at FROM users "
            "WHERE state_mismatch_at IS NOT NULL ORDER BY state_mismatch_at DESC"
        ).fetchall()


def fids_missing_state():
    """Members with no state on file (redemption can't run for them)."""
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        return [r[0] for r in conn.execute(
            "SELECT fid FROM users WHERE kid IS NULL AND alliance IS NOT NULL AND alliance != ''"
        ).fetchall()]


def _alliance_bindings_by_str_id():
    """{str(alliance_id): kid} for every bound, non-multistate alliance."""
    with sqlite3.connect('db/alliance.sqlite', timeout=30.0) as conn:
        return {str(aid): kid for aid, kid in conn.execute(
            "SELECT alliance_id, kid FROM alliance_list "
            "WHERE kid IS NOT NULL AND COALESCE(multistate, 0) = 0").fetchall()}


def assign_alliance_kid_to_missing():
    """No-API backfill: stateless members inherit their alliance's state.
    Returns the fids updated - they were skipped by every redemption until now."""
    bindings = _alliance_bindings_by_str_id()
    if not bindings:
        return []
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        rows = conn.execute(
            "SELECT fid, alliance FROM users WHERE kid IS NULL AND alliance IS NOT NULL AND alliance != ''"
        ).fetchall()
        updated = []
        for fid, alliance in rows:
            kid = bindings.get(str(alliance))
            if kid is not None:
                set_user_kid(fid, kid, conn=conn)
                updated.append(fid)
        conn.commit()
    return updated


async def verify_add_state(cog, fid, alliance_id):
    """Add-time state for a member not yet in `users`. Returns (kid, verified):
    a home-state alliance is probed once - in that state -> (K, True), else (None, False);
    an alliance with no home state returns (None, False) without probing."""
    alliance_kid = await asyncio.to_thread(get_alliance_kid, alliance_id)
    if alliance_kid is None:
        return None, False
    session = _make_session(cog)
    result = "error"
    try:
        for _ in range(MAX_PROBE_RETRIES):
            result = await _probe(cog, session, fid, alliance_kid, _probe_code())
            if result == "throttle":
                await asyncio.sleep(THROTTLE_BACKOFF)
                continue
            break
    finally:
        session.close()
    return (alliance_kid, True) if result == "match" else (None, False)


# --- State scan: settings, targets, per-member resume point --------------------------

SCAN_RETRY_DAYS = 30          # a member a full scan didn't find is retried after this
SCAN_MAX_KINGDOM = 99999
_NEEDS_SCAN = ("(kid IS NULL OR state_mismatch_at IS NOT NULL) "
               "AND alliance IS NOT NULL AND alliance != ''")


def _setting(key):
    with closing(sqlite3.connect('db/settings.sqlite', timeout=30.0)) as conn:
        row = conn.execute("SELECT setting_value FROM bot_global_settings WHERE setting_key = ?",
                           (key,)).fetchone()
    return row[0] if row else None


def _put_setting(key, value):
    with closing(sqlite3.connect('db/settings.sqlite', timeout=30.0)) as conn:
        conn.execute("INSERT OR REPLACE INTO bot_global_settings (setting_key, setting_value) "
                     "VALUES (?, ?)", (key, str(value)))
        conn.commit()


def _highest_kid_on_file():
    with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as conn:
        users_max = conn.execute("SELECT MAX(kid) FROM users").fetchone()[0] or 0
    with closing(sqlite3.connect('db/alliance.sqlite', timeout=30.0)) as conn:
        alliance_max = conn.execute("SELECT MAX(kid) FROM alliance_list").fetchone()[0] or 0
    return max(int(users_max), int(alliance_max), 1)


def scan_enabled():
    return _setting('kingdom_scan_enabled') == '1'


def get_scan_settings():
    """{'enabled', 'min', 'max', 'custom'}; without a saved range: 1 to the highest state on file."""
    lo, hi = _setting('kingdom_scan_min'), _setting('kingdom_scan_max')
    custom = lo is not None and hi is not None
    return {
        'enabled': scan_enabled(),
        'min': int(lo) if custom else 1,
        'max': int(hi) if custom else _highest_kid_on_file(),
        'custom': custom,
    }


def clear_scan_range():
    """Back to the default range: 1 to the highest state on file."""
    with closing(sqlite3.connect('db/settings.sqlite', timeout=30.0)) as conn:
        conn.execute("DELETE FROM bot_global_settings WHERE setting_key IN "
                     "('kingdom_scan_min', 'kingdom_scan_max')")
        conn.commit()
    _write_scan("UPDATE users SET kingdom_scan_next = NULL, kingdom_scan_done_at = NULL", ())


def set_scan_enabled(on):
    _put_setting('kingdom_scan_enabled', '1' if on else '0')


def set_scan_range(lo, hi):
    if not (1 <= lo <= hi <= SCAN_MAX_KINGDOM):
        raise ValueError(f"range must satisfy 1 <= min <= max <= {SCAN_MAX_KINGDOM}")
    if (_setting('kingdom_scan_min'), _setting('kingdom_scan_max')) == (str(lo), str(hi)):
        return
    _put_setting('kingdom_scan_min', lo)
    _put_setting('kingdom_scan_max', hi)
    # Saved resume points and "not found" marks belong to the old range.
    _write_scan("UPDATE users SET kingdom_scan_next = NULL, kingdom_scan_done_at = NULL", ())


def members_to_fix():
    """[(fid, nickname, kid, 'wrong'|'missing'), ...]: rejected states first, newest first."""
    wrong = [(fid, nick, kid, 'wrong') for fid, nick, kid, _a, _f in fids_with_state_mismatch()]
    with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as conn:
        missing = [(fid, nick, None, 'missing') for fid, nick in conn.execute(
            "SELECT fid, nickname FROM users WHERE kid IS NULL AND alliance IS NOT NULL "
            "AND alliance != '' AND state_mismatch_at IS NULL ORDER BY nickname").fetchall()]
    return wrong + missing


def scan_targets(*, auto, now=None):
    """FIDs that need a scan. Auto-scan skips members a full scan missed in the last 30 days."""
    sql = f"SELECT fid FROM users WHERE {_NEEDS_SCAN}"
    params = ()
    if auto:
        cutoff = ((now or datetime.now()) - timedelta(days=SCAN_RETRY_DAYS)).isoformat(timespec='seconds')
        sql += " AND (kingdom_scan_done_at IS NULL OR kingdom_scan_done_at < ?)"
        params = (cutoff,)
    with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as conn:
        return [r[0] for r in conn.execute(sql + " ORDER BY fid", params).fetchall()]


def needs_scan(fid):
    with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as conn:
        return conn.execute(f"SELECT 1 FROM users WHERE fid = ? AND {_NEEDS_SCAN}",
                            (fid,)).fetchone() is not None


def _write_scan(sql, params):
    with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as conn:
        conn.execute(sql, params)
        conn.commit()


def save_scan_position(fid, next_kid):
    _write_scan("UPDATE users SET kingdom_scan_next = ? WHERE fid = ?", (next_kid, fid))


def get_scan_position(fid):
    with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as conn:
        row = conn.execute("SELECT kingdom_scan_next FROM users WHERE fid = ?", (fid,)).fetchone()
    return row[0] if row else None


def mark_scan_done(fid):
    _write_scan("UPDATE users SET kingdom_scan_next = NULL, kingdom_scan_done_at = ? WHERE fid = ?",
                (datetime.now().isoformat(timespec='seconds'), fid))


def reset_scan(fid):
    _write_scan("UPDATE users SET kingdom_scan_next = NULL, kingdom_scan_done_at = NULL WHERE fid = ?",
                (fid,))
