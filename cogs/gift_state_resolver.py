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

import requests
from requests.adapters import HTTPAdapter

from .browser_headers import get_headers

PER_FID_INTERVAL = 2.2      # seconds between probes on the SAME fid (per-FID limit)
THROTTLE_BACKOFF = 4.0      # extra wait after a TOO FREQUENT (40019)
MAX_PROBE_RETRIES = 4       # retries for a single (fid,kid) when throttled/transient
BULK_CONCURRENCY = 6        # fids resolved in parallel (per-IP headroom)
TRANSFER_WINDOW = 200       # state transfers stay within ~200 IDs of the origin (same transfer group)


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
    if status_code in (429, 502, 503, 504):
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
    import time as _time
    payload_in = {"fid": str(fid), "cdk": code, "kid": str(kid), "time": str(int(_time.time()))}
    data = _sign(cog.wos_encrypt_key, payload_in)
    def _post():
        r = session.post(cog.wos_giftcode_url, data=data, timeout=(10, 30))
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, {}
    try:
        code_status, body = await asyncio.to_thread(_post)
    except requests.exceptions.RequestException:
        return "error"
    return classify_probe(code_status, body)


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


async def resolve_state(cog, fid, *, window=TRANSFER_WINDOW, deep_sweep_max=0, pace=PER_FID_INTERVAL):
    """Find `fid`'s true state, or None. Order: alliance/mate candidates, then a transfer
    window (~200 IDs around the member's last-known/alliance state), then an optional
    1..deep_sweep_max sweep. `pace` seconds between same-fid probes."""
    ordered = await asyncio.to_thread(_candidate_kids, fid)
    seen = set(ordered)
    if window:
        center = await asyncio.to_thread(_reference_center, fid)
        if center is not None:
            ordered = list(ordered) + _window_kids(center, window, seen)
    if deep_sweep_max:
        ordered = list(ordered) + [k for k in range(1, deep_sweep_max + 1) if k not in seen]
    if not ordered:
        return None

    session = _make_session(cog)
    code = _probe_code()
    try:
        first = True
        for kid in ordered:
            for _ in range(MAX_PROBE_RETRIES):
                if not first:
                    await asyncio.sleep(pace)
                first = False
                result = await _probe(cog, session, fid, kid, code)
                if result == "throttle":
                    await asyncio.sleep(THROTTLE_BACKOFF)
                    continue
                break
            if result == "match":
                cog.logger.info(f"GiftOps: resolved state for FID {fid} -> {kid}")
                return kid
        return None
    finally:
        session.close()


async def resolve_states_bulk(cog, fids, *, deep_sweep_max=0, pace=PER_FID_INTERVAL,
                              concurrency=BULK_CONCURRENCY):
    """Resolve many fids in parallel (per-IP headroom). Returns {fid: kid} for hits only."""
    sem = asyncio.Semaphore(concurrency)
    results = {}

    async def _one(fid):
        async with sem:
            try:
                kid = await resolve_state(cog, fid, deep_sweep_max=deep_sweep_max, pace=pace)
            except Exception as e:
                cog.logger.warning(f"GiftOps: state resolve failed for FID {fid}: {e}")
                kid = None
            if kid is not None:
                results[fid] = kid

    await asyncio.gather(*(_one(f) for f in fids))
    return results


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

def set_user_kid(fid, kid):
    """Persist a resolved/assigned state for a member."""
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        conn.execute("UPDATE users SET kid = ? WHERE fid = ?", (kid, fid))
        conn.commit()


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
    """Fast, no-API backfill: give NULL-kid members their alliance's bound state.
    Returns the number updated. Members whose alliance is unbound stay NULL."""
    bindings = _alliance_bindings_by_str_id()
    if not bindings:
        return 0
    with sqlite3.connect('db/users.sqlite', timeout=30.0) as conn:
        rows = conn.execute(
            "SELECT fid, alliance FROM users WHERE kid IS NULL AND alliance IS NOT NULL AND alliance != ''"
        ).fetchall()
        updated = 0
        for fid, alliance in rows:
            kid = bindings.get(str(alliance))
            if kid is not None:
                conn.execute("UPDATE users SET kid = ? WHERE fid = ?", (kid, fid))
                updated += 1
        conn.commit()
    return updated


async def resolve_and_persist(cog, fids, *, deep_sweep_max=0, pace=PER_FID_INTERVAL):
    """Resolve each fid's true state via the API probe and write the hits. Returns {fid: kid}."""
    found = await resolve_states_bulk(cog, fids, deep_sweep_max=deep_sweep_max, pace=pace)
    for fid, kid in found.items():
        await asyncio.to_thread(set_user_kid, fid, kid)
    return found


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
