"""New gift codes must always end up in gift_codes, whatever path found them.

Covers two bugs: the History Scan INSERT referenced columns that don't exist
in the real schema (every scan died on the first new code and reported "No
gift codes found"), and a channel-posted code with an inconclusive immediate
validation was never written as 'pending' - once the in-memory backoff chain
died (or the bot restarted) the 2h periodic loop couldn't see it, so the code
was silently dropped despite the embed's re-checking promise.
"""
import asyncio
import logging
import sqlite3
import types

import cogs.gift_redemption as gr


def _giftcode_db():
    """In-memory giftcode.sqlite with the REAL schema (main.py create_tables
    plus the validation_status ALTER from gift_operations)."""
    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE gift_codes (giftcode TEXT PRIMARY KEY, date TEXT)")
    mc.execute("ALTER TABLE gift_codes ADD COLUMN validation_status TEXT DEFAULT 'pending'")
    mc.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT)")
    mc.commit()
    return mc


def _mk_cog(mc):
    return types.SimpleNamespace(
        cursor=mc.cursor(),
        conn=mc,
        bot=types.SimpleNamespace(user=object(), get_channel=lambda cid: None),
        logger=logging.getLogger("test"),
        clean_gift_code=lambda c: c.strip(),
        api=None,
    )


class _Msg:
    def __init__(self, content, embeds=None):
        self.content = content
        self.embeds = embeds or []
        self.author = "player"
        self.reactions = []
        self.reacted = []

    async def add_reaction(self, emoji):
        self.reacted.append(str(emoji))


def _embed(title=None, description=None, fields=()):
    return types.SimpleNamespace(
        title=title, description=description,
        fields=[types.SimpleNamespace(name=n, value=v) for n, v in fields],
    )


class _Chan:
    id = 123
    name = "gift-codes"

    def __init__(self, msgs):
        self._msgs = msgs

    def history(self, limit=None, oldest_first=False):
        async def gen():
            for m in self._msgs:
                yield m
        return gen()


async def _anoop(*a, **k):
    pass


def test_history_scan_persists_new_valid_code(monkeypatch):
    mc = _giftcode_db()
    cog = _mk_cog(mc)

    async def fake_silent(cog_, code):
        return True

    monkeypatch.setattr(gr, "_validate_gift_code_silent", fake_silent)
    monkeypatch.setattr(gr, "_process_auto_use", _anoop)
    monkeypatch.setattr(gr, "_send_scan_results_message", _anoop)
    monkeypatch.setattr(gr.asyncio, "sleep", _anoop)

    msg = _Msg("ABC123")
    results = asyncio.run(gr.scan_historical_messages(cog, _Chan([msg]), 5))

    assert results["total_codes_found"] == 1
    assert results["new_codes"] == ["ABC123"]
    row = mc.execute(
        "SELECT validation_status FROM gift_codes WHERE giftcode='ABC123'"
    ).fetchone()
    assert row is not None, "scan must insert the new code"
    assert row[0] == "validated"


def test_history_scan_inconclusive_stays_pending(monkeypatch):
    mc = _giftcode_db()
    cog = _mk_cog(mc)
    scheduled = []

    async def fake_silent(cog_, code):
        return None  # throttled / no verdict

    monkeypatch.setattr(gr, "_validate_gift_code_silent", fake_silent)
    monkeypatch.setattr(gr, "_send_scan_results_message", _anoop)
    monkeypatch.setattr(gr.asyncio, "sleep", _anoop)
    monkeypatch.setattr(
        gr, "schedule_revalidation", lambda cog_, code, source: scheduled.append(code)
    )

    asyncio.run(gr.scan_historical_messages(cog, _Chan([_Msg("MAYBE1")]), 5))

    row = mc.execute(
        "SELECT validation_status FROM gift_codes WHERE giftcode='MAYBE1'"
    ).fetchone()
    assert row is not None, "inconclusive code must still be persisted"
    assert row[0] == "pending", "inconclusive must not be branded invalid"
    assert scheduled == ["MAYBE1"]


def test_channel_inconclusive_code_parked_as_pending(monkeypatch):
    mc = _giftcode_db()
    cog = _mk_cog(mc)
    scheduled = []

    async def fake_validate(cog_, code, source, force=False):
        return None, "Validation inconclusive (CAPTCHA_TOO_FREQUENT)"

    monkeypatch.setattr(gr, "validate_gift_code_immediately", fake_validate)
    monkeypatch.setattr(
        gr, "schedule_revalidation", lambda cog_, code, source: scheduled.append(code)
    )

    process = {"id": 1, "details": {"giftcode": "NEWCODE", "source": "channel"}}
    asyncio.run(gr.handle_gift_validate_process(cog, process))

    row = mc.execute(
        "SELECT validation_status FROM gift_codes WHERE giftcode='NEWCODE'"
    ).fetchone()
    assert row is not None, "inconclusive channel code must be written as pending"
    assert row[0] == "pending"
    assert scheduled == ["NEWCODE"]


def test_channel_inconclusive_keeps_invalid_row_untouched(monkeypatch):
    """A reactivation candidate (stored 'invalid') whose forced re-probe is
    inconclusive must stay 'invalid' - not get resurrected to pending here."""
    mc = _giftcode_db()
    mc.execute("INSERT INTO gift_codes VALUES ('OLDCODE', '2026-01-01', 'invalid')")
    mc.commit()
    cog = _mk_cog(mc)

    async def fake_validate(cog_, code, source, force=False):
        return None, "inconclusive"

    monkeypatch.setattr(gr, "validate_gift_code_immediately", fake_validate)
    monkeypatch.setattr(gr, "schedule_revalidation", lambda *a: None)

    process = {"id": 2, "details": {"giftcode": "OLDCODE", "source": "channel"}}
    asyncio.run(gr.handle_gift_validate_process(cog, process))

    row = mc.execute(
        "SELECT validation_status FROM gift_codes WHERE giftcode='OLDCODE'"
    ).fetchone()
    assert row[0] == "invalid"


def test_history_scan_reacts_info_on_known_codes(monkeypatch):
    """Already-known codes get the info reaction, matching the live
    already-known response - not a status icon like X on an old invalid."""
    mc = _giftcode_db()
    mc.execute("INSERT INTO gift_codes VALUES ('KNOWNGOOD1', '2026-01-01', 'validated')")
    mc.execute("INSERT INTO gift_codes VALUES ('KNOWNBAD1', '2026-01-01', 'invalid')")
    mc.commit()
    cog = _mk_cog(mc)

    async def fail_validate(cog_, code):
        raise AssertionError("known codes must not be re-validated by the scan")

    monkeypatch.setattr(gr, "_validate_gift_code_silent", fail_validate)
    monkeypatch.setattr(gr, "_send_scan_results_message", _anoop)
    monkeypatch.setattr(gr.asyncio, "sleep", _anoop)

    good, bad = _Msg("KNOWNGOOD1"), _Msg("KNOWNBAD1")
    asyncio.run(gr.scan_historical_messages(cog, _Chan([good, bad]), 5))

    info = str(gr.theme.infoIcon)
    assert good.reacted == [info]
    assert bad.reacted == [info], f"known-invalid must get info, got {bad.reacted}"


def test_history_scan_reads_codes_from_embeds(monkeypatch):
    mc = _giftcode_db()
    cog = _mk_cog(mc)

    async def fake_silent(cog_, code):
        return True

    monkeypatch.setattr(gr, "_validate_gift_code_silent", fake_silent)
    monkeypatch.setattr(gr, "_process_auto_use", _anoop)
    monkeypatch.setattr(gr, "_send_scan_results_message", _anoop)
    monkeypatch.setattr(gr.asyncio, "sleep", _anoop)

    msg = _Msg("", embeds=[_embed(
        title="New Gift Code!",
        description="Redeem now\n**Code:** `EMBEDCODE1`",
    )])
    results = asyncio.run(gr.scan_historical_messages(cog, _Chan([msg]), 5))

    assert results["new_codes"] == ["EMBEDCODE1"], "codes inside embeds must be found"
    row = mc.execute(
        "SELECT validation_status FROM gift_codes WHERE giftcode='EMBEDCODE1'"
    ).fetchone()
    assert row == ("validated",)


def test_history_scan_embed_needs_code_label(monkeypatch):
    """Embed text only counts as a code after a Code: label - standalone
    lines (event names, dates, code-looking words) must not be validated."""
    mc = _giftcode_db()
    cog = _mk_cog(mc)

    async def fake_silent(cog_, code):
        return True

    monkeypatch.setattr(gr, "_validate_gift_code_silent", fake_silent)
    monkeypatch.setattr(gr, "_process_auto_use", _anoop)
    monkeypatch.setattr(gr, "_send_scan_results_message", _anoop)
    monkeypatch.setattr(gr.asyncio, "sleep", _anoop)

    labeled = _Msg("", embeds=[_embed(description="Redeem now\nCode: SUMMER2026")])
    unlabeled = _Msg("", embeds=[_embed(
        description="Gift code drop!\nJULY2026EVENT\nRewards",
        fields=[("Expires", "Aug 1")],
    )])
    results = asyncio.run(gr.scan_historical_messages(cog, _Chan([labeled, unlabeled]), 5))

    assert results["new_codes"] == ["SUMMER2026"], "only Code:-labeled text may match"
