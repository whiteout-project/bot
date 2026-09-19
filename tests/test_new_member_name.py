"""Players added by ID from a screenshot review get the typed name, pre-filled from the screenshot."""
import asyncio
import importlib
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

edit = importlib.import_module("cogs.alliance_member_edit")
bt = importlib.import_module("cogs.bear_track")
review = importlib.import_module("cogs.attendance_ocr_review")

LABEL_LIMIT = 45
FIELD_LIMIT = 5


def _run(coro_or_factory):
    """Some discord.py versions need a running loop to build views and modals."""
    async def wrapper():
        result = coro_or_factory()
        return await result if asyncio.iscoroutine(result) else result
    return asyncio.run(wrapper())


def _type(field, text):
    field._value = text


def _name_field(modal):
    assert modal.name_input in modal.children
    return modal.name_input


# --- naming helpers ---

@pytest.mark.parametrize("ocr, expected", [
    ("XTz", "XTz"),
    ("[NEX]XTz", "XTz"),
    ("[NEX] 6m3", "6m3"),
    ("  Ay   Jl ", "Ay Jl"),
    ("123456789", ""),
    ("", ""),
    (None, ""),
])
def test_suggested_name_uses_readable_screenshot_text(ocr, expected):
    assert edit.suggested_name(ocr) == expected


def test_placeholder_name_is_what_is_placeholder_name_recognizes():
    assert edit.placeholder_name(123) == "Player 123"
    assert edit.is_placeholder_name(edit.placeholder_name(123), 123)


def test_name_field_fits_discord_limits():
    field = edit.new_member_name_input("[NEX]XTz")
    assert len(edit.NEW_NAME_LABEL) <= LABEL_LIMIT
    assert field.default == "XTz"
    assert field.required is False


# --- Bear Tracking forms ---

def _bear_review(rows):
    return SimpleNamespace(rows=rows, original_user_id=1)


def test_bear_edit_row_prefills_the_screenshot_name_for_an_unmatched_row():
    review_view = _bear_review([{"name": "[NEX]XTz", "nickname": None, "fid": None,
                                 "damage": 1000, "rank": 3}])
    modal = _run(lambda: bt.EditRowModal(review_view, 0))

    assert _name_field(modal).default == "XTz"
    assert len(modal.children) <= FIELD_LIMIT


def test_bear_edit_row_leaves_the_name_blank_for_a_matched_row():
    review_view = _bear_review([{"name": "XTz", "nickname": "XTz", "fid": 42,
                                 "damage": 1000, "rank": 3}])
    modal = _run(lambda: bt.EditRowModal(review_view, 0))

    assert _name_field(modal).default == ""


def test_bear_saved_row_prefills_the_screenshot_name_for_an_unmatched_row():
    row = {"id": 7, "raw_name": "6m3", "nickname": None, "fid": None, "damage": 1000, "rank": None}
    modal = _run(lambda: bt.EditSavedPlayerModal(SimpleNamespace(), row))

    assert _name_field(modal).default == "6m3"


@pytest.mark.parametrize("modal_factory", [
    lambda: bt.AddRowModal(_bear_review([])),
    lambda: bt.AddSavedPlayerModal(SimpleNamespace()),
])
def test_bear_add_forms_offer_a_blank_name_field(modal_factory):
    modal = _run(modal_factory)
    assert _name_field(modal).default == ""
    assert len(modal.children) <= FIELD_LIMIT


def _capture_bear_resolve(monkeypatch):
    calls = []

    async def fake_resolve(interaction, view, **kwargs):
        calls.append(kwargs)
    monkeypatch.setattr(bt, "_resolve_and_apply", fake_resolve)
    return calls


def test_bear_edit_row_passes_the_typed_name_to_the_resolver(monkeypatch):
    calls = _capture_bear_resolve(monkeypatch)
    review_view = _bear_review([{"name": "XT2", "nickname": None, "fid": None,
                                 "damage": 1000, "rank": 3}])

    async def go():
        modal = bt.EditRowModal(review_view, 0)
        _type(modal.player_input, "123456789")
        _type(_name_field(modal), "XTz")
        await modal.on_submit(SimpleNamespace())
    _run(go)

    assert calls[0]["text"] == "123456789"
    assert calls[0]["new_name"] == "XTz"


def test_bear_add_saved_player_passes_the_typed_name_to_the_resolver(monkeypatch):
    calls = _capture_bear_resolve(monkeypatch)

    async def go():
        modal = bt.AddSavedPlayerModal(SimpleNamespace())
        _type(modal.player_input, "123456789")
        _type(modal.damage_input, "1000")
        _type(_name_field(modal), "VAL")
        await modal.on_submit(SimpleNamespace())
    _run(go)

    assert calls[0]["new_name"] == "VAL"


# --- Bear confirm card ---

class _Response:
    def __init__(self):
        self.edits, self.messages = [], []

    async def edit_message(self, **kwargs):
        self.edits.append(kwargs)

    async def send_message(self, *args, **kwargs):
        self.messages.append((args, kwargs))


def _confirm_setup(monkeypatch, nickname):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE users (fid INTEGER PRIMARY KEY, nickname TEXT, furnace_lv INTEGER, "
                 "kid TEXT, stove_lv_content TEXT, alliance TEXT)")
    cog = SimpleNamespace(users_conn=conn, get_alliance_roster=lambda alliance_id: [])

    async def reapply(message):
        return None
    parent = SimpleNamespace(cog=cog, alliance_id=1, original_user_id=1, roster=[],
                             reapply_to_message=reapply)
    matched = []
    monkeypatch.setattr(bt, "_write_match_to_row", lambda view, **kw: matched.append(kw) or True)
    view = _run(lambda: bt.PlayerAddConfirmView(
        view=parent, parent_message=None, row_id=0, fid=123456789, kid=245,
        damage=1000, rank=3, raw_name="XT2", nickname=nickname))
    return view, conn, matched


def test_bear_confirm_card_adds_the_member_with_the_given_name(monkeypatch):
    view, conn, matched = _confirm_setup(monkeypatch, "XTz")
    assert "XTz" in view.build_embed().description

    interaction = SimpleNamespace(user=SimpleNamespace(id=1), response=_Response())
    _run(lambda: view._on_confirm(interaction))

    assert conn.execute("SELECT nickname FROM users WHERE fid = 123456789").fetchone() == ("XTz",)
    assert matched[0]["nick"] == "XTz"
    assert matched[0]["raw_name"] == "XT2", "the screenshot text is still what teaches the alias"


def test_bear_confirm_card_falls_back_to_the_placeholder_without_a_name(monkeypatch):
    view, conn, _ = _confirm_setup(monkeypatch, "  ")

    interaction = SimpleNamespace(user=SimpleNamespace(id=1), response=_Response())
    _run(lambda: view._on_confirm(interaction))

    assert conn.execute("SELECT nickname FROM users WHERE fid = 123456789").fetchone() == ("Player 123456789",)


# --- Attendance OCR ---

@pytest.fixture
def users_db(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute("CREATE TABLE users (fid INTEGER PRIMARY KEY, nickname TEXT, furnace_lv INTEGER, "
                     "kid TEXT, stove_lv_content TEXT, alliance TEXT)")
        conn.commit()
    from cogs import gift_state_resolver
    monkeypatch.setattr(gift_state_resolver, "get_alliance_kid", lambda alliance_id: 245)

    async def verified(gift_cog, fid, alliance_id):
        return 245, True
    monkeypatch.setattr(gift_state_resolver, "verify_add_state", verified)
    return tmp_path


def _attendance_interaction():
    return SimpleNamespace(client=SimpleNamespace(get_cog=lambda name: object()))


def _stored_nickname(fid):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        return conn.execute("SELECT nickname FROM users WHERE fid = ?", (fid,)).fetchone()


def test_attendance_add_unknown_id_uses_the_given_name(users_db):
    view = SimpleNamespace(session=SimpleNamespace(alliance_id=1))

    added, nick, note = _run(lambda: review._add_unknown_fid(
        _attendance_interaction(), view, 555, new_name=" VAL "))

    assert (added, nick) == (True, "VAL")
    assert _stored_nickname(555) == ("VAL",)
    assert "VAL" in note


def test_attendance_add_unknown_id_without_a_name_uses_the_placeholder(users_db):
    view = SimpleNamespace(session=SimpleNamespace(alliance_id=1))

    added, nick, _ = _run(lambda: review._add_unknown_fid(_attendance_interaction(), view, 556))

    assert nick == "Player 556"
    assert _stored_nickname(556) == ("Player 556",)


def test_attendance_resolver_hands_the_name_to_the_add_step(monkeypatch):
    seen = {}

    async def fake_add(interaction, view, fid, new_name=None):
        seen["new_name"] = new_name
        return True, new_name, "added"
    monkeypatch.setattr(review, "_add_unknown_fid", fake_add)
    monkeypatch.setattr(review, "load_alliance_roster", lambda alliance_id: [])
    view = SimpleNamespace(session=SimpleNamespace(alliance_id=1), roster=[],
                           _lookup_nickname=lambda fid: None)
    interaction = SimpleNamespace(response=SimpleNamespace(is_done=lambda: True))

    fid, nickname, _, _ = _run(lambda: review._resolve_player_field(
        interaction, view, "777", new_name="6m3"))

    assert (fid, nickname, seen["new_name"]) == (777, "6m3", "6m3")


def _attendance_view(rows):
    async def save_edit(interaction):
        pass
    return SimpleNamespace(
        registered_rows=rows, result_rows=[], has_registration=True, has_result=False,
        registration_value_label="Power", result_value_label="Points",
        _global_to_local=lambda idx: (rows, idx), _save_edit=save_edit,
        _apply_fid_collision=lambda bucket, idx, fid: [], session=SimpleNamespace(alliance_id=1),
    )


def test_attendance_edit_row_prefills_the_screenshot_name_and_passes_it_on(monkeypatch):
    rows = [{"name": "[NEX]XTz", "nickname": None, "fid": None, "value": 10, "_kind": "registration"}]
    view = _attendance_view(rows)
    seen = {}

    async def fake_resolve(interaction, v, text, new_name=None):
        seen.update(text=text, new_name=new_name)
        return 123, new_name, "manual", None
    monkeypatch.setattr(review, "_resolve_player_field", fake_resolve)

    async def go():
        modal = review._EditRowModal(view, 0)
        assert _name_field(modal).default == "XTz"
        _type(modal.player_input, "123")
        await modal.on_submit(SimpleNamespace())
    _run(go)

    assert seen == {"text": "123", "new_name": "XTz"}
    assert rows[0]["nickname"] == "XTz"


def test_attendance_merged_row_and_add_row_carry_the_name_field(monkeypatch):
    view = _attendance_view([])
    merged = {"name": "VAL", "nickname": None, "fid": None, "_reg_idx": None, "_res_idx": None}

    merged_modal = _run(lambda: review._EditMergedRowModal(view, merged))
    add_modal = _run(lambda: review._AddRowModal(view, kind="registration"))

    assert _name_field(merged_modal).default == "VAL"
    assert _name_field(add_modal).default == ""
    assert len(merged_modal.children) <= FIELD_LIMIT and len(add_modal.children) <= FIELD_LIMIT
