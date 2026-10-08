"""Canyon review: a scoreboard card whose state number OCR missed can be filled in, and blocks Submit until it is."""
import sqlite3
from contextlib import closing
from datetime import date

import pytest

from sim_harness import ALLIANCE_ID, labels, modal_fields

pytestmark = pytest.mark.asyncio

NUMBER_WORD = "State"


def _cards(middle_number):
    return [
        {"rank": 449, "legion": None, "tag": "ABC", "name": "Alpha", "score": 900},
        {"rank": middle_number, "legion": None, "tag": "DEF", "name": "Delta", "score": 800},
        {"rank": 312, "legion": None, "tag": "GHI", "name": "Gamma", "score": 700},
    ]


async def _canyon_review(sim, cards):
    from cogs.attendance_ocr_parsers import CanyonClashSession
    from cogs.attendance_ocr_review import EventReviewView
    channel = sim.bot.get_channel(sim.channel.id)
    session = CanyonClashSession(cog=sim.bot.get_cog("AttendanceOCR"), channel=channel,
                                 uploader_id=sim.users["owner"].id, event_type="canyon_clash",
                                 alliance_id=ALLIANCE_ID)
    session.result_rows = [{"name": "Frosty", "value": 1200}]
    session.detected_date = date(2026, 10, 3)
    session.alliance_scores = cards
    view = EventReviewView(session, registration_value_label=session.registration_value_label,
                           result_value_label=session.result_value_label)
    message = await channel.send(embed=view.build_embed(), view=view)
    await sim.env.settle()
    return sim.current(message)


def _saved_numbers():
    with closing(sqlite3.connect("db/attendance.sqlite")) as db:
        return sorted(r[0] for r in db.execute("SELECT rank FROM attendance_session_scoreboard"))


def _clean(click):
    """Answered without errors; the review is a Submit/Cancel workflow, so no Back/Main Menu check."""
    return click.result.acknowledged and not click.raised and not click.logged


def _field_for(shown, tag):
    return next(label for label in modal_fields(shown) if f"[{tag}]" in label)


async def test_missing_number_is_flagged_in_the_review(sim):
    review = await _canyon_review(sim, _cards(None))
    text = review.embeds[0].description
    assert "#None" not in text
    assert f"{NUMBER_WORD.lower()} number missing" in text.lower()


async def test_submit_with_a_missing_number_saves_nothing(sim):
    review = await _canyon_review(sim, _cards(None))
    click = await sim.click(review, "Submit")
    assert _clean(click)
    message, ephemeral = click.screen
    assert ephemeral and "[DEF]" in message.content and "Edit Scoreboard" in message.content
    assert _saved_numbers() == []
    assert "Submit" in labels(sim.current(review))


async def test_filling_the_number_lets_submit_through(sim):
    review = await _canyon_review(sim, _cards(None))
    shown = await sim.click(review, "Edit Scoreboard")
    assert shown.result.modal is not None
    filled = await sim.submit_modal(shown, {_field_for(shown, "DEF"): "777"})
    assert _clean(filled)
    submitted = await sim.click(sim.current(review), "Submit")
    assert _clean(submitted)
    assert _saved_numbers() == [312, 449, 777]


async def test_duplicate_number_is_refused(sim):
    review = await _canyon_review(sim, _cards(None))
    shown = await sim.click(review, "Edit Scoreboard")
    refused = await sim.submit_modal(shown, {_field_for(shown, "DEF"): "449"})
    message, ephemeral = refused.screen
    assert ephemeral and "449" in message.content
    assert (await sim.click(sim.current(review), "Submit")).screen[1]
    assert _saved_numbers() == []


async def test_no_edit_button_without_scoreboard_cards(sim):
    review = await _canyon_review(sim, [])
    assert "Edit Scoreboard" not in labels(review)
