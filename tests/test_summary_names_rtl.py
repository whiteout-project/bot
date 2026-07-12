"""Redemption-summary name lines must left-align even for Arabic/RTL names.
Isolation (LRI/PDI) alone doesn't set line alignment in Discord — the line must
start with a strong LTR mark (LRM, U+200E), the same trick bear_track uses.
"""
import cogs.gift_redemption as gr

LRM = "‎"


def test_every_name_line_starts_with_lrm():
    block = gr._summary_names_block(["Alice", "ملك الظلام", "Bob"])
    lines = [ln for ln in block.split("\n") if ln]
    assert lines and all(ln.startswith(LRM) for ln in lines), block


def test_rtl_name_content_preserved():
    block = gr._summary_names_block(["ملك الظلام"])
    assert "ملك الظلام" in block
