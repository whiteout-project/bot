# tests

Automated regression tests for the Whiteout bot: pure-logic suites (event /
schedule date math, API error classification, payload chunking, input
sanitization), static guardrails (theme-icon contract, cog registration,
dependency resolution, missing-`f`-prefix lint), the OCR / attendance parser
suites, a Python 3.11 f-string compatibility scan, and the upgrade-test harness
in [`upgrade/`](upgrade/README.md).

Requires `pytest` (a dev-only tool, intentionally not in `requirements.txt`):
`python -m pip install pytest`. Run everything with `python -m pytest tests -q`.

This directory lives in the repo but is `export-ignore`d in `.gitattributes`,
so it ships to **no** user via release archives or the in-bot updater — yet
stays versioned with the code it tests. Bot Health's cleanup also removes a
stray `tests/` from any install.

## Layout

```
tests/
├── conftest.py                         # puts repo root + tests/ on sys.path
├── harness.py                          # imports cogs/bear_track.py — bear-hunt parser pipeline
├── harness_attendance.py               # imports cogs/attendance_ocr_parsers.py — attendance OCR pipeline
├── test_layer1_parser.py               # pure-text bear-track tests
├── test_layer2_ocr.py                  # end-to-end bear-track OCR tests (slow)
├── test_attendance_ocr_layer1.py       # pure-text attendance-OCR tests
├── test_attendance_ocr_layer2.py       # end-to-end attendance-OCR tests (slow)
├── test_python311_compat.py            # scans this bot's cogs (+ sibling Kingshot if present) for 3.12-only f-strings
│   # ── pure-logic + guardrail suites (fast, no Discord/DB) ──
├── test_event_schedule.py              # event date/cycle math, time-slot + timezone parsing
├── test_error_classification.py        # network-vs-player error classification (wrongful-removal guard)
├── test_chunkers.py                    # Discord payload-limit message splitting
├── test_sanitization.py                # gift-code cleaning (invisible/control chars)
├── test_invariants.py                  # theme-icon contract, cog registration, deps, f-prefix lint
├── fixtures/                           # bear-track + attendance/ screenshots, expected JSON, rosters
├── upgrade/                            # upgrade-from-old-version harness (see upgrade/README.md)
└── README.md
```

## Running

From the repo root, with the bot venv active:

```
python -m pytest tests/test_layer1_parser.py -v          # bear-track layer 1
python -m pytest tests/test_layer2_ocr.py -v             # bear-track layer 2 (slow)
python -m pytest tests/test_attendance_ocr_layer1.py -v  # attendance OCR layer 1
python -m pytest tests/test_attendance_ocr_layer2.py -v  # attendance OCR layer 2 (slow)
python -m pytest tests/test_python311_compat.py -v       # 3.11 syntax compat scan
python -m pytest tests -v                                # all unit tests
```

Layer 1 tests + the 3.11 compat scan are what get run after every code change;
layer 2 is the once-a-session reality check (~5-30s per fixture).

## The 3.11 compat scan

Production runs on Python 3.11. Two f-string features added in 3.12
will SyntaxError on 3.11 and silently break cog loading:

- backslashes inside `{}` expressions (`f"{'·'.join(parts)}"`)
- reusing the outer quote inside `{}` (`f"{d["key"]}"`)

The scanner parses each `.py` in both bots via `ast`, walks every f-string
node, and inspects each `{}` expression's source text for these patterns.
Both bots must pass before shipping a release.

## Adding a new bear-track fixture

1. Drop a PNG into `fixtures/screenshots/` with a descriptive filename, e.g.
   `arabic_rank_top_001.png`.
2. Create or edit the corresponding roster JSON in `fixtures/rosters/`.
   Multiple screenshots from the same hunt should share a roster.
3. Create `fixtures/expected/<screenshot-name>.json` listing what should be
   extracted (damages, names after match, trap, rallies, total).
4. Re-run layer 2.

## Adding a new attendance-OCR fixture

1. Drop a PNG into `fixtures/attendance/screenshots/` with a descriptive
   filename. Naming convention:
   - `foundry_battle_l<1|2>_<NNN>.png` — Foundry Battle result, legion-tagged
   - `foundry_registration_<NNN>.png` — Foundry combatants list
   - `canyon_clash_l<1|2>_<NNN>.png` — Canyon Clash result
   - `canyon_registration_<NNN>.png` — Canyon combatants list
   - `power_rankings_<NNN>.png` — Power Rankings page
   - `alliance_showdown_<NNN>.png` — Alliance Showdown final
2. Update `_FIXTURE_META` in `harness_attendance.py` so the parser knows
   which session class to apply.
3. Optionally create `fixtures/attendance/expected/<screenshot-name>.json`
   with assertions. Without it, layer 2 only verifies the parser doesn't
   crash on that fixture.
4. Re-run `test_attendance_ocr_layer2.py`.

### Attendance expected-JSON schema

```json
{
  "description": "Free-text note about what this fixture covers",
  "event_type": "foundry_battle",
  "legion": "Legion 2",
  "header_date": "2026-05-03",
  "alliance_rank": 30,
  "min_rows": 6,
  "expected_rows": [
    {"name": "vtr", "value": 512315, "expected_fid": 10001, "min_status": "auto"}
  ]
}
```

All top-level fields are optional. `legion` / `header_date` / `alliance_rank`
only run if non-null. `expected_rows[].expected_fid` and `min_status` only
run if specified.

## Fixture roster format

```json
{
  "members": [
    {"fid": 12345, "nick": "MIMOUN"},
    {"fid": 67890, "nick": "ملك الظلام"}
  ]
}
```

## Expected-output format

```json
{
  "trap": "1",
  "rallies": "53",
  "total_damage": 265469476854,
  "rows": [
    {"damage": 36961409452, "expected_match": {"fid": null, "status": "likely"}},
    {"damage": 31577589708, "expected_match": {"fid": 12345, "status": "auto"}}
  ]
}
```
