"""One-shot helper: run the layer-2 pipeline on each fixture screenshot
and print the raw OCR text + parsed result. Used to align expected JSON
to what the pipeline actually produces today.

Run with:  python dump_ocr_output.py
"""
from __future__ import annotations

import json

from harness import bt, run_ocr_pipeline, discover_fixtures
from test_layer2_ocr import LANG_TO_OCR, LANG_TO_FALLBACKS


def main():
    for case in discover_fixtures():
        expected = case.expected
        roster = case.roster
        ui_lang = expected.get("language", "english")
        primary_lang = LANG_TO_OCR.get(ui_lang, "ch")
        fallback_langs = LANG_TO_FALLBACKS.get(ui_lang, [])

        image_bytes = case.screenshot_path.read_bytes()
        primary_text = bt.ocr_bytes(image_bytes, lang=primary_lang)
        repaired = bt.repair_ocr_digits(primary_text)
        result = run_ocr_pipeline(image_bytes, primary_lang=primary_lang,
                                  fallback_langs=fallback_langs, roster=roster)

        print("=" * 80)
        print(f"{case.screenshot_path.name}  ({ui_lang}, primary={primary_lang}, "
              f"fallbacks={fallback_langs})")
        print("-" * 80)
        print(f"primary OCR text:\n{primary_text!r}")
        print(f"\nrepaired:\n{repaired!r}")
        print(f"\nparsed result:")
        print(json.dumps({
            "trap":         result["trap"],
            "rallies":      result["rallies"],
            "total_damage": result["total_damage"],
            "rows":         [{"damage": r["damage"], "name": r.get("name")}
                             for r in result["rows"]],
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
