"""Screenshots every /settings screen into tests/sim/gallery/index.html.

    python tests/sim/gallery.py [--hub "Gift Codes"]
"""
from __future__ import annotations

import argparse
import asyncio
import html
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parents[1]), str(HERE)]

OUTPUT = HERE / "gallery"
INSTALL = ('python -m pip install "simcord[screenshot]>=2.2.1,<3" tzdata\n'
           "python -m playwright install chromium")


@dataclass
class Shot:
    path: list[str]
    image: str | None
    note: str = ""


def _check_dependencies() -> None:
    try:
        import playwright  # noqa: F401
        import simcord  # noqa: F401
        import zoneinfo
        zoneinfo.ZoneInfo("UTC")
    except Exception as error:
        sys.exit(f"The gallery needs SimCord's screenshot extra ({error}). Install with:\n{INSTALL}")


def _slug(parts: list[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "-", " ".join(parts).lower()).strip("-")


async def _capture(preview, sim, shots, path, message, ephemeral) -> None:
    name = f"{len(shots):02d}-{_slug(path)}.png"
    await preview.screenshot(OUTPUT / name, target=message)
    shots.append(Shot(path, name, "ephemeral reply" if ephemeral else ""))


async def _capture_click(preview, sim, shots, path, click) -> None:
    if click.screen is None:
        shots.append(Shot(path, None, "opens a form (modal)"))
        return
    await _capture(preview, sim, shots, path, *click.screen)


async def _capture_hub(preview, sim, shots, hub) -> None:
    from test_hubs import _hub_buttons
    await _capture_click(preview, sim, shots, ["Settings", hub], await sim.click(await sim.open_settings(), hub))
    for button in await _hub_buttons(sim, hub):
        hub_screen = (await sim.click(await sim.open_settings(), hub)).screen[0]
        await _capture_click(preview, sim, shots, ["Settings", hub, button], await sim.click(hub_screen, button))


async def build(only_hub: str | None) -> list[Shot]:
    import sim_harness
    shots: list[Shot] = []
    async with sim_harness.running("populated") as sim:
        owner = sim.users["owner"]
        async with sim.env.preview(sim.channel, viewers=[owner]) as preview:
            menu = await sim.open_settings()
            await _capture(preview, sim, shots, ["Settings"], menu, False)
            for hub in sim_harness.labels(menu, enabled_only=True):
                if only_hub in (None, hub):
                    await _capture_hub(preview, sim, shots, hub)
    return shots


def write_index(shots: list[Shot]) -> Path:
    sections, current = [], None
    for shot in shots:
        group = shot.path[1] if len(shot.path) > 1 else "Settings"
        if group != current:
            sections.append(f"<h2>{html.escape(group)}</h2>")
            current = group
        caption = html.escape(" → ".join(shot.path)) + (f" <em>({html.escape(shot.note)})</em>" if shot.note else "")
        image = f'<img src="{shot.image}" alt="{html.escape(" → ".join(shot.path))}">' if shot.image else ""
        sections.append(f"<figure>{image}<figcaption>{caption}</figcaption></figure>")
    page = (
        "<!doctype html><meta charset=\"utf-8\"><title>WOS menu gallery</title>"
        "<style>body{font-family:system-ui,sans-serif;background:#1e1f22;color:#dbdee1;margin:24px}"
        "h2{border-bottom:1px solid #3f4147;padding-bottom:4px;margin-top:40px}"
        "figure{margin:16px 0 28px}img{max-width:100%;border:1px solid #3f4147;border-radius:6px}"
        "figcaption{font-size:14px;color:#b5bac1;margin-top:6px}em{color:#949ba4}</style>"
        "<h1>WOS menu gallery</h1>" + "".join(sections)
    )
    index = OUTPUT / "index.html"
    index.write_text(page, encoding="utf-8")
    return index


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hub", help="only this Settings Menu hub, e.g. \"Gift Codes\"")
    args = parser.parse_args()
    _check_dependencies()
    OUTPUT.mkdir(exist_ok=True)
    for old in OUTPUT.glob("*.png"):
        old.unlink()
    with tempfile.TemporaryDirectory(prefix="wos_gallery_", ignore_cleanup_errors=True) as workdir:
        os.chdir(workdir)
        shots = asyncio.run(build(args.hub))
    print(f"{len(shots)} screens: {write_index(shots)}")


if __name__ == "__main__":
    main()
