"""Every /settings hub opens, answers its clicks and offers the right way out."""
import pytest

from sim_harness import SETTINGS_TITLE, labels, title, way_out

pytestmark = pytest.mark.asyncio

# Buttons whose effects reach outside the temp folder.
SKIP_BUTTONS = {
    "Check for Updates": "downloads a release and restarts the bot",
}
# (scenario, hub, button or None for the hub itself): reason. A fixed bug fails until removed here.
KNOWN_BUGS = {
    ("empty", "Attendance", None): "'No Alliances Found' replaces the menu with no buttons",
}


async def _hub_problems(sim, hub):
    menu = await sim.open_settings()
    click = await sim.click(menu, hub)
    problems = click.problems()
    screen = click.screen
    if problems or screen is None or screen[1]:
        return problems
    back = await sim.click(screen[0], way_out(screen[0]))
    landed = back.screen[0] if back.screen else None
    if landed is None or not title(landed).endswith(SETTINGS_TITLE):
        problems.append(f"{way_out(screen[0])} did not return to the Settings Menu")
    return problems + back.problems()


async def _button_problems(sim, hub, button):
    hub_screen = (await sim.click(await sim.open_settings(), hub)).screen[0]
    return (await sim.click(hub_screen, button)).problems()


async def _hub_buttons(sim, hub):
    click = await sim.click(await sim.open_settings(), hub)
    if click.screen is None:
        return []
    return [label for label in labels(click.screen[0], enabled_only=True)
            if label not in (*SKIP_BUTTONS, "Back", "Main Menu")]


def _assert_clean(scenario, found):
    fixed = [key for key in KNOWN_BUGS if key in found and not found[key]]
    assert not fixed, f"known bugs no longer reproduce, remove them from KNOWN_BUGS: {fixed}"
    unexpected = {key: problems for key, problems in found.items() if problems and key not in KNOWN_BUGS}
    assert not unexpected, "\n".join(f"{' > '.join(filter(None, key[1:]))}: {p}"
                                     for key, problems in unexpected.items() for p in problems)


async def _sweep_hubs(sim, scenario):
    hubs = labels(await sim.open_settings(), enabled_only=True)
    assert hubs, "the Settings Menu has no enabled buttons"
    _assert_clean(scenario, {(scenario, hub, None): await _hub_problems(sim, hub) for hub in hubs})


async def _sweep_buttons(sim, scenario):
    found = {}
    for hub in labels(await sim.open_settings(), enabled_only=True):
        for button in await _hub_buttons(sim, hub):
            found[(scenario, hub, button)] = await _button_problems(sim, hub, button)
    _assert_clean(scenario, found)


async def test_hubs_populated(sim):
    await _sweep_hubs(sim, "populated")


async def test_hubs_empty(empty_sim):
    await _sweep_hubs(empty_sim, "empty")


async def test_hub_buttons_populated(sim):
    await _sweep_buttons(sim, "populated")


async def test_hub_buttons_empty(empty_sim):
    await _sweep_buttons(empty_sim, "empty")
