"""Editing member names, furnace levels, states and power by hand.

The player API no longer returns names or levels, so admins maintain them here.
"""

import asyncio
import logging
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

import discord

from . import alliance_power_changes
from .attendance_ocr_parsers import _parse_compact_int
from .bot_level_mapping import format_furnace_level, parse_furnace_level, parse_state
from .gift_state_resolver import set_user_kid
from .pimp_my_bot import theme, check_interaction_user

logger = logging.getLogger(__name__)

PLACEHOLDER_PREFIX = "Player "
NEW_NAME_LABEL = "Name, if adding a new player (optional)"
# In-game alliance tag: exactly 3 alphanumerics in brackets, so a name's own brackets won't match.
ALLIANCE_TAG_RE = re.compile(r"\[[A-Za-z0-9]{3}\]")

_POWER_METRICS = (("power", "power"), ("combat_power", "combat power"))


def placeholder_name(fid) -> str:
    return f"{PLACEHOLDER_PREFIX}{fid}"


def is_placeholder_name(nickname, fid) -> bool:
    """True when a member still carries the auto-generated 'Player <id>' name."""
    return str(nickname or "").strip() == placeholder_name(fid)


def new_member_name(name, fid) -> str:
    """The name typed for a new member, or the placeholder when none was given."""
    return (name or "").strip() or placeholder_name(fid)


def suggested_name(ocr_name) -> str:
    """The screenshot's name without its [TAG], or "" when it holds no letters."""
    name = " ".join(ALLIANCE_TAG_RE.sub(" ", ocr_name or "").split())
    return name[:100] if any(c.isalpha() for c in name) else ""


def new_member_name_input(row_name=None, fid=None):
    """Name field for ID entry forms, pre-filled from an unmatched row's screenshot name."""
    return discord.ui.TextInput(
        label=NEW_NAME_LABEL,
        placeholder="Only used when the ID isn't in the alliance yet",
        default="" if fid else suggested_name(row_name),
        required=False,
        max_length=100,
    )


def _log_change(table: str, fid, old, new):
    """Record a manual edit in the same history the sync used to write."""
    try:
        with closing(sqlite3.connect('db/changes.sqlite', timeout=30.0)) as conn:
            columns = ("old_nickname", "new_nickname") if table == "nickname_changes" \
                else ("old_furnace_lv", "new_furnace_lv")
            conn.execute(
                f"INSERT INTO {table} (fid, {columns[0]}, {columns[1]}, change_date) "
                f"VALUES (?, ?, ?, ?)",
                (fid, old, new, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            conn.commit()
    except sqlite3.Error as e:
        logger.warning(f"Member edit: could not write {table} history for {fid}: {e}")


def parse_power(text):
    """`134512345`, `134,512,345` or `134.5M` -> a positive int, or None."""
    value = _parse_compact_int(re.sub(r"\s", "", str(text or "")))
    return value if value and value > 0 else None


def apply_member_edit(fid, *, nickname=None, furnace_lv=None, kid=None,
                      power=None, combat_power=None, alliance_id=None):
    """Write a member's name/level/state/power, limited to `alliance_id` when given; returns what changed."""
    changed, power_changes = [], []
    where, params = ("fid = ?", (fid,)) if alliance_id is None \
        else ("fid = ? AND alliance = ?", (fid, str(alliance_id)))
    with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as conn:
        row = conn.execute(
            f"SELECT nickname, furnace_lv, kid, power, combat_power FROM users WHERE {where}",
            params).fetchone()
        if not row:
            return changed
        old_nickname, old_furnace, old_kid, *old_powers = row
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")

        if nickname is not None and nickname != old_nickname:
            conn.execute("UPDATE users SET nickname = ? WHERE fid = ?", (nickname, fid))
            changed.append("name")
        if furnace_lv is not None and furnace_lv != old_furnace:
            conn.execute("UPDATE users SET furnace_lv = ? WHERE fid = ?", (furnace_lv, fid))
            changed.append("level")
        if kid is not None and kid != old_kid:
            set_user_kid(fid, kid, conn=conn)
            changed.append("state")
        for (column, label), new, old in zip(_POWER_METRICS, (power, combat_power), old_powers):
            if new is not None and new != old:
                conn.execute(f"UPDATE users SET {column} = ?, {column}_updated_at = ? WHERE fid = ?",
                             (new, now, fid))
                changed.append(label)
                power_changes.append((column, label, old, new))
        conn.commit()

    if "name" in changed:
        _log_change("nickname_changes", fid, old_nickname, nickname)
        logger.info(f"Member edit: {fid} renamed '{old_nickname}' -> '{nickname}'")
    if "level" in changed:
        _log_change("furnace_changes", fid, old_furnace, furnace_lv)
        logger.info(f"Member edit: {fid} furnace {old_furnace} -> {furnace_lv}")
    if "state" in changed:
        logger.info(f"Member edit: {fid} state {old_kid} -> {kid}")
    for column, label, old, new in power_changes:
        alliance_power_changes.record_change(fid, column, old, new, now)
        logger.info(f"Member edit: {fid} {label} {old} -> {new}")
    return changed


def enqueue_catchups(bot, fids):
    """Queue owed-code redemption for members whose state just changed; returns how many."""
    fids = list(dict.fromkeys(fids))
    if not fids:
        return 0
    gift_cog = bot.get_cog('GiftOperations')
    if not gift_cog:
        return 0
    from . import gift_redemption
    caught = 0
    for fid in fids:
        try:
            if gift_redemption.enqueue_member_redemption(gift_cog, fid):
                caught += 1
        except Exception as e:
            logger.warning(f"Member edit: could not queue catch-up for {fid}: {e}")
    return caught


_EDIT_LINE_COLUMNS = (
    (parse_furnace_level, "isn't a furnace level (try `80` or `FC 10`)"),
    (parse_state, "isn't a state number (try `911`)"),
    (parse_power, "isn't a power value (try `134500000` or `134.5M`)"),
    (parse_power, "isn't a combat power value (try `20100000` or `20.1M`)"),
)


def parse_edit_line(line):
    """`id, name, level, state, power, combat power` (all but the id optional) -> 6-tuple, or an error string."""
    parts = [p.strip() for p in line.split(",")]
    fid = parts[0]
    if not fid.isdigit():
        return f"`{line.strip()[:60]}` - doesn't start with a player ID"
    values = [parts[1] if len(parts) > 1 and parts[1] else None]
    for index, (parser, error) in enumerate(_EDIT_LINE_COLUMNS, start=2):
        raw = parts[index] if len(parts) > index else ""
        value = parser(raw) if raw else None
        if raw and value is None:
            return f"`{fid}` - `{raw[:30]}` {error}"
        values.append(value)
    return (fid, *values)


def apply_edit_lines(text, alliance_id=None):
    """Apply edit lines -> (updated, skipped, errors, state_fids needing a code catch-up)."""
    updated, skipped, errors, state_fids = 0, 0, [], []
    for line in (text or "").splitlines():
        if not line.strip():
            continue
        parsed = parse_edit_line(line)
        if isinstance(parsed, str):
            errors.append(parsed)
            continue
        if all(v is None for v in parsed[1:]):
            skipped += 1
            continue
        fid, nickname, furnace_lv, kid, power, combat_power = parsed
        changed = apply_member_edit(fid, nickname=nickname, furnace_lv=furnace_lv, kid=kid,
                                    power=power, combat_power=combat_power,
                                    alliance_id=alliance_id)
        if changed:
            updated += 1
            if "state" in changed:
                state_fids.append(fid)
        else:
            skipped += 1
    return updated, skipped, errors, state_fids


def edit_result_embed(title, updated, skipped, errors, *, added=None, caught=0):
    """Shared outcome embed for the single, bulk and CSV edit paths."""
    lines = [f"{theme.upperDivider}"]
    if added is not None:
        lines.append(f"{theme.addIcon} **Members added:** `{added}`")
    lines.append(f"{theme.verifiedIcon} **Members updated:** `{updated}`")
    if caught:
        lines.append(f"{theme.giftIcon} **Catching up on codes:** `{caught}` member(s) with a new state")
    if skipped:
        lines.append(f"{theme.infoIcon} **Unchanged:** `{skipped}` (already matched, or nothing to set)")
    if errors:
        shown = errors[:10]
        lines.append(f"{theme.deniedIcon} **Couldn't read {len(errors)} line(s):**")
        lines.extend(f"└ {e}" for e in shown)
        if len(errors) > len(shown):
            lines.append(f"└ ...and {len(errors) - len(shown)} more")
    lines.append(f"{theme.lowerDivider}")
    return discord.Embed(
        title=title,
        description="\n".join(lines),
        color=theme.emColor2 if errors and not updated else theme.emColor1,
    )


def _optional_input(label, example, default, max_length):
    return discord.ui.TextInput(
        label=label,
        placeholder=f"e.g. {example}. Leave blank to keep it as it is.",
        default=default,
        required=False,
        max_length=max_length,
    )


class MemberEditModal(discord.ui.Modal):
    """Edit one member's name, furnace level, state and power."""

    def __init__(self, parent_view, fid, nickname, furnace_lv, alliance_id, kid=None,
                 power=None, combat_power=None):
        super().__init__(title="Edit Member")
        self.parent_view = parent_view
        self.fid = fid
        self.alliance_id = alliance_id

        current_name = "" if is_placeholder_name(nickname, fid) else str(nickname or "")
        self.name_input = discord.ui.TextInput(
            label="Name",
            placeholder="The player's in-game name",
            default=current_name,
            required=False,
            max_length=100,
        )
        self.level_input = _optional_input(
            "Furnace level (optional)", "FC 10 - 2, or 82",
            format_furnace_level(furnace_lv) if furnace_lv else "", 20)
        self.state_input = _optional_input("State (optional)", "911", str(kid) if kid else "", 5)
        self.power_input = _optional_input(
            "Power (optional)", "134500000 or 134.5M", f"{power:,}" if power else "", 20)
        self.combat_power_input = _optional_input(
            "Combat Power (optional)", "20100000 or 20.1M",
            f"{combat_power:,}" if combat_power else "", 20)
        for field in (self.name_input, self.level_input, self.state_input,
                      self.power_input, self.combat_power_input):
            self.add_item(field)

    def _parsed_fields(self):
        """(field name, input, parser, error) for every optional field."""
        return (
            ("furnace_lv", self.level_input, parse_furnace_level,
             "isn't a furnace level. Try a number like `82`, or the in-game name like `FC 10 - 2`."),
            ("kid", self.state_input, parse_state,
             "isn't a state number. Enter digits only, like `911`."),
            ("power", self.power_input, parse_power,
             "isn't a power value. Enter the number like `134500000`, or shortened like `134.5M`."),
            ("combat_power", self.combat_power_input, parse_power,
             "isn't a combat power value. Enter the number like `20100000`, or shortened like `20.1M`."),
        )

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.parent_view.author_id):
            return
        values = {}
        for key, field, parser, error in self._parsed_fields():
            raw = field.value.strip()
            values[key] = parser(raw) if raw else None
            if raw and values[key] is None:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} `{raw}` {error}", ephemeral=True)
                return

        changed = await asyncio.to_thread(
            apply_member_edit, self.fid, nickname=self.name_input.value.strip() or None,
            alliance_id=self.alliance_id, **values)
        # The refreshed list shows what changed; only speak up when it can't.
        note = None if changed else f"Nothing changed for `{self.fid}`."
        if "state" in changed and enqueue_catchups(self.parent_view.cog.bot, [self.fid]):
            note = f"Redeeming the codes `{self.fid}` missed while their state was wrong."
        await self.parent_view.refresh_after_edit(interaction, note)


class BulkMemberEditModal(discord.ui.Modal):
    """Edit a whole list of members in one go, one `id, name, level, state, power, CP` per line."""

    def __init__(self, parent_view, alliance_id, prefill=""):
        super().__init__(title="Bulk Edit Members")
        self.parent_view = parent_view
        self.alliance_id = alliance_id
        self.lines_input = discord.ui.TextInput(
            label="Per line: id, name, level, state, power, CP",
            style=discord.TextStyle.paragraph,
            placeholder="306234280, THANOS IS RIGHT, FC 10 - 2, 2560, 134.5M, 20.1M\n123051289, SKORN, 80, 911",
            default=prefill,
            required=True,
            max_length=4000,
        )
        self.add_item(self.lines_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.parent_view.author_id):
            return
        updated, skipped, errors, state_fids = await asyncio.to_thread(
            apply_edit_lines, self.lines_input.value, self.alliance_id)
        caught = enqueue_catchups(self.parent_view.cog.bot, state_fids)
        logger.info(f"Bulk member edit on alliance {self.alliance_id}: "
                    f"{updated} updated, {skipped} unchanged, {len(errors)} rejected")
        await interaction.response.send_message(
            embed=edit_result_embed(f"{theme.editListIcon} Bulk Edit Complete",
                                    updated, skipped, errors, caught=caught),
            ephemeral=True)
        await self.parent_view.refresh_after_edit(interaction, None, already_responded=True)


def build_prefill(members, limit=25):
    """Bulk-modal lines; placeholder names are blanked so the admin types into an empty slot."""
    lines = []
    for m in members[:limit]:
        name = "" if is_placeholder_name(m['nickname'], m['fid']) else m['nickname']
        level = format_furnace_level(m['furnace_lv']) if m.get('furnace_lv') else ""
        # Power as exact digits: commas would split the line and rounding would log a fake change.
        lines.append(f"{m['fid']}, {name}, {level}, {m.get('kid') or ''}, "
                     f"{m.get('power') or ''}, {m.get('combat_power') or ''}")
    return "\n".join(lines)
