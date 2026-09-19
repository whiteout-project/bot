"""Import alliances, members, and gift data from the JS bot's Database.db."""
import asyncio
import json
import logging
import os
import shutil
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime

import discord

from .alliance_member_edit import new_member_name
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, notify_view_expired

logger = logging.getLogger('bot')

GAME_TYPE = "wos"

JS_REQUIRED_TABLES = {
    "alliance", "players", "admins", "gift_codes", "giftcode_usage",
    "id_channels", "furnace_changes", "nickname_changes", "test_ids",
}

TARGET_DB_FILES = ["users.sqlite", "alliance.sqlite", "settings.sqlite",
                   "giftcode.sqlite", "id_channel.sqlite", "changes.sqlite"]

STAT_KEYS = ["alliances_matched", "alliances_new", "players_imported",
             "players_skipped_existing", "players_skipped_orphan",
             "admins_imported", "gift_codes_imported", "redemptions_imported",
             "id_channels_imported", "furnace_changes_imported",
             "nickname_changes_imported", "test_fids_imported"]


def _int(v):
    # Discord snowflakes are TEXT in the JS bot but INTEGER here. None-safe.
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _iso_to_pydate(v):
    # JS stores ISO 8601 ('...T...Z'); this bot uses 'YYYY-MM-DD HH:MM:SS'.
    if not v:
        return None
    s = str(v).strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return s


def _validation_status(js_status):
    s = (js_status or "").strip().lower()
    if s in ("valid", "validated", "active", "success"):
        return "validated"
    if s in ("invalid", "expired", "used", "error"):
        return "invalid"
    return "pending"  # unknown -> let the periodic validator decide


def _usage_status(js_status):
    # Must map to a 'done' value for the player to be skipped on future redeems;
    # unknown values pass through so the player is retried.
    s = (js_status or "").strip().lower()
    mapping = {
        "success": "SUCCESS", "redeemed": "SUCCESS", "ok": "SUCCESS",
        "received": "RECEIVED", "already": "RECEIVED", "already_received": "RECEIVED",
        "same type exchange": "RECEIVED",
        "expired": "TIME_ERROR", "time_error": "TIME_ERROR",
        "used": "USAGE_LIMIT", "usage_limit": "USAGE_LIMIT",
        "cdk_not_found": "CDK_NOT_FOUND", "not_found": "CDK_NOT_FOUND",
    }
    return mapping.get(s, js_status)


def find_local_js_dbs(search_dirs=(".", "backups")):
    """Paths of files named Database.db (case-insensitive) in the bot root and backups/."""
    found = []
    for d in search_dirs:
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.lower() == "database.db":
                found.append(os.path.normpath(os.path.join(d, f)))
    return found


def extract_js_db_from_zip(zip_path, dest_dir):
    """Extract the first Database.db entry from a zip. Returns its path, or None.

    Entries resolving outside dest_dir (path traversal) are rejected.
    """
    import zipfile
    os.makedirs(dest_dir, exist_ok=True)
    dest_root = os.path.realpath(dest_dir)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for info in zf.infolist():
                if os.path.basename(info.filename).lower() != "database.db":
                    continue
                resolved = os.path.realpath(os.path.join(dest_root, info.filename))
                if not resolved.startswith(dest_root + os.sep):
                    continue  # path traversal - reject the entry
                target = os.path.join(dest_root, os.path.basename(info.filename))
                with zf.open(info) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                return target
    except zipfile.BadZipFile:
        return None
    return None


def validate_js_db(path):
    """Return (ok, reason). Checks the file is a SQLite DB with the JS bot's tables."""
    if not os.path.isfile(path):
        return False, "File not found."
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as conn:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}
            missing = JS_REQUIRED_TABLES - tables
            if missing:
                return False, "Not a JS bot database - missing tables: " + ", ".join(sorted(missing))
            cols = {r[1] for r in conn.execute("PRAGMA table_info(alliance)")}
            if "game_type" not in cols:
                return False, "This JS bot database is too old (no game_type column)."
    except sqlite3.DatabaseError:
        return False, "Not a valid SQLite database file."
    return True, "OK"


def _import_alliances(js, alliance_db, gift_db, gt, stats):
    """Map JS alliance IDs to local ones: match by name, else insert with a fresh ID."""
    local_by_name = {name: aid for aid, name in
                     alliance_db.execute("SELECT alliance_id, name FROM alliance_list")}
    mapping = {}
    for a in js.execute("SELECT * FROM alliance WHERE game_type = ?", (gt,)):
        if a["name"] in local_by_name:
            local_id = local_by_name[a["name"]]
            stats["alliances_matched"] += 1
        else:
            cur = alliance_db.execute(
                "INSERT INTO alliance_list (name, discord_server_id, kid) VALUES (?,?,?)",
                (a["name"], _int(a["guide_id"]), a["state"]))
            local_id = cur.lastrowid
            local_by_name[a["name"]] = local_id  # a duplicate JS name merges into this one
            stats["alliances_new"] += 1
        mapping[a["id"]] = local_id
        alliance_db.execute(
            "INSERT OR IGNORE INTO alliancesettings (alliance_id, channel_id, interval) VALUES (?,?,?)",
            (local_id, _int(a["channel_id"]), _int(a["interval"])))
        gift_db.execute(
            "INSERT OR IGNORE INTO giftcodecontrol (alliance_id, status, priority) VALUES (?,?,?)",
            (local_id, 1 if a["auto_redeem"] else 0, a["priority"] or 0))
    return mapping


def _import_players(js, users_db, gt, mapping, stats):
    for p in js.execute("SELECT * FROM players WHERE game_type = ?", (gt,)):
        local_alliance = mapping.get(p["alliance_id"])
        if local_alliance is None:
            stats["players_skipped_orphan"] += 1
            continue
        if users_db.execute("SELECT 1 FROM users WHERE fid = ?", (p["fid"],)).fetchone():
            stats["players_skipped_existing"] += 1
            continue
        nickname = new_member_name(p["nickname"], p["fid"])
        users_db.execute(
            "INSERT INTO users (fid, nickname, furnace_lv, kid, alliance, discord_id) VALUES (?,?,?,?,?,?)",
            (p["fid"], nickname, p["furnace_level"] or 0, p["state"], local_alliance, _int(p["user_id"])))
        stats["players_imported"] += 1


def _import_admins(js, settings_db, mapping, stats):
    # JS admins are not game-scoped; alliance scoping remaps through the ID map.
    for ad in js.execute("SELECT * FROM admins"):
        uid = _int(ad["user_id"])
        if uid is None:
            continue
        # JS owners become Global Admins, never Bot Owner - the local install keeps its single owner.
        is_initial = 1 if ad["is_owner"] else 0
        cur = settings_db.execute(
            "INSERT OR IGNORE INTO admin (id, is_initial, is_owner) VALUES (?,?,0)",
            (uid, is_initial))
        stats["admins_imported"] += cur.rowcount
        try:
            scoped = json.loads(ad["alliances"] or "[]")
        except (ValueError, TypeError):
            scoped = []
        for a_id in scoped:
            if a_id in mapping:
                settings_db.execute(
                    "INSERT OR IGNORE INTO adminserver (admin, alliances_id) VALUES (?,?)",
                    (uid, mapping[a_id]))


def _import_gift_data(js, gift_db, gt, stats):
    for gc in js.execute("SELECT * FROM gift_codes WHERE game_type = ?", (gt,)):
        cur = gift_db.execute(
            "INSERT OR IGNORE INTO gift_codes (giftcode, date, validation_status) VALUES (?,?,?)",
            (gc["gift_code"], gc["date"], _validation_status(gc["status"])))
        stats["gift_codes_imported"] += cur.rowcount
    known = {r[0] for r in gift_db.execute("SELECT giftcode FROM gift_codes")}
    for u in js.execute("SELECT * FROM giftcode_usage WHERE game_type = ?", (gt,)):
        if u["gift_code"] not in known:
            continue
        cur = gift_db.execute(
            "INSERT OR IGNORE INTO user_giftcodes (fid, giftcode, status, last_attempt_at) VALUES (?,?,?,NULL)",
            (u["fid"], u["gift_code"], _usage_status(u["status"])))
        stats["redemptions_imported"] += cur.rowcount


def _import_id_channels(js, idch_db, gt, mapping, stats):
    for ic in js.execute("SELECT * FROM id_channels WHERE game_type = ?", (gt,)):
        if ic["alliance_id"] not in mapping:
            continue
        cur = idch_db.execute(
            "INSERT OR IGNORE INTO id_channels (guild_id, alliance_id, channel_id, created_by) VALUES (?,?,?,?)",
            (_int(ic["guide_id"]), mapping[ic["alliance_id"]], _int(ic["channel_id"]), _int(ic["linked_by"])))
        stats["id_channels_imported"] += cur.rowcount


def _import_changes(js, changes_db, gt, stats):
    # No unique constraint on the target tables - dedupe by exact-row existence.
    for fc in js.execute("SELECT * FROM furnace_changes WHERE game_type = ?", (gt,)):
        row = (fc["fid"], fc["old_furnace_lv"], fc["new_furnace_lv"], _iso_to_pydate(fc["change_date"]))
        cur = changes_db.execute(
            "INSERT INTO furnace_changes (fid, old_furnace_lv, new_furnace_lv, change_date) "
            "SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM furnace_changes "
            "WHERE fid IS ? AND old_furnace_lv IS ? AND new_furnace_lv IS ? AND change_date IS ?)",
            row + row)
        stats["furnace_changes_imported"] += cur.rowcount
    for nc in js.execute("SELECT * FROM nickname_changes WHERE game_type = ?", (gt,)):
        row = (nc["fid"], nc["old_nickname"], nc["new_nickname"], _iso_to_pydate(nc["change_date"]))
        cur = changes_db.execute(
            "INSERT INTO nickname_changes (fid, old_nickname, new_nickname, change_date) "
            "SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM nickname_changes "
            "WHERE fid IS ? AND old_nickname IS ? AND new_nickname IS ? AND change_date IS ?)",
            row + row)
        stats["nickname_changes_imported"] += cur.rowcount


def _import_test_fids(js, settings_db, gt, stats):
    for t in js.execute("SELECT * FROM test_ids WHERE game_type = ?", (gt,)):
        fid = str(t["fid"])
        if settings_db.execute("SELECT 1 FROM test_fid_settings WHERE test_fid = ?", (fid,)).fetchone():
            continue
        settings_db.execute("INSERT INTO test_fid_settings (test_fid, kid) VALUES (?,?)", (fid, t["state"]))
        stats["test_fids_imported"] += 1


def target_summary(db_dir):
    """(existing user count, existing alliance count) - for the pre-import warning."""
    with closing(sqlite3.connect(os.path.join(db_dir, "users.sqlite"), timeout=30.0)) as conn:
        n_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    with closing(sqlite3.connect(os.path.join(db_dir, "alliance.sqlite"), timeout=30.0)) as conn:
        n_alliances = conn.execute("SELECT COUNT(*) FROM alliance_list").fetchone()[0]
    return n_users, n_alliances


def run_import(js_db_path, db_dir, game_type=GAME_TYPE, dry_run=False):
    """Merge one game's data from the JS Database.db into this bot's db/ files.

    Existing rows always win; JS alliance IDs are remapped (match by name,
    else fresh AUTOINCREMENT ID). Returns a stats dict. Sync - call via
    asyncio.to_thread from the bot.
    """
    missing = [f for f in TARGET_DB_FILES if not os.path.isfile(os.path.join(db_dir, f))]
    if missing:
        raise FileNotFoundError("Target database files are missing: " + ", ".join(missing))

    stats = dict.fromkeys(STAT_KEYS, 0)
    js = sqlite3.connect(f"file:{js_db_path}?mode=ro", uri=True)
    js.row_factory = sqlite3.Row
    conns = {f: sqlite3.connect(os.path.join(db_dir, f), timeout=30.0) for f in TARGET_DB_FILES}
    try:
        mapping = _import_alliances(js, conns["alliance.sqlite"], conns["giftcode.sqlite"], game_type, stats)
        _import_players(js, conns["users.sqlite"], game_type, mapping, stats)
        _import_admins(js, conns["settings.sqlite"], mapping, stats)
        _import_gift_data(js, conns["giftcode.sqlite"], game_type, stats)
        _import_id_channels(js, conns["id_channel.sqlite"], game_type, mapping, stats)
        _import_changes(js, conns["changes.sqlite"], game_type, stats)
        _import_test_fids(js, conns["settings.sqlite"], game_type, stats)
        for c in conns.values():
            c.rollback() if dry_run else c.commit()
    finally:
        js.close()
        for c in conns.values():
            c.close()
    return stats


# --------------------------------------------------------------- Discord UI

UPLOAD_WINDOW_SECONDS = 300
MAX_UPLOAD_MB = 200

STAT_LABELS = [
    ("alliances_new", "New alliances"),
    ("alliances_matched", "Alliances matched by name"),
    ("players_imported", "Members"),
    ("players_skipped_existing", "Members already here (skipped)"),
    ("admins_imported", "Admins"),
    ("gift_codes_imported", "Gift codes"),
    ("redemptions_imported", "Redemption records"),
    ("id_channels_imported", "ID channels"),
    ("furnace_changes_imported", "Furnace history entries"),
    ("nickname_changes_imported", "Nickname history entries"),
    ("test_fids_imported", "Test IDs"),
]


async def _check_global_admin(interaction: discord.Interaction) -> bool:
    _, is_global = PermissionManager.is_admin(interaction.user.id)
    if not is_global:
        await interaction.response.send_message(
            f"{theme.deniedIcon} Only global admins can use this menu.", ephemeral=True
        )
        return False
    return True


def _stats_lines(stats: dict) -> str:
    lines = []
    for key, label in STAT_LABELS:
        if stats.get(key, 0):
            lines.append(f"{theme.circleIcon} **{label}:** {stats[key]}")
    return "\n".join(lines) if lines else f"{theme.circleIcon} Nothing to import for this game."


def _cleanup_dir(path):
    if path and os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)


async def show_import_menu(backup_cog, interaction: discord.Interaction):
    """Entry point from the Backup System menu."""
    view = ImportSourceView(backup_cog)
    await interaction.response.edit_message(embed=view.build_embed(), view=view, content=None)
    view.message = await interaction.original_response()


class ImportSourceView(discord.ui.View):
    """Pick where the JS bot's Database.db comes from: a file found on disk,
    or an upload in the channel."""

    def __init__(self, cog, note: str | None = None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.note = note
        self.message = None
        self.found = find_local_js_dbs()
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _check_global_admin(interaction)

    async def on_timeout(self):
        await notify_view_expired(self, "import menu")

    def build_embed(self) -> discord.Embed:
        found_lines = "\n".join(f"{theme.documentIcon} `{p}`" for p in self.found[:3])
        if not self.found:
            found_lines = "No Database.db found in the bot folder or backups/."
        description = (
            f"Copy your alliances, members, admins, and gift code history over "
            f"from the JS bot. Only Whiteout Survival data is imported.\n\n"
            f"Anything already set up in this bot is kept - the import only adds "
            f"what is missing. Alliances with the same name are treated as the "
            f"same alliance.\n\n"
            f"**Found on this server**\n"
            f"{theme.upperDivider}\n"
            f"{found_lines}\n"
            f"{theme.lowerDivider}\n\n"
            f"**Controls**\n"
            f"{theme.upperDivider}\n"
            f"{theme.documentIcon} **Use Found File**\n"
            f"└ Import from a Database.db already on this server\n\n"
            f"{theme.importIcon} **Upload File**\n"
            f"└ Post your Database.db (or a zip of it) in this channel\n\n"
            f"{theme.backIcon} **Back**\n"
            f"└ Return to the Backup System menu\n"
            f"{theme.lowerDivider}"
        )
        embed = discord.Embed(
            title=f"{theme.importIcon} Import from JS Bot",
            description=description,
            color=theme.emColor1,
        )
        if self.note:
            embed.add_field(name=f"{theme.infoIcon} Note", value=self.note, inline=False)
        return embed

    def _build_components(self):
        self.clear_items()
        for path in self.found[:3]:
            btn = discord.ui.Button(
                label=f"Use {path}"[:80],
                emoji=f"{theme.documentIcon}",
                style=discord.ButtonStyle.success,
                row=0,
            )
            btn.callback = self._make_use_file(path)
            self.add_item(btn)

        upload_btn = discord.ui.Button(
            label="Upload File", emoji=f"{theme.importIcon}",
            style=discord.ButtonStyle.primary, row=1,
        )
        upload_btn.callback = self._start_upload
        self.add_item(upload_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=f"{theme.backIcon}",
            style=discord.ButtonStyle.secondary, row=1,
        )
        back_btn.callback = self._back
        self.add_item(back_btn)

    def _make_use_file(self, path):
        async def callback(interaction: discord.Interaction):
            self.stop()
            await _prepare_confirmation(self.cog, interaction, path, cleanup_dir=None)
        return callback

    async def _back(self, interaction: discord.Interaction):
        self.stop()
        await self.cog.show_backup_menu(interaction)

    async def _start_upload(self, interaction: discord.Interaction):
        self.stop()
        wait_view = UploadWaitView(self.cog)
        embed = discord.Embed(
            title=f"{theme.importIcon} Upload Database.db",
            description=(
                f"Post the JS bot's **Database.db** (or a zip containing it) as an "
                f"attachment **in this channel** within the next 5 minutes.\n\n"
                f"You can find the file in the JS bot's folder on the server it "
                f"runs on. The message is removed automatically after the file "
                f"is received."
            ),
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=embed, view=wait_view, content=None)
        wait_view.message = await interaction.original_response()
        await _wait_for_upload(self.cog, interaction, wait_view)


class UploadWaitView(discord.ui.View):
    """Shown while waiting for the admin to post the file."""

    def __init__(self, cog):
        super().__init__(timeout=UPLOAD_WINDOW_SECONDS + 30)
        self.cog = cog
        self.message = None
        self.cancelled = asyncio.Event()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _check_global_admin(interaction)

    @discord.ui.button(label="Cancel", emoji=f"{theme.deniedIcon}", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cancelled.set()
        self.stop()
        view = ImportSourceView(self.cog)
        await interaction.response.edit_message(embed=view.build_embed(), view=view, content=None)
        view.message = await interaction.original_response()


async def _wait_for_upload(cog, interaction: discord.Interaction, wait_view: UploadWaitView):
    user_id = interaction.user.id
    channel_id = interaction.channel_id

    def check(m):
        return (m.author.id == user_id and m.channel.id == channel_id and m.attachments
                and m.attachments[0].filename.lower().endswith((".db", ".sqlite", ".zip")))

    wait_task = asyncio.create_task(cog.bot.wait_for("message", check=check))
    cancel_task = asyncio.create_task(wait_view.cancelled.wait())
    done, pending = await asyncio.wait(
        {wait_task, cancel_task}, timeout=UPLOAD_WINDOW_SECONDS,
        return_when=asyncio.FIRST_COMPLETED,
    )
    for t in pending:
        t.cancel()

    if cancel_task in done:
        return  # the Cancel button already swapped the view back
    if wait_task not in done:  # window elapsed
        wait_view.stop()
        view = ImportSourceView(cog, note="Upload timed out - no file was posted within 5 minutes.")
        await wait_view.message.edit(embed=view.build_embed(), view=view, content=None)
        view.message = wait_view.message
        return

    wait_view.stop()
    msg = wait_task.result()
    attachment = msg.attachments[0]
    tmpdir = tempfile.mkdtemp(prefix="js_import_")
    try:
        if attachment.size > MAX_UPLOAD_MB * 1024 * 1024:
            raise ValueError(f"File is too large ({attachment.size / 1024 / 1024:.0f} MB).")
        local_path = os.path.join(tmpdir, os.path.basename(attachment.filename))
        await attachment.save(local_path)
        try:
            await msg.delete()
        except (discord.Forbidden, discord.HTTPException):
            pass
        if attachment.filename.lower().endswith(".zip"):
            extracted = extract_js_db_from_zip(local_path, os.path.join(tmpdir, "unzipped"))
            if not extracted:
                raise ValueError("The zip does not contain a Database.db file.")
            local_path = extracted
        await _prepare_confirmation(cog, None, local_path, cleanup_dir=tmpdir,
                                    message=wait_view.message, user_id=user_id)
    except ValueError as e:
        _cleanup_dir(tmpdir)
        view = ImportSourceView(cog, note=str(e))
        await wait_view.message.edit(embed=view.build_embed(), view=view, content=None)
        view.message = wait_view.message
    except Exception as e:
        _cleanup_dir(tmpdir)
        logger.error(f"JS import upload failed: {e}")
        print(f"[ERROR] JS import upload failed: {e}")
        view = ImportSourceView(cog, note="Could not read the uploaded file. Please try again.")
        await wait_view.message.edit(embed=view.build_embed(), view=view, content=None)
        view.message = wait_view.message


async def _prepare_confirmation(cog, interaction, js_db_path, cleanup_dir,
                                message=None, user_id=None):
    """Validate the file, dry-run the import, and show the confirmation screen.

    Reached either from a button (interaction set) or after an upload
    (message + user_id set, no live interaction)."""
    if interaction is not None:
        user_id = interaction.user.id
        await interaction.response.defer()
        message = await interaction.original_response()

    ok, reason = validate_js_db(js_db_path)
    if not ok:
        _cleanup_dir(cleanup_dir)
        view = ImportSourceView(cog, note=reason)
        await message.edit(embed=view.build_embed(), view=view, content=None)
        view.message = message
        return

    try:
        stats = await asyncio.to_thread(run_import, js_db_path, "db", GAME_TYPE, True)
        n_users, n_alliances = await asyncio.to_thread(target_summary, "db")
    except Exception as e:
        _cleanup_dir(cleanup_dir)
        logger.error(f"JS import dry run failed: {e}")
        print(f"[ERROR] JS import dry run failed: {e}")
        view = ImportSourceView(cog, note="Could not analyze the database. Check the log for details.")
        await message.edit(embed=view.build_embed(), view=view, content=None)
        view.message = message
        return

    description = (
        f"Here is what the import will add:\n\n"
        f"{theme.upperDivider}\n"
        f"{_stats_lines(stats)}\n"
        f"{theme.lowerDivider}\n\n"
        f"{theme.saveIcon} A backup of the current databases is created before "
        f"anything is written."
    )
    embed = discord.Embed(
        title=f"{theme.importIcon} Confirm Import",
        description=description,
        color=theme.emColor1,
    )
    if n_users or n_alliances:
        embed.add_field(
            name=f"{theme.warnIcon} This bot already has data",
            value=(
                f"{n_alliances} alliance(s) and {n_users} member(s) exist here. "
                f"They are kept as they are - the import only adds entries that "
                f"are missing."
            ),
            inline=False,
        )
    view = ConfirmImportView(cog, js_db_path, cleanup_dir, user_id)
    await message.edit(embed=embed, view=view, content=None)
    view.message = message


class ConfirmImportView(discord.ui.View):
    def __init__(self, cog, js_db_path, cleanup_dir, user_id):
        super().__init__(timeout=60)
        self.cog = cog
        self.js_db_path = js_db_path
        self.cleanup_dir = cleanup_dir
        self.user_id = user_id
        self.message = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _check_global_admin(interaction)

    async def on_timeout(self):
        _cleanup_dir(self.cleanup_dir)
        await notify_view_expired(self, "import confirmation")

    @discord.ui.button(label="Start Import", emoji=f"{theme.verifiedIcon}", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        embed = discord.Embed(
            title=f"{theme.processingIcon} Importing...",
            description="Creating a backup and copying the JS bot's data. This can take a moment.",
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=embed, view=None, content=None)
        message = await interaction.original_response()

        try:
            backup_file = await self.cog.create_backup(str(self.user_id), "Preimport", save_locally=True)
            if not backup_file:
                view = ImportSourceView(self.cog, note="Could not create the pre-import backup - nothing was imported. Check disk space.")
                await message.edit(embed=view.build_embed(), view=view, content=None)
                view.message = message
                return

            stats = await asyncio.to_thread(run_import, self.js_db_path, "db", GAME_TYPE, False)
            logger.info(f"JS bot import finished: {stats}")

            result = discord.Embed(
                title=f"{theme.verifiedIcon} Import Complete",
                description=(
                    f"{theme.upperDivider}\n"
                    f"{_stats_lines(stats)}\n"
                    f"{theme.lowerDivider}\n\n"
                    f"{theme.saveIcon} Pre-import backup: `{backup_file}`\n"
                    f"The imported data is live right away - no restart needed. "
                    f"Reopen any open menus to see it."
                ),
                color=theme.emColor3,
            )
            view = ImportResultView(self.cog)
            await message.edit(embed=result, view=view, content=None)
            view.message = message
        except Exception as e:
            logger.error(f"JS bot import failed: {e}")
            print(f"[ERROR] JS bot import failed: {e}")
            view = ImportSourceView(self.cog, note="The import failed - check the log for details. The pre-import backup is untouched.")
            await message.edit(embed=view.build_embed(), view=view, content=None)
            view.message = message
        finally:
            _cleanup_dir(self.cleanup_dir)

    @discord.ui.button(label="Cancel", emoji=f"{theme.deniedIcon}", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        _cleanup_dir(self.cleanup_dir)
        view = ImportSourceView(self.cog)
        await interaction.response.edit_message(embed=view.build_embed(), view=view, content=None)
        view.message = await interaction.original_response()


class ImportResultView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=7200)
        self.cog = cog
        self.message = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _check_global_admin(interaction)

    async def on_timeout(self):
        from .pimp_my_bot import disable_expired_view
        await disable_expired_view(self)

    @discord.ui.button(label="Back", emoji=f"{theme.backIcon}", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await self.cog.show_backup_menu(interaction)
