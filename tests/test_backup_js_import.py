"""In-bot JS-bot import: merge semantics with alliance-ID remapping.

The JS bot keeps both games in one Database.db scoped by game_type; this bot
splits data across db/*.sqlite. The importer must merge into a possibly
non-empty target: alliances match by name or get fresh IDs (never carry a
colliding JS ID through), existing rows always win, and re-running must not
duplicate anything.
"""
import importlib
import sqlite3
from contextlib import closing

bji = importlib.import_module("cogs.bot_backup_import")


# ---------------------------------------------------------------- fixtures

def make_js_db(path):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE alliance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_type TEXT NOT NULL DEFAULT 'wos',
            priority INTEGER NOT NULL,
            name TEXT NOT NULL,
            state INTEGER, guide_id TEXT, channel_id TEXT, interval TEXT,
            auto_redeem BOOLEAN, created_by TEXT,
            UNIQUE (game_type, priority)
        );
        CREATE TABLE players (
            game_type TEXT NOT NULL DEFAULT 'wos',
            fid INTEGER NOT NULL, user_id TEXT, nickname TEXT,
            furnace_level INTEGER, state INTEGER, state_override INTEGER,
            image_url TEXT, alliance_id INTEGER, added_by TEXT NOT NULL,
            is_rich BOOLEAN DEFAULT 0, vip_count INTEGER DEFAULT 0,
            exist INTEGER DEFAULT 0,
            PRIMARY KEY (game_type, fid)
        );
        CREATE TABLE admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE NOT NULL, added_by TEXT, added_at TEXT,
            permissions INTEGER DEFAULT 0, alliances TEXT,
            is_owner BOOLEAN DEFAULT 0
        );
        CREATE TABLE gift_codes (
            game_type TEXT NOT NULL DEFAULT 'wos',
            gift_code TEXT NOT NULL, date TEXT, status TEXT, added_by TEXT,
            source TEXT, api_pushed BOOLEAN DEFAULT 0, last_validated TEXT,
            is_vip BOOLEAN DEFAULT 0,
            PRIMARY KEY (game_type, gift_code)
        );
        CREATE TABLE giftcode_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_type TEXT NOT NULL DEFAULT 'wos',
            fid INTEGER NOT NULL, gift_code TEXT NOT NULL, status TEXT
        );
        CREATE TABLE id_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_type TEXT NOT NULL DEFAULT 'wos',
            guide_id TEXT, alliance_id INTEGER NOT NULL,
            channel_id TEXT NOT NULL, linked_by TEXT, auto_clean INTEGER DEFAULT 0
        );
        CREATE TABLE furnace_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_type TEXT NOT NULL DEFAULT 'wos',
            fid INTEGER NOT NULL, old_furnace_lv INTEGER,
            new_furnace_lv INTEGER, change_date TEXT
        );
        CREATE TABLE nickname_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_type TEXT NOT NULL DEFAULT 'wos',
            fid INTEGER NOT NULL, old_nickname TEXT,
            new_nickname TEXT, change_date TEXT
        );
        CREATE TABLE test_ids (
            game_type TEXT NOT NULL DEFAULT 'wos',
            id INTEGER NOT NULL CHECK (id <= 2),
            fid INTEGER NOT NULL, state INTEGER, is_default BOOLEAN DEFAULT 0,
            set_by TEXT, set_at TEXT,
            PRIMARY KEY (game_type, id)
        );
    """)
    conn.commit()
    return conn


def make_target(db_dir):
    db_dir.mkdir(exist_ok=True)
    scripts = {
        "users.sqlite": """
            CREATE TABLE users (
                fid INTEGER PRIMARY KEY, nickname TEXT,
                furnace_lv INTEGER DEFAULT 0, kid INTEGER,
                stove_lv_content TEXT, alliance TEXT, discord_id INTEGER
            );
        """,
        "alliance.sqlite": """
            CREATE TABLE alliance_list (
                alliance_id INTEGER PRIMARY KEY, name TEXT,
                discord_server_id INTEGER, kid INTEGER
            );
            CREATE TABLE alliancesettings (
                alliance_id INTEGER PRIMARY KEY, channel_id INTEGER, interval INTEGER
            );
        """,
        "settings.sqlite": """
            CREATE TABLE admin (id INTEGER PRIMARY KEY, is_initial INTEGER, is_owner INTEGER DEFAULT 0);
            CREATE TABLE adminserver (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin INTEGER NOT NULL, alliances_id INTEGER NOT NULL,
                UNIQUE(admin, alliances_id)
            );
            CREATE TABLE test_fid_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT, test_fid TEXT NOT NULL, kid INTEGER
            );
        """,
        "giftcode.sqlite": """
            CREATE TABLE gift_codes (giftcode TEXT PRIMARY KEY, date TEXT, validation_status TEXT DEFAULT 'pending');
            CREATE TABLE user_giftcodes (
                fid INTEGER, giftcode TEXT, status TEXT, last_attempt_at TEXT,
                PRIMARY KEY (fid, giftcode)
            );
            CREATE TABLE giftcodecontrol (alliance_id INTEGER PRIMARY KEY, status INTEGER, priority INTEGER DEFAULT 0);
        """,
        "id_channel.sqlite": """
            CREATE TABLE id_channels (
                guild_id INTEGER, alliance_id INTEGER, channel_id INTEGER,
                created_at TEXT, created_by INTEGER, info_message_id INTEGER,
                UNIQUE(guild_id, channel_id)
            );
        """,
        "changes.sqlite": """
            CREATE TABLE furnace_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, fid INTEGER,
                old_furnace_lv INTEGER, new_furnace_lv INTEGER, change_date TEXT
            );
            CREATE TABLE nickname_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, fid INTEGER,
                old_nickname TEXT, new_nickname TEXT, change_date TEXT
            );
        """,
    }
    for name, script in scripts.items():
        with closing(sqlite3.connect(db_dir / name)) as conn:
            conn.executescript(script)
            conn.commit()


def seed_basic_js(js):
    js.execute("INSERT INTO alliance (id, game_type, priority, name, guide_id, channel_id, interval, auto_redeem) "
               "VALUES (1, 'wos', 1, 'Alpha', '111', '222', '60', 1)")
    js.execute("INSERT INTO alliance (id, game_type, priority, name, guide_id, channel_id, interval, auto_redeem) "
               "VALUES (2, 'ks', 1, 'KsOnly', '111', '333', '60', 0)")
    js.execute("INSERT INTO players (game_type, fid, user_id, nickname, furnace_level, state, alliance_id, added_by) "
               "VALUES ('wos', 100, '900', 'PlayerA', 30, 555, 1, 'x')")
    js.execute("INSERT INTO players (game_type, fid, user_id, nickname, furnace_level, state, alliance_id, added_by) "
               "VALUES ('ks', 200, NULL, 'KsPlayer', 10, 7, 2, 'x')")
    js.execute("INSERT INTO admins (user_id, alliances, is_owner) VALUES ('42', '[1]', 1)")
    js.execute("INSERT INTO gift_codes (game_type, gift_code, date, status) VALUES ('wos', 'CODE1', '2025-01-01', 'valid')")
    js.execute("INSERT INTO giftcode_usage (game_type, fid, gift_code, status) VALUES ('wos', 100, 'CODE1', 'success')")
    js.execute("INSERT INTO id_channels (game_type, guide_id, alliance_id, channel_id, linked_by) "
               "VALUES ('wos', '111', 1, '444', '42')")
    js.execute("INSERT INTO furnace_changes (game_type, fid, old_furnace_lv, new_furnace_lv, change_date) "
               "VALUES ('wos', 100, 29, 30, '2025-01-02T03:04:05.000Z')")
    js.execute("INSERT INTO nickname_changes (game_type, fid, old_nickname, new_nickname, change_date) "
               "VALUES ('wos', 100, 'Old', 'PlayerA', '2025-01-02T03:04:05.000Z')")
    js.execute("INSERT INTO test_ids (game_type, id, fid, state) VALUES ('wos', 1, 777, 555)")
    js.commit()


def q(db_dir, dbfile, sql, params=()):
    with closing(sqlite3.connect(db_dir / dbfile)) as conn:
        return conn.execute(sql, params).fetchall()


# ------------------------------------------------------------------ tests

def test_fresh_import_copies_wos_rows_and_skips_other_game(tmp_path):
    js_path = tmp_path / "Database.db"
    seed_basic_js(make_js_db(js_path))
    make_target(tmp_path / "db")

    stats = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert stats["alliances_new"] == 1
    assert stats["alliances_matched"] == 0
    assert stats["players_imported"] == 1
    assert q(tmp_path / "db", "alliance.sqlite", "SELECT name FROM alliance_list") == [("Alpha",)]
    assert q(tmp_path / "db", "users.sqlite", "SELECT fid, nickname, kid, discord_id FROM users") == \
        [(100, "PlayerA", 555, 900)]
    assert q(tmp_path / "db", "giftcode.sqlite", "SELECT giftcode, validation_status FROM gift_codes") == \
        [("CODE1", "validated")]
    assert q(tmp_path / "db", "giftcode.sqlite", "SELECT fid, giftcode, status FROM user_giftcodes") == \
        [(100, "CODE1", "SUCCESS")]
    assert q(tmp_path / "db", "changes.sqlite", "SELECT fid, change_date FROM furnace_changes") == \
        [(100, "2025-01-02 03:04:05")]
    assert q(tmp_path / "db", "settings.sqlite", "SELECT test_fid, kid FROM test_fid_settings") == \
        [("777", 555)]


def test_colliding_alliance_id_gets_remapped(tmp_path):
    js_path = tmp_path / "Database.db"
    js = make_js_db(js_path)
    js.execute("INSERT INTO alliance (id, game_type, priority, name) VALUES (1, 'wos', 1, 'Bravo')")
    js.execute("INSERT INTO players (game_type, fid, nickname, alliance_id, added_by) "
               "VALUES ('wos', 300, 'BravoPlayer', 1, 'x')")
    js.commit()
    make_target(tmp_path / "db")
    with closing(sqlite3.connect(tmp_path / "db" / "alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_list (alliance_id, name) VALUES (1, 'LocalExisting')")
        conn.commit()

    stats = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert stats["alliances_new"] == 1
    rows = q(tmp_path / "db", "alliance.sqlite", "SELECT alliance_id, name FROM alliance_list ORDER BY alliance_id")
    assert rows[0] == (1, "LocalExisting")
    new_id = rows[1][0]
    assert new_id != 1 and rows[1][1] == "Bravo"
    assert q(tmp_path / "db", "users.sqlite", "SELECT alliance FROM users WHERE fid = 300") == [(str(new_id),)] or \
        q(tmp_path / "db", "users.sqlite", "SELECT alliance FROM users WHERE fid = 300") == [(new_id,)]


def test_alliance_matched_by_name_keeps_local_settings(tmp_path):
    js_path = tmp_path / "Database.db"
    js = make_js_db(js_path)
    js.execute("INSERT INTO alliance (id, game_type, priority, name, channel_id, interval) "
               "VALUES (5, 'wos', 1, 'Alpha', '999', '30')")
    js.execute("INSERT INTO players (game_type, fid, nickname, alliance_id, added_by) "
               "VALUES ('wos', 400, 'AlphaGuy', 5, 'x')")
    js.commit()
    make_target(tmp_path / "db")
    with closing(sqlite3.connect(tmp_path / "db" / "alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_list (alliance_id, name) VALUES (2, 'Alpha')")
        conn.execute("INSERT INTO alliancesettings (alliance_id, channel_id, interval) VALUES (2, 123, 60)")
        conn.commit()

    stats = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert stats["alliances_matched"] == 1
    assert stats["alliances_new"] == 0
    assert q(tmp_path / "db", "alliance.sqlite", "SELECT COUNT(*) FROM alliance_list") == [(1,)]
    # existing settings win over the JS bot's channel/interval
    assert q(tmp_path / "db", "alliance.sqlite", "SELECT channel_id, interval FROM alliancesettings WHERE alliance_id = 2") == \
        [(123, 60)]
    alliance_val = q(tmp_path / "db", "users.sqlite", "SELECT alliance FROM users WHERE fid = 400")[0][0]
    assert int(alliance_val) == 2


def test_existing_player_wins(tmp_path):
    js_path = tmp_path / "Database.db"
    seed_basic_js(make_js_db(js_path))
    make_target(tmp_path / "db")
    with closing(sqlite3.connect(tmp_path / "db" / "users.sqlite")) as conn:
        conn.execute("INSERT INTO users (fid, nickname, alliance) VALUES (100, 'LocalName', '9')")
        conn.commit()

    stats = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert stats["players_imported"] == 0
    assert stats["players_skipped_existing"] == 1
    assert q(tmp_path / "db", "users.sqlite", "SELECT nickname, alliance FROM users WHERE fid = 100") == \
        [("LocalName", "9")]


def test_rerun_is_idempotent(tmp_path):
    js_path = tmp_path / "Database.db"
    seed_basic_js(make_js_db(js_path))
    make_target(tmp_path / "db")

    bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")
    stats2 = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert stats2["alliances_new"] == 0
    assert stats2["players_imported"] == 0
    assert q(tmp_path / "db", "alliance.sqlite", "SELECT COUNT(*) FROM alliance_list") == [(1,)]
    assert q(tmp_path / "db", "changes.sqlite", "SELECT COUNT(*) FROM furnace_changes") == [(1,)]
    assert q(tmp_path / "db", "changes.sqlite", "SELECT COUNT(*) FROM nickname_changes") == [(1,)]
    assert q(tmp_path / "db", "settings.sqlite", "SELECT COUNT(*) FROM test_fid_settings") == [(1,)]
    assert q(tmp_path / "db", "settings.sqlite", "SELECT COUNT(*) FROM adminserver") == [(1,)]


def test_admin_scoping_remapped(tmp_path):
    js_path = tmp_path / "Database.db"
    seed_basic_js(make_js_db(js_path))
    make_target(tmp_path / "db")
    with closing(sqlite3.connect(tmp_path / "db" / "alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_list (alliance_id, name) VALUES (1, 'Occupied')")
        conn.commit()

    bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert q(tmp_path / "db", "settings.sqlite", "SELECT id, is_initial, is_owner FROM admin") == [(42, 1, 0)]
    new_id = q(tmp_path / "db", "alliance.sqlite",
               "SELECT alliance_id FROM alliance_list WHERE name = 'Alpha'")[0][0]
    assert q(tmp_path / "db", "settings.sqlite", "SELECT admin, alliances_id FROM adminserver") == [(42, new_id)]


def test_orphan_player_skipped(tmp_path):
    js_path = tmp_path / "Database.db"
    js = make_js_db(js_path)
    js.execute("INSERT INTO players (game_type, fid, nickname, alliance_id, added_by) "
               "VALUES ('wos', 500, 'Orphan', 99, 'x')")
    js.commit()
    make_target(tmp_path / "db")

    stats = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert stats["players_imported"] == 0
    assert stats["players_skipped_orphan"] == 1
    assert q(tmp_path / "db", "users.sqlite", "SELECT COUNT(*) FROM users") == [(0,)]


def test_dry_run_reports_counts_without_writing(tmp_path):
    js_path = tmp_path / "Database.db"
    seed_basic_js(make_js_db(js_path))
    make_target(tmp_path / "db")

    stats = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos", dry_run=True)

    assert stats["alliances_new"] == 1
    assert stats["players_imported"] == 1
    assert q(tmp_path / "db", "alliance.sqlite", "SELECT COUNT(*) FROM alliance_list") == [(0,)]
    assert q(tmp_path / "db", "users.sqlite", "SELECT COUNT(*) FROM users") == [(0,)]


def test_validate_rejects_non_sqlite_file(tmp_path):
    bad = tmp_path / "Database.db"
    bad.write_bytes(b"this is not a database")
    ok, reason = bji.validate_js_db(str(bad))
    assert not ok
    assert reason


def test_validate_rejects_wrong_sqlite(tmp_path):
    other = tmp_path / "Database.db"
    with closing(sqlite3.connect(other)) as conn:
        conn.execute("CREATE TABLE unrelated (x INTEGER)")
        conn.commit()
    ok, reason = bji.validate_js_db(str(other))
    assert not ok


def test_validate_accepts_js_db(tmp_path):
    js_path = tmp_path / "Database.db"
    make_js_db(js_path).close()
    ok, reason = bji.validate_js_db(str(js_path))
    assert ok, reason


# ------------------------------------------------------- file sourcing

def test_find_local_js_dbs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "backups").mkdir()
    (tmp_path / "Database.db").write_bytes(b"x")
    (tmp_path / "backups" / "database.DB").write_bytes(b"x")
    (tmp_path / "backups" / "other.zip").write_bytes(b"x")

    found = bji.find_local_js_dbs()

    names = sorted(p.replace("\\", "/") for p in found)
    assert names == ["Database.db", "backups/database.DB"]


def test_extract_js_db_from_zip(tmp_path):
    import zipfile
    src = tmp_path / "Database.db"
    make_js_db(src).close()
    zpath = tmp_path / "backup.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.write(src, "some/folder/Database.db")

    out = bji.extract_js_db_from_zip(str(zpath), str(tmp_path / "out"))

    assert out is not None
    ok, reason = bji.validate_js_db(out)
    assert ok, reason


def test_extract_js_db_from_zip_rejects_traversal_and_missing(tmp_path):
    import zipfile
    zpath = tmp_path / "evil.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("../Database.db", b"nope")
    assert bji.extract_js_db_from_zip(str(zpath), str(tmp_path / "out1")) is None

    zpath2 = tmp_path / "nodb.zip"
    with zipfile.ZipFile(zpath2, "w") as zf:
        zf.writestr("readme.txt", b"hi")
    assert bji.extract_js_db_from_zip(str(zpath2), str(tmp_path / "out2")) is None


def test_target_summary_counts_existing_rows(tmp_path):
    make_target(tmp_path / "db")
    with closing(sqlite3.connect(tmp_path / "db" / "users.sqlite")) as conn:
        conn.execute("INSERT INTO users (fid, nickname) VALUES (1, 'a')")
        conn.commit()
    with closing(sqlite3.connect(tmp_path / "db" / "alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_list (alliance_id, name) VALUES (1, 'A')")
        conn.execute("INSERT INTO alliance_list (alliance_id, name) VALUES (2, 'B')")
        conn.commit()

    assert bji.target_summary(str(tmp_path / "db")) == (1, 2)

def test_null_nickname_and_furnace_get_defaults(tmp_path):
    js_path = tmp_path / "Database.db"
    js = make_js_db(js_path)
    js.execute("INSERT INTO alliance (id, game_type, priority, name) VALUES (1, 'wos', 1, 'Alpha')")
    js.execute("INSERT INTO players (game_type, fid, nickname, furnace_level, alliance_id, added_by) "
               "VALUES ('wos', 600, NULL, NULL, 1, 'x')")
    js.commit()
    make_target(tmp_path / "db")

    bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert q(tmp_path / "db", "users.sqlite", "SELECT nickname, furnace_lv FROM users WHERE fid = 600") ==         [("Player 600", 0)]


def test_duplicate_js_alliance_names_merge_locally(tmp_path):
    js_path = tmp_path / "Database.db"
    js = make_js_db(js_path)
    js.execute("INSERT INTO alliance (id, game_type, priority, name) VALUES (1, 'wos', 1, 'Alpha')")
    js.execute("INSERT INTO alliance (id, game_type, priority, name) VALUES (2, 'wos', 2, 'Alpha')")
    js.execute("INSERT INTO players (game_type, fid, nickname, alliance_id, added_by) VALUES ('wos', 700, 'A', 1, 'x')")
    js.execute("INSERT INTO players (game_type, fid, nickname, alliance_id, added_by) VALUES ('wos', 701, 'B', 2, 'x')")
    js.commit()
    make_target(tmp_path / "db")

    stats = bji.run_import(str(js_path), str(tmp_path / "db"), game_type="wos")

    assert stats["alliances_new"] == 1
    assert stats["alliances_matched"] == 1
    assert q(tmp_path / "db", "alliance.sqlite", "SELECT COUNT(*) FROM alliance_list") == [(1,)]
    a1 = q(tmp_path / "db", "users.sqlite", "SELECT alliance FROM users WHERE fid = 700")
    a2 = q(tmp_path / "db", "users.sqlite", "SELECT alliance FROM users WHERE fid = 701")
    assert a1 == a2
