import importlib, sqlite3
import pytest

pm_mod = importlib.import_module("cogs.permission_handler")
PermissionManager = pm_mod.PermissionManager


def _fresh_db(tmp_path, monkeypatch):
    db = tmp_path / "settings.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE admin (id INTEGER PRIMARY KEY, is_initial INTEGER, is_owner INTEGER)")
        conn.execute("CREATE TABLE adminserver (admin INTEGER, alliances_id INTEGER)")
        conn.execute("CREATE TABLE admin_role (role_id INTEGER PRIMARY KEY, guild_id INTEGER, is_initial INTEGER NOT NULL DEFAULT 0)")
        conn.execute("CREATE TABLE adminserver_role (role_id INTEGER, alliances_id INTEGER, PRIMARY KEY (role_id, alliances_id))")
        conn.commit()
    monkeypatch.setattr(PermissionManager, "SETTINGS_DB", str(db))
    return db


def test_add_and_derive_role_tiers(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    PermissionManager.add_role_grant(111, 900, tier="global")
    PermissionManager.add_role_grant(222, 900, tier="server")
    PermissionManager.add_role_grant(333, 900, tier="alliance", alliance_ids=[5, 6])
    assert PermissionManager.get_role_tier(111) == "global"
    assert PermissionManager.get_role_tier(222) == "server"
    assert PermissionManager.get_role_tier(333) == "alliance"
    assert PermissionManager.get_role_tier(999) == "none"
    assert sorted(PermissionManager.get_role_alliance_assignments(333)) == [5, 6]


def test_role_grant_rejects_owner_and_empty_alliance(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        PermissionManager.add_role_grant(111, 900, tier="owner")
    with pytest.raises(ValueError):
        PermissionManager.add_role_grant(111, 900, tier="alliance", alliance_ids=[])


def test_set_tier_replaces_scope_and_remove(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    PermissionManager.add_role_grant(333, 900, tier="alliance", alliance_ids=[5])
    PermissionManager.set_role_tier(333, "alliance", alliance_ids=[7, 8])
    assert sorted(PermissionManager.get_role_alliance_assignments(333)) == [7, 8]
    PermissionManager.set_role_tier(333, "global")
    assert PermissionManager.get_role_tier(333) == "global"
    assert PermissionManager.get_role_alliance_assignments(333) == []
    PermissionManager.remove_role_grant(333)
    assert PermissionManager.get_role_tier(333) == "none"


def test_resolve_member_roles_broadest_and_union(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    PermissionManager.add_role_grant(10, 900, tier="alliance", alliance_ids=[1])
    PermissionManager.add_role_grant(20, 900, tier="alliance", alliance_ids=[2, 3])
    PermissionManager._bust_role_cache()
    g = PermissionManager.resolve_member_roles([10, 20, 99])  # 99 not granted
    assert g["is_admin"] and not g["is_global"] and not g["server"]
    assert sorted(g["alliance_ids"]) == [1, 2, 3]

    PermissionManager.add_role_grant(30, 900, tier="global")
    PermissionManager._bust_role_cache()
    g2 = PermissionManager.resolve_member_roles([10, 30])
    assert g2["is_admin"] and g2["is_global"]

def test_resolve_member_roles_none(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    g = PermissionManager.resolve_member_roles([1, 2, 3])
    assert g == {"is_admin": False, "is_global": False, "server": False, "alliance_ids": []}


def _seed_user(db_path, uid, is_initial=0, is_owner=0, alliances=()):
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT OR REPLACE INTO admin (id, is_initial, is_owner) VALUES (?, ?, ?)", (uid, is_initial, is_owner))
        for aid in alliances:
            conn.execute("INSERT INTO adminserver (admin, alliances_id) VALUES (?, ?)", (uid, aid))
        conn.commit()

def test_role_only_member_is_admin(tmp_path, monkeypatch):
    db = _fresh_db(tmp_path, monkeypatch)
    PermissionManager.add_role_grant(10, 900, tier="alliance", alliance_ids=[1])
    PermissionManager._bust_role_cache()
    PermissionManager._admin_cache = None  # bust user cache
    # user 42 has no personal grant, but holds role 10
    PermissionManager.cache_member_role_grant(42, [10], 900)
    is_admin, is_global = PermissionManager.is_admin(42)
    assert is_admin and not is_global
    ids, glob = PermissionManager.get_admin_alliance_ids(42, guild_id=900)
    assert glob is False and ids == [1]

def test_user_and_role_union(tmp_path, monkeypatch):
    db = _fresh_db(tmp_path, monkeypatch)
    _seed_user(db, 42, is_initial=0, alliances=[1])   # Alliance Admin for 1
    PermissionManager.add_role_grant(10, 900, tier="alliance", alliance_ids=[2])
    PermissionManager._bust_role_cache(); PermissionManager._admin_cache = None
    PermissionManager.cache_member_role_grant(42, [10], 900)
    ids, glob = PermissionManager.get_admin_alliance_ids(42, guild_id=900)
    assert glob is False and sorted(ids) == [1, 2]

def test_role_global_beats_user_alliance(tmp_path, monkeypatch):
    db = _fresh_db(tmp_path, monkeypatch)
    _seed_user(db, 42, is_initial=0, alliances=[1])
    PermissionManager.add_role_grant(10, 900, tier="global")
    PermissionManager._bust_role_cache(); PermissionManager._admin_cache = None
    PermissionManager.cache_member_role_grant(42, [10], 900)
    is_admin, is_global = PermissionManager.is_admin(42)
    assert is_admin and is_global
    ids, glob = PermissionManager.get_admin_alliance_ids(42, guild_id=900)
    assert glob is True and ids == []

def test_is_member_admin_direct(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    PermissionManager.add_role_grant(10, 900, tier="server")
    PermissionManager._bust_role_cache(); PermissionManager._admin_cache = None
    assert PermissionManager.is_member_admin(77, [10]) == (True, False)
    assert PermissionManager.is_member_admin(77, [999]) == (False, False)


def test_role_admin_passes_upload_gate(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    PermissionManager.add_role_grant(10, 900, tier="alliance", alliance_ids=[1])
    PermissionManager._bust_role_cache(); PermissionManager._admin_cache = None
    # a member holding role 10 is recognized as admin at the message gate
    ok, _ = PermissionManager.is_member_admin(555, [10])
    assert ok is True


def test_server_role_does_not_bleed_across_guilds(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    PermissionManager.add_role_grant(10, 900, tier="server")
    PermissionManager._bust_role_cache(); PermissionManager._admin_cache = None
    PermissionManager.cache_member_role_grant(42, [10], 900)   # earned in guild 900
    # querying a DIFFERENT guild must NOT grant that guild's alliances
    ids, glob = PermissionManager.get_admin_alliance_ids(42, guild_id=901)
    assert glob is False and ids == []
