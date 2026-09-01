"""
Centralized permission handler. Manages three admin tiers plus the
Bot Owner anchor:
- Bot Owner (is_owner=1, also is_initial=1): exactly one; recovery anchor,
  can't be removed except via explicit Transfer Owner.
- Global Admin (is_initial=1): all alliances, admin management, bot settings.
  Multiple allowed; not the same as the Owner.
- Server Admin (is_initial=0, no adminserver rows): all alliances on their
  Discord server.
- Alliance Admin (is_initial=0, has adminserver rows): only assigned
  alliance(s).
"""

import sqlite3
import time
import logging
from contextlib import closing
from datetime import datetime, timezone
from typing import Tuple, List, Optional

logger = logging.getLogger('bot')


TIER_OWNER = 'owner'
TIER_GLOBAL = 'global'
TIER_SERVER = 'server'
TIER_ALLIANCE = 'alliance'
TIER_NONE = 'none'

class PermissionManager:
    """Centralized permission handler"""

    SETTINGS_DB = 'db/settings.sqlite'
    ALLIANCE_DB = 'db/alliance.sqlite'
    USERS_DB = 'db/users.sqlite'

    # Short-TTL cache of the admin table ({id: is_initial}). is_admin is the
    # hottest permission primitive (per-keystroke autocompletes, per-click gates);
    # caching avoids opening a connection on every call. Staleness is bounded to
    # the TTL, so an added/removed admin takes effect within a few seconds.
    _ADMIN_CACHE_TTL = 5.0
    _admin_cache = None
    _admin_cache_at = 0.0

    @classmethod
    def _admin_map(cls) -> dict:
        now = time.monotonic()
        if cls._admin_cache is None or (now - cls._admin_cache_at) > cls._ADMIN_CACHE_TTL:
            with sqlite3.connect(cls.SETTINGS_DB) as db:
                rows = db.execute("SELECT id, is_initial FROM admin").fetchall()
            cls._admin_cache = {int(r[0]): r[1] for r in rows}
            cls._admin_cache_at = now
        return cls._admin_cache

    @staticmethod
    def is_admin(user_id: int) -> Tuple[bool, bool]:
        """
        Check if user is admin and their level, merging any cached role grant.
        Guild-blind by design; alliance scope is enforced (guild-aware) in get_admin_alliance_ids.

        Returns:
            (is_admin, is_global) - is_global True means access to all alliances
        """
        admin_map = PermissionManager._admin_map()
        user_admin = user_id in admin_map
        user_global = user_admin and admin_map[user_id] == 1
        rg = PermissionManager._cached_role_grant(user_id)
        is_admin = user_admin or bool(rg and rg["is_admin"])
        is_global = user_global or bool(rg and rg["is_global"])
        return is_admin, is_global

    @staticmethod
    def get_admin_alliance_ids(user_id: int, guild_id: int) -> Tuple[List[int], bool]:
        """
        Get alliance IDs the admin can access, merging any cached role grant.

        Returns:
            (alliance_ids, is_global)
            - If global: ([], True) - empty list means "all"
            - If server admin: (list of IDs, False)
        """
        is_admin, is_global = PermissionManager.is_admin(user_id)
        if not is_admin:
            return [], False
        if is_global:
            return [], True

        rg = PermissionManager._cached_role_grant(user_id) or {"server": False, "alliance_ids": [], "guild_id": None}
        with closing(sqlite3.connect(PermissionManager.SETTINGS_DB)) as db:
            user_assigned = [r[0] for r in db.execute(
                "SELECT alliances_id FROM adminserver WHERE admin = ?", (user_id,)).fetchall()]
        user_is_server_tier = (user_id in PermissionManager._admin_map()) and not user_assigned
        # Role server-tier is guild-scoped: only when the cached grant came from this guild.
        role_server = rg["server"] and rg.get("guild_id") == guild_id
        if role_server or user_is_server_tier:
            with closing(sqlite3.connect(PermissionManager.ALLIANCE_DB)) as adb:
                return [r[0] for r in adb.execute(
                    "SELECT alliance_id FROM alliance_list WHERE discord_server_id = ?", (guild_id,)).fetchall()], False
        return sorted(set(user_assigned) | set(rg["alliance_ids"])), False

    @staticmethod
    def get_admin_alliances(user_id: int, guild_id: int) -> Tuple[List[Tuple], bool]:
        """
        Get alliance tuples (id, name) for admin, merging any cached role grant.
        Used by most cogs for alliance selection dropdowns.

        Returns:
            (alliances, is_global)
        """
        is_admin, is_global = PermissionManager.is_admin(user_id)
        if not is_admin:
            return [], False

        if is_global:
            with closing(sqlite3.connect(PermissionManager.ALLIANCE_DB)) as db:
                return db.execute("SELECT DISTINCT alliance_id, name FROM alliance_list ORDER BY name").fetchall(), True

        ids, _ = PermissionManager.get_admin_alliance_ids(user_id, guild_id)
        if not ids:
            return [], False
        with closing(sqlite3.connect(PermissionManager.ALLIANCE_DB)) as db:
            ph = ','.join('?' * len(ids))
            return db.execute(
                f"SELECT DISTINCT alliance_id, name FROM alliance_list WHERE alliance_id IN ({ph}) ORDER BY name", ids
            ).fetchall(), False

    @staticmethod
    def get_admin_users(user_id: int, guild_id: int = None) -> List[Tuple]:
        """
        Get users the admin can see based on their permissions.

        Returns:
            list of (fid, nickname, alliance) tuples
        """
        is_admin, is_global = PermissionManager.is_admin(user_id)

        if not is_admin:
            return []

        if is_global:
            # Global admin - return ALL users
            with sqlite3.connect(PermissionManager.USERS_DB) as db:
                cursor = db.cursor()
                cursor.execute("SELECT fid, nickname, alliance FROM users ORDER BY LOWER(nickname)")
                return cursor.fetchall()

        # Server admin - get alliance IDs they can access
        alliance_ids, _ = PermissionManager.get_admin_alliance_ids(user_id, guild_id)

        if not alliance_ids:
            return []

        # Get users from those alliances
        with sqlite3.connect(PermissionManager.USERS_DB) as db:
            cursor = db.cursor()
            placeholders = ','.join('?' * len(alliance_ids))
            cursor.execute(f"""
                SELECT fid, nickname, alliance
                FROM users
                WHERE alliance IN ({placeholders})
                ORDER BY LOWER(nickname)
            """, alliance_ids)
            return cursor.fetchall()

    # ---------------------------------------------------------------
    # Owner / tier helpers
    # ---------------------------------------------------------------

    @staticmethod
    def list_alliances() -> List[Tuple[int, str]]:
        """Return [(alliance_id, name), ...] sorted by name."""
        with sqlite3.connect(PermissionManager.ALLIANCE_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT alliance_id, name FROM alliance_list ORDER BY LOWER(name)")
            return cur.fetchall()

    @staticmethod
    def get_admin_alliance_assignments(user_id: int) -> List[int]:
        """Return [alliance_id, ...] for the rows in `adminserver` belonging
        to this admin. Empty list when the admin has no specific assignments
        (Server-tier or Global)."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT alliances_id FROM adminserver WHERE admin = ?", (user_id,))
            return [int(row[0]) for row in cur.fetchall()]

    @staticmethod
    def is_owner(user_id: int) -> bool:
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT is_owner FROM admin WHERE id = ?", (user_id,))
            row = cur.fetchone()
        return bool(row and row[0])

    @staticmethod
    def get_owner_id() -> Optional[int]:
        """Return the Discord user id of the bot owner, or None when no
        owner has been claimed yet."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT id FROM admin WHERE is_owner = 1 LIMIT 1")
            row = cur.fetchone()
        return int(row[0]) if row else None

    @staticmethod
    def count_globals() -> int:
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM admin WHERE is_initial = 1")
            return int(cur.fetchone()[0])

    @staticmethod
    def list_admins() -> List[dict]:
        """Return every admin row enriched with tier + alliance count.

        Tier is derived: 'owner' overrides 'global', otherwise the presence
        or absence of `adminserver` rows distinguishes 'alliance' from 'server'.
        """
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("""
                SELECT a.id, a.is_initial, a.is_owner,
                       (SELECT COUNT(*) FROM adminserver s WHERE s.admin = a.id) AS alliance_count
                FROM admin a
                ORDER BY a.is_owner DESC, a.is_initial DESC, a.id
            """)
            rows = cur.fetchall()
        out = []
        for uid, is_initial, is_owner, alliance_count in rows:
            if is_owner:
                tier = TIER_OWNER
            elif is_initial:
                tier = TIER_GLOBAL
            elif alliance_count:
                tier = TIER_ALLIANCE
            else:
                tier = TIER_SERVER
            out.append({
                'id': int(uid),
                'tier': tier,
                'is_initial': bool(is_initial),
                'is_owner': bool(is_owner),
                'alliance_count': int(alliance_count),
            })
        return out

    @staticmethod
    def get_tier(user_id: int) -> str:
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT a.is_initial, a.is_owner, "
                "(SELECT COUNT(*) FROM adminserver s WHERE s.admin = a.id) "
                "FROM admin a WHERE a.id = ?",
                (user_id,),
            )
            row = cur.fetchone()
        if not row:
            return TIER_NONE
        is_initial, is_owner, alliance_count = row
        if is_owner:
            return TIER_OWNER
        if is_initial:
            return TIER_GLOBAL
        return TIER_ALLIANCE if alliance_count else TIER_SERVER

    @staticmethod
    def add_admin(user_id: int, *, tier: str, alliance_ids: Optional[List[int]] = None) -> None:
        """Insert a new admin at the given tier. If no owner exists yet
        (brand-new install), the first admin gets is_owner=1 automatically."""
        if tier not in (TIER_OWNER, TIER_GLOBAL, TIER_SERVER, TIER_ALLIANCE):
            raise ValueError(f"Unknown tier: {tier}")
        if tier == TIER_ALLIANCE and not alliance_ids:
            raise ValueError("Alliance tier requires at least one alliance_id")
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM admin")
            total_admins = int(cur.fetchone()[0])
            # Brand-new install: the very first admin added auto-becomes
            # owner (and Global) regardless of the tier the caller picked,
            # because the bot can't be useful without an owner.
            # If admins already exist but ownership is unclaimed (the
            # multi-global migration case), the new admin does NOT become
            # owner — Claim Owner is the only path for those installs.
            is_initial = 1 if tier in (TIER_OWNER, TIER_GLOBAL) else 0
            is_owner = 1 if (tier == TIER_OWNER or total_admins == 0) else 0
            if is_owner:
                is_initial = 1
            cur.execute(
                "INSERT OR REPLACE INTO admin (id, is_initial, is_owner) VALUES (?, ?, ?)",
                (user_id, is_initial, is_owner),
            )
            if tier == TIER_ALLIANCE:
                for aid in alliance_ids or []:
                    cur.execute(
                        "INSERT OR IGNORE INTO adminserver (admin, alliances_id) VALUES (?, ?)",
                        (user_id, aid),
                    )
            db.commit()

    @staticmethod
    def set_tier(user_id: int, tier: str, *, alliance_ids: Optional[List[int]] = None) -> None:
        """Change an existing admin's tier. Owner tier is never settable
        through this method — use `transfer_owner` instead."""
        if tier == TIER_OWNER:
            raise ValueError("Use transfer_owner() to change the bot owner")
        if tier not in (TIER_GLOBAL, TIER_SERVER, TIER_ALLIANCE):
            raise ValueError(f"Unknown tier: {tier}")
        if tier == TIER_ALLIANCE and not alliance_ids:
            raise ValueError("Alliance tier requires at least one alliance_id")
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT is_owner FROM admin WHERE id = ?", (user_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"User {user_id} is not an admin")
            if row[0]:
                raise ValueError("Cannot demote the bot owner; transfer ownership first")
            is_initial = 1 if tier == TIER_GLOBAL else 0
            cur.execute("UPDATE admin SET is_initial = ? WHERE id = ?", (is_initial, user_id))
            # Replace alliance assignments wholesale to match the new tier.
            cur.execute("DELETE FROM adminserver WHERE admin = ?", (user_id,))
            if tier == TIER_ALLIANCE:
                for aid in alliance_ids or []:
                    cur.execute(
                        "INSERT OR IGNORE INTO adminserver (admin, alliances_id) VALUES (?, ?)",
                        (user_id, aid),
                    )
            db.commit()

    @staticmethod
    def remove_admin(user_id: int) -> None:
        """Delete an admin and all their alliance assignments. Owner is
        guarded — caller must ensure the target isn't the owner."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT is_owner FROM admin WHERE id = ?", (user_id,))
            row = cur.fetchone()
            if row and row[0]:
                raise ValueError("Cannot remove the bot owner; transfer ownership first")
            cur.execute("DELETE FROM adminserver WHERE admin = ?", (user_id,))
            cur.execute("DELETE FROM admin WHERE id = ?", (user_id,))
            db.commit()

    @staticmethod
    def claim_owner(user_id: int) -> bool:
        """Atomic 'first global admin to claim wins' flow. Used when the
        bot starts with multiple existing globals and no owner. Returns
        True if this user just became the owner, False if someone else
        already had it."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT 1 FROM admin WHERE is_owner = 1 LIMIT 1")
            if cur.fetchone():
                return False
            cur.execute(
                "UPDATE admin SET is_owner = 1, is_initial = 1 WHERE id = ? AND is_initial = 1",
                (user_id,),
            )
            changed = cur.rowcount > 0
            db.commit()
            return changed

    @staticmethod
    def transfer_owner(from_user_id: int, to_user_id: int) -> None:
        """Move the is_owner flag atomically. Both must be admins; the
        recipient must already be Global tier (caller guards this in UI)."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT is_owner FROM admin WHERE id = ?", (from_user_id,))
            row = cur.fetchone()
            if not row or not row[0]:
                raise ValueError("Source user is not the current owner")
            cur.execute("SELECT is_initial FROM admin WHERE id = ?", (to_user_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("Target user is not an admin")
            if not row[0]:
                raise ValueError("Target user must be Global tier before receiving ownership")
            cur.execute("UPDATE admin SET is_owner = 0 WHERE id = ?", (from_user_id,))
            cur.execute("UPDATE admin SET is_owner = 1 WHERE id = ?", (to_user_id,))
            db.commit()

    @staticmethod
    def describe_state(user_id: int) -> str:
        """Human-readable admin state for the audit log (e.g. 'Alliance Admin (2 alliances)')."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT is_initial, is_owner FROM admin WHERE id = ?", (user_id,))
            row = cur.fetchone()
            if not row:
                return "Not an admin"
            is_initial, is_owner = row
            cur.execute("SELECT alliances_id FROM adminserver WHERE admin = ? ORDER BY alliances_id", (user_id,))
            alliances = [r[0] for r in cur.fetchall()]
        if is_owner:
            return "Bot Owner"
        if is_initial:
            return "Global Admin"
        if alliances:
            count = len(alliances)
            return f"Alliance Admin ({count} alliance{'s' if count != 1 else ''})"
        return "Server Admin"

    @staticmethod
    def log_change(actor_id: int, action: str, target_id: int,
                   before_state: Optional[str], after_state: Optional[str]) -> None:
        """Append a row to permission_audit_log. Called by the UI after every admin mutation."""
        try:
            with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
                db.execute(
                    "INSERT INTO permission_audit_log "
                    "(actor_id, action, target_id, before_state, after_state, timestamp) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (actor_id, action, target_id, before_state, after_state,
                     datetime.now(timezone.utc).isoformat()),
                )
                db.commit()
        except Exception as e:
            logger.error(f"Failed to write permission audit log: {e}")
            print(f"Failed to write permission audit log: {e}")

    @staticmethod
    def get_audit_log_page(offset: int = 0, limit: int = 10) -> Tuple[List[dict], int]:
        """Return (rows, total_count) for the audit log, newest first."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("SELECT COUNT(*) FROM permission_audit_log")
            total = int(cur.fetchone()[0])
            cur.execute(
                "SELECT actor_id, action, target_id, before_state, after_state, timestamp "
                "FROM permission_audit_log ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )
            rows = [
                {
                    'actor_id': r[0], 'action': r[1], 'target_id': r[2],
                    'before_state': r[3], 'after_state': r[4], 'timestamp': r[5],
                }
                for r in cur.fetchall()
            ]
        return rows, total

    # ---------------------------------------------------------------
    # Role-grant CRUD (a Discord role holding a tier, mirroring admin/adminserver)
    # ---------------------------------------------------------------

    @staticmethod
    def add_role_grant(role_id: int, guild_id: int, *, tier: str, alliance_ids: Optional[List[int]] = None) -> None:
        if tier not in (TIER_GLOBAL, TIER_SERVER, TIER_ALLIANCE):
            raise ValueError(f"Roles cannot be granted tier: {tier}")
        if tier == TIER_ALLIANCE and not alliance_ids:
            raise ValueError("Alliance tier requires at least one alliance_id")
        is_initial = 1 if tier == TIER_GLOBAL else 0
        with closing(sqlite3.connect(PermissionManager.SETTINGS_DB)) as db, db:
            db.execute("INSERT OR REPLACE INTO admin_role (role_id, guild_id, is_initial) VALUES (?, ?, ?)",
                       (role_id, guild_id, is_initial))
            db.execute("DELETE FROM adminserver_role WHERE role_id = ?", (role_id,))
            if tier == TIER_ALLIANCE:
                for aid in alliance_ids or []:
                    db.execute("INSERT OR IGNORE INTO adminserver_role (role_id, alliances_id) VALUES (?, ?)", (role_id, aid))
        PermissionManager._bust_role_cache()

    @staticmethod
    def set_role_tier(role_id: int, tier: str, *, alliance_ids: Optional[List[int]] = None) -> None:
        with closing(sqlite3.connect(PermissionManager.SETTINGS_DB)) as db:
            row = db.execute("SELECT guild_id FROM admin_role WHERE role_id = ?", (role_id,)).fetchone()
        if not row:
            raise ValueError(f"Role {role_id} is not granted")
        PermissionManager.add_role_grant(role_id, row[0], tier=tier, alliance_ids=alliance_ids)

    @staticmethod
    def remove_role_grant(role_id: int) -> None:
        with closing(sqlite3.connect(PermissionManager.SETTINGS_DB)) as db, db:
            db.execute("DELETE FROM adminserver_role WHERE role_id = ?", (role_id,))
            db.execute("DELETE FROM admin_role WHERE role_id = ?", (role_id,))
        PermissionManager._bust_role_cache()

    @staticmethod
    def get_role_alliance_assignments(role_id: int) -> List[int]:
        with closing(sqlite3.connect(PermissionManager.SETTINGS_DB)) as db:
            return [int(r[0]) for r in db.execute(
                "SELECT alliances_id FROM adminserver_role WHERE role_id = ?", (role_id,)).fetchall()]

    @staticmethod
    def get_role_tier(role_id: int) -> str:
        with closing(sqlite3.connect(PermissionManager.SETTINGS_DB)) as db:
            row = db.execute("SELECT is_initial FROM admin_role WHERE role_id = ?", (role_id,)).fetchone()
            if not row:
                return TIER_NONE
            if row[0]:
                return TIER_GLOBAL
            n = db.execute("SELECT COUNT(*) FROM adminserver_role WHERE role_id = ?", (role_id,)).fetchone()[0]
        return TIER_ALLIANCE if n else TIER_SERVER

    @staticmethod
    def list_role_grants() -> List[dict]:
        with closing(sqlite3.connect(PermissionManager.SETTINGS_DB)) as db:
            rows = db.execute(
                "SELECT r.role_id, r.guild_id, r.is_initial, "
                "(SELECT COUNT(*) FROM adminserver_role s WHERE s.role_id = r.role_id) "
                "FROM admin_role r ORDER BY r.is_initial DESC, r.role_id").fetchall()
        out = []
        for role_id, guild_id, is_initial, count in rows:
            tier = TIER_GLOBAL if is_initial else (TIER_ALLIANCE if count else TIER_SERVER)
            out.append({'role_id': int(role_id), 'guild_id': guild_id, 'tier': tier, 'alliance_count': int(count)})
        return out

    @staticmethod
    def describe_role_state(role_id: int) -> str:
        tier = PermissionManager.get_role_tier(role_id)
        if tier == TIER_GLOBAL:
            return "Global Admin (role)"
        if tier == TIER_ALLIANCE:
            n = len(PermissionManager.get_role_alliance_assignments(role_id))
            return f"Alliance Admin (role, {n} alliance{'s' if n != 1 else ''})"
        if tier == TIER_SERVER:
            return "Server Admin (role)"
        return "Not a granted role"

    # 5s TTL cache of the role-grant tables ({role_id: (tier, [alliance_ids])}).
    _role_map_cache = None
    _role_map_cache_at = 0.0
    _ROLE_MAP_TTL = 5.0

    @classmethod
    def _role_admin_map(cls) -> dict:
        now = time.monotonic()
        if cls._role_map_cache is None or (now - cls._role_map_cache_at) > cls._ROLE_MAP_TTL:
            out = {}
            with closing(sqlite3.connect(cls.SETTINGS_DB)) as db:
                for role_id, is_initial in db.execute("SELECT role_id, is_initial FROM admin_role").fetchall():
                    aids = [int(r[0]) for r in db.execute(
                        "SELECT alliances_id FROM adminserver_role WHERE role_id = ?", (role_id,)).fetchall()]
                    tier = TIER_GLOBAL if is_initial else (TIER_ALLIANCE if aids else TIER_SERVER)
                    out[int(role_id)] = (tier, aids)
            cls._role_map_cache = out
            cls._role_map_cache_at = now
        return cls._role_map_cache

    @staticmethod
    def resolve_member_roles(role_ids: List[int]) -> dict:
        """Resolve a member's Discord role ids to their effective role-only grant."""
        role_map = PermissionManager._role_admin_map()
        is_global = server = is_admin = False
        alliance_ids = set()
        for rid in role_ids:
            grant = role_map.get(int(rid))
            if not grant:
                continue
            is_admin = True
            tier, aids = grant
            if tier == TIER_GLOBAL:
                is_global = True
            elif tier == TIER_SERVER:
                server = True
            else:
                alliance_ids.update(aids)
        return {"is_admin": is_admin, "is_global": is_global, "server": server, "alliance_ids": sorted(alliance_ids)}

    # Per-user cache of the resolved role grant, populated by the interaction boundary hook so `is_admin` et al. don't re-resolve roles every call.
    _role_grant_by_user = {}   # {user_id: (RoleGrant, guild_id, expiry_monotonic)}
    _ROLE_GRANT_TTL = 30.0

    @classmethod
    def cache_member_role_grant(cls, user_id: int, role_ids: List[int], guild_id: int | None = None) -> None:
        grant = cls.resolve_member_roles(role_ids)
        cls._role_grant_by_user[int(user_id)] = (grant, guild_id, time.monotonic() + cls._ROLE_GRANT_TTL)

    @classmethod
    def _cached_role_grant(cls, user_id: int) -> Optional[dict]:
        entry = cls._role_grant_by_user.get(int(user_id))
        if not entry:
            return None
        grant, guild_id, expiry = entry
        if time.monotonic() > expiry:
            cls._role_grant_by_user.pop(int(user_id), None)
            return None
        return {**grant, "guild_id": guild_id}

    @staticmethod
    def is_member_admin(user_id: int, role_ids: List[int]) -> Tuple[bool, bool]:
        """Direct merge of a user's personal grant with their role grant, bypassing
        the per-user cache. For the message-path site that has roles inline."""
        admin_map = PermissionManager._admin_map()
        user_admin = user_id in admin_map
        user_global = user_admin and admin_map[user_id] == 1
        rg = PermissionManager.resolve_member_roles(role_ids)
        return (user_admin or rg["is_admin"], user_global or rg["is_global"])

    @classmethod
    def _bust_role_cache(cls):
        cls._role_map_cache = None
        cls._role_grant_by_user.clear()
