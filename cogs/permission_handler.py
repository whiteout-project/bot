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
import logging
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

    @staticmethod
    def is_admin(user_id: int) -> Tuple[bool, bool]:
        """
        Check if user is admin and their level.

        Returns:
            (is_admin, is_global) - is_global True means access to all alliances
        """
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cursor = db.cursor()
            cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (user_id,))
            result = cursor.fetchone()

            if not result:
                return False, False

            return True, result[0] == 1

    @staticmethod
    def get_admin_alliance_ids(user_id: int, guild_id: int) -> Tuple[List[int], bool]:
        """
        Get alliance IDs the admin can access.

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

        # Server admin - check for specific assignments
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cursor = db.cursor()
            cursor.execute("SELECT alliances_id FROM adminserver WHERE admin = ?", (user_id,))
            assigned = [row[0] for row in cursor.fetchall()]

        if assigned:
            # Alliance Admin: Has specific assignments - use ONLY those
            return assigned, False
        else:
            # Server Admin: No assignments - use all alliances on their Discord server
            with sqlite3.connect(PermissionManager.ALLIANCE_DB) as alliance_db:
                ac = alliance_db.cursor()
                ac.execute("SELECT alliance_id FROM alliance_list WHERE discord_server_id = ?", (guild_id,))
                return [row[0] for row in ac.fetchall()], False

    @staticmethod
    def get_admin_alliances(user_id: int, guild_id: int) -> Tuple[List[Tuple], bool]:
        """
        Get alliance tuples (id, name) for admin.
        Used by most cogs for alliance selection dropdowns.

        Returns:
            (alliances, is_global)
        """
        is_admin, is_global = PermissionManager.is_admin(user_id)

        if not is_admin:
            return [], False

        if is_global:
            # Global admin - return all alliances
            with sqlite3.connect(PermissionManager.ALLIANCE_DB) as db:
                cursor = db.cursor()
                cursor.execute("""
                    SELECT DISTINCT alliance_id, name
                    FROM alliance_list
                    ORDER BY name
                """)
                return cursor.fetchall(), True

        # Server admin - get their allowed alliances
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cursor = db.cursor()
            cursor.execute("SELECT alliances_id FROM adminserver WHERE admin = ?", (user_id,))
            assigned_ids = [row[0] for row in cursor.fetchall()]

        if assigned_ids:
            # Alliance Admin: Has specific assignments - use ONLY those
            with sqlite3.connect(PermissionManager.ALLIANCE_DB) as db:
                cursor = db.cursor()
                placeholders = ','.join('?' * len(assigned_ids))
                cursor.execute(f"""
                    SELECT DISTINCT alliance_id, name
                    FROM alliance_list
                    WHERE alliance_id IN ({placeholders})
                    ORDER BY name
                """, assigned_ids)
                return cursor.fetchall(), False
        else:
            # Server Admin: No assignments - use all alliances on their Discord server
            with sqlite3.connect(PermissionManager.ALLIANCE_DB) as db:
                cursor = db.cursor()
                cursor.execute("""
                    SELECT DISTINCT alliance_id, name
                    FROM alliance_list
                    WHERE discord_server_id = ?
                    ORDER BY name
                """, (guild_id,))
                return cursor.fetchall(), False

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
    def set_alliance_assignments(user_id: int, alliance_ids: List[int]) -> None:
        """Replace an admin's alliance assignments wholesale. Empty list
        leaves them with zero rows in adminserver (i.e. effective Server
        Admin tier). For Globals / Owner this is a no-op data-wise
        because their tier ignores adminserver rows."""
        with sqlite3.connect(PermissionManager.SETTINGS_DB) as db:
            cur = db.cursor()
            cur.execute("DELETE FROM adminserver WHERE admin = ?", (user_id,))
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
