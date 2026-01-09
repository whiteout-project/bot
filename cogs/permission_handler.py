"""
Centralized Permission Handler

Permission Tiers:
- Global Admin (is_initial=1): Access to ALL alliances, admin management, bot settings
- Server Admin (is_initial=0, no adminserver entries): All alliances on their Discord server
- Alliance Admin (is_initial=0, has adminserver entries): Only their assigned alliance(s)
"""

import sqlite3
from typing import Tuple, List

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
