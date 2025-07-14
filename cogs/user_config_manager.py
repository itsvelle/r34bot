import aiosqlite
import logging
import asyncio
from typing import Optional

log = logging.getLogger(__name__)


class UserConfigManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self.db_initialized = False

    async def init_db(self):
        if self.db_initialized:
            return
        async with self._lock:
            if self.db_initialized:
                return
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS user_config (
                            user_id INTEGER PRIMARY KEY,
                            default_ephemeral BOOLEAN,
                            allow_others_to_use_buttons BOOLEAN
                        )
                    """
                    )
                    await conn.commit()
                self.db_initialized = True
                log.info("User config database initialized successfully.")
            except aiosqlite.Error as e:
                log.error(f"User config database error during initialization: {e}")

    async def get_config(self, user_id: int) -> dict:
        await self.init_db()
        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    async with conn.execute(
                        "SELECT default_ephemeral, allow_others_to_use_buttons FROM user_config WHERE user_id=?",
                        (user_id,),
                    ) as cursor:
                        row = await cursor.fetchone()
                if row:
                    return {
                        "default_ephemeral": row[0],
                        "allow_others_to_use_buttons": row[1],
                    }
            except aiosqlite.Error as e:
                log.error(f"Failed to get user config for user '{user_id}': {e}")
        return {}

    async def set_config(
        self,
        user_id: int,
        default_ephemeral: Optional[bool] = None,
        allow_others_to_use_buttons: Optional[bool] = None,
    ):
        await self.init_db()

        # Build the query dynamically based on which values are provided
        updates = []
        params = []
        if default_ephemeral is not None:
            updates.append("default_ephemeral = ?")
            params.append(default_ephemeral)
        if allow_others_to_use_buttons is not None:
            updates.append("allow_others_to_use_buttons = ?")
            params.append(allow_others_to_use_buttons)

        if not updates:
            return

        params.append(user_id)

        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    # First, try to insert a new record. If it fails due to a UNIQUE constraint, then update the existing one.
                    try:
                        await conn.execute(
                            f"INSERT INTO user_config (user_id, {', '.join(k.split(' = ')[0] for k in updates)}) VALUES (?, {', '.join(['?'] * (len(updates)))})",
                            (user_id, *(p for p in params if p is not user_id)),
                        )
                    except aiosqlite.IntegrityError:
                        # The user already has a config, so update it
                        await conn.execute(
                            f"UPDATE user_config SET {', '.join(updates)} WHERE user_id = ?",
                            tuple(params),
                        )
                    await conn.commit()
                log.info(f"User config set for user: {user_id}")
            except aiosqlite.Error as e:
                log.error(f"Failed to set user config for user '{user_id}': {e}")
