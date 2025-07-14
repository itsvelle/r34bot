import aiosqlite
import json
import time
import logging
import asyncio
from typing import Optional, List, Any, Dict

log = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, db_path: str, cache_ttl: int = 86400):
        """
        Initializes the CacheManager.

        :param db_path: Path to the SQLite database file.
        :param cache_ttl: Time-to-live for cache entries in seconds. Default is 24 hours.
        """
        self.db_path = db_path
        self.cache_ttl = cache_ttl
        self._lock = asyncio.Lock()
        # Defer DB initialization to an async method
        self.db_initialized = False

    async def init_db(self):
        """Initializes the database and creates the cache table if it doesn't exist."""
        if self.db_initialized:
            return
        async with self._lock:
            # Double-check after acquiring the lock
            if self.db_initialized:
                return
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS cache (
                            tags TEXT PRIMARY KEY,
                            timestamp INTEGER,
                            results TEXT
                        )
                    """)
                    await conn.commit()
                self.db_initialized = True
                log.info("Database initialized successfully.")
            except aiosqlite.Error as e:
                log.error(f"Database error during initialization: {e}")

    async def get(self, tags: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieves cached results for a given set of tags if they are not expired.

        :param tags: The tags to look up in the cache.
        :return: A list of cached results, or None if not found or expired.
        """
        await self.init_db()  # Ensure DB is initialized
        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    async with conn.execute("SELECT timestamp, results FROM cache WHERE tags=?", (tags,)) as cursor:
                        row = await cursor.fetchone()

                if row:
                    timestamp, results_json = row
                    if time.time() - timestamp < self.cache_ttl:
                        log.info(f"Cache hit for tags: {tags}")
                        return json.loads(results_json)
                    else:
                        log.info(f"Cache expired for tags: {tags}")
                        return None
            except aiosqlite.Error as e:
                log.error(f"Failed to get cache for tags '{tags}': {e}")
            except json.JSONDecodeError as e:
                log.error(f"Failed to decode JSON for tags '{tags}': {e}")
        return None

    async def set(self, tags: str, results: List[Dict[str, Any]]):
        """
        Saves results to the cache for a given set of tags.

        :param tags: The tags to save the results for.
        :param results: The list of results to cache.
        """
        await self.init_db()  # Ensure DB is initialized
        async with self._lock:
            try:
                results_json = json.dumps(results)
                timestamp = int(time.time())
                async with aiosqlite.connect(self.db_path) as conn:
                    await conn.execute(
                        "INSERT OR REPLACE INTO cache (tags, timestamp, results) VALUES (?, ?, ?)",
                        (tags, timestamp, results_json)
                    )
                    await conn.commit()
                log.info(f"Cache set for tags: {tags}")
            except aiosqlite.Error as e:
                log.error(f"Failed to set cache for tags '{tags}': {e}")

    async def prune_expired(self):
        """Removes all expired entries from the cache."""
        await self.init_db()  # Ensure DB is initialized
        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    expiration_time = int(time.time()) - self.cache_ttl
                    await conn.execute("DELETE FROM cache WHERE timestamp < ?", (expiration_time,))
                    await conn.commit()
                    changes = conn.total_changes
                    if changes > 0:
                        log.info(f"Pruned {changes} expired entries from the cache.")
            except aiosqlite.Error as e:
                log.error(f"Failed to prune expired cache entries: {e}")

    async def start_pruning_loop(self, interval: int = 3600):
        """
        Starts a background task to periodically prune expired cache entries.

        :param interval: The interval in seconds between pruning runs. Default is 1 hour.
        """
        await self.init_db() # Ensure DB is initialized before starting loop
        log.info("Starting background cache pruning loop.")
        while True:
            await asyncio.sleep(interval)
            await self.prune_expired()