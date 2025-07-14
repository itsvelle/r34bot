import aiosqlite
import json
import time
import logging
import asyncio
from typing import Optional, List, Any, Dict, Tuple

log = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, db_path: str, cache_ttl: int = 86400, cache_max_lifetime: int = 2592000):
        """
        Initializes the CacheManager.

        :param db_path: Path to the SQLite database file.
        :param cache_ttl: Time-to-live for cache entries in seconds. Default is 24 hours.
        :param cache_max_lifetime: Max lifetime for cache entries before pruning. Default is 30 days.
        """
        self.db_path = db_path
        self.cache_ttl = cache_ttl
        self.cache_max_lifetime = cache_max_lifetime
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

    @staticmethod
    def _parse_tags_for_lookup(tags_str: str) -> set:
        """Parses a tag string into a set of tags, ignoring exclusions for this purpose."""
        return {tag for tag in tags_str.strip().lower().split() if not tag.startswith('-')}

    async def get(self, tags: str) -> Optional[Tuple[List[Dict[str, Any]], bool, str]]:
        """
        Retrieves the best-matching cached results for a given set of tags.

        It looks for a cached entry whose tags are a subset of the requested tags.
        The best match is the one with the most tags in common.

        :param tags: The tags to look up in the cache.
        :return: A tuple containing:
                    - A list of cached results.
                    - A boolean indicating if the cache is stale.
                    - The tags of the matched cache entry.
                    Returns None if no suitable cache is found.
        """
        await self.init_db()  # Ensure DB is initialized
        requested_tags_set = self._parse_tags_for_lookup(tags)
        
        best_candidate = None
        best_candidate_tags_len = -1

        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    async with conn.execute("SELECT tags, timestamp, results FROM cache") as cursor:
                        all_rows = await cursor.fetchall()

                for row_tags_str, timestamp, results_json in all_rows:
                    candidate_tags_set = self._parse_tags_for_lookup(row_tags_str)
                    
                    if candidate_tags_set.issubset(requested_tags_set):
                        if len(candidate_tags_set) > best_candidate_tags_len:
                            best_candidate = (row_tags_str, timestamp, results_json)
                            best_candidate_tags_len = len(candidate_tags_set)

                if best_candidate:
                    cached_tags_str, timestamp, results_json = best_candidate
                    is_stale = time.time() - timestamp >= self.cache_ttl
                    log.info(f"Cache hit for tags '{tags}' using candidate '{cached_tags_str}'. Stale: {is_stale}")
                    return json.loads(results_json), is_stale, cached_tags_str

            except aiosqlite.Error as e:
                log.error(f"Failed to get cache for tags '{tags}': {e}")
            except json.JSONDecodeError as e:
                log.error(f"Failed to decode JSON for candidate: {e}")

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
        """Removes all very old entries from the cache."""
        await self.init_db()  # Ensure DB is initialized
        async with self._lock:
            try:
                async with aiosqlite.connect(self.db_path) as conn:
                    expiration_time = int(time.time()) - self.cache_max_lifetime
                    await conn.execute("DELETE FROM cache WHERE timestamp < ?", (expiration_time,))
                    await conn.commit()
                    changes = conn.total_changes
                    if changes > 0:
                        log.info(f"Pruned {changes} entries older than {self.cache_max_lifetime / 86400} days from the cache.")
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