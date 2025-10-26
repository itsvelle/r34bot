import discord
from discord.ext import commands
from discord import app_commands
import typing  # Need this for Optional
import logging
import os
import json # Added for parsing Gelbooru's JSON response
from .gelbooru_watcher_base_cog import GelbooruWatcherBaseCog

# Setup logger for this cog
log = logging.getLogger(__name__)


class GelbooruCog(GelbooruWatcherBaseCog):
    def __init__(self, bot: commands.Bot):
        super().__init__(
            bot=bot,
            cog_name="Gelbooru",
            api_base_url="https://gelbooru.com/index.php",
            default_tags="hatsune_miku",  # Example default, will be overridden if tags are required
            is_nsfw_site=True,
            command_group_name="gelwatch",  # For potential use in base class messages
            main_command_name="gelbooru",  # For potential use in base class messages
            post_url_template="https://gelbooru.com/index.php?page=post&s=view&id={}",
        )
        self.api_key = os.getenv("GELBOORU_API_KEY")
        self.user_id = os.getenv("GELBOORU_USER_ID")
        # The __init__ in base class handles session creation and task starting.

    def _get_extra_api_params(self) -> dict:
        """Returns Gelbooru specific API parameters."""
        params = {}
        if self.api_key:
            params["api_key"] = self.api_key
        if self.user_id:
            params["user_id"] = self.user_id
        return params

    def _parse_api_response(self, raw_response_text: str) -> list:
        """
        Parses the raw JSON response text from Gelbooru.
        Gelbooru sometimes wraps the list in a dict with a 'post' key.
        """
        data = json.loads(raw_response_text)
        if isinstance(data, dict) and "post" in data:
            return data["post"]
        return data if isinstance(data, list) else []

    # --- Slash Command ---
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    @app_commands.command(
        name="gelbooru",
        description="Get random image from Gelbooru with specified tags",
        nsfw=True,
    )
    @app_commands.describe(
        tags="The tags to search for (e.g., 'hatsune_miku rating:safe')",
        hidden="Set to True to make the response visible only to you (default: False)",
    )
    async def gelbooru_slash(
        self,
        interaction: discord.Interaction,
        tags: str,
        hidden: typing.Optional[bool] = None,
    ):
        """Slash command version of gelbooru."""
        # The _slash_command_logic method from the base cog will handle deferring the interaction.
        await self._slash_command_logic(interaction, tags, hidden)

    # --- New Browse Command ---
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    @app_commands.command(
        name="gelboorubrowse",
        description="Browse Gelbooru results with navigation buttons",
        nsfw=True,
    )
    @app_commands.describe(
        tags="The tags to search for (e.g., 'hatsune_miku rating:safe')",
        hidden="Set to True to make the response visible only to you (default: False)",
    )
    async def gelbooru_browse_slash(
        self,
        interaction: discord.Interaction,
        tags: str,
        hidden: typing.Optional[bool] = None,
    ):
        """Browse Gelbooru results with navigation buttons."""
        # The _browse_slash_command_logic method from the base cog will handle deferring the interaction.
        await self._browse_slash_command_logic(interaction, tags, hidden)


async def setup(bot: commands.Bot):
    await bot.add_cog(GelbooruCog(bot))
    log.info("GelbooruCog added to bot.")