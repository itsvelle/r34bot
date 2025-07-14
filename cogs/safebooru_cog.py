import discord
from discord.ext import commands
from discord import app_commands
import typing  # Need this for Optional
import logging

from .gelbooru_watcher_base_cog import GelbooruWatcherBaseCog

# Setup logger for this cog
log = logging.getLogger(__name__)


class SafebooruCog(GelbooruWatcherBaseCog):
    def __init__(self, bot: commands.Bot):
        super().__init__(
            bot=bot,
            cog_name="Safebooru",
            api_base_url="https://safebooru.org/index.php",
            default_tags="hatsune_miku 1girl",
            is_nsfw_site=False,
            command_group_name="safebooruwatch",
            main_command_name="safebooru",
            post_url_template="https://safebooru.org/index.php?page=post&s=view&id={}",
        )

    # --- Slash Command ---
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    @app_commands.command(
        name="safebooru",
        description="Get random image from Safebooru with specified tags",
    )
    @app_commands.describe(
        tags="The tags to search for (e.g., '1girl cat_ears')",
        hidden="Set to True to make the response visible only to you (default: False)",
    )
    async def safebooru_slash(
        self,
        interaction: discord.Interaction,
        tags: str,
        hidden: typing.Optional[bool]
    ):
        """Slash command version of safebooru."""
        actual_tags = tags or self.default_tags
        await self._slash_command_logic(interaction, actual_tags, hidden)

    # --- New Browse Command ---
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    @app_commands.command(
        name="safeboorubrowse",
        description="Browse Safebooru results with navigation buttons",
    )
    @app_commands.describe(
        tags="The tags to search for (e.g., '1girl dog_ears')",
        hidden="Set to True to make the response visible only to you (default: False)",
    )
    async def safebooru_browse_slash(
        self,
        interaction: discord.Interaction,
        tags: str,
        hidden: typing.Optional[bool],
    ):
        """Browse Safebooru results with navigation buttons."""
        actual_tags = tags or self.default_tags
        await self._browse_slash_command_logic(interaction, actual_tags, hidden)

async def setup(bot: commands.Bot):
    await bot.add_cog(SafebooruCog(bot))
    log.info("SafebooruCog added to bot.")
