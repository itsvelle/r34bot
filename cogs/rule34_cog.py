import discord
from discord.ext import commands
from discord import app_commands
import typing  # Need this for Optional
import logging
from .gelbooru_watcher_base_cog import GelbooruWatcherBaseCog

# Setup logger for this cog
log = logging.getLogger(__name__)


class Rule34Cog(GelbooruWatcherBaseCog):
    def __init__(self, bot: commands.Bot):
        super().__init__(
            bot=bot,
            cog_name="Rule34",
            api_base_url="https://api.rule34.xxx/index.php",
            default_tags="kasane_teto breast_milk",  # Example default, will be overridden if tags are required
            is_nsfw_site=True,
            command_group_name="r34watch",  # For potential use in base class messages
            main_command_name="rule34",  # For potential use in base class messages
            post_url_template="https://rule34.xxx/index.php?page=post&s=view&id={}",
        )
        # The __init__ in base class handles session creation and task starting.

    # --- Slash Command ---
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    @app_commands.command(
        name="rule34", description="Get random image from Rule34 with specified tags", nsfw=True
    )
    @app_commands.describe(
        tags="The tags to search for (e.g., 'hatsune_miku rating:safe')",
        hidden="Set to True to make the response visible only to you (default: False)",
    )
    async def rule34_slash(
        self, interaction: discord.Interaction, tags: str, hidden: typing.Optional[bool]
    ):
        """Slash command version of rule34."""
        # The _slash_command_logic method from the base cog will handle deferring the interaction.
        await self._slash_command_logic(interaction, tags, hidden)

    # --- New Browse Command ---
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    @app_commands.command(
        name="rule34browse", description="Browse Rule34 results with navigation buttons", nsfw=True
    )
    @app_commands.describe(
        tags="The tags to search for (e.g., 'hatsune_miku rating:safe')",
        hidden="Set to True to make the response visible only to you (default: False)",
    )
    async def rule34_browse_slash(
        self, interaction: discord.Interaction, tags: str, hidden: typing.Optional[bool]
    ):
        """Browse Rule34 results with navigation buttons."""
        # The _browse_slash_command_logic method from the base cog will handle deferring the interaction.
        await self._browse_slash_command_logic(interaction, tags, hidden)


async def setup(bot: commands.Bot):
    await bot.add_cog(Rule34Cog(bot))
    log.info("Rule34Cog added to bot.")
