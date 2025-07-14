import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional

log = logging.getLogger(__name__)


class SettingsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.user_config_manager = bot.user_config_manager

    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    @app_commands.command(
        name="config", description="Configure your personal settings for the bot."
    )
    @app_commands.describe(
        default_ephemeral="Set if the bot's responses to you should be ephemeral by default.",
        allow_others_to_use_buttons="Set if others can use buttons on your command responses.",
    )
    async def config(
        self,
        interaction: discord.Interaction,
        default_ephemeral: Optional[bool] = None,
        allow_others_to_use_buttons: Optional[bool] = None,
    ):
        if default_ephemeral is None and allow_others_to_use_buttons is None:
            # Show current settings
            config = await self.user_config_manager.get_config(interaction.user.id)
            eph = config.get("default_ephemeral", False)
            buttons = config.get("allow_others_to_use_buttons", True)
            await interaction.response.send_message(
                f"Your current settings:\n- Default Ephemeral: {'On' if eph else 'Off'}\n- Allow others to use buttons: {'On' if buttons else 'Off'}",
                ephemeral=True,
            )
            return

        await self.user_config_manager.set_config(
            interaction.user.id, default_ephemeral, allow_others_to_use_buttons
        )

        response_parts = []
        if default_ephemeral is not None:
            response_parts.append(
                f"Default ephemeral responses set to {'On' if default_ephemeral else 'Off'}."
            )
        if allow_others_to_use_buttons is not None:
            response_parts.append(
                f"Allowing others to use buttons set to {'On' if allow_others_to_use_buttons else 'Off'}."
            )

        await interaction.response.send_message(
            "\n".join(response_parts), ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(SettingsCog(bot))
    log.info("SettingsCog added to bot.")
