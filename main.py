import discord
from discord.ext import commands
import os
import logging
import asyncio
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Bot Configuration ---
# It's recommended to use environment variables for your token.
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

# --- Logging Setup ---
# Configure logging to show timestamps and log levels.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- Bot Definition ---
class SimpleBooruBot(commands.Bot):
    def __init__(self):

        intents = discord.Intents.none() # Don't need intents for slash commands
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        """The setup_hook is called when the bot is ready to start."""
        log.info("Running setup_hook...")
        # Load the cogs.
        initial_extensions = [
            "cogs.rule34_cog",
            "cogs.safebooru_cog",
        ]
        for extension in initial_extensions:
            try:
                await self.load_extension(extension)
                log.info(f"Successfully loaded extension: {extension}")
            except Exception as e:
                log.error(f"Failed to load extension {extension}: {e}")
        try:
            synced = await self.tree.sync()
            log.info(f"Synced {len(synced)} application commands.")
        except Exception as e:
            log.error(f"Failed to sync application commands: {e}")

    async def on_ready(self):
        """Called when the bot is fully connected and ready."""
        log.info(f"Logged in as {self.user} (ID: {self.user.id})")
        log.info("Bot is ready and online!")

# --- Main Execution ---
async def main():
    """The main entry point for the bot."""
    if not BOT_TOKEN:
        log.fatal("FATAL: DISCORD_BOT_TOKEN environment variable not set.")
        return

    bot = SimpleBooruBot()

    # --- Global Error Handling ---
    @bot.tree.error
    async def on_app_command_error(
        interaction: discord.Interaction, error: discord.app_commands.AppCommandError
    ):
        """A global error handler for all slash commands."""
        log.error(f"An error occurred in command '{interaction.command.name if interaction.command else 'unknown'}': {error}", exc_info=True)
        
        # Prepare a user-friendly error message
        if isinstance(error, discord.app_commands.CommandOnCooldown):
            message = f"This command is on cooldown. Please try again in {error.retry_after:.2f} seconds."
        elif isinstance(error, discord.app_commands.MissingPermissions):
            message = "You don't have the required permissions to run this command."
        elif isinstance(error, discord.app_commands.CheckFailure):
            message = "You are not allowed to use this command."
        else:
            # For any other errors, provide a generic message.
            message = "An unexpected error occurred. Please try again later."

        # Send the error message to the user.
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    try:
        await bot.start(BOT_TOKEN)
    except discord.LoginFailure:
        log.error("FATAL: Improper token has been passed. Check your DISCORD_BOT_TOKEN.")
    except Exception as e:
        log.error(f"FATAL: An unexpected error occurred during bot startup: {e}")
    finally:
        if not bot.is_closed():
            await bot.close()
        log.info("Bot has been shut down.")

if __name__ == "__main__":
    asyncio.run(main())