import discord
from discord.ext import commands
from discord import app_commands
import logging
import time
import psutil
import subprocess
import os

# Setup logger for this cog
log = logging.getLogger(__name__)


class UtilsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.process = psutil.Process(os.getpid())
        self.process.cpu_percent(interval=None)  # Initialize cpu percent

    def get_git_revision_hash(self) -> str:
        """Gets the short git revision hash of the current repo."""
        try:
            # This assumes the .git directory is at the root of the project.
            # It constructs a path to the .git dir relative to this cog file.
            git_dir = os.path.join(os.path.dirname(__file__), "..", ".git")
            work_tree = os.path.join(os.path.dirname(__file__), "..")
            if not os.path.isdir(git_dir):
                # Fallback for when not running from a git repo or if the structure is different
                return (
                    subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
                    .decode("ascii")
                    .strip()
                )
            return (
                subprocess.check_output(
                    [
                        "git",
                        f"--git-dir={git_dir}",
                        f"--work-tree={work_tree}",
                        "rev-parse",
                        "--short",
                        "HEAD",
                    ]
                )
                .decode("ascii")
                .strip()
            )
        except Exception:
            return "unknown"

    @app_commands.command(name="ping", description="Check the bot's latency.")
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    async def ping(self, interaction: discord.Interaction):
        """Replies with the bot's latency."""
        start_time = time.time()
        # defer() is needed for the API latency calculation to be meaningful
        await interaction.response.defer(ephemeral=True, thinking=True)
        end_time = time.time()

        latency = round(self.bot.latency * 1000)
        api_latency = round((end_time - start_time) * 1000)

        await interaction.followup.send(
            f"Pong! Latency: {latency}ms | API Latency: {api_latency}ms"
        )

    @app_commands.command(
        name="info", description="Get some information about the bot."
    )
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    async def info(self, interaction: discord.Interaction):
        """Shows an embed with bot information."""
        embed = discord.Embed(
            title="Bot Information",
            description="A bot for browsing boorus.",
            color=discord.Color.blue(),
        )

        app_info = await self.bot.application_info()
        creator = app_info.owner

        embed.add_field(name="Version", value=self.get_git_revision_hash(), inline=True)
        embed.add_field(name="Creator", value=str(creator), inline=True)
        embed.add_field(
            name="Library", value=f"discord.py {discord.__version__}", inline=True
        )
        embed.add_field(name="Servers", value=str(len(self.bot.guilds)), inline=True)
        embed.add_field(name="Users", value=str(len(self.bot.users)), inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="debug", description="Show detailed debug information.")
    @app_commands.allowed_contexts(dms=True, guilds=True, private_channels=True)
    async def debug(self, interaction: discord.Interaction):
        """Shows detailed debug info, formatted as requested."""
        await interaction.response.defer(ephemeral=True)

        ping = round(self.bot.latency * 1000)

        # Determine context
        if interaction.guild:
            context_str = "Guild"
        elif isinstance(interaction.channel, discord.DMChannel):
            context_str = "Private Channel"
        else:
            context_str = "Other"

        # Get memory usage
        mem_info = self.process.memory_info()
        mem_used_gb = mem_info.rss / (1024**3)
        mem_total_gb = psutil.virtual_memory().total / (1024**3)

        swap = psutil.swap_memory()
        swap_used_b = swap.used
        swap_total_b = swap.total

        # Get CPU usage
        cpu_percent = self.process.cpu_percent(interval=None)

        debug_info = {
            "Ping": f"{ping}ms",
            "Integration Type": "User",
            "Context": context_str,
            "Shard ID": f"{interaction.client.shard_id if interaction.client.shard_id is not None else 0}",
            "Guild ID": f"{interaction.guild_id if interaction.guild_id else 'None'}",
            "Channel ID": f"{interaction.channel_id}",
            "Author ID": f"{interaction.user.id}",
            "Version": f"{self.get_git_revision_hash()}",
            "CPU": f"{cpu_percent:.0f}%",
            "Memory": f"{mem_used_gb:.1f} GB Used / {mem_total_gb:.1f} GB (Swap: {swap_used_b} B Used / {swap_total_b} B)",
        }

        response_str = "Debug\n"
        response_str += "```\n"
        for key, value in debug_info.items():
            response_str += f"{key}: {value}\n"
        response_str += "```"

        await interaction.followup.send(response_str)


async def setup(bot: commands.Bot):
    try:
        import psutil
    except ImportError:
        log.error(
            "psutil is not installed, so the debug command will not work. Please run `pip install psutil`"
        )
        return  # Do not load cog if psutil is not available
    await bot.add_cog(UtilsCog(bot))
    log.info("UtilsCog added to bot.")
