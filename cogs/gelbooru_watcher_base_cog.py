import os
import discord
from discord.ext import commands
from discord import app_commands
from discord import ui
import random
import aiohttp
import time
import json
import typing  # Need this for Optional
import asyncio
import logging  # For logging
import abc  # For Abstract Base Class
from .cache_manager import CacheManager

# Setup logger for this cog
log = logging.getLogger(__name__)


# Combined metaclass to resolve conflicts between CogMeta and ABCMeta
class GelbooruWatcherMeta(commands.CogMeta, abc.ABCMeta):
    pass


class GelbooruWatcherBaseCog(commands.Cog, abc.ABC, metaclass=GelbooruWatcherMeta):
    def __init__(
        self,
        bot: commands.Bot,
        cog_name: str,
        api_base_url: str,
        default_tags: str,
        is_nsfw_site: bool,
        command_group_name: str,
        main_command_name: str,
        post_url_template: str,
    ):
        self.bot = bot
        # Ensure super().__init__() is called for Cog's metaclass features, especially if 'name' was passed to Cog.
        # However, 'name' is handled by the derived classes (Rule34Cog, SafebooruCog)
        # For the base class, we don't pass 'name' to commands.Cog constructor directly.
        # The `name` parameter in `Rule34Cog(..., name="Rule34")` is handled by CogMeta.
        # The base class itself doesn't need a Cog 'name' in the same way.
        # commands.Cog.__init__(self, bot) # This might be needed if Cog's __init__ does setup
        # Let's rely on the derived class's super() call to handle Cog's __init__ properly.

        self.cog_name = cog_name
        self.api_base_url = api_base_url
        self.default_tags = default_tags
        self.is_nsfw_site = is_nsfw_site
        self.command_group_name = command_group_name
        self.main_command_name = main_command_name
        self.post_url_template = post_url_template

        self.session: typing.Optional[aiohttp.ClientSession] = None
        
        # Initialize CacheManager
        db_path = f"{self.cog_name.lower()}_cache.db"
        self.cache_manager = CacheManager(db_path=db_path)

        if bot.is_ready():
            asyncio.create_task(self.initialize_cog_async())
        else:
            asyncio.create_task(self.start_task_when_ready())

    async def initialize_cog_async(self):
        """Asynchronous part of cog initialization."""
        log.info(f"Initializing {self.cog_name}Cog...")
        await self.cache_manager.init_db()
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            log.info(f"aiohttp ClientSession created for {self.cog_name}Cog.")

    async def start_task_when_ready(self):
        """Waits until bot is ready, then initializes and starts tasks."""
        await self.bot.wait_until_ready()
        await self.initialize_cog_async()
        # Start the pruning loop as a background task
        self.bot.loop.create_task(self.cache_manager.start_pruning_loop())

    async def cog_load(self):
        log.info(f"{self.cog_name}Cog loaded.")
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            log.info(
                f"aiohttp ClientSession (re)created during cog_load for {self.cog_name}Cog."
            )

    async def cog_unload(self):
        """Clean up resources when the cog is unloaded."""
        if self.session and not self.session.closed:
            await self.session.close()
            log.info(f"aiohttp ClientSession closed for {self.cog_name}Cog.")


    async def _fetch_posts_logic(
        self,
        interaction_or_ctx: typing.Union[discord.Interaction, commands.Context, str],
        tags: str,
        pid_override: typing.Optional[int] = None,
        limit_override: typing.Optional[int] = None,
        hidden: bool = False,
    ) -> typing.Union[str, tuple[str, list], list]:
        all_results = []
        current_pid = pid_override if pid_override is not None else 0
        # API has a hard limit of 1000 results per request, so we'll use that as our per-page limit
        per_page_limit = 1000
        # If limit_override is provided, use it, otherwise default to 3000 (3 pages of results)
        total_limit = limit_override if limit_override is not None else 100000

        # For internal calls with specific pid/limit, use those exact values
        if pid_override is not None or limit_override is not None:
            use_pagination = False
            api_limit = limit_override if limit_override is not None else per_page_limit
        else:
            use_pagination = True
            api_limit = per_page_limit

        if not isinstance(interaction_or_ctx, str) and interaction_or_ctx:
            if self.is_nsfw_site:
                is_nsfw_channel = False
                channel = interaction_or_ctx.channel
                if isinstance(channel, discord.TextChannel) and channel.is_nsfw():
                    is_nsfw_channel = True
                elif isinstance(channel, discord.DMChannel):
                    is_nsfw_channel = True

                # For Gelbooru-like APIs, 'rating:safe', 'rating:general', 'rating:questionable' might be SFW-ish
                # We'll stick to 'rating:safe' for simplicity as it was in Rule34Cog
                allow_in_non_nsfw = "rating:safe" in tags.lower()

                if not is_nsfw_channel and not allow_in_non_nsfw:
                    return f"This command for {self.cog_name} can only be used in age-restricted (NSFW) channels, DMs, or with the `rating:safe` tag."

            is_interaction = not isinstance(interaction_or_ctx, commands.Context)
            if is_interaction:
                if not interaction_or_ctx.response.is_done():
                    await interaction_or_ctx.response.defer(ephemeral=hidden)
            elif hasattr(interaction_or_ctx, "reply"):  # Prefix command
                await interaction_or_ctx.reply(
                    f"Fetching data from {self.cog_name}, please wait..."
                )

        # Check cache first if not using specific pagination
        if not use_pagination:
            # Skip cache for internal calls with specific pid/limit
            pass
        else:
            cache_key = tags.lower().strip()
            cached_results = await self.cache_manager.get(cache_key)
            if cached_results:
                all_results = cached_results
                random_result = random.choice(all_results)
                # Construct the post URL for the response
                post_url = self.post_url_template.format(random_result["id"])
                return (f"<{post_url}>\n{random_result['file_url']}", all_results)

        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
            log.info(
                f"Recreated aiohttp.ClientSession in _fetch_posts_logic for {self.cog_name}"
            )

        all_results = []

        # If using pagination, we'll make multiple requests
        if use_pagination:
            max_pages = (total_limit + per_page_limit - 1) // per_page_limit
            for page in range(max_pages):
                # Stop if we've reached our total limit or if we got fewer results than the per-page limit
                if len(all_results) >= total_limit or (
                    page > 0 and len(all_results) % per_page_limit != 0
                ):
                    break

                api_params = {
                    "page": "dapi",
                    "s": "post",
                    "q": "index",
                    "limit": per_page_limit,
                    "pid": page,
                    "tags": tags,
                    "json": 1,
                }

                try:
                    async with self.session.get(
                        self.api_base_url, params=api_params
                    ) as response:
                        if response.status == 200:
                            try:
                                data = await response.json()
                            except aiohttp.ContentTypeError:
                                log.warning(
                                    f"{self.cog_name} API returned non-JSON for tags: {tags}, pid: {page}, params: {api_params}"
                                )
                                data = None

                            if data and isinstance(data, list):
                                # If we got fewer results than requested, we've reached the end
                                all_results.extend(data)
                                if len(data) < per_page_limit:
                                    break
                            elif isinstance(data, list) and len(data) == 0:
                                # Empty page, no more results
                                break
                            else:
                                log.warning(
                                    f"Unexpected API response format from {self.cog_name} (not list or empty list): {data} for tags: {tags}, pid: {page}, params: {api_params}"
                                )
                                break
                        else:
                            log.error(
                                f"Failed to fetch {self.cog_name} data. HTTP Status: {response.status} for tags: {tags}, pid: {page}, params: {api_params}"
                            )
                            if page == 0:  # Only return error if first page fails
                                return f"Failed to fetch data from {self.cog_name}. HTTP Status: {response.status}"
                            break
                except aiohttp.ClientError as e:
                    log.error(
                        f"aiohttp.ClientError in _fetch_posts_logic for {self.cog_name} tags {tags}: {e}"
                    )
                    if page == 0:  # Only return error if first page fails
                        return f"Network error fetching data from {self.cog_name}: {e}"
                    break
                except Exception as e:
                    log.exception(
                        f"Unexpected error in _fetch_posts_logic API call for {self.cog_name} tags {tags}: {e}"
                    )
                    if page == 0:  # Only return error if first page fails
                        return f"An unexpected error occurred during {self.cog_name} API call: {e}"
                    break

                # Limit to the total we want
                if len(all_results) > total_limit:
                    all_results = all_results[:total_limit]
                    break
        else:
            # Single request with specific pid/limit
            api_params = {
                "page": "dapi",
                "s": "post",
                "q": "index",
                "limit": api_limit,
                "pid": current_pid,
                "tags": tags,
                "json": 1,
            }

            try:
                async with self.session.get(
                    self.api_base_url, params=api_params
                ) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                        except aiohttp.ContentTypeError:
                            log.warning(
                                f"{self.cog_name} API returned non-JSON for tags: {tags}, pid: {current_pid}, params: {api_params}"
                            )
                            data = None

                        if data and isinstance(data, list):
                            all_results.extend(data)
                        elif isinstance(data, list) and len(data) == 0:
                            pass
                        else:
                            log.warning(
                                f"Unexpected API response format from {self.cog_name} (not list or empty list): {data} for tags: {tags}, pid: {current_pid}, params: {api_params}"
                            )
                            if pid_override is not None or limit_override is not None:
                                return f"Unexpected API response format from {self.cog_name}: {response.status}"
                    else:
                        log.error(
                            f"Failed to fetch {self.cog_name} data. HTTP Status: {response.status} for tags: {tags}, pid: {current_pid}, params: {api_params}"
                        )
                        return f"Failed to fetch data from {self.cog_name}. HTTP Status: {response.status}"
            except aiohttp.ClientError as e:
                log.error(
                    f"aiohttp.ClientError in _fetch_posts_logic for {self.cog_name} tags {tags}: {e}"
                )
                return f"Network error fetching data from {self.cog_name}: {e}"
            except Exception as e:
                log.exception(
                    f"Unexpected error in _fetch_posts_logic API call for {self.cog_name} tags {tags}: {e}"
                )
                return (
                    f"An unexpected error occurred during {self.cog_name} API call: {e}"
                )

        if pid_override is not None or limit_override is not None:
            return all_results

        if all_results:
            cache_key = tags.lower().strip()
            await self.cache_manager.set(cache_key, all_results)

        if not all_results:
            return f"No results found from {self.cog_name} for the given tags."
        else:
            random_result = random.choice(all_results)
            post_url = self.post_url_template.format(random_result["id"])
            return (f"<{post_url}>\n{random_result['file_url']}", all_results)

    class GelbooruButtons(ui.LayoutView):
        container = ui.Container()
        buttons = ui.ActionRow()

        def __init__(
            self,
            cog: "GelbooruWatcherBaseCog",
            tags: str,
            all_results: list,
            hidden: bool = False,
        ):
            super().__init__(timeout=300)
            self.cog = cog
            self.tags = tags
            self.all_results = all_results
            self.hidden = hidden
            self.current_index = 0

            if self.all_results:
                self._update_container(random.choice(self.all_results))

        def _update_container(self, result: dict):
            self.container.clear_items()
            gallery = ui.MediaGallery()
            gallery.add_item(media=result["file_url"])
            self.container.add_item(gallery)
            self.container.add_item(
                ui.TextDisplay(f"{self.cog.cog_name} result for tags `{self.tags}`:")
            )
            post_url = self.cog.post_url_template.format(result["id"])
            self.container.add_item(ui.TextDisplay(post_url))

        @buttons.button(label="New Random", style=discord.ButtonStyle.primary)
        async def new_random(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            random_result = random.choice(self.all_results)
            self._update_container(random_result)
            await interaction.response.edit_message(content="", view=self)

        @buttons.button(
            label="Random In New Message", style=discord.ButtonStyle.success
        )
        async def new_message(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            random_result = random.choice(self.all_results)
            new_view = self.cog.GelbooruButtons(
                self.cog, self.tags, self.all_results, self.hidden
            )
            new_view._update_container(random_result)
            await interaction.response.send_message(
                content="", view=new_view, ephemeral=self.hidden
            )

        @buttons.button(label="Browse Results", style=discord.ButtonStyle.secondary)
        async def browse_results(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            if not self.all_results:
                await interaction.response.send_message(
                    "No results to browse.", ephemeral=True
                )
                return
            self.current_index = 0
            view = self.cog.BrowseView(
                self.cog, self.tags, self.all_results, self.hidden, self.current_index
            )
            await interaction.response.edit_message(content="", view=view)

        @buttons.button(label="Pin", style=discord.ButtonStyle.danger)
        async def pin_message(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            if interaction.message:
                try:
                    await interaction.message.pin()
                    await interaction.response.send_message(
                        "Message pinned successfully!", ephemeral=True
                    )
                except discord.Forbidden:
                    await interaction.response.send_message(
                        "I don't have permission to pin messages.", ephemeral=True
                    )
                except discord.HTTPException as e:
                    await interaction.response.send_message(
                        f"Failed to pin: {e}", ephemeral=True
                    )

    class BrowseView(ui.LayoutView):
        container = ui.Container()
        nav_row = ui.ActionRow()
        extra_row = ui.ActionRow()

        def __init__(
            self,
            cog: "GelbooruWatcherBaseCog",
            tags: str,
            all_results: list,
            hidden: bool = False,
            current_index: int = 0,
        ):
            super().__init__(timeout=300)
            self.cog = cog
            self.tags = tags
            self.all_results = all_results
            self.hidden = hidden
            self.current_index = current_index

            if self.all_results:
                self._refresh_container()

        def _refresh_container(self):
            self.container.clear_items()
            result = self.all_results[self.current_index]
            gallery = ui.MediaGallery()
            gallery.add_item(media=result["file_url"])
            self.container.add_item(gallery)
            idx_label = f"Result {self.current_index + 1}/{len(self.all_results)} for tags `{self.tags}`:"
            self.container.add_item(ui.TextDisplay(idx_label))
            post_url = self.cog.post_url_template.format(result["id"])
            self.container.add_item(ui.TextDisplay(post_url))

        async def _update_message(self, interaction: discord.Interaction):
            self._refresh_container()
            await interaction.response.edit_message(content="", view=self)

        @nav_row.button(label="First", style=discord.ButtonStyle.secondary, emoji="⏪")
        async def first(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            self.current_index = 0
            await self._update_message(interaction)

        @nav_row.button(
            label="Previous", style=discord.ButtonStyle.secondary, emoji="◀️"
        )
        async def previous(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            self.current_index = (self.current_index - 1 + len(self.all_results)) % len(
                self.all_results
            )
            await self._update_message(interaction)

        @nav_row.button(label="Next", style=discord.ButtonStyle.primary, emoji="▶️")
        async def next_result(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            self.current_index = (self.current_index + 1) % len(self.all_results)
            await self._update_message(interaction)

        @nav_row.button(label="Last", style=discord.ButtonStyle.secondary, emoji="⏩")
        async def last(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            self.current_index = len(self.all_results) - 1
            await self._update_message(interaction)

        @extra_row.button(label="Go To", style=discord.ButtonStyle.primary)
        async def goto(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            modal = self.cog.GoToModal(len(self.all_results))
            await interaction.response.send_modal(modal)
            await modal.wait()
            if modal.value is not None:
                self.current_index = modal.value - 1
                await interaction.followup.edit_message(
                    interaction.message.id, content="", view=self
                )

        @extra_row.button(
            label="Back to Main Controls", style=discord.ButtonStyle.danger
        )
        async def back_to_main(
            self, interaction: discord.Interaction, button: discord.ui.Button
        ):
            # Send a random one from all_results as the content
            if not self.all_results:  # Should not happen if browse was initiated
                await interaction.response.edit_message(
                    content="No results available.", view=None
                )
                return
            random_result = random.choice(self.all_results)
            view = self.cog.GelbooruButtons(
                self.cog, self.tags, self.all_results, self.hidden
            )
            view._update_container(random_result)
            await interaction.response.edit_message(content="", view=view)

    class GoToModal(discord.ui.Modal):
        def __init__(self, max_pages: int):
            super().__init__(title="Go To Page")
            self.value = None
            self.max_pages = max_pages
            self.page_num = discord.ui.TextInput(
                label=f"Page Number (1-{max_pages})",
                placeholder=f"Enter a number between 1 and {max_pages}",
                min_length=1,
                max_length=len(str(max_pages)),
            )
            self.add_item(self.page_num)

        async def on_submit(self, interaction: discord.Interaction):
            try:
                num = int(self.page_num.value)
                if 1 <= num <= self.max_pages:
                    self.value = num
                    await interaction.response.defer()  # Defer here, followup in BrowseView.goto
                else:
                    await interaction.response.send_message(
                        f"Please enter a number between 1 and {self.max_pages}",
                        ephemeral=True,
                    )
            except ValueError:
                await interaction.response.send_message(
                    "Please enter a valid number", ephemeral=True
                )



    async def _slash_command_logic(
        self, interaction: discord.Interaction, tags: str, hidden: bool
    ):
        response = await self._fetch_posts_logic(interaction, tags, hidden=hidden)

        if isinstance(response, tuple):
            _, all_results = response
            view = self.GelbooruButtons(self, tags, all_results, hidden)
            if interaction.response.is_done():
                await interaction.followup.send(content="", view=view, ephemeral=hidden)
            else:
                await interaction.response.send_message(
                    content="", view=view, ephemeral=hidden
                )
        elif isinstance(response, str):  # Error
            ephemeral_error = hidden
            if self.is_nsfw_site and response.startswith(
                f"This command for {self.cog_name} can only be used"
            ):
                ephemeral_error = (
                    True  # Always make NSFW warnings ephemeral if possible
                )

            if not interaction.response.is_done():
                await interaction.response.send_message(
                    response, ephemeral=ephemeral_error
                )
            else:
                try:
                    await interaction.followup.send(response, ephemeral=ephemeral_error)
                except discord.HTTPException as e:
                    log.error(
                        f"{self.cog_name} slash command: Failed to send error followup for tags '{tags}': {e}"
                    )

    async def _browse_slash_command_logic(
        self, interaction: discord.Interaction, tags: str, hidden: bool
    ):
        response = await self._fetch_posts_logic(interaction, tags, hidden=hidden)

        if isinstance(response, tuple):
            _, all_results = response
            if not all_results:
                content = f"No results found from {self.cog_name} for the given tags."
                if not interaction.response.is_done():
                    await interaction.response.send_message(content, ephemeral=hidden)
                else:
                    await interaction.followup.send(content, ephemeral=hidden)
                return

            view = self.BrowseView(self, tags, all_results, hidden, current_index=0)
            if interaction.response.is_done():
                await interaction.followup.send(content="", view=view, ephemeral=hidden)
            else:
                await interaction.response.send_message(
                    content="", view=view, ephemeral=hidden
                )
        elif isinstance(response, str):  # Error
            ephemeral_error = hidden
            if self.is_nsfw_site and response.startswith(
                f"This command for {self.cog_name} can only be used"
            ):
                ephemeral_error = True
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    response, ephemeral=ephemeral_error
                )
            else:
                await interaction.followup.send(response, ephemeral=ephemeral_error)


