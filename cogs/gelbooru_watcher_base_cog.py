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
log.level = logging.DEBUG


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
        self.user_config_manager = bot.user_config_manager
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

        # Hardcoded tag aliases
        self.tag_aliases = {
        }

    def _get_extra_api_params(self) -> dict:
        """
        Returns extra parameters for the API request.
        This can be overridden by subclasses for site-specific parameters.
        """
        return {}

    async def cog_load(self):
        """Handles asynchronous setup when the cog is loaded."""
        log.info(f"Loading {self.cog_name}Cog...")

        # Initialize the database.
        await self.cache_manager.init_db()

        # Create the aiohttp session.
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            log.info(f"aiohttp ClientSession created for {self.cog_name}Cog.")

        # Start the cache pruning loop as a background task.
        self.bot.loop.create_task(self.cache_manager.start_pruning_loop())

        log.info(f"{self.cog_name}Cog loaded and tasks started.")

    async def cog_unload(self):
        """Clean up resources when the cog is unloaded."""
        if self.session and not self.session.closed:
            await self.session.close()
            log.info(f"aiohttp ClientSession closed for {self.cog_name}Cog.")

    @staticmethod
    def _parse_tags(tags_str: str) -> typing.Tuple[set, set]:
        """Parses a tag string into positive and negative tag sets."""
        positive_tags = set()
        negative_tags = set()
        for tag in tags_str.strip().lower().split():
            if tag.startswith("-"):
                negative_tags.add(tag[1:])
            elif tag:
                positive_tags.add(tag)
        return positive_tags, negative_tags

    def _filter_results(
        self, results: list, required_tags: set, excluded_tags: set
    ) -> list:
        """Filters a list of posts based on required and excluded tags."""
        if not required_tags and not excluded_tags:
            return results

        filtered_results = []
        for post in results:
            post_tags = set(post.get("tags", "").split())
            if required_tags.issubset(post_tags) and not excluded_tags.intersection(
                post_tags
            ):
                filtered_results.append(post)
        return filtered_results

    async def _fetch_posts_logic(
        self,
        interaction_or_ctx: typing.Union[discord.Interaction, commands.Context, str],
        tags: str,
        pid_override: typing.Optional[int] = None,
        limit_override: typing.Optional[int] = None,
        hidden: bool = False,
    ) -> typing.Union[str, tuple[str, list], list]:

        # --- 1. Tag Processing ---
        tags_to_process = tags
        if "-ai_generated" not in tags_to_process:
            tags_to_process += " -ai_generated"

        # --- 2. Initial Setup & Defer ---
        if not isinstance(interaction_or_ctx, str) and interaction_or_ctx:
            if self.is_nsfw_site:
                channel = interaction_or_ctx.channel
                is_nsfw_channel = (
                    isinstance(channel, discord.TextChannel) and channel.is_nsfw()
                ) or isinstance(channel, discord.DMChannel)
                allow_in_non_nsfw = "rating:safe" in tags_to_process.lower()
                if not is_nsfw_channel and not allow_in_non_nsfw:
                    return f"This command for {self.cog_name} can only be used in age-restricted (NSFW) channels, DMs, or with the `rating:safe` tag."

            is_interaction = not isinstance(interaction_or_ctx, commands.Context)
            if is_interaction and not interaction_or_ctx.response.is_done():
                await interaction_or_ctx.response.defer(ephemeral=hidden)
            elif hasattr(interaction_or_ctx, "reply"):
                await interaction_or_ctx.reply(
                    f"Fetching data from {self.cog_name}, please wait..."
                )

        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
            log.info(
                f"Recreated aiohttp.ClientSession in _fetch_posts_logic for {self.cog_name}"
            )

        # --- 3. Tag Parsing and Aliasing ---
        original_positive_tags, original_negative_tags = self._parse_tags(
            tags_to_process
        )

        api_tags_set = original_positive_tags.copy()
        for tag in list(api_tags_set):
            if tag in self.tag_aliases:
                api_tags_set.remove(tag)
                api_tags_set.add(self.tag_aliases[tag])

        # Sort for consistent cache keys
        api_tags_str = " ".join(sorted(list(api_tags_set)))

        # --- 3. Cache Check ---
        cache_response = await self.cache_manager.get(api_tags_str)

        source_data = []
        matched_cache_key = api_tags_str

        # --- 4. Fetching Data (Cache, Incremental, or Full) ---
        if cache_response:
            cached_results, is_stale, matched_cache_key = cache_response
            source_data = cached_results

            if is_stale:
                log.info(
                    f"Stale cache hit for '{tags}' using key '{matched_cache_key}'. Performing incremental fetch."
                )
                latest_id = (
                    max(int(p["id"]) for p in cached_results) if cached_results else 0
                )

                newly_fetched_posts = []
                for page in range(10):  # Fetch up to 10 pages of new content
                    api_params = {
                        "page": "dapi",
                        "s": "post",
                        "q": "index",
                        "limit": 1000,
                        "pid": page,
                        "tags": matched_cache_key,
                        "json": 1,
                    }
                    api_params.update(self._get_extra_api_params())
                    log.debug(
                        f"Incremental fetch for {self.cog_name}: URL={self.api_base_url}, Params={api_params}"
                    )
                    try:
                        async with self.session.get(
                            self.api_base_url, params=api_params
                        ) as response:
                            log.debug(
                                f"Incremental fetch response status for {self.cog_name}: {response.status}"
                            )
                            if response.status == 200:
                                data = await response.json()
                                if not data or not isinstance(data, list):
                                    log.debug(
                                        f"Incremental fetch for {self.cog_name}: No data or invalid data format."
                                    )
                                    break

                                page_new_posts = [
                                    post for post in data if int(post["id"]) > latest_id
                                ]
                                newly_fetched_posts.extend(page_new_posts)

                                if len(page_new_posts) < len(
                                    data
                                ):  # We found an overlap
                                    break
                            else:
                                break
                    except Exception as e:
                        log.error(
                            f"Error during incremental fetch for {self.cog_name}: {e}"
                        )
                        break

                if newly_fetched_posts:
                    log.info(
                        f"Found {len(newly_fetched_posts)} new posts for key '{matched_cache_key}'."
                    )
                    # Combine, remove duplicates, and update source_data
                    existing_ids = {p["id"] for p in source_data}
                    unique_new_posts = [
                        p for p in newly_fetched_posts if p["id"] not in existing_ids
                    ]
                    source_data = unique_new_posts + source_data
                    await self.cache_manager.set(matched_cache_key, source_data)
            else:
                log.info(
                    f"Fresh cache hit for '{tags}' using key '{matched_cache_key}'."
                )

        else:  # Cache Miss
            log.info(
                f"Cache miss for '{tags}' (key: '{api_tags_str}'). Performing full fetch."
            )
            all_fetched_results = []
            for page in range(10):  # Fetch up to 10000 results
                api_params = {
                    "page": "dapi",
                    "s": "post",
                    "q": "index",
                    "limit": 1000,
                    "pid": page,
                    "tags": api_tags_str,
                    "json": 1,
                }
                api_params.update(self._get_extra_api_params())
                log.debug(
                    f"Full fetch for {self.cog_name}: URL={self.api_base_url}, Params={api_params}"
                )
                try:
                    async with self.session.get(
                        self.api_base_url, params=api_params
                    ) as response:
                        log.debug(
                            f"Full fetch response status for {self.cog_name}: {response.status}"
                        )
                        if response.status == 200:
                            data = await response.json()
                            if data and isinstance(data, list):
                                log.debug(
                                    f"Full fetch for {self.cog_name}: Received {len(data)} posts."
                                )
                                all_fetched_results.extend(data)
                                if len(data) < 1000:
                                    log.debug(
                                        f"Full fetch for {self.cog_name}: Less than 1000 results, stopping pagination."
                                    )
                                    break
                            else:
                                log.debug(
                                    f"Full fetch for {self.cog_name}: No data or invalid data format."
                                )
                                break
                        else:
                            log.warning(
                                f"Full fetch for {self.cog_name}: Non-200 status code: {response.status}"
                            )
                            if page == 0:
                                return f"Failed to fetch data from {self.cog_name}. HTTP Status: {response.status}"
                            break
                except Exception as e:
                    log.error(f"Error during full fetch for {self.cog_name}: {e}")
                    if page == 0:
                        return f"Network error fetching data from {self.cog_name}: {e}"
                    break

            source_data = all_fetched_results
            if source_data:
                await self.cache_manager.set(api_tags_str, source_data)

        # --- 5. Final Filtering and Response ---
        final_results = self._filter_results(
            source_data, original_positive_tags, original_negative_tags
        )

        if not final_results:
            return f"No results found from {self.cog_name} for the tags: `{tags}`."

        # This part is for internal calls that expect a list
        if pid_override is not None or limit_override is not None:
            return final_results

        random_result = random.choice(final_results)
        post_url = self.post_url_template.format(random_result["id"])
        return (
            f"<{post_url}>\n{random_result['file_url']}",
            final_results,
            tags_to_process,
        )

    class GelbooruButtons(ui.LayoutView):
        container = ui.Container()
        buttons = ui.ActionRow()

        def __init__(
            self,
            cog: "GelbooruWatcherBaseCog",
            tags: str,
            all_results: list,
            original_interaction: discord.Interaction,
            hidden: bool = False,
        ):
            super().__init__(timeout=3600)
            self.cog = cog
            self.tags = tags
            self.all_results = all_results
            self.hidden = hidden
            self.original_interaction_user_id = original_interaction.user.id
            self.current_index = 0

            if self.all_results:
                self._update_container(random.choice(self.all_results))

            # Conditionally add pin button
            if isinstance(original_interaction.channel, discord.DMChannel):
                pin_button = discord.ui.Button(
                    label="Pin",
                    style=discord.ButtonStyle.danger,
                    custom_id=f"{self.cog.cog_name}_pin_message_button",
                )
                pin_button.callback = self.pin_message
                self.buttons.add_item(pin_button)

        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            config = await self.cog.user_config_manager.get_config(
                self.original_interaction_user_id
            )
            allow_others = config.get(
                "allow_others_to_use_buttons", True
            )  # Default to True if not set
            if (
                not allow_others
                and interaction.user.id != self.original_interaction_user_id
            ):
                await interaction.response.send_message(
                    "You are not allowed to use the buttons on this message.",
                    ephemeral=True,
                )
                return False
            return True

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
                self.cog, self.tags, self.all_results, interaction, self.hidden
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
                self.cog,
                self.tags,
                self.all_results,
                interaction,
                self.hidden,
                self.current_index,
            )
            await interaction.response.edit_message(content="", view=view)

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
            original_interaction: discord.Interaction,
            hidden: bool = False,
            current_index: int = 0,
        ):
            super().__init__(timeout=3600)
            self.cog = cog
            self.tags = tags
            self.all_results = all_results
            self.hidden = hidden
            self.original_interaction_user_id = original_interaction.user.id
            self.current_index = current_index

            if self.all_results:
                self._refresh_container()

        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            config = await self.cog.user_config_manager.get_config(
                self.original_interaction_user_id
            )
            allow_others = config.get("allow_others_to_use_buttons", True)
            if (
                not allow_others
                and interaction.user.id != self.original_interaction_user_id
            ):
                await interaction.response.send_message(
                    "You are not allowed to use the buttons on this message.",
                    ephemeral=True,
                )
                return False
            return True

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
                self.cog, self.tags, self.all_results, interaction, self.hidden
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
        self, interaction: discord.Interaction, tags: str, hidden: typing.Optional[bool]
    ):
        user_config = await self.user_config_manager.get_config(interaction.user.id)
        if hidden is None:
            hidden = user_config.get("default_ephemeral", False)

        response = await self._fetch_posts_logic(interaction, tags, hidden=hidden)

        if isinstance(response, tuple):
            _, all_results, processed_tags = response
            view = self.GelbooruButtons(
                self, processed_tags, all_results, interaction, hidden
            )
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
        self, interaction: discord.Interaction, tags: str, hidden: typing.Optional[bool]
    ):
        user_config = await self.user_config_manager.get_config(interaction.user.id)
        if hidden is None:
            hidden = user_config.get("default_ephemeral", False)

        response = await self._fetch_posts_logic(interaction, tags, hidden=hidden)

        if isinstance(response, tuple):
            _, all_results, processed_tags = response
            if not all_results:
                content = f"No results found from {self.cog_name} for the given tags."
                if not interaction.response.is_done():
                    await interaction.response.send_message(content, ephemeral=hidden)
                else:
                    await interaction.followup.send(content, ephemeral=hidden)
                return

            view = self.BrowseView(
                self, processed_tags, all_results, interaction, hidden, current_index=0
            )
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
