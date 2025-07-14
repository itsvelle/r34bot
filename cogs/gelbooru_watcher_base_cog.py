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
            # Vocaloid
            "hatsune_miku": "vocaloid",
            "kagamine_rin": "vocaloid",
            "kagamine_len": "vocaloid",
            "megurine_luka": "vocaloid",
            "meiko_(vocaloid)": "vocaloid",
            "kaiko_(vocaloid)": "vocaloid",
            "kaito_(vocaloid)": "vocaloid",
            "gumi_(vocaloid)": "vocaloid",
            "gakupo_kamui": "vocaloid",
            "gakupo_(vocaloid)": "vocaloid",
            "ia_(vocaloid)": "vocaloid",
            "lily_(vocaloid)": "vocaloid",
            "seeu_(vocaloid)": "vocaloid",
            "yuzuki_yukari": "vocaloid",
            "vflower": "vocaloid",
            "v4_flower": "vocaloid",
            "miki_(vocaloid)": "vocaloid",
            "sf-a2_miki": "vocaloid",
            "kokone_(vocaloid)": "vocaloid",
            "mayu_(vocaloid)": "vocaloid",
            "zola_project": "vocaloid",
            "tone_rion": "vocaloid",
            "galaco": "vocaloid",
            "anon_(vocaloid)": "vocaloid",
            "kanon_(vocaloid)": "vocaloid",
            "fukase_(vocaloid)": "vocaloid",
            "cyber_diva": "vocaloid",
            "cyber_songman": "vocaloid",
            "una_(vocaloid)": "vocaloid",
            "otomachi_una": "vocaloid",
            "kokone": "vocaloid",
            "mew_(vocaloid)": "vocaloid",
            "v_yuma": "vocaloid",
            "v_yuuma": "vocaloid",
            "v_yuuma_(vocaloid)": "vocaloid",
            "v_yuma_(vocaloid)": "vocaloid",
            "dex_(vocaloid)": "vocaloid",
            "daina_(vocaloid)": "vocaloid",

            # UTAU
            "kasane_teto": "utau",
            "defoko": "utau",
            "utane_uta": "utau",
            "momone_momo": "utau",
            "yamine_renri": "utau",
            "matsudappoiyo": "utau",
            "namine_ritsu": "utau",
            "makune_hachi": "utau",
            "yufu_sekka": "utau",
            "sukone_tei": "utau",
            "momone_momo_(utau)": "utau",
            "yamine_renri_(utau)": "utau",
            "momo_momone": "utau",
            "kikyuune_aisen": "utau",
            "kikyuune_aisen_(utau)": "utau",
            "yukari_yuzuki_(utau)": "utau",
            "kikyuune_aisen": "utau",
            "sukone_tei_(utau)": "utau",
            "makune_hachi_(utau)": "utau",
            "namine_ritsu_(utau)": "utau",
            "sekka_yufu": "utau",
            "sekka_yufu_(utau)": "utau",

            # Zenless Zone Zero
            "anby_demara": "zenless_zone_zero",
            "anton_ivanov": "zenless_zone_zero",
            "asaba_harumasa": "zenless_zone_zero",
            "astra_yao": "zenless_zone_zero",
            "belle_(zenless_zone_zero)": "zenless_zone_zero",
            "ben_bigger": "zenless_zone_zero",
            "billy_kid": "zenless_zone_zero",
            "burnice_white": "zenless_zone_zero",
            "caesar_king": "zenless_zone_zero",
            "corin_wickes": "zenless_zone_zero",
            "ellen_joe": "zenless_zone_zero",
            "evelyn_chevalier": "zenless_zone_zero",
            "grace_howard": "zenless_zone_zero",
            "hoshimi_miyabi": "zenless_zone_zero",
            "hugo_vlad": "zenless_zone_zero",
            "jane_doe_(zenless_zone_zero)": "zenless_zone_zero",
            "ju_fufu": "zenless_zone_zero",
            "koleda_belobog": "zenless_zone_zero",
            "lighter_(zenless_zone_zero)": "zenless_zone_zero",
            "luciana_de_montefio": "zenless_zone_zero",
            "lucy_(zenless_zone_zero)": "zenless_zone_zero",
            "nekomiya_mana": "zenless_zone_zero",
            "nicole_demara": "zenless_zone_zero",
            "pan_yinhu": "zenless_zone_zero",
            "piper_wheel": "zenless_zone_zero",
            "pulchra_fellini": "zenless_zone_zero",
            "qingyi_(zenless_zone_zero)": "zenless_zone_zero",
            "rina_(zenless_zone_zero)": "zenless_zone_zero",
            "seth_lowell": "zenless_zone_zero",
            "soldier_11_(zenless_zone_zero)": "zenless_zone_zero",
            "soukaku": "zenless_zone_zero",
            "soukaku_(zenless_zone_zero)": "zenless_zone_zero",
            "trigger_(zenless_zone_zero)": "zenless_zone_zero",
            "tsukishiro_yanagi": "zenless_zone_zero",
            "vivian_(zenless_zone_zero)": "zenless_zone_zero",
            "vivian_banshee": "zenless_zone_zero",
            "von_lycaon": "zenless_zone_zero",
            "wise_(zenless_zone_zero)": "zenless_zone_zero",
            "yixuan_(zenless_zone_zero)": "zenless_zone_zero",
            "zhu_yuan": "zenless_zone_zero",
        }

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
            if tag.startswith('-'):
                negative_tags.add(tag[1:])
            elif tag:
                positive_tags.add(tag)
        return positive_tags, negative_tags

    def _filter_results(self, results: list, required_tags: set, excluded_tags: set) -> list:
        """Filters a list of posts based on required and excluded tags."""
        if not required_tags and not excluded_tags:
            return results

        filtered_results = []
        for post in results:
            post_tags = set(post.get("tags", "").split())
            if required_tags.issubset(post_tags) and not excluded_tags.intersection(post_tags):
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
        
        # --- 1. Initial Setup & Defer ---
        if not isinstance(interaction_or_ctx, str) and interaction_or_ctx:
            if self.is_nsfw_site:
                channel = interaction_or_ctx.channel
                is_nsfw_channel = (isinstance(channel, discord.TextChannel) and channel.is_nsfw()) or \
                                  isinstance(channel, discord.DMChannel)
                allow_in_non_nsfw = "rating:safe" in tags.lower()
                if not is_nsfw_channel and not allow_in_non_nsfw:
                    return f"This command for {self.cog_name} can only be used in age-restricted (NSFW) channels, DMs, or with the `rating:safe` tag."

            is_interaction = not isinstance(interaction_or_ctx, commands.Context)
            if is_interaction and not interaction_or_ctx.response.is_done():
                await interaction_or_ctx.response.defer(ephemeral=hidden)
            elif hasattr(interaction_or_ctx, "reply"):
                await interaction_or_ctx.reply(f"Fetching data from {self.cog_name}, please wait...")

        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
            log.info(f"Recreated aiohttp.ClientSession in _fetch_posts_logic for {self.cog_name}")

        # --- 2. Tag Parsing and Aliasing ---
        original_positive_tags, original_negative_tags = self._parse_tags(tags)
        
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
                log.info(f"Stale cache hit for '{tags}' using key '{matched_cache_key}'. Performing incremental fetch.")
                latest_id = max(int(p['id']) for p in cached_results) if cached_results else 0
                
                newly_fetched_posts = []
                for page in range(10): # Fetch up to 10 pages of new content
                    api_params = {"page": "dapi", "s": "post", "q": "index", "limit": 1000, "pid": page, "tags": matched_cache_key, "json": 1}
                    try:
                        async with self.session.get(self.api_base_url, params=api_params) as response:
                            if response.status == 200:
                                data = await response.json()
                                if not data or not isinstance(data, list): break
                                
                                page_new_posts = [post for post in data if int(post['id']) > latest_id]
                                newly_fetched_posts.extend(page_new_posts)
                                
                                if len(page_new_posts) < len(data): # We found an overlap
                                    break
                            else: break
                    except Exception as e:
                        log.error(f"Error during incremental fetch for {self.cog_name}: {e}")
                        break
                
                if newly_fetched_posts:
                    log.info(f"Found {len(newly_fetched_posts)} new posts for key '{matched_cache_key}'.")
                    # Combine, remove duplicates, and update source_data
                    existing_ids = {p['id'] for p in source_data}
                    unique_new_posts = [p for p in newly_fetched_posts if p['id'] not in existing_ids]
                    source_data = unique_new_posts + source_data
                    await self.cache_manager.set(matched_cache_key, source_data)
            else:
                log.info(f"Fresh cache hit for '{tags}' using key '{matched_cache_key}'.")

        else: # Cache Miss
            log.info(f"Cache miss for '{tags}' (key: '{api_tags_str}'). Performing full fetch.")
            all_fetched_results = []
            for page in range(10): # Fetch up to 10000 results
                api_params = {"page": "dapi", "s": "post", "q": "index", "limit": 1000, "pid": page, "tags": api_tags_str, "json": 1}
                try:
                    async with self.session.get(self.api_base_url, params=api_params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data and isinstance(data, list):
                                all_fetched_results.extend(data)
                                if len(data) < 1000: break
                            else: break
                        else:
                            if page == 0: return f"Failed to fetch data from {self.cog_name}. HTTP Status: {response.status}"
                            break
                except Exception as e:
                    log.error(f"Error during full fetch for {self.cog_name}: {e}")
                    if page == 0: return f"Network error fetching data from {self.cog_name}: {e}"
                    break
            
            source_data = all_fetched_results
            if source_data:
                await self.cache_manager.set(api_tags_str, source_data)

        # --- 5. Final Filtering and Response ---
        final_results = self._filter_results(source_data, original_positive_tags, original_negative_tags)

        if not final_results:
            return f"No results found from {self.cog_name} for the tags: `{tags}`."
        
        # This part is for internal calls that expect a list
        if pid_override is not None or limit_override is not None:
            return final_results

        random_result = random.choice(final_results)
        post_url = self.post_url_template.format(random_result["id"])
        return (f"<{post_url}>\n{random_result['file_url']}", final_results)


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
            super().__init__(timeout=3600)
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
            super().__init__(timeout=3600)
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
