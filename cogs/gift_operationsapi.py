"""
Gift code distribution API client. Syncs codes with the shared gift-code-api service.
"""
import json
import aiohttp
import asyncio
import sqlite3
import re
import random
from datetime import datetime
import discord
import ssl
import logging
from .pimp_my_bot import theme
from .browser_headers import get_headers

logger = logging.getLogger('gift')

class GiftCodeAPI:
    def __init__(self, bot):
        self.bot = bot
        self.api_url = "http://gift-code-api.whiteout-bot.com/giftcode_api.php"
        self.api_key = "super_secret_bot_token_nobody_will_ever_find"
        
        # Random 5-10min check interval to help reduce API load
        self.min_check_interval = 300
        self.max_check_interval = 600
        self.check_interval = random.randint(self.min_check_interval, self.max_check_interval)
        
        # Rate limiting controls
        self.last_api_call = 0
        self.min_api_call_interval = 3
        self.error_backoff_time = 30
        self.cloudflare_backoff_time = 15
        self.max_backoff_time = 300
        self.current_backoff = self.error_backoff_time
        
        if hasattr(bot, 'conn'):
            self.conn = bot.conn
            self.cursor = self.conn.cursor()
            self._owns_conn = False
        else:
            self.conn = sqlite3.connect('db/giftcode.sqlite', timeout=30.0, check_same_thread=False)
            self.cursor = self.conn.cursor()
            self._owns_conn = True

        self.settings_conn = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.settings_cursor = self.settings_conn.cursor()

        self.users_conn = sqlite3.connect('db/users.sqlite', timeout=30.0, check_same_thread=False)
        self.users_cursor = self.users_conn.cursor()
        
        # Configure SQLite for better concurrent access, avoid DB locks
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=10000")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.commit()
        
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        self.logger = logging.getLogger('gift')

        self._api_check_task = asyncio.create_task(self.start_api_check())

    async def _execute_with_retry(self, operation, *args, max_retries=3, delay=0.1):
        """Execute a database operation with retry logic for handling locks."""
        for attempt in range(max_retries):
            try:
                return operation(*args)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    self.logger.warning(f"Database locked, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    raise

    async def _safe_commit(self, conn, operation_name="operation"):
        """Safely commit database changes with retry logic."""
        try:
            await self._execute_with_retry(conn.commit)
        except sqlite3.OperationalError as e:
            self.logger.error(f"Failed to commit {operation_name}: {e}")
            raise

    async def start_api_check(self):
        """Start periodic API synchronization with exponential backoff on failures."""
        try:
            await asyncio.sleep(60)
            
            while True:
                try:
                    success = await self.sync_with_api()
                    
                    if success: # Reset backoff on success
                        self.current_backoff = self.error_backoff_time
                        self.check_interval = random.randint(self.min_check_interval, self.max_check_interval)
                        await asyncio.sleep(self.check_interval)
                    else: # Added jitter on failure to prevent thundering herd
                        jitter = random.uniform(0.75, 1.25)
                        backoff_time = min(self.current_backoff * jitter, self.max_backoff_time)
                        self.logger.warning(f"API sync failed, backing off for {backoff_time:.1f} seconds")
                        await asyncio.sleep(backoff_time)
                        self.current_backoff = min(self.current_backoff * 2, self.max_backoff_time)
                
                except Exception as e:
                    self.logger.exception(f"Error in API check loop: {e}")
                    sleep_time = min(self.current_backoff * random.uniform(0.75, 1.25), self.max_backoff_time)
                    await asyncio.sleep(sleep_time)
                    self.current_backoff = min(self.current_backoff * 2, self.max_backoff_time)
                    
        except Exception as e:
            self.logger.exception(f"Fatal error in API check loop: {e}")

    async def cog_unload(self):
        """Clean up database connections and the API check task when unloaded."""
        task = getattr(self, '_api_check_task', None)
        if task is not None and not task.done():
            task.cancel()
        try:
            if getattr(self, '_owns_conn', True):
                self.conn.close()
            self.settings_conn.close()
            self.users_conn.close()
        except Exception:
            pass
    
    async def _wait_for_rate_limit(self):
        """Enforce rate limiting between API calls."""
        now = datetime.now().timestamp()
        time_since_last_call = now - self.last_api_call
        
        if time_since_last_call < self.min_api_call_interval:
            sleep_time = self.min_api_call_interval - time_since_last_call
            sleep_time += random.uniform(0, 0.5)
            await asyncio.sleep(sleep_time)
            
        self.last_api_call = datetime.now().timestamp()
    
    async def _handle_api_error(self, response, response_text):
        """Handle API errors with appropriate backoff strategies."""
        if response.status == 429 or response.status == 1015: # Rate limit triggered - standard backoff
            self.logger.warning(f"Rate limit triggered: {response.status}")
            backoff = max(self.cloudflare_backoff_time, self.current_backoff)
            backoff *= random.uniform(1.0, 1.5)
            self.current_backoff = min(self.current_backoff * 2, self.max_backoff_time)
            return backoff
        elif response.status in [502, 503, 504]: # Server errors - back off with increasing delay
            self.logger.warning(f"Server error: {response.status}")
            backoff = self.current_backoff * random.uniform(0.75, 1.25)
            self.current_backoff = min(self.current_backoff * 2, self.max_backoff_time)
            return backoff
        elif response.status == 400 and "previously marked invalid" in response_text:
            # Expected behavior for expired/invalid codes - log at debug level
            self.logger.info(f"Code already marked invalid on API: {response_text[:100]}")
            return 0  # No backoff needed for expected invalid codes
        else: # Other errors - standard backoff
            self.logger.error(f"API error: {response.status}, {response_text[:200]}")
            return self.current_backoff * random.uniform(0.75, 1.25)
                
    async def sync_with_api(self):
        """Synchronize gift codes with the API."""
        try:
            self.logger.info("Starting API synchronization")
            self.cursor.execute("SELECT giftcode, date, validation_status FROM gift_codes")
            db_codes = {row[0]: (row[1], row[2]) for row in self.cursor.fetchall()}
            
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
                headers = get_headers()
                headers['Content-Type'] = 'application/json'
                headers['X-API-Key'] = self.api_key
                
                await self._wait_for_rate_limit()
                
                try:
                    async with session.get(self.api_url, headers=headers) as response:
                        response_text = await response.text()
                        
                        if response.status != 200:
                            backoff_time = await self._handle_api_error(response, response_text)
                            self.logger.warning(f"API request failed, backing off for {backoff_time:.1f} seconds")
                            await asyncio.sleep(backoff_time)
                            return False
                        
                        try:
                            result = json.loads(response_text)
                            if 'error' in result or 'detail' in result:
                                error_msg = result.get('error', result.get('detail', 'Unknown error'))
                                self.logger.error(f"API returned error: {error_msg}")
                                return False
                            
                            api_giftcodes = result.get('codes', [])
                            self.logger.info(f"Received {len(api_giftcodes)} codes from API")
                            
                            valid_codes = []
                            invalid_codes = []
                            for code_line in api_giftcodes:
                                parts = code_line.strip().split()
                                if len(parts) != 2:
                                    invalid_codes.append(code_line)
                                    continue
                                    
                                code, date_str = parts
                                if not re.match("^[a-zA-Z0-9]+$", code):
                                    invalid_codes.append(code_line)
                                    continue
                                    
                                try:
                                    date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                                    valid_codes.append((code, date_obj))
                                except ValueError:
                                    invalid_codes.append(code_line)
                                    continue
                            
                            if invalid_codes: # Report invalid codes for cleanup
                                self.logger.warning(f"Found {len(invalid_codes)} invalid code formats from API")
                                
                                for invalid_code in invalid_codes:
                                    try:
                                        code = invalid_code.split()[0] if ' ' in invalid_code else invalid_code.strip()
                                        data = {'code': code}
                                        
                                        await self._wait_for_rate_limit()
                                        
                                        async with session.delete(self.api_url, json=data, headers=headers) as del_response:
                                            if del_response.status != 200:
                                                self.logger.warning(f"Failed to delete invalid code {code}: {del_response.status}")
                                                backoff_time = await self._handle_api_error(del_response, await del_response.text())
                                                await asyncio.sleep(backoff_time)
                                            else:
                                                self.logger.info(f"Successfully deleted invalid code format: {code}")
                                        
                                    except Exception as e:
                                        self.logger.exception(f"Error deleting invalid code {invalid_code}: {e}")

                            new_codes = []
                            for code, date_obj in valid_codes:
                                formatted_date = date_obj.strftime("%Y-%m-%d")
                                prior = db_codes.get(code)
                                if prior is None:
                                    try:
                                        # First add as pending
                                        self.cursor.execute(
                                            "INSERT OR IGNORE INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)",
                                            (code, formatted_date, "pending")
                                        )
                                        new_codes.append((code, formatted_date, False))
                                    except Exception as e:
                                        self.logger.exception(f"Error inserting new code {code}: {e}")
                                elif prior[1] == 'invalid':
                                    # API still distributing a code we hold invalid. Don't trust it -
                                    # re-validate ourselves; only our own check can confirm a reactivation.
                                    new_codes.append((code, formatted_date, True))

                            try:
                                await self._safe_commit(self.conn, "new codes insertion")

                                if new_codes: # Notify and process new codes
                                    self.logger.info(f"Added {len(new_codes)} new codes from API - validating...")
                                    
                                    # Validate new codes immediately
                                    valid_codes_count = 0
                                    invalid_codes_count = 0
                                    
                                    for code, formatted_date, was_invalid in new_codes:
                                        try:
                                            # Get GiftOperations cog to validate
                                            gift_operations = self.bot.get_cog('GiftOperations')
                                            if gift_operations:
                                                is_valid, validation_msg = await gift_operations.validate_gift_code_immediately(code, "api", force=was_invalid)

                                                if is_valid is None:
                                                    # Inconclusive (usually the FID's captcha cooldown). Don't rapid-retry
                                                    # here - that just sustains the throttle. The serialized, spaced
                                                    # re-validation chain below picks it up cleanly.
                                                    self.logger.info(f"API code '{code}' not conclusively validated yet ({validation_msg}); scheduling spaced re-validation.")

                                                if is_valid:
                                                    valid_codes_count += 1
                                                    self.logger.info(f"API code '{code}' validated successfully")

                                                    # A code we held invalid that our OWN re-validation now confirms valid
                                                    # is a genuine reactivation: clear old redemptions so members can claim again.
                                                    cleared_redemptions = 0
                                                    if was_invalid:
                                                        try:
                                                            self.cursor.execute("SELECT COUNT(*) FROM user_giftcodes WHERE giftcode = ?", (code,))
                                                            count_row = self.cursor.fetchone()
                                                            cleared_redemptions = count_row[0] if count_row else 0
                                                            self.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ?", (code,))
                                                            await self._safe_commit(self.conn, f"clear redemption history for reactivated code {code}")
                                                            self.logger.info(f"🔄 REACTIVATION: '{code}' re-validated valid; cleared {cleared_redemptions} old redemption records")
                                                        except Exception as e:
                                                            self.logger.error(f"Error clearing reactivation history for code '{code}': {e}")

                                                    validation_status = f"✅ Validated (🔄 Reactivated - {cleared_redemptions} cleared)" if was_invalid else "✅ Validated"

                                                    try:
                                                        await self._execute_with_retry(
                                                            lambda: self.cursor.execute("SELECT alliance_id FROM giftcodecontrol WHERE status = 1 ORDER BY priority ASC, alliance_id ASC")
                                                        )
                                                        auto_alliances = self.cursor.fetchall() or []
                                                    except sqlite3.OperationalError as e:
                                                        error_msg = f"Auto-alliance query failed after retries for code '{code}': {e}"
                                                        self.logger.error(error_msg)
                                                        print(f"ERROR: {error_msg}")
                                                        auto_alliances = []
                                                    except Exception as e:
                                                        error_msg = f"Unexpected error in auto-alliance query for code '{code}': {e}"
                                                        self.logger.error(error_msg)
                                                        print(f"ERROR: {error_msg}")
                                                        auto_alliances = []
                                                elif is_valid is False:
                                                    invalid_codes_count += 1
                                                    self.logger.warning(f"API code '{code}' is invalid: {validation_msg}")
                                                    validation_status = f"{theme.deniedIcon} Invalid: {validation_msg}"
                                                    auto_alliances = []
                                                else:
                                                    self.logger.warning(f"API code '{code}' validation inconclusive after retries: {validation_msg}")
                                                    validation_status = f"{theme.warnIcon} Validating"
                                                    auto_alliances = []
                                                    # Re-test soon instead of waiting for the 2h periodic loop.
                                                    gift_operations.schedule_revalidation(code, "api")
                                            else:
                                                self.logger.error("GiftOperations cog not found for validation!")
                                                validation_status = f"{theme.deniedIcon} Error"
                                                auto_alliances = []

                                            # A code we already held invalid that didn't re-validate as valid isn't
                                            # news (avoids re-notifying every sync when the API keeps distributing it).
                                            notify = (not was_invalid) or (is_valid is True)

                                            self.settings_cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
                                            admin_ids = self.settings_cursor.fetchall()
                                            if notify and admin_ids:
                                                embed_description = (
                                                    f"**Gift Code Details**\n"
                                                    f"{theme.upperDivider}\n"
                                                    f"{theme.giftIcon} **Code:** `{code}`\n"
                                                    f"{theme.calendarIcon} **Date:** `{formatted_date}`\n"
                                                    f"{theme.listIcon} **Validation Status:** `{validation_status}`\n"
                                                    f"{theme.linkIcon} **Source:** `Retrieved from Bot API`\n"
                                                    f"{theme.alarmClockIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                                                )

                                                if is_valid:
                                                    if auto_alliances:
                                                        embed_description += f"{theme.refreshIcon} **Auto-redemption:** Queued for `{len(auto_alliances)}` Alliances\n"
                                                    else:
                                                        embed_description += f"{theme.refreshIcon} **Auto-redemption** is not enabled\n"
                                                elif is_valid is None:
                                                    from . import gift_redemption
                                                    embed_description += f"{gift_redemption.PENDING_REVALIDATION_NOTICE}\n"

                                                embed_description += f"{theme.lowerDivider}\n"

                                                embed_color = discord.Color.green() if is_valid else (discord.Color.red() if is_valid is False else discord.Color.orange())

                                                admin_embed = discord.Embed(
                                                    title="🎁 New Gift Code Found!",
                                                    description=embed_description,
                                                    color=embed_color
                                                )

                                                for admin_id in admin_ids:
                                                    try:
                                                        admin_user = await self.bot.fetch_user(admin_id[0])
                                                        if admin_user:
                                                            await admin_user.send(embed=admin_embed)
                                                    except Exception as e:
                                                        self.logger.exception(f"Error sending notification to admin {admin_id[0]}: {e}")

                                            # Send notification to all gift code channels
                                            try:
                                                self.cursor.execute("SELECT DISTINCT channel_id FROM giftcode_channel")
                                                gift_channels = self.cursor.fetchall()

                                                if notify and gift_channels:
                                                    channel_desc = (
                                                        f"**Code:** `{code}`\n"
                                                        f"**Status:** {validation_status}\n"
                                                    )
                                                    if is_valid:
                                                        if auto_alliances:
                                                            channel_desc += f"**Auto-redemption:** Queued for {len(auto_alliances)} Alliances"
                                                        else:
                                                            channel_desc += "**Auto-redemption** is not enabled"
                                                    elif is_valid is None:
                                                        channel_desc += "**Auto-redemption:** Runs automatically as soon as the code validates"
                                                    else:
                                                        channel_desc += "**Auto-redemption:** Not redeemed (code is invalid)"
                                                    channel_embed = discord.Embed(
                                                        title="🎁 New Gift Code Retrieved",
                                                        description=channel_desc,
                                                        color=embed_color
                                                    )
                                                    channel_embed.set_footer(text="Retrieved via Gift Code Distribution API")

                                                    for (channel_id,) in gift_channels:
                                                        try:
                                                            channel = self.bot.get_channel(channel_id)
                                                            if channel:
                                                                await channel.send(embed=channel_embed)
                                                        except Exception as e:
                                                            self.logger.warning(f"Failed to send API code notification to channel {channel_id}: {e}")
                                            except Exception as e:
                                                self.logger.exception(f"Error sending gift code channel notifications: {e}")

                                            if auto_alliances:
                                                gift_operations = self.bot.get_cog('GiftOperations')
                                                if gift_operations:
                                                    self.logger.info(f"Queueing auto-distribution for code {code} to {len(auto_alliances)} alliances")
                                                    from . import gift_redemption
                                                    for alliance in auto_alliances:
                                                        try:
                                                            await gift_redemption.enqueue_redemption(
                                                                gift_operations,
                                                                giftcode=code,
                                                                alliance_id=alliance[0],
                                                                source='api-auto',
                                                            )
                                                        except Exception as e:
                                                            self.logger.exception(f"Error queueing auto-distribution for code {code} to alliance {alliance[0]}: {e}")
                                                else:
                                                    self.logger.error("GiftOperations cog not found!")
                                        except Exception as e:
                                            self.logger.exception(f"Error processing new code {code}: {e}")
                            except Exception as e:
                                self.logger.exception(f"Error committing new codes: {e}")
                            
                            api_code_set = {code for code, _ in valid_codes}
                            codes_to_push = []
                            for db_code, (db_date, db_status) in db_codes.items(): # Push our valid codes to the API if they're not already there
                                if db_status != 'invalid' and db_status != 'pending':
                                    if db_code not in api_code_set:
                                        codes_to_push.append((db_code, db_date))
                            
                            if codes_to_push:
                                self.logger.info(f"Pushing {len(codes_to_push)} validated codes to API")
                                
                                for db_code, db_date in codes_to_push:
                                    try:
                                        exists_in_api = await self.check_giftcode(db_code)
                                        if exists_in_api:
                                            self.logger.info(f"Code {db_code} already exists in API (verified via check)")
                                            continue

                                        date_obj = datetime.strptime(db_date, "%Y-%m-%d")
                                        formatted_date = date_obj.strftime("%d.%m.%Y")
                                        
                                        data = {
                                            'code': db_code,
                                            'date': formatted_date
                                        }
                                        
                                        await self._wait_for_rate_limit()
                                        
                                        async with session.post(self.api_url, json=data, headers=headers) as post_response:
                                            if post_response.status == 409:
                                                self.logger.info(f"Code {db_code} already exists in API")
                                            elif post_response.status == 200:
                                                self.logger.info(f"Successfully pushed code {db_code} to API")
                                            else:
                                                response_text = await post_response.text()

                                                # Check if this is an expected "previously marked invalid" response
                                                if post_response.status == 400 and "previously marked invalid" in response_text:
                                                    self.logger.info(f"Code {db_code} already expired/invalid on API - updating local status")
                                                    self.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (db_code,))
                                                    await self._safe_commit(self.conn, "mark code invalid")
                                                elif "invalid" in response_text.lower():
                                                    # Other invalid code responses
                                                    self.logger.info(f"Code {db_code} rejected as invalid by API - updating local status")
                                                    self.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (db_code,))
                                                    await self._safe_commit(self.conn, "mark code invalid")
                                                else:
                                                    # Unexpected error - log as warning
                                                    self.logger.warning(f"Failed to push code {db_code}: {post_response.status}, {response_text[:200]}")

                                                backoff_time = await self._handle_api_error(post_response, response_text)
                                                if backoff_time > 0:
                                                    await asyncio.sleep(backoff_time)
                                    except Exception as e:
                                        self.logger.exception(f"Error pushing code {db_code} to API: {e}")
                                        await asyncio.sleep(self.error_backoff_time)

                            self.current_backoff = self.error_backoff_time
                            self.logger.info("API synchronization completed successfully")
                            return True
                            
                        except json.JSONDecodeError as e:
                            self.logger.exception(f"JSON decode error: {e}, Response: {response_text[:200]}")
                            return False
                            
                except aiohttp.ClientError as e:
                    self.logger.warning(f"Connection error syncing with Gift Code API: {type(e).__name__}")
                    return False
            
        except Exception as e:
            self.logger.exception(f"Unexpected error in sync_with_api: {e}")
            return False
            
    async def add_giftcode(self, giftcode: str) -> bool:
        """Add a gift code to the API."""
        try: # Check if code already exists in our database
            self.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,))
            result = self.cursor.fetchone()
            
            if result:
                if result[0] in ['invalid', 'pending']:
                    return False
            
            exists_in_api = await self.check_giftcode(giftcode)
            if exists_in_api: # Make sure we don't bother API with POST if it's not needed
                self.logger.info(f"Code {giftcode} already exists in API - skipping POST")
                return True
            
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
                headers = get_headers()
                headers['Content-Type'] = 'application/json'
                headers['X-API-Key'] = self.api_key
                
                date_str = datetime.now().strftime("%d.%m.%Y")
                data = {
                    'code': giftcode,
                    'date': date_str
                }
                
                await self._wait_for_rate_limit()
                
                try:
                    async with session.post(self.api_url, json=data, headers=headers) as response:
                        response_text = await response.text()
                        
                        if response.status == 200:
                            try:
                                result = json.loads(response_text)
                                if result.get('success') == True:
                                    self.logger.info(f"Successfully added code {giftcode} to API")
                                    self.cursor.execute(
                                        "INSERT OR REPLACE INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)", 
                                        (giftcode, datetime.now().strftime("%Y-%m-%d"), "validated")
                                    )
                                    await self._safe_commit(self.conn, "add giftcode")
                                    return True
                                else:
                                    self.logger.warning(f"API didn't confirm success for code {giftcode}: {response_text[:200]}")
                                    return False
                            except json.JSONDecodeError:
                                self.logger.warning(f"Invalid JSON response when adding code {giftcode}: {response_text[:200]}")
                                return False
                        elif response.status == 409: # Consider this a success since the code is in the API
                            self.logger.info(f"Code {giftcode} already exists in API")
                            return True
                        else:
                            self.logger.warning(f"Failed to add code {giftcode} to API: {response.status}, {response_text[:200]}")
                            if "invalid" in response_text.lower(): # Code was rejected as invalid by API, mark it as invalid locally
                                self.logger.warning(f"Code {giftcode} marked invalid by API")
                                self.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (giftcode,))
                                await self._safe_commit(self.conn, "mark code invalid")
                            backoff_time = await self._handle_api_error(response, response_text)
                            await asyncio.sleep(backoff_time)
                            return False

                except aiohttp.ClientError as e:
                    self.logger.warning(f"Connection error adding code {giftcode} to API: {type(e).__name__}")
                    return False
            
        except Exception as e:
            self.logger.exception(f"Unexpected error adding code {giftcode}: {e}")
            return False
            
    async def remove_giftcode(self, giftcode: str, from_validation: bool = False) -> bool:
        """Remove a gift code from the API."""
        try:
            if not from_validation:
                self.logger.warning(f"Attempted to remove code {giftcode} without validation flag")
                return False
            
            exists_in_api = await self.check_giftcode(giftcode)
            if not exists_in_api: # Make sure we don't bother API with DELETE if it's not needed
                self.logger.info(f"Code {giftcode} not found in API - no need to remove")
                self.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (giftcode,))
                await self._safe_commit(self.conn, "mark code invalid")
                return True
            
            self.logger.info(f"Removing invalid code {giftcode} from API")
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
                headers = get_headers()
                headers['Content-Type'] = 'application/json'
                headers['X-API-Key'] = self.api_key
                data = {'code': giftcode}
                
                await self._wait_for_rate_limit()
                
                try:
                    async with session.delete(self.api_url, json=data, headers=headers) as response:
                        response_text = await response.text()
                        
                        if response.status == 200:
                            try:
                                result = json.loads(response_text)
                                if result.get('success') == True:
                                    self.logger.info(f"Successfully removed code {giftcode} from API")
                                    self.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (giftcode,))
                                    await self._safe_commit(self.conn, "remove giftcode")
                                    return True
                                else:
                                    self.logger.warning(f"API didn't confirm removal of code {giftcode}: {response_text[:200]}")
                                    return False
                            except json.JSONDecodeError:
                                self.logger.warning(f"Invalid JSON response when removing code {giftcode}: {response_text[:200]}")
                                return False
                        else:
                            self.logger.warning(f"Failed to remove code {giftcode} from API: {response.status}, {response_text[:200]}")
                            backoff_time = await self._handle_api_error(response, response_text)
                            await asyncio.sleep(backoff_time)
                            return False
                except aiohttp.ClientError as e:
                    self.logger.warning(f"Connection error removing code {giftcode} from API: {type(e).__name__}")
                    return False
        except Exception as e:
            self.logger.exception(f"Unexpected error removing code {giftcode}: {e}")
            return False
            
    async def check_giftcode(self, giftcode: str) -> bool:
        """Check if a gift code exists in the API."""
        try:
            connector = aiohttp.TCPConnector(ssl=self.ssl_context)
            async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
                headers = get_headers()
                headers['X-API-Key'] = self.api_key
                
                await self._wait_for_rate_limit()
                
                try:
                    async with session.get(f"{self.api_url}?action=check&giftcode={giftcode}", headers=headers) as response:
                        if response.status == 200:
                            try:
                                result = await response.json()
                                return result.get('exists', False)
                            except json.JSONDecodeError:
                                self.logger.warning(f"Invalid JSON response when checking code {giftcode}")
                                return False
                        else:
                            self.logger.warning(f"Failed to check code {giftcode}: {response.status}")
                            backoff_time = await self._handle_api_error(response, await response.text())
                            await asyncio.sleep(backoff_time)
                            return False
                except aiohttp.ClientError as e:
                    self.logger.warning(f"Connection error checking code {giftcode} in API: {type(e).__name__}")
                    return False
        except Exception as e:
            self.logger.exception(f"Unexpected error checking code {giftcode}: {e}")
            return False 

