"""Gift code validation engine, queue system, batch operations, and redemption logic."""

import asyncio
import base64
import hashlib
import json
import os
import random
import re
import sqlite3
import time
import traceback
from datetime import datetime, timedelta

import discord
import requests
from requests.adapters import HTTPAdapter

from .pimp_my_bot import theme
from .browser_headers import get_headers
from .process_queue import GIFT_VALIDATE, GIFT_REDEEM, PreemptedException


async def enqueue_validation(cog, giftcode, source, message=None, channel=None):
    """Enqueue a gift code validation operation in the ProcessQueue."""

    process_queue = cog.bot.get_cog('ProcessQueue')
    if not process_queue:
        cog.logger.error("ProcessQueue cog not available, cannot enqueue validation")
        return

    details = {
        'giftcode': giftcode,
        'source': source,
    }
    if channel:
        details['channel_id'] = channel.id
    if message:
        details['message_id'] = message.id

    process_queue.enqueue(
        action='gift_validate',
        priority=GIFT_VALIDATE,
        details=details,
    )
    cog.logger.info(f"Enqueued validation for code '{giftcode}' (source: {source})")


async def enqueue_redemption(cog, giftcode, alliance_id, source='manual', batch_id=None):
    """Enqueue a gift code redemption operation in the ProcessQueue."""
    process_queue = cog.bot.get_cog('ProcessQueue')
    if not process_queue:
        cog.logger.error("ProcessQueue cog not available, cannot enqueue redemption")
        return

    details = {
        'giftcode': giftcode,
        'source': source,
    }
    if batch_id:
        details['batch_id'] = batch_id

    process_queue.enqueue(
        action='gift_redeem',
        priority=GIFT_REDEEM,
        alliance_id=alliance_id,
        details=details,
    )
    cog.logger.info(f"Enqueued redemption for code '{giftcode}' alliance {alliance_id}")


async def handle_gift_validate_process(cog, process):
    """ProcessQueue handler for gift_validate actions."""
    details = process.get('details', {})
    giftcode = details.get('giftcode')
    source = details.get('source', 'unknown')
    channel_id = details.get('channel_id')
    message_id = details.get('message_id')

    if not giftcode:
        cog.logger.error(f"gift_validate process {process['id']} missing giftcode")
        return

    cog.logger.info(f"Processing gift code validation '{giftcode}' from queue (source: {source})")

    # Look up message and channel if IDs were provided
    channel = None
    message = None
    if channel_id:
        channel = cog.bot.get_channel(channel_id)
        if channel and message_id:
            try:
                message = await channel.fetch_message(message_id)
            except Exception:
                message = None

    # Check if code already exists
    cog.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (giftcode,))
    if cog.cursor.fetchone():
        cog.logger.info(f"Code '{giftcode}' already exists in database.")
        if message and channel:
            await _send_existing_code_response(cog, message, giftcode, channel)
        return

    # Show processing message if from channel
    processing_message = None
    if message and channel:
        processing_embed = discord.Embed(
            title=f"{theme.refreshIcon} Processing Gift Code...",
            description=f"Validating `{giftcode}`",
            color=theme.emColor1
        )
        try:
            processing_message = await channel.send(embed=processing_embed)
        except Exception:
            processing_message = None

    # Perform validation
    is_valid, validation_msg = await validate_gift_code_immediately(cog, giftcode, source)

    # Handle validation result
    if message and channel:
        await _send_validation_response(cog, message, giftcode, is_valid, validation_msg, processing_message)

    # Process auto-use if valid
    if is_valid:
        await _process_auto_use(cog, giftcode)


async def _record_batch_start(cog, batch_id, alliance_id):
    """Mark an alliance as processing in a batch and refresh the progress embed."""
    if not batch_id or batch_id not in cog.redemption_batches:
        return
    batch = cog.redemption_batches[batch_id]
    if alliance_id not in batch['alliances']:
        return
    batch['alliances'][alliance_id]['status'] = 'processing'
    await _update_batch_progress(cog, batch_id)


async def _record_batch_result(cog, batch_id, alliance_id, success):
    """Record one code's completion (success or error) for an alliance in a batch.

    Increments the alliance's code counter, flips its status once all codes are
    done, refreshes the progress embed, and cleans up the batch if every
    alliance is finished.
    """
    if not batch_id or batch_id not in cog.redemption_batches:
        return
    batch = cog.redemption_batches[batch_id]
    alliances = batch['alliances']
    if alliance_id not in alliances:
        return

    total_codes = batch.get('total_codes', 1)
    alliances[alliance_id]['codes_completed'] = alliances[alliance_id].get('codes_completed', 0) + 1
    codes_done = alliances[alliance_id]['codes_completed']

    if codes_done >= total_codes:
        alliances[alliance_id]['status'] = 'completed' if success else 'error'
    elif success:
        alliances[alliance_id]['status'] = 'processing'
    else:
        alliances[alliance_id]['status'] = 'error'

    await _update_batch_progress(cog, batch_id)

    if all(info['status'] in ('completed', 'error') for info in alliances.values()):
        del cog.redemption_batches[batch_id]


async def handle_gift_redeem_process(cog, process):
    """ProcessQueue handler for gift_redeem actions."""
    details = process.get('details', {})
    giftcode = details.get('giftcode')
    alliance_id = process.get('alliance_id')
    batch_id = details.get('batch_id')

    if not giftcode or not alliance_id:
        cog.logger.error(f"gift_redeem process {process['id']} missing giftcode or alliance_id")
        return

    cog.logger.info(f"Processing gift code redemption '{giftcode}' for alliance {alliance_id}")

    await _record_batch_start(cog, batch_id, alliance_id)

    # Pin the captcha model resident for the whole alliance batch so individual
    # solve_captcha() calls don't pay reload cost between players. The model
    # unloads ~2 min after the last batch finishes (handled by onnx_lifecycle).
    captcha_wrapper = (
        cog.captcha_solver._model_wrapper
        if cog.captcha_solver and getattr(cog.captcha_solver, '_model_wrapper', None)
        else None
    )
    if captcha_wrapper is not None:
        await captcha_wrapper.acquire()

    try:
        await use_giftcode_for_alliance(cog, alliance_id, giftcode)
    except PreemptedException:
        # Let the processor re-queue this process; don't touch batch state
        raise
    except Exception as e:
        cog.logger.exception(f"Error in redemption for alliance {alliance_id}: {e}")
        await _record_batch_result(cog, batch_id, alliance_id, success=False)
        raise
    finally:
        if captcha_wrapper is not None:
            await captcha_wrapper.release()

    await _record_batch_result(cog, batch_id, alliance_id, success=True)


async def _send_existing_code_response(cog, message, giftcode, channel):
    """Send response for existing gift code."""
    reply_embed = discord.Embed(title=f"{theme.infoIcon} Gift Code Already Known", color=theme.emColor1)
    reply_embed.description = (
        f"**Gift Code Details**\n{theme.upperDivider}\n"
        f"{theme.userIcon} **Sender:** {message.author.mention}\n"
        f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
        f"{theme.editListIcon} **Status:** Already in database.\n"
        f"{theme.lowerDivider}\n"
    )
    await channel.send(embed=reply_embed)

    try:
        await message.add_reaction(theme.infoIcon)
    except (discord.Forbidden, discord.NotFound):
        pass


async def _send_validation_response(cog, message, giftcode, is_valid, validation_msg, processing_message=None):
    """Send validation response to channel."""
    if is_valid:
        reply_embed = discord.Embed(title=f"{theme.verifiedIcon} Gift Code Validated", color=theme.emColor3)
        reply_embed.description = (
            f"**Gift Code Details**\n{theme.upperDivider}\n"
            f"{theme.userIcon} **Sender:** {message.author.mention}\n"
            f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
            f"{theme.verifiedIcon} **Status:** {validation_msg}\n"
            f"{theme.lowerDivider}\n"
        )
        reaction = f"{theme.verifiedIcon}"
    elif is_valid is False:
        reply_embed = discord.Embed(title=f"{theme.deniedIcon} Invalid Gift Code", color=theme.emColor2)
        reply_embed.description = (
            f"**Gift Code Details**\n{theme.upperDivider}\n"
            f"{theme.userIcon} **Sender:** {message.author.mention}\n"
            f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
            f"{theme.deniedIcon} **Status:** {validation_msg}\n"
            f"{theme.editListIcon} **Action:** Code not added to database\n"
            f"{theme.lowerDivider}\n"
        )
        reaction = f"{theme.deniedIcon}"
    else:
        reply_embed = discord.Embed(title=f"{theme.warnIcon} Gift Code Added (Pending)", color=discord.Color.yellow())
        reply_embed.description = (
            f"**Gift Code Details**\n{theme.upperDivider}\n"
            f"{theme.userIcon} **Sender:** {message.author.mention}\n"
            f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
            f"{theme.warnIcon} **Status:** {validation_msg}\n"
            f"{theme.editListIcon} **Action:** Added for later validation\n"
            f"{theme.lowerDivider}\n"
        )
        reaction = theme.warnIcon

    if processing_message:
        await processing_message.edit(embed=reply_embed)
    else:
        await message.channel.send(embed=reply_embed)

    try:
        await message.add_reaction(reaction)
    except (discord.Forbidden, discord.NotFound):
        pass


async def _process_auto_use(cog, giftcode):
    """Process auto-use for valid gift codes."""
    cog.cursor.execute("SELECT alliance_id FROM giftcodecontrol WHERE status = 1 ORDER BY priority ASC, alliance_id ASC")
    auto_alliances = cog.cursor.fetchall()

    if auto_alliances:
        cog.logger.info(f"Queueing auto-use for {len(auto_alliances)} alliances for code '{giftcode}'")
        for alliance in auto_alliances:
            await enqueue_redemption(cog, giftcode=giftcode, alliance_id=alliance[0], source='auto')


async def get_queue_status(cog):
    """Get current queue status from the ProcessQueue cog.

    Returns a dict with `queue_length` (total queued) and `queue_by_code`
    (per-gift-code breakdown of queued operations).
    """
    process_queue = cog.bot.get_cog('ProcessQueue')
    if not process_queue:
        return {'queue_length': 0, 'queue_by_code': {}}

    queue_size = process_queue.get_queue_info()['queue_size']

    # Build per-code breakdown across gift_validate and gift_redeem actions
    queue_by_code = {}
    position = 1
    for action in ('gift_validate', 'gift_redeem'):
        for proc in process_queue.get_queued_processes_by_action(action):
            details = proc.get('details', {})
            code = details.get('giftcode', 'unknown')
            queue_by_code.setdefault(code, []).append({
                'position': position,
                'alliance_id': proc.get('alliance_id'),
                'source': details.get('source', 'unknown'),
            })
            position += 1

    return {
        'queue_length': queue_size,
        'queue_by_code': queue_by_code,
    }


async def add_manual_redemption_to_queue(cog, giftcodes, alliance_ids, interaction):
    """Add manual redemption requests to ProcessQueue.

    Args:
        giftcodes: Single gift code string or list of gift codes
        alliance_ids: List of alliance IDs
        interaction: Discord interaction for progress messages
    """
    # Normalize giftcodes to list
    if isinstance(giftcodes, str):
        giftcodes = [giftcodes]

    queue_positions = []
    total_redemptions = len(giftcodes) * len(alliance_ids)

    # Create batch for multiple redemptions
    batch_id = None
    if total_redemptions > 1 and interaction:
        import uuid
        batch_id = str(uuid.uuid4())

        # Get alliance names for the batch
        alliances_info = {}
        for aid in alliance_ids:
            cog.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (aid,))
            result = cog.alliance_cursor.fetchone()
            name = result[0] if result else f"Alliance {aid}"
            alliances_info[aid] = {'name': name, 'status': 'pending', 'codes_completed': 0}

        # Send initial consolidated progress message
        embed = _build_batch_progress_embed(giftcodes, alliances_info)
        progress_message = await interaction.followup.send(embed=embed, ephemeral=True)

        # Store batch info
        cog.redemption_batches[batch_id] = {
            'message': progress_message,
            'alliances': alliances_info,
            'giftcodes': giftcodes,
            'total_codes': len(giftcodes)
        }

    # Queue order: Alliance 1 -> all codes, then Alliance 2 -> all codes, etc.
    for alliance_id in alliance_ids:
        for giftcode in giftcodes:
            await enqueue_redemption(
                cog,
                giftcode=giftcode,
                alliance_id=alliance_id,
                source='manual',
                batch_id=batch_id,
            )

            queue_status = await get_queue_status(cog)
            queue_positions.append(queue_status['queue_length'])

    return queue_positions


def _build_batch_progress_embed(giftcodes, alliances_info, total_codes=None):
    """Build the consolidated progress embed for batch redemption."""
    # Handle both single code (string) and multiple codes (list)
    if isinstance(giftcodes, str):
        giftcodes = [giftcodes]

    if total_codes is None:
        total_codes = len(giftcodes)

    lines = []
    for aid, info in alliances_info.items():
        status = info['status']
        codes_completed = info.get('codes_completed', 0)

        if status == 'pending':
            icon = f"{theme.timeIcon}"
        elif status == 'processing':
            icon = f"{theme.refreshIcon}"
        elif status == 'completed':
            icon = f"{theme.verifiedIcon}"
        elif status == 'error':
            icon = f"{theme.deniedIcon}"
        else:
            icon = f"{theme.timeIcon}"

        # Show code progress for multi-code batches
        if total_codes > 1:
            lines.append(f"{icon} **{info['name']}** ({codes_completed}/{total_codes} codes)")
        else:
            lines.append(f"{icon} **{info['name']}**")

    completed_alliances = sum(1 for info in alliances_info.values() if info['status'] == 'completed')
    total_alliances = len(alliances_info)

    # Build description based on single or multiple codes
    if total_codes > 1:
        code_display = f"ALL ({total_codes} codes)"
    else:
        code_display = f"`{giftcodes[0]}`"

    embed = discord.Embed(
        title=f"{theme.giftIcon} Batch Redemption Progress",
        description=f"**Gift Code{'s' if total_codes > 1 else ''}:** {code_display}\n**Progress:** {completed_alliances}/{total_alliances} alliances\n\n" + "\n".join(lines),
        color=theme.emColor3 if completed_alliances == total_alliances else discord.Color.blue()
    )
    return embed


async def _update_batch_progress(cog, batch_id):
    """Update the batch progress message."""
    if batch_id not in cog.redemption_batches:
        return

    batch = cog.redemption_batches[batch_id]
    giftcodes = batch.get('giftcodes', batch.get('giftcode', []))
    total_codes = batch.get('total_codes', 1)
    embed = _build_batch_progress_embed(giftcodes, batch['alliances'], total_codes)

    try:
        await batch['message'].edit(embed=embed)
    except Exception as e:
        cog.logger.warning(f"Failed to update batch progress message: {e}")


async def validate_gift_code_immediately(cog, giftcode, source="unknown"):
    """Immediately validate a gift code when it's added from any source.

    Args:
        giftcode: The gift code to validate
        source: Where the code came from ('api', 'button', 'channel')

    Returns:
        tuple: (is_valid, status_message)
    """
    try:
        # Clean the gift code
        giftcode = cog.clean_gift_code(giftcode)

        # Get the best ID for validation
        validation_fid, fid_source = await cog.get_validation_fid()

        cog.logger.info(f"Validating gift code '{giftcode}' from {source} using {fid_source} ID: {validation_fid}")

        # Check if already validated
        cog.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,))
        existing = cog.cursor.fetchone()

        if existing:
            status = existing[0]
            if status == 'invalid':
                cog.logger.info(f"Gift code '{giftcode}' already marked as invalid")
                return False, "Code already marked as invalid"
            elif status == 'validated':
                cog.logger.info(f"Gift code '{giftcode}' already validated")
                return True, "Code already validated"

        # Perform validation using the selected ID
        status = await claim_giftcode_rewards_wos(cog, validation_fid, giftcode)

        # Handle validation results
        if status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE", "TOO_SMALL_SPEND_MORE", "TOO_POOR_SPEND_MORE"]:
            # Valid code - mark as validated
            cog.cursor.execute("""
                INSERT OR REPLACE INTO gift_codes (giftcode, date, validation_status)
                VALUES (?, date('now'), 'validated')
            """, (giftcode,))
            cog.conn.commit()

            # These statuses mean the code is valid but has requirements
            if status in ["TOO_SMALL_SPEND_MORE", "TOO_POOR_SPEND_MORE"]:
                validation_msg = f"Code validated (has requirements)"
                cog.logger.info(f"Gift code '{giftcode}' is valid but has requirements: {status}")
            else:
                validation_msg = f"Code validated successfully ({status})"
                cog.logger.info(f"Gift code '{giftcode}' validated successfully using {fid_source} ID")

            return True, validation_msg

        elif status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
            # Invalid code - mark as invalid
            mark_code_invalid(cog, giftcode)

            reason_map = {
                "TIME_ERROR": "Code has expired",
                "CDK_NOT_FOUND": "Code not found or incorrect",
                "USAGE_LIMIT": "Usage limit reached"
            }
            reason = reason_map.get(status, f"Invalid ({status})")

            cog.logger.warning(f"Gift code '{giftcode}' is invalid: {reason}")

            # Remove from API if needed
            if hasattr(cog, 'api') and cog.api:
                asyncio.create_task(cog.api.remove_giftcode(giftcode, from_validation=True))

            return False, reason

        else: # Other statuses - don't mark as invalid yet
            cog.logger.warning(f"Gift code '{giftcode}' validation returned: {status}")
            return None, f"Validation inconclusive ({status})"

    except Exception as e:
        cog.logger.exception(f"Error validating gift code '{giftcode}': {e}")
        return None, f"Validation error: {str(e)}"


def encode_data(cog, data):
    secret = cog.wos_encrypt_key
    sorted_keys = sorted(data.keys())
    encoded_data = "&".join(
        [
            f"{key}={json.dumps(data[key]) if isinstance(data[key], dict) else data[key]}"
            for key in sorted_keys
        ]
    )
    sign = hashlib.md5(f"{encoded_data}{secret}".encode()).hexdigest()
    return {"sign": sign, **data}


def batch_insert_user_giftcodes(cog, user_giftcode_data):
    """Batch insert/update user giftcode records for better performance."""
    if not user_giftcode_data:
        return

    try: # Executemany for batch operations - much faster than individual inserts
        cog.cursor.executemany("""
            INSERT OR REPLACE INTO user_giftcodes (fid, giftcode, status)
            VALUES (?, ?, ?)
        """, user_giftcode_data)

        cog.conn.commit()
        cog.logger.info(f"GiftOps: Batch inserted/updated {len(user_giftcode_data)} user giftcode records")

    except Exception as e:
        cog.logger.exception(f"GiftOps: Error in batch_insert_user_giftcodes: {e}")
        cog.conn.rollback()


def batch_update_gift_codes_validation(cog, giftcodes_to_validate):
    """Batch update gift codes validation status."""
    if not giftcodes_to_validate:
        return

    try:
        validation_data = [(giftcode,) for giftcode in giftcodes_to_validate]
        cog.cursor.executemany("""
            UPDATE gift_codes
            SET validation_status = 'validated'
            WHERE giftcode = ? AND validation_status = 'pending'
        """, validation_data)

        cog.conn.commit()
        updated_count = cog.cursor.rowcount
        if updated_count > 0:
            cog.logger.info(f"GiftOps: Batch validated {updated_count} gift codes")

    except Exception as e:
        cog.logger.exception(f"GiftOps: Error in batch_update_gift_codes_validation: {e}")
        cog.conn.rollback()


def batch_get_user_giftcode_status(cog, giftcode, fids):
    """Batch retrieve user giftcode status for multiple IDs."""
    if not fids:
        return {}

    try:
        placeholders = ','.join('?' * len(fids))
        cog.cursor.execute(f"""
            SELECT fid, status FROM user_giftcodes
            WHERE giftcode = ? AND fid IN ({placeholders})
        """, (giftcode, *fids))

        results = dict(cog.cursor.fetchall())
        cog.logger.debug(f"GiftOps: Batch retrieved {len(results)} user giftcode statuses")
        return results

    except Exception as e:
        cog.logger.exception(f"GiftOps: Error in batch_get_user_giftcode_status: {e}")
        return {}


def mark_code_invalid(cog, giftcode):
    """Mark a single gift code as invalid."""
    try:
        cog.cursor.execute("""
            UPDATE gift_codes
            SET validation_status = 'invalid'
            WHERE giftcode = ? AND validation_status != 'invalid'
        """, (giftcode,))

        cog.conn.commit()
        if cog.cursor.rowcount > 0:
            cog.logger.info(f"GiftOps: Marked gift code '{giftcode}' as invalid")

    except Exception as e:
        cog.logger.exception(f"GiftOps: Error marking code '{giftcode}' as invalid: {e}")
        cog.conn.rollback()


def batch_process_alliance_results(cog, results_batch):
    """Process a batch of alliance redemption results efficiently."""
    if not results_batch:
        return

    try:
        # Separate successful results
        successful_records = []
        codes_to_validate = set()

        for fid, giftcode, status in results_batch:
            if status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
                successful_records.append((fid, giftcode, status))
                codes_to_validate.add(giftcode)

        # Batch insert successful records
        if successful_records:
            batch_insert_user_giftcodes(cog, successful_records)

        # Batch validate codes
        if codes_to_validate:
            batch_update_gift_codes_validation(cog, list(codes_to_validate))

        cog.logger.info(f"GiftOps: Batch processed {len(successful_records)} successful, {len(codes_to_validate)} validated")

    except Exception as e:
        cog.logger.exception(f"GiftOps: Error in batch_process_alliance_results: {e}")


async def get_stove_info_wos(cog, player_id):
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=cog.retry_config))
    session.headers.update(get_headers(cog.wos_giftcode_redemption_url))

    data_to_encode = {
        "fid": f"{player_id}",
        "time": f"{int(datetime.now().timestamp())}",
    }
    data = encode_data(cog, data_to_encode)

    try:
        response_stove_info = await asyncio.to_thread(
            session.post,
            cog.wos_player_info_url,
            data=data,
            timeout=(10, 30),
        )
        return session, response_stove_info
    except requests.exceptions.ConnectionError as e:
        session.close()
        cog.logger.warning(f"Connection error reaching WOS API for player {player_id}: {type(e).__name__}")
        raise
    except requests.exceptions.Timeout as e:
        session.close()
        cog.logger.warning(f"Timeout reaching WOS API for player {player_id}")
        raise
    except requests.exceptions.RequestException as e:
        session.close()
        cog.logger.warning(f"Request error reaching WOS API for player {player_id}: {type(e).__name__}")
        raise


async def attempt_gift_code_with_api(cog, player_id, giftcode, session):
    """Attempt to redeem a gift code."""
    max_ocr_attempts = 4

    for attempt in range(max_ocr_attempts):
        cog.logger.info(f"GiftOps: Attempt {attempt + 1}/{max_ocr_attempts} to fetch/solve captcha for ID {player_id}")

        # Fetch captcha
        captcha_image_base64, error = await fetch_captcha(cog, player_id, session)

        if error:
            if error == "CAPTCHA_TOO_FREQUENT":
                cog.logger.info(f"GiftOps: API returned CAPTCHA_TOO_FREQUENT for ID {player_id}")
                return "CAPTCHA_TOO_FREQUENT", None, None, None
            else:
                cog.logger.error(f"GiftOps: Captcha fetch error for ID {player_id}: {error}")
                return "CAPTCHA_FETCH_ERROR", None, None, None

        if not captcha_image_base64:
            cog.logger.warning(f"GiftOps: No captcha image returned for ID {player_id}")
            return "CAPTCHA_FETCH_ERROR", None, None, None

        # Decode captcha image
        try:
            if captcha_image_base64.startswith("data:image"):
                img_b64_data = captcha_image_base64.split(",", 1)[1]
            else:
                img_b64_data = captcha_image_base64
            image_bytes = base64.b64decode(img_b64_data)
        except Exception as decode_err:
            cog.logger.error(f"Failed to decode base64 image for ID {player_id}: {decode_err}")
            return "CAPTCHA_FETCH_ERROR", None, None, None

        # Solve captcha
        cog.processing_stats["ocr_solver_calls"] += 1
        captcha_code, success, method, confidence, _ = await cog.captcha_solver.solve_captcha(
            image_bytes, fid=player_id, attempt=attempt)

        if not success:
            cog.logger.info(f"GiftOps: OCR failed for ID {player_id} on attempt {attempt + 1}")
            if attempt == max_ocr_attempts - 1:
                return "MAX_CAPTCHA_ATTEMPTS_REACHED", None, None, None
            continue

        cog.processing_stats["ocr_valid_format"] += 1
        cog.logger.info(f"GiftOps: OCR solved for {player_id}: {captcha_code} (method:{method}, conf:{confidence:.2f}, attempt:{attempt+1})")

        # Submit gift code with solved captcha
        data_to_encode = {
            "fid": f"{player_id}",
            "cdk": giftcode,
            "captcha_code": captcha_code,
            "time": f"{int(datetime.now().timestamp()*1000)}"
        }
        data = encode_data(cog, data_to_encode)
        cog.processing_stats["captcha_submissions"] += 1

        # Submit to gift code API
        response_giftcode = await asyncio.to_thread(
            session.post, cog.wos_giftcode_url, data=data, timeout=(10, 30)
        )

        # Log the redemption attempt
        log_entry_redeem = f"\n{datetime.now()} API REQ - Gift Code Redeem\nID:{player_id}, Code:{giftcode}, Captcha:{captcha_code}\n"
        try:
            response_json_redeem = response_giftcode.json()
            log_entry_redeem += f"Resp Code: {response_giftcode.status_code}\nResponse JSON:\n{json.dumps(response_json_redeem, indent=2)}\n"
        except json.JSONDecodeError:
            response_json_redeem = {}
            log_entry_redeem += f"Resp Code: {response_giftcode.status_code}\nResponse Text (Not JSON): {response_giftcode.text[:500]}...\n"
        log_entry_redeem += "-" * 50 + "\n"
        cog.giftlog.info(log_entry_redeem.strip())

        # Parse response
        msg = str(response_json_redeem.get("msg", "Unknown Error")).strip('.')
        err_code = response_json_redeem.get("err_code")

        # Check if this is a rate limit error - these need special handling
        rate_limit_errors = {
            ("CAPTCHA GET TOO FREQUENT", 40100),
            ("CAPTCHA CHECK TOO FREQUENT", 40101)
        }

        if (msg, err_code) in rate_limit_errors:
            cog.logger.info(f"GiftOps: Rate limit hit for ID {player_id} (msg: {msg}, code: {err_code})")
            return "CAPTCHA_TOO_FREQUENT", image_bytes, captcha_code, method

        # Handle other captcha errors with retry logic
        other_captcha_errors = {
            ("CAPTCHA CHECK ERROR", 40103),
            ("CAPTCHA EXPIRED", 40102)
        }

        if (msg, err_code) in other_captcha_errors:
            cog.processing_stats["server_validation_failure"] += 1
            if attempt == max_ocr_attempts - 1:
                return "CAPTCHA_INVALID", image_bytes, captcha_code, method
            else:
                cog.logger.info(f"GiftOps: CAPTCHA_INVALID for ID {player_id} on attempt {attempt + 1} (msg: {msg}). Retrying...")
                await asyncio.sleep(random.uniform(1.5, 2.5))
                continue
        else:
            cog.processing_stats["server_validation_success"] += 1

        # Determine final status
        if msg == "SUCCESS":
            status = "SUCCESS"
        elif msg == "RECEIVED" and err_code == 40008:
            status = "RECEIVED"
        elif msg == "SAME TYPE EXCHANGE" and err_code == 40011:
            status = "SAME TYPE EXCHANGE"
        elif msg == "TIME ERROR" and err_code == 40007:
            status = "TIME_ERROR"
        elif msg == "CDK NOT FOUND" and err_code == 40014:
            status = "CDK_NOT_FOUND"
        elif msg == "USED" and err_code == 40005:
            status = "USAGE_LIMIT"
        elif msg == "TIMEOUT RETRY" and err_code == 40004:
            status = "TIMEOUT_RETRY"
        elif msg == "NOT LOGIN":
            status = "LOGIN_EXPIRED_MID_PROCESS"
        elif "sign error" in msg.lower():
            status = "SIGN_ERROR"
            cog.logger.error(f"[SIGN ERROR] Sign error detected for ID {player_id}, code {giftcode}")
            cog.logger.error(f"[SIGN ERROR] Response: {response_json_redeem}")
        elif msg == "STOVE_LV ERROR" and err_code == 40006:
            status = "TOO_SMALL_SPEND_MORE"
            cog.logger.error(f"[FURNACE LVL ERROR] Furnace level is too low for ID {player_id}, code {giftcode}")
            cog.logger.error(f"[FURNACE LVL ERROR] Response: {response_json_redeem}")
        elif (msg == "RECHARGE_MONEY ERROR" and err_code == 40017) or (msg == "RECHARGE_MONEY_VIP ERROR" and err_code == 40018):
            status = "TOO_POOR_SPEND_MORE"
            cog.logger.error(f"[VIP LEVEL ERROR] VIP level is too low for ID {player_id}, code {giftcode}")
            cog.logger.error(f"[VIP LEVEL ERROR] Response: {response_json_redeem}")
        else:
            status = "UNKNOWN_API_RESPONSE"
            cog.logger.info(f"Unknown API response for {player_id}: msg='{msg}', err_code={err_code}")

        return status, image_bytes, captcha_code, method

    return "MAX_CAPTCHA_ATTEMPTS_REACHED", None, None, None


async def claim_giftcode_rewards_wos(cog, player_id, giftcode):

    giftcode = cog.clean_gift_code(giftcode)
    process_start_time = time.time()
    status = "ERROR"
    image_bytes = None
    captcha_code = None
    method = "N/A"
    session = None

    try:
        # Cache Check
        test_fid = cog.get_test_fid()
        if player_id != test_fid:
            cog.cursor.execute("SELECT status FROM user_giftcodes WHERE fid = ? AND giftcode = ?", (player_id, giftcode))
            existing_record = cog.cursor.fetchone()
            if existing_record:
                if existing_record[0] in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE", "TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                    cog.logger.info(f"CACHE HIT - User {player_id} code '{giftcode}' status: {existing_record[0]}")
                    return existing_record[0]

        # Check if OCR Enabled and Solver Ready
        cog.settings_cursor.execute("SELECT enabled FROM ocr_settings ORDER BY id DESC LIMIT 1")
        ocr_settings_row = cog.settings_cursor.fetchone()
        ocr_enabled = ocr_settings_row[0] if ocr_settings_row else 0

        if not (ocr_enabled == 1 and cog.captcha_solver):
            status = "OCR_DISABLED" if ocr_enabled == 0 else "SOLVER_ERROR"
            log_msg = f"{datetime.now()} Skipping captcha: OCR disabled (Enabled={ocr_enabled}) or Solver not ready ({cog.captcha_solver is None}) for ID {player_id}.\n"
            cog.logger.info(log_msg.strip())
            return status

        # Initialize captcha solver stats
        cog.logger.info(f"GiftOps: OCR enabled and solver initialized for ID {player_id}.")
        cog.captcha_solver.reset_run_stats()

        # Get player session
        session, response_stove_info = await get_stove_info_wos(cog, player_id=player_id)
        log_entry_player = f"\n{datetime.now()} API REQUEST - Player Info\nPlayer ID: {player_id}\n"
        try:
            response_json_player = response_stove_info.json()
            log_entry_player += f"Response Code: {response_stove_info.status_code}\nResponse JSON:\n{json.dumps(response_json_player, indent=2)}\n"
        except json.JSONDecodeError:
            log_entry_player += f"Response Code: {response_stove_info.status_code}\nResponse Text (Not JSON): {response_stove_info.text[:500]}...\n"
        log_entry_player += "-" * 50 + "\n"
        cog.giftlog.info(log_entry_player.strip())

        try:
            player_info_json = response_stove_info.json()
        except json.JSONDecodeError:
            player_info_json = {}
        login_successful = player_info_json.get("msg") == "success"

        if not login_successful:
            status = "LOGIN_FAILED"
            log_message = f"{datetime.now()} Login failed for ID {player_id}: {player_info_json.get('msg', 'Unknown')}\n"
            cog.giftlog.info(log_message.strip())
            return status

        # Try gift code redemption
        cog.logger.info(f"GiftOps: Starting gift code redemption for ID {player_id}")

        status, image_bytes, captcha_code, method = await attempt_gift_code_with_api(
            cog, player_id, giftcode, session
        )

        # Handle database updates for successful redemptions
        if player_id != cog.get_test_fid() and status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
            try:
                user_giftcode_data = [(player_id, giftcode, status)]
                batch_insert_user_giftcodes(cog, user_giftcode_data)

                # Check if code needs validation
                cog.cursor.execute("""
                    SELECT validation_status FROM gift_codes
                    WHERE giftcode = ? AND validation_status = 'pending'
                """, (giftcode,))

                if cog.cursor.fetchone():
                    giftcodes_to_validate = [giftcode]
                    batch_update_gift_codes_validation(cog, giftcodes_to_validate)

                    # If this code was just validated for the first time, send to API
                    cog.logger.info(f"Code '{giftcode}' validated for the first time - sending to API")
                    try:
                        asyncio.create_task(cog.api.add_giftcode(giftcode))
                    except Exception as api_err:
                        cog.logger.exception(f"Error sending validated code '{giftcode}' to API: {api_err}")

                cog.giftlog.info(f"DATABASE - Saved/Updated status for User {player_id}, Code '{giftcode}', Status {status}\n")
            except Exception as db_err:
                cog.giftlog.exception(f"DATABASE ERROR saving/replacing status for {player_id}/{giftcode}: {db_err}\n")
                cog.giftlog.exception(f"STACK TRACE: {traceback.format_exc()}\n")

    except requests.exceptions.ConnectionError:
        cog.logger.warning(f"GiftOps: Connection error for ID {player_id}. Check bot connectivity to the WOS Gift Code API.")
        status = "CONNECTION_ERROR"
    except requests.exceptions.Timeout:
        cog.logger.warning(f"GiftOps: Timeout for ID {player_id}. Check bot connectivity to the WOS Gift Code API.")
        status = "CONNECTION_ERROR"
    except requests.exceptions.RequestException as e:
        cog.logger.warning(f"GiftOps: Request error for ID {player_id}: {type(e).__name__}")
        status = "CONNECTION_ERROR"
    except Exception as e:
        error_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_details = traceback.format_exc()
        log_message = (
            f"\n--- UNEXPECTED ERROR in claim_giftcode_rewards_wos ({error_timestamp}) ---\n"
            f"Player ID: {player_id}, Gift Code: {giftcode}\nError: {str(e)}\n"
            f"Traceback:\n{error_details}\n"
            f"---------------------------------------------------------------------\n"
        )
        cog.logger.exception(f"GiftOps: UNEXPECTED Error claiming code {giftcode} for ID {player_id}. Details logged.")
        try:
            cog.giftlog.error(log_message.strip())
        except Exception as log_e: cog.logger.exception(f"GiftOps: CRITICAL - Failed to write unexpected error log: {log_e}")
        status = "ERROR"

    finally:
        if session:
            session.close()
        process_end_time = time.time()
        duration = process_end_time - process_start_time
        cog.processing_stats["total_fids_processed"] += 1
        cog.processing_stats["total_processing_time"] += duration
        cog.logger.info(f"GiftOps: claim_giftcode_rewards_wos completed for ID {player_id}. Status: {status}, Duration: {duration:.3f}s")

    # Image save handling
    if image_bytes and cog.captcha_solver and cog.captcha_solver.save_images_mode > 0:
        save_mode = cog.captcha_solver.save_images_mode
        should_save = False
        filename_base = None
        log_prefix = ""

        is_success = status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]
        is_fail_server = status == "CAPTCHA_INVALID"

        if is_success and save_mode in [2, 3]:
            should_save = True
            log_prefix = f"Captcha OK (Solver: {method})"
            solved_code_str = captcha_code if captcha_code else "UNKNOWN_SOLVE"
            filename_base = f"{solved_code_str}.png"
        elif is_fail_server and save_mode in [1, 3]:
            should_save = True
            log_prefix = f"Captcha Fail Server (Solver: {method} -> {status})"
            solved_code_str = captcha_code if captcha_code else "UNKNOWN_SENT"
            timestamp = int(time.time())
            filename_base = f"FAIL_SERVER_{solved_code_str}_{timestamp}.png"

        if should_save and filename_base:
            try:
                save_path = os.path.join(cog.captcha_solver.captcha_dir, filename_base)
                counter = 1
                base, ext = os.path.splitext(filename_base)
                while os.path.exists(save_path) and counter <= 100:
                    save_path = os.path.join(cog.captcha_solver.captcha_dir, f"{base}_{counter}{ext}")
                    counter += 1

                if counter > 100:
                    cog.logger.warning(f"Could not find unique filename for {filename_base} after 100 tries. Discarding image.")
                else:
                    with open(save_path, "wb") as f:
                        f.write(image_bytes)
                    cog.logger.info(f"GiftOps: {log_prefix} - Saved captcha image as {os.path.basename(save_path)}")

            except Exception as save_err:
                cog.logger.exception(f"GiftOps: Error saving captcha image ({filename_base}): {save_err}")

    cog.logger.info(f"GiftOps: Final status for ID {player_id} / Code '{giftcode}': {status}")
    return status


async def scan_historical_messages(cog, channel: discord.TextChannel, alliance_id: int) -> dict:
    """Scan historical messages in a channel for gift codes with consolidated results.

    Args:
        channel: The Discord channel to scan
        alliance_id: The alliance ID for this channel

    Returns:
        dict: Scan results with detailed breakdown
    """
    try:
        fetch_limit = 75  # Limit to prevent excessive scanning

        cog.logger.info(f"Scanning historical messages in channel {channel.id} for alliance {alliance_id}")

        # Collect messages to process
        messages_to_process = []
        async for message in channel.history(limit=fetch_limit, oldest_first=False):
            # Skip bot messages and empty messages
            if message.author == cog.bot.user or not message.content:
                continue

            # Check if we've already reacted to this message
            bot_reactions = {str(reaction.emoji) for reaction in message.reactions if reaction.me}
            if bot_reactions.intersection([f"{theme.verifiedIcon}", f"{theme.deniedIcon}", f"{theme.warnIcon}", f"{theme.questionIcon}", f"{theme.infoIcon}"]):
                continue

            messages_to_process.append(message)

        cog.logger.info(f"Found {len(messages_to_process)} messages to process")

        # Results tracking
        scan_results = {
            'total_codes_found': 0,
            'new_codes': [],
            'existing_valid': [],
            'existing_invalid': [],
            'existing_pending': [],
            'validation_results': {},
            'messages_scanned': len(messages_to_process)
        }

        # Process each message and collect codes
        codes_to_validate = []
        message_code_map = {}

        for message in messages_to_process:
            content = message.content.strip()
            giftcode = None

            # Check for gift code patterns
            if len(content.split()) == 1:
                if re.match(r'^[a-zA-Z0-9]+$', content):
                    giftcode = content
            else:
                code_match = re.search(r'Code:\s*(\S+)', content, re.IGNORECASE)
                if code_match:
                    potential_code = code_match.group(1)
                    if re.match(r'^[a-zA-Z0-9]+$', potential_code):
                        giftcode = potential_code

            if giftcode:
                giftcode = cog.clean_gift_code(giftcode)
                scan_results['total_codes_found'] += 1
                message_code_map[giftcode] = message

                # Check if code already exists
                cog.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,))
                result = cog.cursor.fetchone()

                if result:
                    # Code exists, categorize by status
                    status = result[0]
                    if status == 'validated':
                        scan_results['existing_valid'].append(giftcode)
                    elif status == 'invalid':
                        scan_results['existing_invalid'].append(giftcode)
                    else:
                        scan_results['existing_pending'].append(giftcode)
                else:
                    # New code found - will need validation
                    scan_results['new_codes'].append(giftcode)
                    codes_to_validate.append(giftcode)

        # Validate new codes in batch without individual messages
        if codes_to_validate:
            cog.logger.info(f"Validating {len(codes_to_validate)} new codes from history scan")

            for giftcode in codes_to_validate:
                # Add to database first
                cog.cursor.execute("""
                    INSERT OR IGNORE INTO gift_codes (giftcode, alliance_id, validation_status, created_at)
                    VALUES (?, ?, 'pending', ?)
                """, (giftcode, alliance_id, datetime.now().isoformat()))
                cog.conn.commit()

                # Validate the code silently (no individual messages)
                is_valid = await _validate_gift_code_silent(cog, giftcode)

                # Update database with result
                new_status = 'validated' if is_valid else 'invalid'
                cog.cursor.execute("""
                    UPDATE gift_codes
                    SET validation_status = ?
                    WHERE giftcode = ?
                """, (new_status, giftcode))
                cog.conn.commit()

                # Store validation result
                scan_results['validation_results'][giftcode] = is_valid

                # Add appropriate reaction to message
                if giftcode in message_code_map:
                    message = message_code_map[giftcode]
                    emoji = f"{theme.verifiedIcon}" if is_valid else f"{theme.deniedIcon}"
                    await message.add_reaction(emoji)

                # Small delay between validations
                await asyncio.sleep(1.0)

        # Add reactions to existing codes
        for giftcode in scan_results['existing_valid']:
            if giftcode in message_code_map:
                await message_code_map[giftcode].add_reaction(f"{theme.verifiedIcon}")

        for giftcode in scan_results['existing_invalid']:
            if giftcode in message_code_map:
                await message_code_map[giftcode].add_reaction(f"{theme.deniedIcon}")

        for giftcode in scan_results['existing_pending']:
            if giftcode in message_code_map:
                await message_code_map[giftcode].add_reaction(f"{theme.warnIcon}")

        # Send consolidated results message
        await _send_scan_results_message(cog, channel, scan_results, alliance_id)

        cog.logger.info(f"History scan complete. Results: {scan_results}")
        return scan_results

    except Exception as e:
        cog.logger.exception(f"Error scanning historical messages: {e}")
        return {'total_codes_found': 0, 'messages_scanned': 0}


async def _validate_gift_code_silent(cog, giftcode: str) -> bool:
    """Validate a gift code silently without sending Discord messages.

    Args:
        giftcode: The gift code to validate

    Returns:
        bool: True if valid, False if invalid
    """
    try:
        # Use the existing validate_gift_code_immediately function
        is_valid, validation_msg = await validate_gift_code_immediately(cog, giftcode, "historical_scan")
        return is_valid
    except Exception as e:
        cog.logger.exception(f"Error in silent validation for {giftcode}: {e}")
        return False


async def _send_scan_results_message(cog, channel: discord.TextChannel, results: dict, alliance_id: int):
    """Send a consolidated scan results message to the channel.

    Args:
        channel: The Discord channel to send the message to
        results: The scan results dictionary
        alliance_id: The alliance ID
    """
    try:
        # Get alliance name
        cog.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
        alliance_result = cog.alliance_cursor.fetchone()
        alliance_name = alliance_result[0] if alliance_result else f"Alliance {alliance_id}"

        # Build results embed
        embed = discord.Embed(
            title=f"{theme.searchIcon} History Scan Results",
            description=f"**Alliance:** {alliance_name}\n**Channel:** #{channel.name}",
            color=theme.emColor1
        )

        # Summary stats
        total_found = results['total_codes_found']
        messages_scanned = results['messages_scanned']

        embed.add_field(
            name=f"{theme.chartIcon} Scan Summary",
            value=f"**Messages Scanned:** {messages_scanned}\n**Total Codes Found:** {total_found}",
            inline=False
        )

        # New codes validation results
        if results['new_codes']:
            new_valid = [code for code, is_valid in results['validation_results'].items() if is_valid]
            new_invalid = [code for code, is_valid in results['validation_results'].items() if not is_valid]

            validation_text = ""
            if new_valid:
                validation_text += f"{theme.verifiedIcon} **Valid Codes ({len(new_valid)}):**\n"
                for code in new_valid[:5]: # Limit display to avoid message length issues
                    validation_text += f"  • `{code}`\n"
                if len(new_valid) > 5:
                    validation_text += f"  • ... and {len(new_valid) - 5} more\n"
                validation_text += "\n"

            if new_invalid:
                validation_text += f"{theme.deniedIcon} **Invalid Codes ({len(new_invalid)}):**\n"
                for code in new_invalid[:5]:
                    validation_text += f"  • `{code}`\n"
                if len(new_invalid) > 5:
                    validation_text += f"  • ... and {len(new_invalid) - 5} more\n"

            if validation_text:
                embed.add_field(
                    name=f"{theme.newIcon} New Codes Validated",
                    value=validation_text,
                    inline=False
                )

        # Existing codes summary
        existing_summary = ""
        if results['existing_valid']:
            existing_summary += f"{theme.verifiedIcon} Previously Valid: {len(results['existing_valid'])}\n"
        if results['existing_invalid']:
            existing_summary += f"{theme.deniedIcon} Previously Invalid: {len(results['existing_invalid'])}\n"
        if results['existing_pending']:
            existing_summary += f"{theme.warnIcon} Pending Validation: {len(results['existing_pending'])}\n"

        if existing_summary:
            embed.add_field(
                name=f"{theme.listIcon} Previously Found Codes",
                value=existing_summary,
                inline=False
            )

        # Add footer
        embed.set_footer(text="History scan complete. Check message reactions for individual code status.")

        # Send the message
        await channel.send(embed=embed)

    except Exception as e:
        cog.logger.exception(f"Error sending scan results message: {e}")


async def cleanup_old_invalid_codes(cog):
    """Remove invalid gift codes older than 7 days from the database."""
    try:
        # Calculate the cutoff date (7 days ago)
        cutoff_date = (datetime.now() - timedelta(days=7)).isoformat()

        # Get count of codes that will be deleted for logging
        cog.cursor.execute("""
            SELECT COUNT(*) FROM gift_codes
            WHERE validation_status = 'invalid'
            AND date < ?
        """, (cutoff_date,))
        delete_count = cog.cursor.fetchone()[0]

        if delete_count > 0:
            # Delete old invalid codes
            cog.cursor.execute("""
                DELETE FROM gift_codes
                WHERE validation_status = 'invalid'
                AND date < ?
            """, (cutoff_date,))

            # Also clean up any related user_giftcodes entries for deleted codes
            cog.cursor.execute("""
                DELETE FROM user_giftcodes
                WHERE giftcode NOT IN (SELECT giftcode FROM gift_codes)
            """)

            cog.conn.commit()
            cog.logger.info(f"Cleaned up {delete_count} invalid gift codes older than 7 days")
        else:
            cog.logger.info("No old invalid gift codes found for cleanup")

    except Exception as e:
        cog.logger.exception(f"Error during invalid codes cleanup: {e}")


async def periodic_validation_loop_body(cog):
    """Body of the periodic validation loop. Called from the @tasks.loop on the cog."""
    loop_start_time = datetime.now()
    cog.logger.info(f"\nGiftOps: periodic_validation_loop running at {loop_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Check if we need to run daily cleanup (once per day)
        current_date = loop_start_time.date()
        if cog._last_cleanup_date != current_date:
            cog.logger.info("Running daily cleanup of old invalid gift codes...")
            await cleanup_old_invalid_codes(cog)
            cog._last_cleanup_date = current_date

        # Check if validation is already in progress to avoid conflicts
        async with cog._validation_lock:
            # Get codes that need validation (pending or validated)
            cog.cursor.execute("""
                SELECT giftcode, validation_status
                FROM gift_codes
                WHERE validation_status IN ('pending', 'validated')
            """)
            codes_to_check = cog.cursor.fetchall()

            if not codes_to_check:
                cog.logger.info("GiftOps: No codes need periodic validation.")
                return

            cog.logger.info(f"GiftOps: Found {len(codes_to_check)} codes to validate periodically.")

            # Get test ID for validation
            test_fid, fid_source = await cog.get_validation_fid()
            cog.logger.info(f"GiftOps: Using {fid_source} ID {test_fid} for periodic validation.")

            codes_checked = 0
            codes_invalidated = 0
            codes_still_valid = 0

            for giftcode, current_status in codes_to_check:
                # Skip if we've checked too many codes (to prevent long-running loops)
                if codes_checked >= 20:
                    cog.logger.info("GiftOps: Reached periodic validation limit of 20 codes per run.")
                    break

                try:
                    cog.logger.info(f"GiftOps: Periodically validating code '{giftcode}' (current status: {current_status})")

                    # Check the code with test ID
                    status = await claim_giftcode_rewards_wos(cog, test_fid, giftcode)
                    codes_checked += 1

                    if status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]: # Code is now invalid
                        cog.logger.info(f"GiftOps: Code '{giftcode}' is now invalid (status: {status}). Updating database.")

                        cog.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (giftcode,))
                        # Clear redemption status for the test fid
                        cog.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ? AND fid = ?", (giftcode, test_fid))
                        cog.conn.commit()

                        codes_invalidated += 1

                        # Remove from API if present
                        if hasattr(cog, 'api') and cog.api:
                            asyncio.create_task(cog.api.remove_giftcode(giftcode, from_validation=True))

                        # Notify admins about invalidated code
                        cog.settings_cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
                        admin_ids = [row[0] for row in cog.settings_cursor.fetchall()]

                        for admin_id in admin_ids:
                            try:
                                admin_user = await cog.bot.fetch_user(admin_id)
                                if admin_user:
                                    embed = discord.Embed(
                                        title=f"{theme.deniedIcon} Gift Code Invalidated",
                                        description=f"Code `{giftcode}` has been invalidated during periodic validation.\nStatus: {status}",
                                        color=theme.emColor2,
                                        timestamp=datetime.now()
                                    )
                                    await admin_user.send(embed=embed)
                            except Exception as e:
                                cog.logger.exception(f"Error notifying admin {admin_id}: {e}")

                    elif status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE", "TOO_SMALL_SPEND_MORE", "TOO_POOR_SPEND_MORE"]:
                        codes_still_valid += 1

                        if current_status == 'pending':
                            cog.logger.info(f"GiftOps: Code '{giftcode}' confirmed valid. Updating status to 'validated'.")
                            cog.cursor.execute("UPDATE gift_codes SET validation_status = 'validated' WHERE giftcode = ? AND validation_status = 'pending'", (giftcode,))
                            cog.conn.commit()

                            if hasattr(cog, 'api') and cog.api:
                                asyncio.create_task(cog.api.add_giftcode(giftcode))

                            try:
                                await cog._execute_with_retry(
                                    lambda: cog.cursor.execute("SELECT alliance_id FROM giftcodecontrol WHERE status = 1 ORDER BY priority ASC, alliance_id ASC")
                                )
                                auto_alliances = cog.cursor.fetchall() or []
                            except sqlite3.OperationalError as e:
                                error_msg = f"Auto-alliance query failed after retries for code '{giftcode}': {e}"
                                cog.logger.error(error_msg)
                                print(f"ERROR: {error_msg}")
                                auto_alliances = []
                            except Exception as e:
                                error_msg = f"Unexpected error in auto-alliance query for code '{giftcode}': {e}"
                                cog.logger.error(error_msg)
                                print(f"ERROR: {error_msg}")
                                auto_alliances = []

                            if auto_alliances:
                                cog.logger.info(f"GiftOps: Triggering delayed auto-redemption for code '{giftcode}' to {len(auto_alliances)} alliances")

                                for alliance in auto_alliances:
                                    try:
                                        await enqueue_redemption(
                                            cog,
                                            giftcode=giftcode,
                                            alliance_id=alliance[0],
                                            source='periodic-auto',
                                        )
                                    except Exception as e:
                                        cog.logger.exception(f"Error queueing delayed auto-redemption for code {giftcode} to alliance {alliance[0]}: {e}")

                                cog.settings_cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
                                admin_ids = [row[0] for row in cog.settings_cursor.fetchall()]

                                for admin_id in admin_ids:
                                    try:
                                        admin_user = await cog.bot.fetch_user(admin_id)
                                        if admin_user:
                                            embed = discord.Embed(
                                                title=f"{theme.verifiedIcon} Auto-Redemption Started",
                                                description=f"Code `{giftcode}` has been validated and auto-redemption is now starting for {len(auto_alliances)} alliance(s).",
                                                color=theme.emColor3,
                                                timestamp=datetime.now()
                                            )
                                            await admin_user.send(embed=embed)
                                    except Exception as e:
                                        cog.logger.exception(f"Error notifying admin {admin_id} about delayed auto-redemption: {e}")

                    else:
                        cog.logger.info(f"GiftOps: Code '{giftcode}' returned status '{status}' during periodic validation.")

                        # Extra delay for CAPTCHA_TOO_FREQUENT errors
                        if status == "CAPTCHA_TOO_FREQUENT":
                            cog.logger.info(f"GiftOps: Encountered CAPTCHA_TOO_FREQUENT, waiting 60-90 seconds before next validation")
                            await asyncio.sleep(random.uniform(60.0, 90.0))
                            continue

                    # Wait between validations to avoid rate limiting
                    await asyncio.sleep(random.uniform(30.0, 60.0))

                except Exception as e:
                    cog.logger.exception(f"Error validating code '{giftcode}' during periodic check: {e}")
                    await asyncio.sleep(5) # Longer wait on error

            cog.logger.info(f"GiftOps: Periodic validation complete. Checked: {codes_checked}, Invalidated: {codes_invalidated}, Still valid: {codes_still_valid}")

        loop_end_time = datetime.now()
        cog.logger.info(f"GiftOps: periodic_validation_loop finished at {loop_end_time.strftime('%Y-%m-%d %H:%M:%S')}. Duration: {loop_end_time - loop_start_time}\n")

    except Exception as e:
        cog.logger.exception(f"GiftOps: Error in periodic_validation_loop: {str(e)}")
        # Wait before next attempt to avoid rapid error loops
        await asyncio.sleep(60)


async def before_periodic_validation_loop_body(cog):
    """Body of the before_loop for periodic validation. Called from the cog's before_loop."""
    cog.logger.info("GiftOps: Waiting for bot to be ready before starting periodic_validation_loop...")
    await cog.bot.wait_until_ready()
    cog.logger.info("GiftOps: Bot is ready, periodic_validation_loop will start.")


async def fetch_captcha(cog, player_id, session=None):
    """Fetch a captcha image for a player ID."""
    owns_session = session is None
    if owns_session:
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=cog.retry_config))
        session.headers.update(get_headers(cog.wos_giftcode_redemption_url))

    data_to_encode = {
        "fid": player_id,
        "time": f"{int(datetime.now().timestamp() * 1000)}",
        "init": "0"
    }
    data = encode_data(cog, data_to_encode)

    try:
        response = await asyncio.to_thread(
            session.post,
            cog.wos_captcha_url,
            data=data,
            timeout=(10, 30),
        )

        if response.status_code == 200:
            captcha_data = response.json()
            if captcha_data.get("code") == 1 and captcha_data.get("msg") == "CAPTCHA GET TOO FREQUENT.":
                return None, "CAPTCHA_TOO_FREQUENT"

            if "data" in captcha_data and "img" in captcha_data["data"]:
                return captcha_data["data"]["img"], None

        return None, "CAPTCHA_FETCH_ERROR"
    except Exception as e:
        cog.logger.exception(f"Error fetching captcha: {e}")
        return None, f"CAPTCHA_EXCEPTION: {str(e)}"
    finally:
        if owns_session:
            session.close()


async def validate_gift_codes(cog):
    try:
        cog.cursor.execute("SELECT giftcode, validation_status FROM gift_codes WHERE validation_status != 'invalid'")
        all_codes = cog.cursor.fetchall()

        cog.settings_cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
        admin_ids = [row[0] for row in cog.settings_cursor.fetchall()]

        if not all_codes:
            cog.logger.info("[validate_gift_codes] No codes found needing validation.")
            return

        for giftcode, current_db_status in all_codes:
            if current_db_status == 'invalid':
                cog.logger.info(f"[validate_gift_codes] Skipping already invalid code: {giftcode}")
                continue

            cog.logger.info(f"[validate_gift_codes] Validating code: {giftcode} (current DB status: {current_db_status})")
            test_fid = cog.get_test_fid()
            status = await claim_giftcode_rewards_wos(cog, test_fid, giftcode)

            if status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                cog.logger.info(f"[validate_gift_codes] Code {giftcode} found to be invalid with status: {status}. Updating DB.")

                cog.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (giftcode,))
                test_fid = cog.get_test_fid()
                cog.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ? AND fid = ?", (giftcode, test_fid))
                cog.conn.commit()

                if hasattr(cog, 'api') and cog.api:
                    asyncio.create_task(cog.api.remove_giftcode(giftcode, from_validation=True))

                reason_map = {
                    "TIME_ERROR": "Code has expired (TIME_ERROR)",
                    "CDK_NOT_FOUND": "Code not found or incorrect (CDK_NOT_FOUND)",
                    "USAGE_LIMIT": "Usage limit reached (USAGE_LIMIT)"
                }
                detailed_reason = reason_map.get(status, f"Code invalid ({status})")

                admin_embed = discord.Embed(
                    title=f"{theme.giftIcon} Gift Code Invalidated",
                    description=(
                        f"**Gift Code Details**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
                        f"{theme.deniedIcon} **Status:** {detailed_reason}\n"
                        f"{theme.editListIcon} **Action:** Code marked as invalid in database\n"
                        f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                        f"{theme.lowerDivider}\n"
                    ),
                    color=discord.Color.orange()
                )

                for admin_id in admin_ids:
                    try:
                        admin_user = await cog.bot.fetch_user(admin_id)
                        if admin_user:
                            await admin_user.send(embed=admin_embed)
                    except Exception as e:
                        cog.logger.exception(f"Error sending message to admin {admin_id}: {str(e)}")

            elif status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE", "TOO_SMALL_SPEND_MORE", "TOO_POOR_SPEND_MORE"] and current_db_status == 'pending':
                cog.logger.info(f"[validate_gift_codes] Code {giftcode} confirmed valid. Updating status to 'validated'.")
                cog.cursor.execute("UPDATE gift_codes SET validation_status = 'validated' WHERE giftcode = ? AND validation_status = 'pending'", (giftcode,))
                cog.conn.commit()

                if hasattr(cog, 'api') and cog.api:
                    asyncio.create_task(cog.api.add_giftcode(giftcode))

            await asyncio.sleep(60)

    except Exception as e:
        cog.logger.exception(f"Error in validate_gift_codes: {str(e)}")


async def use_giftcode_for_alliance(cog, alliance_id, giftcode):
    MEMBER_PROCESS_DELAY = 1.0
    API_RATE_LIMIT_COOLDOWN = 60.0
    CAPTCHA_CYCLE_COOLDOWN = 60.0
    MAX_RETRY_CYCLES = 10

    cog.logger.info(f"\nGiftOps: Starting use_giftcode_for_alliance for Alliance {alliance_id}, Code {giftcode}")

    try:
        # Initialize error tracking for summary
        error_summary = {}

        # Initial Setup (Get channel, alliance name)
        cog.alliance_cursor.execute("SELECT channel_id FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
        channel_result = cog.alliance_cursor.fetchone()
        cog.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
        name_result = cog.alliance_cursor.fetchone()

        if not channel_result or not name_result:
            cog.logger.error(f"GiftOps: Could not find channel or name for alliance {alliance_id}.")
            return False

        channel_id, alliance_name = channel_result[0], name_result[0]
        channel = cog.bot.get_channel(channel_id)

        if not channel:
            cog.logger.error(f"GiftOps: Bot cannot access channel {channel_id} for alliance {alliance_name}.")
            return False

        # Check if OCR is enabled
        cog.settings_cursor.execute("SELECT enabled FROM ocr_settings ORDER BY id DESC LIMIT 1")
        ocr_settings_row = cog.settings_cursor.fetchone()
        ocr_enabled = ocr_settings_row[0] if ocr_settings_row else 0

        if not (ocr_enabled == 1 and cog.captcha_solver):
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon} OCR/Captcha Solver Disabled",
                description=(
                    f"**Gift Code:** `{giftcode}`\n"
                    f"**Alliance:** `{alliance_name}`\n\n"
                    f"{theme.warnIcon} Gift code redemption requires the OCR/captcha solver to be enabled.\n"
                    f"Please enable it first using the settings command."
                ),
                color=theme.emColor2
            )
            await channel.send(embed=error_embed)
            cog.logger.info(f"GiftOps: Skipping alliance {alliance_id} - OCR disabled or solver not ready")
            return False

        # Check if this code has been validated before
        cog.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,))
        master_code_status_row = cog.cursor.fetchone()
        master_code_status = master_code_status_row[0] if master_code_status_row else None
        final_invalid_reason_for_embed = None

        if master_code_status == 'invalid':
            cog.logger.info(f"GiftOps: Code {giftcode} is already marked as 'invalid' in the database.")
            final_invalid_reason_for_embed = "Code previously marked as invalid"
        else:
            # If not marked 'invalid' in master table, check with test ID if status is 'pending' or for other cached issues
            test_fid = cog.get_test_fid()
            cog.cursor.execute("SELECT status FROM user_giftcodes WHERE fid = ? AND giftcode = ?", (test_fid, giftcode))
            validation_fid_status_row = cog.cursor.fetchone()

            if validation_fid_status_row:
                fid_status = validation_fid_status_row[0]
                if fid_status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                    cog.logger.info(f"GiftOps: Code {giftcode} known to be invalid via test ID (status: {fid_status}). Marking invalid.")
                    mark_code_invalid(cog, giftcode)
                    if hasattr(cog, 'api') and cog.api:
                        asyncio.create_task(cog.api.remove_giftcode(giftcode, from_validation=True))

                    reason_map_fid = {
                        "TIME_ERROR": "Code has expired (TIME_ERROR)",
                        "CDK_NOT_FOUND": "Code not found or incorrect (CDK_NOT_FOUND)",
                        "USAGE_LIMIT": "Usage limit reached (USAGE_LIMIT)"
                    }
                    final_invalid_reason_for_embed = reason_map_fid.get(fid_status, f"Code invalid ({fid_status})")

        if final_invalid_reason_for_embed:
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon} Gift Code Invalid",
                description=(
                    f"**Gift Code Details**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
                    f"{theme.allianceIcon} **Alliance:** `{alliance_name}`\n"
                    f"{theme.deniedIcon} **Status:** {final_invalid_reason_for_embed}\n"
                    f"{theme.editListIcon} **Action:** Code status is 'invalid' in database\n"
                    f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                    f"{theme.lowerDivider}\n"
                ),
                color=theme.emColor2
            )
            await channel.send(embed=error_embed)
            return False

        # Get Members
        with sqlite3.connect('db/users.sqlite') as users_conn:
            users_cursor = users_conn.cursor()
            users_cursor.execute("SELECT fid, nickname FROM users WHERE alliance = ?", (str(alliance_id),))
            members = users_cursor.fetchall()
        if not members:
            cog.logger.info(f"GiftOps: No members found for alliance {alliance_id} ({alliance_name}).")
            return False

        total_members = len(members)
        cog.logger.info(f"GiftOps: Found {total_members} members for {alliance_name}.")

        # Initialize State
        processed_count = 0
        success_count = 0
        received_count = 0
        failed_count = 0
        successful_users = []
        already_used_users = []
        failed_users_dict = {}

        retry_queue = []
        active_members_to_process = []

        # Batch Processing
        batch_results = []
        batch_size = 10

        # Check Cache & Populate Initial List
        member_ids = [m[0] for m in members]
        cached_member_statuses = batch_get_user_giftcode_status(cog, giftcode, member_ids)

        for fid, nickname in members:
            if fid in cached_member_statuses:
                status = cached_member_statuses[fid]
                if status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
                    received_count += 1
                    already_used_users.append(nickname)
                processed_count += 1
            else:
                active_members_to_process.append((fid, nickname, 0))
        cog.logger.info(f"GiftOps: Pre-processed {len(cached_member_statuses)} members from cache. {len(active_members_to_process)} remaining.")

        # Progress Embed
        embed = discord.Embed(title=f"{theme.giftIcon} Gift Code Redemption: {giftcode}", color=theme.emColor1)
        def update_embed_description(include_errors=False):
            base_description = (
                f"**Status for Alliance:** `{alliance_name}`\n"
                f"{theme.upperDivider}\n"
                f"{theme.membersIcon} **Total Members:** `{total_members}`\n"
                f"{theme.verifiedIcon} **Success:** `{success_count}`\n"
                f"{theme.infoIcon} **Already Redeemed:** `{received_count}`\n"
                f"{theme.refreshIcon} **Retrying:** `{len(retry_queue)}`\n"
                f"{theme.deniedIcon} **Failed:** `{failed_count}`\n"
                f"{theme.hourglassIcon} **Processed:** `{processed_count}/{total_members}`\n"
                f"{theme.lowerDivider}\n"
            )

            if include_errors and failed_count > 0:
                non_success_errors = {k: v for k, v in error_summary.items() if k != "SUCCESS"}
                if non_success_errors:
                    # Define user-friendly messages for each error type
                    error_descriptions = {
                        "TOO_POOR_SPEND_MORE": f"{theme.warnIcon} **" + "{count}" + "** members failed to spend enough to reach VIP12.",
                        "TOO_SMALL_SPEND_MORE": f"{theme.warnIcon} **" + "{count}" + "** members failed due to insufficient furnace level.",
                        "TIMEOUT_RETRY": f"{theme.timeIcon} **" + "{count}" + "** members were staring into the void, until the void finally timed out on them.",
                        "LOGIN_EXPIRED_MID_PROCESS": f"{theme.lockIcon} **" + "{count}" + "** members login failed mid-process. How'd that even happen?",
                        "LOGIN_FAILED": f"{theme.lockIcon} **" + "{count}" + "** members failed due to login issues. Try logging it off and on again!",
                        "CAPTCHA_SOLVING_FAILED": f"{theme.robotIcon} **" + "{count}" + "** members lost the battle against CAPTCHA. You sure those weren't just bots?",
                        "CAPTCHA_SOLVER_ERROR": f"{theme.settingsIcon} **" + "{count}" + "** members failed due to a CAPTCHA solver issue. We're still trying to solve that one.",
                        "OCR_DISABLED": f"{theme.deniedIcon} **" + "{count}" + "** members failed since OCR is disabled. Try turning it on first!",
                        "SIGN_ERROR": f"{theme.lockIcon} **" + "{count}" + "** members failed due to a signature error. Something went wrong.",
                        "ERROR": f"{theme.deniedIcon} **" + "{count}" + "** members failed due to a general error. Might want to check the logs.",
                        "UNKNOWN_API_RESPONSE": f"{theme.infoIcon} **" + "{count}" + "** members failed with an unknown API response. Say what?",
                        "CONNECTION_ERROR": f"{theme.globeIcon} **" + "{count}" + "** members failed due to bot connection issues. Did the admin trip over the cable again?"
                    }

                    base_description += "\n**Error Breakdown:**\n"

                    # Build message for each error type
                    for error_type, count in sorted(non_success_errors.items(), key=lambda x: x[1], reverse=True):
                        if error_type in error_descriptions:
                            base_description += error_descriptions[error_type].format(count=count) + "\n"
                        else:
                            # Handle any unexpected error types
                            base_description += f"❗ **{count}** members failed with status: {error_type}\n"

            return base_description
        embed.description = update_embed_description()
        try: status_message = await channel.send(embed=embed)
        except Exception as e: cog.logger.exception(f"GiftOps: Error sending initial status embed: {e}"); return False

        # Main Processing Loop
        last_embed_update = time.time()
        code_is_invalid = False

        # Cooperative preemption: yield to higher-priority work between players
        process_queue_cog = cog.bot.get_cog('ProcessQueue')

        while active_members_to_process or retry_queue:
            if code_is_invalid:
                cog.logger.info(f"GiftOps: Code {giftcode} detected as invalid, stopping redemption.")
                break

            # On preempt, only terminal statuses in batch_results are persisted;
            # retry_queue and unfinalised failed_users_dict are dropped and
            # re-attempted from scratch on resume (DB dedup keeps correctness).
            if process_queue_cog and process_queue_cog.should_preempt():
                cog.logger.info(
                    f"GiftOps: Preempting redemption for {alliance_name} - higher priority work waiting "
                    f"(pending retry_queue={len(retry_queue)}, unfinalised failed={len(failed_users_dict)}, "
                    f"remaining active={len(active_members_to_process)})"
                )
                if batch_results:
                    batch_process_alliance_results(cog, batch_results)
                    batch_results = []
                raise PreemptedException()

            current_time = time.time()

            # Dequeue Ready Retries
            ready_to_retry = []
            remaining_in_queue = []
            for item in retry_queue:
                if current_time >= item[3]:
                    ready_to_retry.append(item[:3])
                else:
                    remaining_in_queue.append(item)
            retry_queue = remaining_in_queue
            active_members_to_process.extend(ready_to_retry)

            if not active_members_to_process:
                if retry_queue:
                    next_retry_ts = min(item[3] for item in retry_queue)
                    wait_time = max(0.1, next_retry_ts - current_time)
                    await asyncio.sleep(wait_time)
                else:
                    break
                continue

            # Process One Member
            fid, nickname, current_cycle_count = active_members_to_process.pop(0)

            cog.logger.info(f"GiftOps: Processing ID {fid} ({nickname}), Cycle {current_cycle_count + 1}/{MAX_RETRY_CYCLES}")

            response_status = "ERROR"
            try:
                await asyncio.sleep(random.uniform(MEMBER_PROCESS_DELAY * 0.7, MEMBER_PROCESS_DELAY * 1.3))
                response_status = await claim_giftcode_rewards_wos(cog, fid, giftcode)
            except Exception as claim_err:
                cog.logger.exception(f"GiftOps: Unexpected error during claim for {fid}: {claim_err}")
                response_status = "ERROR"

            # Check if code is invalid
            if response_status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                code_is_invalid = True
                cog.logger.info(f"GiftOps: Code {giftcode} became invalid (status: {response_status}) while processing {fid}. Marking as invalid in DB.")

                # Mark as invalid
                mark_code_invalid(cog, giftcode)

                if hasattr(cog, 'api') and cog.api:
                    asyncio.create_task(cog.api.remove_giftcode(giftcode, from_validation=True))

                reason_map_runtime = {
                    "TIME_ERROR": "Code has expired (TIME_ERROR)",
                    "CDK_NOT_FOUND": "Code not found or incorrect (CDK_NOT_FOUND)",
                    "USAGE_LIMIT": "Usage limit reached (USAGE_LIMIT)"
                }
                status_reason_runtime = reason_map_runtime.get(response_status, f"Code invalid ({response_status})")

                embed.title = f"{theme.deniedIcon} Gift Code Invalid: {giftcode}"
                embed.color = discord.Color.red()
                embed.description = (
                    f"**Gift Code Redemption Halted**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
                    f"{theme.allianceIcon} **Alliance:** `{alliance_name}`\n"
                    f"{theme.deniedIcon} **Reason:** {status_reason_runtime}\n"
                    f"{theme.editListIcon} **Action:** Code marked as invalid in database. Remaining members for this alliance will not be processed.\n"
                    f"{theme.chartIcon} **Processed before halt:** {processed_count}/{total_members}\n"
                    f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                    f"{theme.lowerDivider}\n"
                )
                embed.clear_fields()

                try:
                    await status_message.edit(embed=embed)
                except Exception as embed_edit_err:
                    cog.logger.warning(f"GiftOps: Failed to update progress embed to show code invalidation: {embed_edit_err}")

                if fid not in failed_users_dict:
                    processed_count +=1
                    failed_count +=1
                    failed_users_dict[fid] = (nickname, f"Led to code invalidation ({response_status})", current_cycle_count + 1)
                continue

            if response_status == "SIGN_ERROR":
                cog.logger.error(f"GiftOps: Sign error detected (likely wrong encrypt key). Stopping redemption for alliance {alliance_id}.")

                embed.title = f"{theme.settingsIcon} Sign Error: {giftcode}"
                embed.color = discord.Color.red()
                embed.description = (
                    f"**Bot Configuration Error**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
                    f"{theme.allianceIcon} **Alliance:** `{alliance_name}`\n"
                    f"{theme.settingsIcon} **Reason:** Sign Error (check bot config/encrypt key)\n"
                    f"{theme.editListIcon} **Action:** Redemption stopped. Check bot configuration.\n"
                    f"{theme.chartIcon} **Processed before halt:** {processed_count}/{total_members}\n"
                    f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                    f"{theme.lowerDivider}\n"
                )
                embed.clear_fields()

                try:
                    await status_message.edit(embed=embed)
                except Exception as embed_edit_err:
                    cog.logger.warning(f"GiftOps: Failed to update progress embed for sign error: {embed_edit_err}")

                break

            # Handle Response
            mark_processed = False
            add_to_failed = False
            queue_for_retry = False
            retry_delay = 0

            if response_status == "SUCCESS":
                success_count += 1
                successful_users.append(nickname)
                batch_results.append((fid, giftcode, response_status))
                mark_processed = True
            elif response_status in ["RECEIVED", "SAME TYPE EXCHANGE"]:
                received_count += 1
                already_used_users.append(nickname)
                batch_results.append((fid, giftcode, response_status))
                mark_processed = True
            elif response_status == "OCR_DISABLED":
                add_to_failed = True
                mark_processed = True
                fail_reason = "OCR Disabled"
                error_summary["OCR_DISABLED"] = error_summary.get("OCR_DISABLED", 0) + 1
            elif response_status in ["SOLVER_ERROR", "CAPTCHA_FETCH_ERROR"]:
                add_to_failed = True
                mark_processed = True
                fail_reason = f"Solver Error ({response_status})"
                error_summary["CAPTCHA_SOLVER_ERROR"] = error_summary.get("CAPTCHA_SOLVER_ERROR", 0) + 1
            elif response_status in ["LOGIN_FAILED", "LOGIN_EXPIRED_MID_PROCESS", "ERROR", "UNKNOWN_API_RESPONSE"]:
                add_to_failed = True
                mark_processed = True
                fail_reason = f"Processing Error ({response_status})"
                error_summary[response_status] = error_summary.get(response_status, 0) + 1
            elif response_status == "TIMEOUT_RETRY":
                queue_for_retry = True
                retry_delay = API_RATE_LIMIT_COOLDOWN
                fail_reason = "API Rate Limited"
                if current_cycle_count + 1 >= MAX_RETRY_CYCLES: # Track as error if this is the final attempt
                    error_summary["TIMEOUT_RETRY"] = error_summary.get("TIMEOUT_RETRY", 0) + 1
            elif response_status == "TOO_POOR_SPEND_MORE":
                add_to_failed = True
                mark_processed = True
                fail_reason = "VIP level too low"
                error_summary["TOO_POOR_SPEND_MORE"] = error_summary.get("TOO_POOR_SPEND_MORE", 0) + 1
            elif response_status == "TOO_SMALL_SPEND_MORE":
                add_to_failed = True
                mark_processed = True
                fail_reason = "Furnace level too low"
                error_summary["TOO_SMALL_SPEND_MORE"] = error_summary.get("TOO_SMALL_SPEND_MORE", 0) + 1
            elif response_status == "CAPTCHA_TOO_FREQUENT":
                # Queue for retry with rate limit delay (60s max)
                queue_for_retry = True
                retry_delay = 60.0
                fail_reason = "Captcha API rate limited (too frequent)"
                cog.logger.info(f"GiftOps: ID {fid} hit CAPTCHA_TOO_FREQUENT. Queuing for retry in {retry_delay:.1f}s.")
                if current_cycle_count + 1 >= MAX_RETRY_CYCLES:
                    error_summary["CAPTCHA_TOO_FREQUENT"] = error_summary.get("CAPTCHA_TOO_FREQUENT", 0) + 1
            elif response_status in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED", "OCR_FAILED_ATTEMPT"]:
                if current_cycle_count + 1 < MAX_RETRY_CYCLES:
                    queue_for_retry = True
                    retry_delay = CAPTCHA_CYCLE_COOLDOWN
                    fail_reason = "Captcha Cycle Failed"
                    cog.logger.info(f"GiftOps: ID {fid} failed captcha cycle {current_cycle_count + 1}. Queuing for retry cycle {current_cycle_count + 2} in {retry_delay}s.")
                else:
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = f"Failed after {MAX_RETRY_CYCLES} captcha cycles (Last Status: {response_status})"
                    cog.logger.info(f"GiftOps: Max ({MAX_RETRY_CYCLES}) retry cycles reached for ID {fid}. Marking as failed.")
                    # Track based on error type
                    if response_status in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED"]:
                        error_summary["CAPTCHA_SOLVING_FAILED"] = error_summary.get("CAPTCHA_SOLVING_FAILED", 0) + 1
                    else:  # OCR_FAILED_ATTEMPT
                        error_summary["CAPTCHA_SOLVER_ERROR"] = error_summary.get("CAPTCHA_SOLVER_ERROR", 0) + 1
            else:
                add_to_failed = True
                mark_processed = True
                fail_reason = f"Unhandled status: {response_status}"
                error_summary[response_status] = error_summary.get(response_status, 0) + 1

            # Update State Based on Outcome
            if mark_processed:
                processed_count += 1
                if add_to_failed:
                    failed_count += 1
                    cycle_failed_on = current_cycle_count + 1 if response_status not in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED", "OCR_FAILED_ATTEMPT"] or (current_cycle_count + 1 >= MAX_RETRY_CYCLES) else MAX_RETRY_CYCLES
                    failed_users_dict[fid] = (nickname, fail_reason, cycle_failed_on)

            if queue_for_retry:
                retry_after_ts = time.time() + retry_delay
                cycle_for_next_retry = current_cycle_count + 1 if response_status in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED", "OCR_FAILED_ATTEMPT"] else current_cycle_count
                retry_queue.append((fid, nickname, cycle_for_next_retry, retry_after_ts))

            # Batch process results when reaching batch size
            if len(batch_results) >= batch_size:
                batch_process_alliance_results(cog, batch_results)
                batch_results = []

            # Update Embed Periodically
            current_time = time.time()
            if current_time - last_embed_update > 5 and not code_is_invalid:
                embed.description = update_embed_description()
                try:
                    await status_message.edit(embed=embed)
                    last_embed_update = current_time
                except Exception as embed_edit_err:
                    cog.logger.warning(f"GiftOps: WARN - Failed to edit progress embed: {embed_edit_err}")

        # Final Embed Update
        if not code_is_invalid:
            cog.logger.info(f"GiftOps: Alliance {alliance_id} processing loop finished. Preparing final update.")
            final_title = f"{theme.giftIcon} Gift Code Process Complete: {giftcode}"
            final_color = discord.Color.green() if failed_count == 0 and total_members > 0 else \
                          discord.Color.orange() if success_count > 0 or received_count > 0 else \
                          discord.Color.red()
            if total_members == 0:
                final_title = f"{theme.infoIcon} No Members to Process for Code: {giftcode}"
                final_color = discord.Color.light_grey()

            embed.title = final_title
            embed.color = final_color
            embed.description = update_embed_description(include_errors=True)

            try:
                await status_message.edit(embed=embed)
                cog.logger.info(f"GiftOps: Successfully edited final status embed for alliance {alliance_id}.")
            except discord.NotFound:
                cog.logger.warning(f"GiftOps: WARN - Failed to edit final progress embed for alliance {alliance_id}: Original message not found.")
            except discord.Forbidden:
                cog.logger.warning(f"GiftOps: WARN - Failed to edit final progress embed for alliance {alliance_id}: Missing permissions.")
            except Exception as final_embed_err:
                cog.logger.exception(f"GiftOps: WARN - Failed to edit final progress embed for alliance {alliance_id}: {final_embed_err}")

        summary_lines = [
            "\n",
            "--- Redemption Summary Start ---",
            f"Alliance: {alliance_name} ({alliance_id})",
            f"Gift Code: {giftcode}",
        ]
        try:
            master_status_log = cog.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,)).fetchone()
            summary_lines.append(f"Master Code Status at Log Time: {master_status_log[0] if master_status_log else 'NOT_FOUND_IN_DB'}")
        except Exception as e_log:
            summary_lines.append(f"Master Code Status at Log Time: Error fetching - {e_log}")

        summary_lines.extend([
            f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "------------------------",
            f"Total Members: {total_members}",
            f"Successful: {success_count}",
            f"Already Redeemed: {received_count}",
            f"Failed: {failed_count}",
            "------------------------",
        ])

        if successful_users:
            summary_lines.append(f"\nSuccessful Users ({len(successful_users)}):")
            summary_lines.extend(successful_users)

        if already_used_users:
            summary_lines.append(f"\nAlready Redeemed Users ({len(already_used_users)}):")
            summary_lines.extend(already_used_users)

        final_failed_log_details = []
        if code_is_invalid and retry_queue:
             for f_fid, f_nick, f_cycle, _ in retry_queue:
                 if f_fid not in failed_users_dict:
                     final_failed_log_details.append(f"- {f_nick} ({f_fid}): Halted in retry (Next Cycle: {f_cycle})")

        for fid_failed, (nick_failed, reason_failed, cycles_attempted) in failed_users_dict.items():
            final_failed_log_details.append(f"- {nick_failed} ({fid_failed}): {reason_failed} (Cycles Attempted: {cycles_attempted})")

        if final_failed_log_details:
            summary_lines.append(f"\nFailed Users ({len(final_failed_log_details)}):")
            summary_lines.extend(final_failed_log_details)

        summary_lines.append("--- Redemption Summary End ---\n")
        summary_log_message = "\n".join(summary_lines)
        cog.logger.info(summary_log_message)

        # Process any remaining batch results
        if batch_results:
            batch_process_alliance_results(cog, batch_results)
            batch_results = []

        return True

    except PreemptedException:
        raise
    except Exception as e:
        cog.logger.exception(f"GiftOps: UNEXPECTED ERROR in use_giftcode_for_alliance for {alliance_id}/{giftcode}: {str(e)}")
        cog.logger.exception(f"Traceback: {traceback.format_exc()}")
        try:
            if 'channel' in locals() and channel: await channel.send(f"{theme.warnIcon} An unexpected error occurred processing `{giftcode}` for {alliance_name}.")
        except Exception: pass
        return False
