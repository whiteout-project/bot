from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import pytz

# Event type configuration metadata
EVENT_CONFIG = {
    "Bear Trap": {
        "emoji": "🐻",
        "duration_minutes": 30,
        "schedule_type": "custom",  # Alliance-defined schedule
        "description": "The %e %n opens in %t. Get your buffs on and prepare your marches for the hunt!",
        "default_notification_type": 2,  # 10, 5, 0 minutes before
        "time_slots": "5min",  # Can be scheduled in 5-minute increments
        "instances_per_cycle": 2,  # Trap 1 and 2
        "image_url": "",  # Placeholder for event image
        "thumbnail_url": "https://i.imgur.com/tVExgj4.png",
    },
    "Crazy Joe": {
        "emoji": "🤪",
        "duration_minutes": 30,
        "schedule_type": "global_biweekly",
        "fixed_days": "Tuesday and Thursday every 4 weeks",
        "reference_date": "2025-11-18",  # Reference occurrence date
        "cycle_weeks": 4,
        "description": "%n is coming to town in %t. Come online and join the defense!",
        "default_notification_type": 2,
        "time_slots": "5min",
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/qwNM7Br.png",
    },
    "Foundry Battle": {
        "emoji": "🏭",
        "duration_minutes": 60,
        "schedule_type": "global_biweekly",
        "fixed_days": "Every 2 weeks on Sunday",
        "reference_date": "2025-11-16",
        "cycle_weeks": 2,
        "available_times": ["02:00", "12:00", "14:00", "19:00"],
        "description": "Foundry Battle starts in %t minutes, get ready!",  # Default/fallback
        "descriptions": {
            "legion1": "%n Legion 1 at %e starts in %t. Buff up, heal up, recall your marches and get ready to fight!",
            "legion2": "%n Legion 2 at %e starts in %t. Buff up, heal up, recall your marches and get ready to fight!"
        },
        "default_notification_type": 2,
        "instances_per_cycle": 2,  # Legion 1 and Legion 2
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/u3pmvW1.png",
    },
    "Canyon Clash": {
        "emoji": "⚔️",
        "duration_minutes": 60,
        "schedule_type": "global_monthly",
        "fixed_days": "Monthly on Saturday",
        "reference_date": "2025-11-29",
        "cycle_weeks": 4,
        "available_times": ["02:00", "12:00", "14:00", "19:00", "21:00"],
        "description": "Canyon Clash starts in %t minutes, get ready!",
        "descriptions": {
            "legion1": "%n Legion 1 at %e starts in %t. Buff up and get ready to fight!",
            "legion2": "%n Legion 2 at %e starts in %t. Buff up and get ready to fight!"
        },
        "default_notification_type": 2,
        "instances_per_cycle": 2,
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/eKiHavB.png",
    },
    "Fortress Battle": {
        "emoji": "🏰",
        "duration_minutes": 120,  # Up to 2 hours
        "schedule_type": "global_weekly",
        "fixed_days": "Every Friday",
        "available_times": ["03:00", "09:00", "13:00", "14:00", "17:00"],
        "description": "%e %n is starting in %t. Prepare to rally and fight for any registered Strongholds and Forts.",
        "default_notification_type": 2,
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/ryZW1kF.png",
    },
    "Frostfire Mine": {
        "emoji": "⛏️",
        "duration_minutes": 30,
        "schedule_type": "global_monthly",
        "fixed_days": "Monthly on Tuesday",
        "reference_date": "2025-11-18",
        "cycle_weeks": 4,
        "available_times": ["03:00", "05:00", "11:00", "14:00", "16:00", "18:00", "21:00"],
        "description": "The %e %n is opening soon! Come online and recall your troops if you are joining at this time.",
        "default_notification_type": 2,
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/gC5S9Rt.png",
    },
    "Castle Battle": {
        "emoji": "☀️",
        "duration_minutes": 360,  # 6 hours (12:00-18:00)
        "schedule_type": "global_4weekly",
        "fixed_days": "Every 4 weeks on Saturday",
        "reference_date": "2025-11-22",
        "cycle_weeks": 4,
        "fixed_time": "12:00",  # UTC
        "description": "%n starts in %t, get ready!",
        "descriptions": {
            "teleport_window": "%n teleport window opens in %t! Get ready to take your places.",
            "battle_start": "%n battle starts in %t. Get ready to fight!"
        },
        "default_notification_type": 2,
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/NPu9yFh.png",
    },
    "SvS": {
        "emoji": "⚡",
        "duration_minutes": 360,
        "schedule_type": "global_4weekly_alt",
        "fixed_days": "Every 4 weeks on Saturday (alternating with Sunfire)",
        "reference_date": "2025-12-06",  # Reference occurrence date (always 2 weeks after Sunfire)
        "cycle_weeks": 4,
        "fixed_time": "12:00",
        "description": "%n starts in %t, get ready!",
        "descriptions": {
            "borders_open": "State borders open in %t! Shield up or you could get raided!",
            "teleport_window": "%n teleport window opens in %t! Get ready to take your places.",
            "battle_start": "%n battle starts in %t. Get ready to battle and win this for the glory of our state!"
        },
        "default_notification_type": 2,
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/HUwpmTd.png",
    },
    "Mercenary Bosses": {
        "emoji": "🗡️",
        "duration_minutes": 60,  # Each boss session ~1 hour
        "schedule_type": "global_4weekly_multiday",
        "fixed_days": "Every 4 weeks, 3-day window",
        "reference_date": "2025-12-06",
        "cycle_weeks": 4,
        "event_duration_days": 3,
        "time_slots": "5min",
        "max_instances": 5,  # Up to 5 mercenary bosses
        "description": "%n is spawning in %t. Prepare to send only one march as instructed!",
        "default_notification_type": 2,
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/zb6y3Dg.png",
    },
    "Daily Reset": {
        "emoji": "🔄",
        "duration_minutes": 0,
        "schedule_type": "daily",
        "fixed_time": "00:00",
        "description": "Daily Reset in %t - make sure you have done your dailies and arena battles!",
        "default_notification_type": 2,
        "image_url": "",
        "thumbnail_url": "https://i.imgur.com/1qeelNq.png",
    },
}

# List of all event types for dropdowns
EVENT_TYPES = list(EVENT_CONFIG.keys())

# Event type icons for quick access
EVENT_TYPE_ICONS = {event: config["emoji"] for event, config in EVENT_CONFIG.items()}

def get_event_config(event_type: str) -> Optional[Dict]:
    """
    Get configuration for a specific event type

    Args:
        event_type: Name of the event type

    Returns:
        Dictionary with event configuration or None if not found
    """
    return EVENT_CONFIG.get(event_type)

def get_event_types() -> List[str]:
    """Get list of all available event types"""
    return EVENT_TYPES.copy()

def get_event_icon(event_type: str) -> str:
    """
    Get emoji icon for an event type

    Args:
        event_type: Name of the event type

    Returns:
        Emoji string or empty string if not found
    """
    return EVENT_TYPE_ICONS.get(event_type, "📅")

def validate_time_slot(time_str: str, slot_type: str = "5min") -> bool:
    """
    Validate if a time string matches the required slot format

    Args:
        time_str: Time string in HH:MM format
        slot_type: Type of slot validation ("5min", "any", "fixed")

    Returns:
        True if valid, False otherwise
    """
    try:
        hours, minutes = map(int, time_str.split(":"))

        if not (0 <= hours <= 23 and 0 <= minutes <= 59):
            return False

        if slot_type == "5min":
            # Must be in 5-minute increments (0, 5, 10, 15, etc.)
            return minutes % 5 == 0

        return True
    except (ValueError, AttributeError):
        return False

def round_to_5min_slot(dt: datetime) -> datetime:
    """
    Round a datetime to the nearest 5-minute slot

    Args:
        dt: Datetime object to round

    Returns:
        Rounded datetime object
    """
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)

def calculate_next_occurrence(event_type: str, from_date: Optional[datetime] = None) -> Optional[datetime]:
    """
    Calculate the next occurrence date for global events

    Args:
        event_type: Name of the event type
        from_date: Calculate from this date (defaults to now)

    Returns:
        Datetime of next occurrence or None if event has custom scheduling
    """
    config = get_event_config(event_type)
    if not config:
        return None

    if from_date is None:
        from_date = datetime.now(pytz.UTC)

    schedule_type = config.get("schedule_type")

    # Custom schedule events (Bear Trap) - no automatic calculation
    if schedule_type == "custom":
        return None

    # Daily reset
    if schedule_type == "daily":
        next_reset = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_reset <= from_date:
            next_reset += timedelta(days=1)
        return next_reset

    # Weekly events (Stronghold & Fort Battle)
    if schedule_type == "global_weekly":
        # Every Friday
        days_until_friday = (4 - from_date.weekday()) % 7
        if days_until_friday == 0 and from_date.hour >= 17:  # Past last time slot
            days_until_friday = 7
        next_friday = from_date + timedelta(days=days_until_friday)
        return next_friday.replace(hour=0, minute=0, second=0, microsecond=0)

    # Biweekly events (Foundry Battle)
    if schedule_type == "global_biweekly":
        reference = datetime.strptime(config["reference_date"], "%Y-%m-%d")
        reference = pytz.UTC.localize(reference)

        # Calculate weeks since reference
        weeks_diff = (from_date - reference).days // 7
        cycle_weeks = config.get("cycle_weeks", 2)

        # Find next occurrence
        if weeks_diff < 0:
            return reference

        cycles_passed = weeks_diff // cycle_weeks
        next_occurrence = reference + timedelta(weeks=cycles_passed * cycle_weeks)

        # If we've passed this occurrence, get the next one
        if next_occurrence <= from_date:
            next_occurrence += timedelta(weeks=cycle_weeks)

        return next_occurrence

    # Monthly events (Canyon Clash, Frostfire Mine)
    if schedule_type == "global_monthly":
        reference = datetime.strptime(config["reference_date"], "%Y-%m-%d")
        reference = pytz.UTC.localize(reference)

        # Calculate months since reference (using 4-week cycles)
        weeks_diff = (from_date - reference).days // 7
        cycle_weeks = config.get("cycle_weeks", 4)

        if weeks_diff < 0:
            return reference

        cycles_passed = weeks_diff // cycle_weeks
        next_occurrence = reference + timedelta(weeks=cycles_passed * cycle_weeks)

        if next_occurrence <= from_date:
            next_occurrence += timedelta(weeks=cycle_weeks)

        return next_occurrence

    # 4-weekly events (Sunfire Castle, SvS, Mercenary Bosses)
    if schedule_type in ["global_4weekly", "global_4weekly_alt", "global_4weekly_multiday"]:
        reference = datetime.strptime(config["reference_date"], "%Y-%m-%d")
        reference = pytz.UTC.localize(reference)

        weeks_diff = (from_date - reference).days // 7
        cycle_weeks = 4

        if weeks_diff < 0:
            return reference

        cycles_passed = weeks_diff // cycle_weeks
        next_occurrence = reference + timedelta(weeks=cycles_passed * cycle_weeks)

        if next_occurrence <= from_date:
            next_occurrence += timedelta(weeks=cycle_weeks)

        return next_occurrence

    # Biweekly with two days (Crazy Joe - Tuesday and Thursday)
    if schedule_type == "global_biweekly":
        # This is handled above, but for Crazy Joe specifically we need both days
        pass

    return None

def calculate_crazy_joe_dates(from_date: Optional[datetime] = None) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Calculate next Tuesday and Thursday dates for Crazy Joe event

    Args:
        from_date: Calculate from this date (defaults to now)

    Returns:
        Tuple of (next_tuesday, next_thursday) datetime objects
    """
    config = get_event_config("Crazy Joe")
    if not config:
        return None, None

    if from_date is None:
        from_date = datetime.now(pytz.UTC)

    reference_tue = datetime.strptime(config["reference_date"], "%Y-%m-%d")
    reference_tue = pytz.UTC.localize(reference_tue)

    cycle_weeks = 4

    # Calculate next Tuesday
    weeks_diff = (from_date - reference_tue).days // 7
    if weeks_diff < 0:
        next_tuesday = reference_tue
    else:
        cycles_passed = weeks_diff // cycle_weeks
        next_tuesday = reference_tue + timedelta(weeks=cycles_passed * cycle_weeks)
        if next_tuesday <= from_date:
            next_tuesday += timedelta(weeks=cycle_weeks)

    next_thursday = next_tuesday + timedelta(days=2)

    return next_tuesday, next_thursday

def get_available_time_slots(event_type: str) -> Optional[List[str]]:
    """
    Get list of available time slots for an event type

    Args:
        event_type: Name of the event type

    Returns:
        List of time strings (HH:MM format) or None if times are custom
    """
    config = get_event_config(event_type)
    if not config:
        return None

    return config.get("available_times")

def get_fixed_time(event_type: str) -> Optional[str]:
    """
    Get fixed time for events that don't have selectable times

    Args:
        event_type: Name of the event type

    Returns:
        Time string (HH:MM format) or None if not applicable
    """
    config = get_event_config(event_type)
    if not config:
        return None

    return config.get("fixed_time")

def format_event_schedule_description(event_type: str) -> str:
    """
    Generate a human-readable description of when an event occurs

    Args:
        event_type: Name of the event type

    Returns:
        Formatted description string
    """
    config = get_event_config(event_type)
    if not config:
        return "Unknown event schedule"

    schedule_type = config.get("schedule_type")

    if schedule_type == "custom":
        return "Custom schedule set by your alliance"
    elif schedule_type == "daily":
        return "Daily at 00:00 UTC"
    elif schedule_type == "global_weekly":
        return f"Every {config.get('fixed_days', 'week')}"
    elif schedule_type == "global_biweekly":
        if event_type == "Crazy Joe":
            return "Every 4 weeks on Tuesday and Thursday"
        return f"Every 2 weeks on {config.get('fixed_days', 'schedule')}"
    elif schedule_type in ["global_monthly", "global_4weekly", "global_4weekly_alt"]:
        return config.get("fixed_days", "Monthly event")

    return "Event schedule varies"