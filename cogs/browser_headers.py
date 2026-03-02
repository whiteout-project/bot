"""
Browser header randomization to bypass bot detection.
Rotates browser type, version, OS, and related sec-* headers on every call.
"""
import random


BROWSER_PROFILES = [
    {
        'browser': 'Chrome',
        'versions': [124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135],
        'platforms': [
            {'os': 'Windows NT 10.0; Win64; x64', 'secPlatform': '"Windows"'},
            {'os': 'Windows NT 11.0; Win64; x64', 'secPlatform': '"Windows"'},
            {'os': 'Macintosh; Intel Mac OS X 10_15_7', 'secPlatform': '"macOS"'},
            {'os': 'X11; Linux x86_64', 'secPlatform': '"Linux"'}
        ]
    },
    {
        'browser': 'Brave',
        'versions': [132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145],
        'platforms': [
            {'os': 'Windows NT 10.0; Win64; x64', 'secPlatform': '"Windows"'},
            {'os': 'Windows NT 11.0; Win64; x64', 'secPlatform': '"Windows"'},
            {'os': 'Macintosh; Intel Mac OS X 10_15_7', 'secPlatform': '"macOS"'}
        ]
    },
    {
        'browser': 'Edge',
        'versions': [124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135],
        'platforms': [
            {'os': 'Windows NT 10.0; Win64; x64', 'secPlatform': '"Windows"'},
            {'os': 'Windows NT 11.0; Win64; x64', 'secPlatform': '"Windows"'},
            {'os': 'Macintosh; Intel Mac OS X 10_15_7', 'secPlatform': '"macOS"'}
        ]
    }
]


def _build_sec_ua(browser: str, version: int) -> str:
    """Build the sec-ch-ua header value based on browser and version."""
    if browser == 'Chrome':
        return f'"Not:A-Brand";v="99", "Google Chrome";v="{version}", "Chromium";v="{version}"'
    elif browser == 'Brave':
        return f'"Not:A-Brand";v="99", "Brave";v="{version}", "Chromium";v="{version}"'
    elif browser == 'Edge':
        return f'"Not A(B)rand";v="8", "Chromium";v="{version}", "Microsoft Edge";v="{version}"'
    return ""


def get_headers(origin: str = None) -> dict:
    """
    Generate randomized browser-like headers to avoid server-side bot detection.
    Rotates browser type, version, OS, and related sec-* headers on every call.
    
    Args:
        origin: Optional origin URL for the headers (e.g., 'https://api.example.com')
    
    Returns:
        dict: Headers object with randomized browser information
    """
    # Select random profile and values
    profile = random.choice(BROWSER_PROFILES)
    version = random.choice(profile['versions'])
    platform = random.choice(profile['platforms'])
    
    # Build User-Agent string
    user_agent = f"Mozilla/5.0 ({platform['os']}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version}.0.0.0 Safari/537.36"
    
    # Build sec-ch-ua header
    sec_ch_ua = _build_sec_ua(profile['browser'], version)
    
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'priority': 'u=1, i',
        'user-agent': user_agent,
        'sec-ch-ua': sec_ch_ua,
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': platform['secPlatform'],
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'sec-gpc': '1',
    }
    
    # Add origin and referer if provided
    if origin:
        headers['origin'] = origin
        headers['referer'] = f"{origin}/"
    
    return headers
