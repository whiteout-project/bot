#!/bin/bash

# Bootstrap script for Discord Bot
# This script ensures proper environment setup before starting the bot

echo "=== Discord Bot Bootstrap ==="
echo "Starting Whiteout Survival Discord Bot..."

# Check if running in container
if [ -f /.dockerenv ] || [ -f /var/run/secrets/kubernetes.io ]; then
    echo "Running in container environment"
    export CONTAINER_ENV=true
else
    echo "Running in host environment"
    export CONTAINER_ENV=false
fi

# Ensure required directories exist
mkdir -p /app/data/db /app/data/log

# Check for bot token
if [ -z "${DISCORD_BOT_TOKEN}" ]; then
    if [ ! -f "/app/bot_token.txt" ]; then
        echo "ERROR: No Discord bot token found!"
        echo "Please set DISCORD_BOT_TOKEN environment variable or provide bot_token.txt file"
        exit 1
    else
        echo "Using bot token from file: bot_token.txt"
    fi
else
    echo "Using bot token from environment variable"
    echo "${DISCORD_BOT_TOKEN}" > /app/bot_token.txt
fi

# Handle AUTO_UPDATE environment variable
if [ -n "${AUTO_UPDATE}" ]; then
    case "${AUTO_UPDATE}" in
        true|TRUE|1|yes|YES|on|ON|enabled|ENABLED)
            echo "Auto-update: ENABLED"
            export AUTO_UPDATE=true
            ;;
        false|FALSE|0|no|NO|off|OFF|disabled|DISABLED)
            echo "Auto-update: DISABLED"
            export AUTO_UPDATE=false
            ;;
        *)
            echo "Warning: Invalid AUTO_UPDATE value '${AUTO_UPDATE}'. Using default (enabled)."
            export AUTO_UPDATE=true
            ;;
    esac
else
    echo "Auto-update: DEFAULT (enabled)"
    export AUTO_UPDATE=true
fi

# USE_COMMERCIAL_CAPTCHA
if [ -n "${USE_COMMERCIAL_CAPTCHA}" ]; then
    case "${USE_COMMERCIAL_CAPTCHA}" in
        true|TRUE|1|yes|YES|on|ON|enabled|ENABLED)
            echo "USE_COMMERCIAL_CAPTCHA: ENABLED"
            export USE_COMMERCIAL_CAPTCHA=true
            export COMMERCIAL_CAPTCHA_KEY="${COMMERCIAL_CAPTCHA_KEY:-}"
            ;;
        false|FALSE|0|no|NO|off|OFF|disabled|DISABLED)
            echo "USE_COMMERCIAL_CAPTCHA: DISABLED"
            export USE_COMMERCIAL_CAPTCHA=false
            ;;
        *)
            echo "Warning: Invalid USE_COMMERCIAL_CAPTCHA value '${USE_COMMERCIAL_CAPTCHA}'. Using default (disabled)."
            export USE_COMMERCIAL_CAPTCHA=false
            ;;
    esac
else
    echo "USE_COMMERCIAL_CAPTCHA: DEFAULT (disabled)"
    export USE_COMMERCIAL_CAPTCHA=false
fi

# 

# Set proper permissions and switch to bot user if running as root
if [ "$CONTAINER_ENV" = "true" ]; then
    if [ "$(id -u)" = "0" ]; then
        # If running as root, switch to bot user
        echo "Switching to bot user..."
        chown -R botuser:botuser /app
        # Use gosu to switch to bot user and re-execute this script
        exec gosu botuser "$0" "$@"
    fi
fi

# Change to app directory
cd /app

echo "Starting Discord bot..."
exec python main.py --no-venv
