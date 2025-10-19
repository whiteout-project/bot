#!/bin/sh

echo "[WSDB] For more information please see https://github.com/whiteout-project/bot"

cd /app

if [ ! -n "${DISCORD_BOT_TOKEN}" ]; then
	echo "please set DISCORD_BOT_TOKEN"
	exit
fi

if [ -n "${DISCORD_BOT_TOKEN}" ]; then
    echo "${DISCORD_BOT_TOKEN}" > bot_token.txt
fi

python main.py --autoupdate