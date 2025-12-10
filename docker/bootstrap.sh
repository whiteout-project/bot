#!/bin/sh

echo "[WSDB] for more information please see https://github.com/whiteout-project/bot"

cd /app

if [ -z "${DISCORD_BOT_TOKEN}" ]; then
    echo "please set DISCORD_BOT_TOKEN"
    exit 1
fi

echo "${DISCORD_BOT_TOKEN}" > bot_token.txt

if [ "${UPDATE}" = "0" ]; then
    ARGS="--no-update"
else
	ARGS="--autoupdate"
fi

if [ "${BETA}" = "1" ]; then
    ARGS="$ARGS --beta"
fi

if [ "${DEBUG}" = "1" ]; then
    ARGS="$ARGS --debug"
fi

python main.py $ARGS