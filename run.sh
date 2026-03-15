#!/bin/bash
# Auto-restart wrapper for the Polymarket bot.
# Logs crashes with timestamp, waits 10s, then restarts.

cd ~/polymarket-bot
MODE=${1:---live}

echo "[$(date '+%H:%M:%S')] Starting bot ($MODE)..."

while true; do
    python3 bot.py $MODE
    EXIT_CODE=$?
    TIMESTAMP=$(date '+%H:%M:%S')
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$TIMESTAMP] Bot exited cleanly (code 0). Not restarting."
        break
    else
        echo "[$TIMESTAMP] ⚠️  Bot crashed (exit code $EXIT_CODE). Restarting in 10s..."
        sleep 10
    fi
done
