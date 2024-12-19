#!/bin/sh

python "$SCRIPT.py" & tail -f /app/log/app.log
