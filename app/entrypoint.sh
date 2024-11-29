#!/bin/sh

python app.py & tail -f /app/log/app.log
