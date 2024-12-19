# logs.py

import sys
from datetime import datetime

log_file_path = '/app/log/app.log'

def print_log(*args, **kwargs):
    message = ' '.join(map(str, args))
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")