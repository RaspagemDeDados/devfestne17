#!/bin/bash

pip install -r /app/requirements.txt

exec tail -f /dev/null "$@"