#!/bin/bash

# Run db migrations

flask db upgrade &

# Run worker
python worker.py &

# Run app
python app.py