#!/bin/bash

# 启动 worker
python worker.py &

# 启动 app
python app.py