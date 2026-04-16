#!/bin/zsh
set -euo pipefail

cd /Users/wj/Documents/smart_money_tracker

if [ -f ".venv/bin/activate" ]; then
  source ".venv/bin/activate"
fi

exec python smart_money_monitor.py
