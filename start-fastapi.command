#!/bin/zsh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR" || exit 1

[ -f ".secrets.local" ] && source ".secrets.local"

uv run uvicorn main:app --reload
