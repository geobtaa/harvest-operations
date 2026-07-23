#!/bin/zsh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR" || exit 1

[ -f ".secrets.local" ] && source ".secrets.local"

if command -v uv >/dev/null 2>&1; then
  uv run --locked uvicorn main:app --reload
else
  echo "Could not find uv."
  echo "Install uv, then create the repository environment:"
  echo "  brew install uv"
  echo "  uv sync"
  exit 1
fi
