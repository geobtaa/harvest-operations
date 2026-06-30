#!/bin/zsh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR" || exit 1

[ -f ".secrets.local" ] && source ".secrets.local"

if [ -x ".venv/bin/python" ]; then
  ".venv/bin/python" -m uvicorn main:app --reload
elif command -v uv >/dev/null 2>&1; then
  uv run uvicorn main:app --reload
else
  echo "Could not find .venv/bin/python or uv."
  echo "Create the local environment first:"
  echo "  python3 -m venv .venv"
  echo "  source .venv/bin/activate"
  echo "  pip install -r requirements.txt"
  exit 1
fi
