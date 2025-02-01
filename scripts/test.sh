#!/bin/bash
# shellcheck disable=SC1091

VENV="venv"

if [ -f "$VENV/bin/activate" ]; then
	echo "Virtual environment found. Activating..."
	source "$VENV/bin/activate"
else
	echo "Error: Virtual environment not found at $VENV/bin/activate"
	exit 1
fi

python -m unittest discover
