#!/bin/bash
# shellcheck disable=SC1091

python() {
	# Ensure we are in the project root
	cd "$(dirname "$0")/.." || exit

	VENV="venv"

	# Check if the virtual environment exists
	if [ ! -d "$VENV" ]; then
		echo "Error: Virtual environment not found at $VENV"
		exit 1
	fi

	# Run Python tests inside the virtual environment directly
	if cd python; then
		"$PWD/../venv/bin/python" -m unittest discover
	fi

}

rust() {
	if cd rust; then
		cargo test -- --nocapture
	fi
}

options() {
	echo "Which tests would you like to run?"
	echo "1 - rust"
	echo "2 - python"

}

# Main
while true; do
	options
	read -r option

	case $option in
	1)
		rust
		break
		;;
	2)
		python
		break
		;;

	*) echo "Please choose a different one." ;;
	esac
done
