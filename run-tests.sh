#!/bin/bash

_main() (
	rm "$HOME"/.cache/yfinance.cache
	
	ls tests | while read F ; do
		if [ "$F" = "context.py" ] || [ "$F" = "__init__.py" ] || [ "$F" = "__pycache__" ]; then
			continue
		elif [ -d tests/"$F" ]; then
			continue
		elif [ "$F" = "yfc_adjust.py" ] || [ "$F" = "yfc_interface.py" ]; then
			continue
		fi

		echo "Running tests in tests/$F ..."
		F2=`basename -s .py $F`
		python -m tests.$F2
	done

	F=yfc_adjust
	echo "Running tests in tests/$F.py ..."
	python -m tests.$F

	F=yfc_interface
	echo "Running tests in tests/$F.py ..."
	python -m tests.$F
)

_main
