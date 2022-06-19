#!/bin/bash

_main() (
	ls tests | while read F ; do
		if [ "$F" = "context.py" ]; then
			continue
		elif [ "$F" = "__init__.py" ]; then
			continue
		elif [ "$F" = "__pycache__" ]; then
			continue
		fi

		echo "Running tests in tests/$F ..."
		F2=`basename -s .py $F`
		python -m tests.$F2
	done
)

_main
