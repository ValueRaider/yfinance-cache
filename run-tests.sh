#!/bin/bash

_main() (
	EXCHANGES=(usa nze asx tlv)
	PERIODS=(h d w)

	set -e
	
	TESTS=(cache datetime-assumptions utils time_utils)
	for T in "${TESTS[@]}" ; do
		echo "Running tests in tests/$T ..."
		python -m tests.test_$T
	done

	for E in "${EXCHANGES[@]}" ; do
		T="market_schedules_$E"
		echo "Running tests in tests/$T ..."
		python -m tests.test_$T
	done
	for E in "${EXCHANGES[@]}" ; do
		T="market_intervals_$E"
		echo "Running tests in tests/$T ..."
		python -m tests.test_$T
	done
	for E in "${EXCHANGES[@]}" ; do
		T="missing_intervals_$E"
		echo "Running tests in tests/$T ..."
		python -m tests.test_$T
	done

	for P in "${PERIODS[@]}" ; do
		T="price_data_aging_1$P"
		echo "Running tests in tests/$T ..."
		python -m tests.test_$T
	done

	Y=(yf_assumptions yfc_backend yfc_adjust yfc_interface)
	for T in "${Y[@]}" ; do
		echo "Running tests in tests/$T ..."
		python -m tests.test_$T
	done

)

_main
