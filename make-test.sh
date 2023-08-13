#!/bin/bash

_make() (
	set -e
	set -u

	_name=yfinance-cache2
	_ver=`cat version-test`
	_ddir=dist-test

	_nameu=`echo "$_name" | sed "s/-/_/g"`
	_outdir="$_ddir/$_ver"

	cp setup.cfg.template setup.cfg
	sed -i "s/<NAME>/$_name/g" setup.cfg
	sed -i "s/<VERSION>/$_ver/g" setup.cfg

	if [ -d src/"$_nameu".egg-info ]; then
		rm -r src/"$_nameu".egg-info
	fi

	if [ -d "$_outdir" ]; then rm -r "$_outdir" ; fi
	mkdir -p "$_outdir"
	python -m build --outdir "$_outdir"

	python -m twine upload --repository testpypi "$_outdir"/*

	# Note: to download from Test PYPI successfully,
	# need to download dependencies from PYPI like this:
	pip install --no-deps --index-url https://test.pypi.org/simple yfinance_cache2
	_reqs=`pip show yfinance_cache2 | grep "^Requires:" | cut -d':' -f2 | sed 's/,//g'`
	pip install -v $_reqs
)

_make

