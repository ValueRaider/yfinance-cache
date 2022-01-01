#!/bin/bash

_make() (
	set -e
	set -u

	# _name=yfinance-cache
	_name=yfinance-cache2
	_nameu=`echo "$_name" | sed "s/-/_/g"`

	_ver=`cat version-test`
	
	cp setup.cfg.template setup.cfg
	sed -i "s/<NAME>/$_name/g" setup.cfg
	sed -i "s/<VERSION>/$_ver/g" setup.cfg

	if [ -d src/"$_nameu".egg-info ]; then
		rm -r src/"$_nameu".egg-info
	fi

	_ddir="dist-test/$_ver"
	mkdir -p "$_ddir"
	rm -f "$_ddir"/*
	python3 -m build --outdir "$_ddir"

	if [ -d src/"$_nameu".egg-info ]; then
		rm -r src/"$_nameu".egg-info
	fi
	python3 -m twine upload --repository testpypi "$_ddir"/*
)

_make

