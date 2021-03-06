#!/bin/bash

_make() (
	set -e
	set -u

	_name=yfinance-cache2
	_ver=`cat version-test`
	_ddir=dist-test

	cp setup.cfg.template setup.cfg
	sed -i "s/<NAME>/$_name/g" setup.cfg
	sed -i "s/<VERSION>/$_ver/g" setup.cfg

	_nameu=`echo "$_name" | sed "s/-/_/g"`
	if [ -d src/"$_nameu".egg-info ]; then
		rm -r src/"$_nameu".egg-info
	fi

	_outdir="$_ddir/$_ver"
	if [ -d "$_outdir" ]; then rm -r "$_outdir" ; fi
	mkdir -p "$_outdir"
	python -m build --outdir "$_outdir"

	if [ -d src/"$_nameu".egg-info ]; then
		rm -r src/"$_nameu".egg-info
	fi
	python -m twine upload --repository testpypi "$_outdir"/*
)

_make

