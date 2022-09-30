#!/bin/bash

_make() (
	set -e
	set -u

	_name=yfinance-cache
	_ver=`cat version-live`
	_ddir=dist-live

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

	python -m twine upload "$_outdir"/*
)

_make

