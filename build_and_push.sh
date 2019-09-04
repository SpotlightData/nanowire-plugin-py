#!/usr/bin/env bash

echo "removing old versions"

rm ./dist/*

echo "building new version"

python3 setup.py sdist bdist_wheel

echo "pushing new version to pypi"

twine upload dist/*
