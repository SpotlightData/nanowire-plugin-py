#!/bin/env bash
# Sets environment variables and runs the test script

export POD_NAME=test_python_pod
export PLUGIN_ID=test-python-plugin
export CONTROLLER_BASE_URI=http://localhost:3000/

python test.py

