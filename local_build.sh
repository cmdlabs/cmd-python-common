#!/bin/bash
set -e
python setup.py test sdist bdist_wheel bdist --format=zip
pip uninstall -y dist/runcmd-1.0.3a0-py3-none-any.whl
pip install dist/runcmd-1.0.3a0-py3-none-any.whl
