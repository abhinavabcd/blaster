#!/bin/bash

rm ./dist/*
python3 setup.py sdist bdist_wheel
pip install  ./dist/blaster-server*.gz
echo -n "Upload to pypi ? [y/N] "
read REPLY
if [[ $REPLY == y* ]]; then
python3 -m twine upload dist/*
fi