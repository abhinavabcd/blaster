rm ./dist/*
python3 setup.py sdist bdist_wheel
pip install  ./dist/blaster-server*.gz
python3 -m twine upload dist/*
