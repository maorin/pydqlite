#!/usr/bin/env bash

pyenv install 3.6.8
pyenv virtualenv 3.6.8 mypyrqlite
pyenv activate mypyrqlite
pip install --upgrade pip
pip install pytest

python setup.py install
