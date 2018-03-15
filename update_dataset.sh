#!/bin/bash

rm *.pkl
rm *.db
python downloader.py
python clean.py
python to-pickle.py
