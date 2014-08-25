#!/bin/sh

#find .. -name '*.py[co]' -delete
PWD=`pwd`
python -m unittest discover -s $PWD/test -t $PWD/..
