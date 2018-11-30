#!/bin/bash
echo $1

exec python pyramid.py "$@"
