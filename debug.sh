#!/bin/bash

nohup python3 manage.py runserver 0.0.0.0:9123  --insecure &

# Start Django Q cluster
nohup python3 manage.py qcluster &
