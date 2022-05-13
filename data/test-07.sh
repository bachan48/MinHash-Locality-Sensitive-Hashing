#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'run data/articles_10000.txt  " " 0.9 3 100 4 false true false'
