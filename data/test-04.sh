#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'run data/articles_1000.txt  " " 0.7 3 200 10 true true false'
