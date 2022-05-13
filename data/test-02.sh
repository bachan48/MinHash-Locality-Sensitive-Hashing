#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'run data/articles_1000.txt  " " 0.8 3 100 4 true true false'
