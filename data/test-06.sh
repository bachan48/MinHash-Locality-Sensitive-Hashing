#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'run data/articles_100.txt  " " 0.15 2 20 3 true true false'
