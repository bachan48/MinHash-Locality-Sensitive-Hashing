#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'run data/articles_100.txt  " " 0.8 1 100 4 true true true'
