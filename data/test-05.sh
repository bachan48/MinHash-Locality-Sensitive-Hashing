#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'run data/articles_2500.txt  " " 0.8 3 100 4 false false false'
