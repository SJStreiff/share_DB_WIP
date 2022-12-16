#!/usr/bin/env bash

# THIS FILE LET'S YOU CONFIGURE YOUR RECORDCLEANER RUN


# VARIABLES TO MODIFY, REQUIRED!!


INPUT='/in_directory/'
DAT_FORM='P'
WDIR='/working_directory/'
OUT_DIR='/out_directory/'
PREFIX='prefix_'

# OPTIONAL VARIABLES, FACULTATIVE!

VERBOSE='-v' #
NOCOLLECTOR='' # if '-nc', the name standardisation step is skipped! Use with caution!
......
WIP


# AND NOW LETS LAUNCH RECORDCLEANER

python ./3_scripts/recordcleaner.py -h
or
python ./3_scripts/recordcleaner.py $INPUT $DAT_FORM  $WDIR $OUT_DIR $PREFIX $VERBOSE $NOCOLLECTOR
