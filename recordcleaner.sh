#!/usr/bin/env bash

# THIS FILE LET'S YOU CONFIGURE YOUR RECORDCLEANER RUN


# VARIABLES TO MODIFY, REQUIRED!!


INPUT='/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/1_data_raw/TEST_DATA.csv' # path to your input file
DAT_FORM='P'           # format of your input file
WDIR='/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/1a_WIP/' # directory where intermediate files will be written to
OUT_DIR='/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/2_data_out/' # directory where final file is written to
PREFIX='TEST_'          # prefix for all intermediate and final files

# OPTIONAL VARIABLES, FACULTATIVE!

VERBOSE='-v 1' # prints intermediate information to STDOUT, which might be helpful for debugging in case of issues.
               # -v 2 also outputs debugging information
NOCOLLECTOR='' # if '-nc', the name standardisation step is skipped! Use with caution!


# AND NOW LETS LAUNCH RECORDCLEANER

#python ./3_scripts/recordcleaner.py -h
#or
python ./3_scripts/recordcleaner.py $INPUT $DAT_FORM  $WDIR $OUT_DIR $PREFIX $NOCOLLECTOR $VERBOSE
