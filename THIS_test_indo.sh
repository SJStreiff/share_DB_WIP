#!/usr/bin/env bash

# THIS FILE LET'S YOU CONFIGURE YOUR RECORDCLEANER RUN

STEP2ASWELL='YES' # defaults to 'YES'.
# if you don't want to perform the database merge immediately, change this to anything else

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Step 1 RECORDCLEANER
# VARIABLES TO MODIFY, REQUIRED!!
INPUT='/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/1_data_raw/20230106_gbif_indo.txt' # path to your input file
DAT_FORM='GBIF'           # format of your input file
WDIR='/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/1a_WIP/' # directory where intermediate files will be written to
OUT_DIR='/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/2_data_out/' # directory where final file is written to
PREFIX='G_indo_'          # prefix for all intermediate and final files

# OPTIONAL VARIABLES, FACULTATIVE!

VERBOSE='-v 1' # prints intermediate information to STDOUT, which might be helpful for debugging in case of issues.
               # -v 2 also outputs debugging information
NOCOLLECTOR='' # if '-nc', the name standardisation step is skipped! Use with caution!


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Step 2 RECORDFILER
INPUT_2=$(echo $OUT_DIR$PREFIX'cleaned.csv')
MASTERDB='the database'



# AND NOW LETS LAUNCH RECORDCLEANER

#python ./3_scripts/recordcleaner.py -h
#or
python ./3_scripts/recordcleaner.py $INPUT $DAT_FORM  $WDIR $OUT_DIR $PREFIX $NOCOLLECTOR $VERBOSE

if STEP2ASWELL='YES' do
  python ./3_scripts/recordfiler.py $INPUT_2 $MASTERDB
  
