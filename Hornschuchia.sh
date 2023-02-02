#!/usr/bin/env bash

# THIS FILE LET'S YOU CONFIGURE YOUR RECORDCLEANER RUN
STEP2ASWELL='YES' # defaults to 'YES'.
# if you don't want to perform the database merge immediately, change this to anything else

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Step 1 RECORDCLEANER
# VARIABLES TO MODIFY, REQUIRED!!
INPUT='//Users/fin/Sync/1_Annonaceae/share_DB_WIP/1_data_raw/Horn_20230127.csv'
DAT_FORM='GBIF'           # format of your input file
WDIR='/Users/fin/Sync/1_Annonaceae/share_DB_WIP/1a_WIP/' # directory where intermediate files will be written to
OUT_DIR='/Users/fin/Sync/1_Annonaceae/8_SDM/1_data' # directory where final file is written to
PREFIX='Horn_'          # prefix for all intermediate and final files
# for GLOBAL I am trying to to data source (i.e. GBIF = G) followed by ISO2 coutry code (e.g. Indonesia=ID)

# OPTIONAL VARIABLES, FACULTATIVE!

VERBOSE='-v 1' # prints intermediate information to STDOUT, which might be helpful for debugging in case of issues.
               # -v 2 also outputs debugging information
NOCOLLECTOR='' # if '-nc', the name standardisation step is skipped! Use with caution!

INPUT_2=$(echo $OUT_DIR$PREFIX'cleaned.csv')
OUT_2=$(echo $OUT_DIR$PREFIX'spatialvalid.csv')

# AND NOW LETS LAUNCH RECORDCLEANER

#python ./3_scripts/recordcleaner.py -h
#or
# working for now, commented out...
python ./3_scripts/recordcleaner.py $INPUT $DAT_FORM  $WDIR $OUT_DIR $PREFIX $NOCOLLECTOR $VERBOSE

echo "R script tests"
Rscript 3_scripts/r_coordinate_check.R --input $INPUT_2 --output $OUT_2
echo "How are we doing??"



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Step 2 RECORDFILER
MASTERDB='GLOBAL'
HOSTNAME='10.4.91.57'
TABLE='phil_test_221209'
SCHEMA='serafin_test'




#echo "Here comes an R script for checking coordinates... WIP...  but first do SQL stuff"
#echo $INPUT_2 $MASTERDB $HOSTNAME $TABLE $SCHEMA
#if STEP2ASWELL='YES' do
#python ./3_scripts/recordfiler.py $INPUT_2 $MASTERDB $HOSTNAME $TABLE $SCHEMA
#done





# see if this works
