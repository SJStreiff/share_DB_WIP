#!/usr/bin/env python3
# -*- coding: utf-8 -*-






##########
---------------------
##-------> this function deprecated. Moved into z_merging!!!!!!




'''
DEPRECATED!

All the functions called from the main script for duplicate detection and tratment

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created


CONTAINS:
    duplicate_stats():
        does some stats on duplicates found with different combinations
    duplicate_cleaner():
        actually goes in and removes, merges and cleans duplicates
'''
stop

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import swifter
from datetime import date


import z_dependencies






def duplicated_barcodes(master_db, new_occs, verbose=True, debugging=False):
    """
    FInd all duplicated barcodes from new occurrences in the master database
    > master_db: Master database for crossreference
    > new_occs:  The new records to check

    duplicated_barcodes() goes through and finds any matches of barcodes in 'new_occs' with 'master_db'. Any matches value is copied from the master into the new data. Like this, in subsequent steps duplicates can easily
    be identified. 
    """

    # split new record barcode fields (just to check if there are multiple barcodes there)
    bc_dupli_split = new_occs['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    bc_dupli_split.columns = [f'bc_{i}' for i in range(bc_dupli_split.shape[1])] # give the columns names..
    bc_dupli_split = bc_dupli_split.apply(lambda x: x.str.strip())
    if debugging:
        print('NEW OCCS:\n', bc_dupli_split)

    master_bc_split = master_db['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    master_bc_split.columns = [f'bc_{i}' for i in range(master_bc_split.shape[1])]
    master_bc_split = master_bc_split.apply(lambda x: x.str.strip())  #important to strip all leading/trailing white spaces!
    if debugging:
        print('master OCCS:\n', master_bc_split)
        print('master OCCS:\n', master_bc_split.dtypes)


    if debugging:
        print('Shape of new_occs', len(new_occs))
    # then iterate through all barcodes of the new occurrences
    # for every row
    for i in range(len(new_occs)):
        # i = 11
        if verbose:
            print('working on row', i)
        barcode = list(bc_dupli_split.loc[i].astype(str))
        print((barcode))

        # if multiple barcodes in the barcode field, iterate across them
        for x in  range(len(barcode)):
        #    print(x)
            bar = barcode[x]

            if bar == 'None':
            # this happens a lot. skip.
                if verbose:
                    print('Values <None> are skipped.')
            else:
                # -> keep working with the barcode
                if verbose:
                    print('Working on barcode:\n', bar)
            
                # now iterate over columns to find any matches

                selection_frame = pd.DataFrame()  # df to hold resulting True/False mask   
                for col in master_bc_split.columns:
                    # iterate through rows. the 'in' function doesn't work otherwise
                    if verbose:
                        print('checking master columns')
                    f1 = master_bc_split[col] == bar # get true/false column
                    selection_frame = pd.concat([selection_frame, f1], axis=1) # and merge with previos columns
                   # end of loop over columns
            
                # when selection frame finished, get out the rows we need including master value
                sel_sum = selection_frame.sum(axis = 1)
                sel_sum = sel_sum >= 1 # any value >1 is a True => match 
                if verbose:
                    print('this should be our final selection object:', sel_sum)
    
                if sel_sum.sum() == 0:
                    if verbose:
                        print('NO MATCHES FOUND!')
                    out_barcode = bar
                    # and now print this barcode
                else:
                    out_barcode = master_db.barcode[sel_sum]
                    out_barcode = out_barcode[i].strip('[]')

              # <- end of bar==None condition
      
        # replace i-th element of the new barcodes with the matched complete range of barcodes from master
        #print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        if verbose:
            print('Input:', new_occs.at[i, 'barcode'])
            print('Master:', str(out_barcode)) # these are the abrcodes retreived from the master file
        #out_barcode.replace('[', '', inplace = True)
        #out_barcode = out_barcode.replace(']', '')
        new_occs.at[i, 'barcode'] = out_barcode # replace original value with new value
        #new_occs.at[i, 'barcode'] = new_occs.at[i, 'barcode'].astype(str).str.strip('[]')
    if verbose:
        print(new_occs.barcode, 'FINISHED')
        # done.
    return new_occs



        #bc_dupli_split['copied_barcode'] = 1
    # go through the columns 
    # check if the barcode in memory is found anywhere in the master database.
        # if yes: 
            # copy the barcodes over into the new occurrence data (i.e. the column 'barcode' is afterwards identical for these records)




master_db = pd.read_csv('/Users/Serafin/Sync/1_Annonaceae/share_DB_WIP/4_DB_tmp/master_db.csv', sep =';')
new_occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/2_data_out/exp_debug_spatialvalid.csv' , sep = ';')

test = duplicated_barcodes(master_db=master_db, new_occs=new_occs)