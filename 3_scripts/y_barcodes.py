#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
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
    """


    # if step=='Master':
    #     occs=occs.astype(z_dependencies.final_col_for_import_type)
    # else:
    #     occs = occs.astype(z_dependencies.final_col_type) # double checking
    
    new_occs['DUPLI_BC'] = '0' # empty column to fill

    # split new record barcode fields (just to check if there are multiple barcodes there)

    bc_dupli_split = new_occs['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    bc_dupli_split.columns = [f'bc_{i}' for i in range(bc_dupli_split.shape[1])] # give the columns names..
    print('NEW OCCS:\n', bc_dupli_split)

    master_bc_split = master_db['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    master_bc_split.columns = [f'bc_{i}' for i in range(master_bc_split.shape[1])]
    print('master OCCS:\n', master_bc_split)

    # then iterate through all barcodes of the new occurrences
    # for every row
   # for i in range(new_occs.shape[1]):
    i = 11
    print('working on row', i)
    barcode = list(bc_dupli_split.loc[i].astype(str))
    print((barcode))
    #barcode = barcode.replace('None', ' ')


    """
    The current problem is that all bc in the first column are detected, but if the duplicate is across columns it doesn't work. potentially because the query is ['barcode', 'None']??
    """

    #arcode = barcode.notnull()

    #for x in  range(len(barcode)):
    #print(x)
    bar = barcode #[x]]
    bar.reverse()
    print('Working on barcode:\n', bar)
    #print(barcode.dtypes)
    # for row i in dataframe
    # for bc_col in bc_dupli_split.columns:
        # print('working on column', bc_col)

        # # for column/cell j in row i 
        # barcode = bc_dupli_split.loc[i,bc_col]
        # print('Working on barcode:', barcode)
    print('TEST:\n', master_bc_split.isin(bar))
    print('TEST:\n', master_bc_split.dtypes)
    if master_bc_split.isin(bar).any(axis = None):
        # mark and copy
        print('I have found a match') # we have a value which is True!
        # get the master row with same barcode:
        tf_df = master_bc_split.isin(bar)
        t_df = tf_df.sum(axis =1).astype(bool)

        
        #    print('new test\n', t_df)
        full_bc = master_db.barcode[t_df]
        print(full_bc)


        bc_dupli_split['copied_barcode'] = 1
    # go through the columns 
    # check if the barcode in memory is found anywhere in the master database.
        # if yes: 
            # copy the barcodes over into the new occurrence data (i.e. the column 'barcode' is afterwards identical for these records)




master_db = pd.read_csv('/Users/Serafin/Sync/1_Annonaceae/share_DB_WIP/4_DB_tmp/master_db.csv', sep =';')
new_occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/2_data_out/exp_debug_spatialvalid.csv' , sep = ';')

test = duplicated_barcodes(master_db=master_db, new_occs=new_occs)