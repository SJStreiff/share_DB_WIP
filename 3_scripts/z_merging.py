#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for basic format changes, subsetting, ...

2022-12-19 sjs

CHANGELOG:
    2022-12-19: created


CONTAINS:
    duplicated_barcodes():
        consolidates barcodes for later detecting as duplicates.

    check_premerge():
        checks the new data with the master-DB for potential duplicates and ?

'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re

#custom dependencies
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
    if debugging: # some information if there are issues
        print('NEW OCCS:\n', bc_dupli_split)
        print('NEW OCCS:\n', type(bc_dupli_split))
    master_bc_split = master_db['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    master_bc_split.columns = [f'bc_{i}' for i in range(master_bc_split.shape[1])]
    master_bc_split = master_bc_split.apply(lambda x: x.str.strip())  #important to strip all leading/trailing white spaces!
    if debugging: # some information if there are issues
        print('master OCCS:\n', master_bc_split)
        print('master OCCS:\n', master_bc_split.dtypes)


    if debugging: # some information if there are issues
        print('Shape of new_occs', len(new_occs))

    # then iterate through all barcodes of the new occurrences
    # for every row
    for i in range(len(new_occs)):
        if verbose:
            print('working on row', i)
            print('BC to test:' ,bc_dupli_split.loc[i])
        barcode = list(bc_dupli_split.loc[i].astype(str))
        
        # if multiple barcodes in the barcode field, iterate across them
        for x in  range(len(barcode)):
            bar = barcode[x]

            if bar == 'None':
            # this happens a lot. skip if this is the case.
                if verbose:
                    print('Values <None> are skipped.')
            else:
                # -> keep working with the barcode
                if verbose:
                    print('Working on barcode:\n', bar)
            

                selection_frame = pd.DataFrame()  # df to hold resulting True/False mask  
                # now iterate over columns to find any matches
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
                    print('this should be our final selection object length:', sel_sum.sum())
    
                if sel_sum.sum() == 0:
                    if verbose:
                        print('NO MATCHES FOUND!')
                    #out_barcode = pd.DataFrame([bar])

                    # in this case we do not modify anything!
                    
                else:
                    out_barcode = pd.Series(master_db.barcode[sel_sum]).astype(str)
                    out_barcode.reset_index(drop = True, inplace = True)
                   
      
        # replace i-th element of the new barcodes with the matched complete range of barcodes from master
                    if verbose:
                        print('i is:', i)
                        print('Input:', new_occs.at[i, 'barcode'])
                        print('Master:', out_barcode[0]) # these are the barcodes retreived from the master file
                   
                    new_occs.at[i, 'barcode'] = out_barcode[0] # replace original value with new value
                    # print('the replaced value:',  new_occs.at[i, 'barcode'])
                    # print('the replaced value:',  type(new_occs.at[i, 'barcode']))

                # <- end of bar==None condition

    if verbose:
        print(new_occs.barcode, 'FINISHED')
 
        # done.
    return new_occs



# master_db = pd.read_csv('/Users/Serafin/Sync/1_Annonaceae/share_DB_WIP/4_DB_tmp/master_db.csv', sep =';')
# new_occs = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/share_DB_WIP/2_data_out/exp_debug_spatialvalid.csv' , sep = ';')

# test = duplicated_barcodes(master_db=master_db, new_occs=new_occs)




def check_premerge(mdb, occs, verbose=True, debugging=False):
    '''function to compare the master database to the records to be included, fresh from Cleaning
    I check for duplicates.

    '''

    occs.reset_index(drop = True, inplace = True)
    mdb.reset_index(drop = True, inplace = True)
    print('NEW:', occs.barcode)
    print('MASTER:', mdb.barcode)
    # modify the barcodes to standardise...
    occs = duplicated_barcodes(mdb, occs, verbose=verbose, debugging=False)
    

    # 
    tmp_mast = pd.concat([mdb, occs])
    print('Columns!', tmp_mast.columns)

    print('\n \n Some stats about potential duplicates being integrated: \n .................................................\n')
    print('\n By surname, number, sufix and col_year & country ID', 
    tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum', 'sufix', 'col_year', 'country_id' ], keep=False)].shape)
    print('\n By surname & full collectionnumber', 
    tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum_full' ], keep=False)].shape)
    print('\n By surname, number, genus & specific epithet', 
    tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colnum', 'genus', 'specific_epithet' ], keep=False)].shape)

    print('\n By barcode', tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)].shape)

    print('\n .................................................\n')
    if len(tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)]) != 0:
        print(tmp_mast[tmp_mast.duplicated(subset=['barcode'], keep=False)]['barcode'])

    print(len(mdb), 'the master_db download')
    print(len(occs), 'cleaned occurences')

    print(len(tmp_mast), 'combined')


    return tmp_mast



