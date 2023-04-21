#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for basic format changes, subsetting, ...

2022-12-19 sjs

CHANGELOG:
    2022-12-19: created


CONTAINS:
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

def check_premerge(mdb, occs, verbose=True):
    '''function to compare the master database to the records to be included, fresh from Cleaning
    I check for duplicates.

    '''

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

    print(len(mdb), 'the master_db download')
    print(len(occs), 'cleaned occurences')

    print(len(tmp_mast), 'combined')


    return tmp_mast






## NOTE:
