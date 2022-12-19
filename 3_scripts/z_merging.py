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


    print('\n \n Some stats about potential duplicates being integrated: \n .................................................\n')
    print('\n By surname, number, sufix and collyear', tmp_mast[tmp_mast.duplicated(subset=[ 'coll_surname', 'colNum', 'sufix', 'colYear' ], keep=False)].shape)
    print('\n .................................................\n')

    print(len(mdb), 'the non-cleaned reference')
    print(len(occs), 'cleaned occurences')

    print(len(tmp_mast), 'combined')

    return tmp_mast










## NOTE:
