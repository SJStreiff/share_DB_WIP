#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Cleaning up data before entering into Database
2023-06-15 sjs

CHANGELOG:
    2023-06-15: created


CONTAINS:
   cleanup(): function reducing duplicated values within cells
'''


import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import swifter
import datetime 


import z_dependencies





def cleanup(occs, cols_to_clean, verbose = True, debugging = False):
    """
    occs: the dataframe to clean
    cols_to_clean: columns to check for duplicated values
    verbose: verbose output
    debugging: even more output
    """


    for col in cols_to_clean:
            occs[ col] = occs[col].apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell

    return occs