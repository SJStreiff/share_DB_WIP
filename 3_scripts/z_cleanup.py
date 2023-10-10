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


import logging
import pandas as pd






def cleanup(occs, cols_to_clean, verbose = True, debugging = False):
    """
    occs: the dataframe to clean
    cols_to_clean: columns to check for duplicated values
    verbose: verbose output
    debugging: even more output
    """


    for col in cols_to_clean:
            if col == 'det_by':
                logging.info('col to clean in -det_by-')
                occs[col] = occs[col].apply(lambda x: ' / '.join(set(x.split(' / '))))    # this combines all duplicated values within a cell     
                occs[col] = occs[col].str.strip()
                occs[col] = occs[col].str.strip('/') 
                print('det_by cleaning')

            if col == 'link':
                logging.info('col to clean in -link-')
                occs[col] = occs[col].apply(lambda x: ' - '.join(set(x.split(' - '))))    # this combines all duplicated values within a cell     
                occs[col] = occs[col].str.strip()
                occs[col] = occs[col].str.strip('-') 
                print('link cleaning')

            else:
                print('cleaning', col)     
                logging.info(f'col to clean in {col}')                  
                occs[col] = occs[col].apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated values within a cell
                occs[col] = occs[col].str.strip()
                occs[col] = occs[col].str.strip(',')
    return occs



def clean_up_nas(occs, NA_target):
     """"
     takes database and transforms all data to the desired NA value
     """
     # Replace NaN values with the chosen replacement value
     occs = occs.fillna(NA_target)

    # Replace 0 values with the chosen replacement value
     occs = occs.replace(0, NA_target)

     return occs




# #######--- DEBUG ----##########
# debug_master = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/debug/smallexp_debug.csv', sep =';')
# debug = clean_up_nas(debug_master, '-9999')
# print(debug)

# debug.to_csv('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/debug/smallexp_debug_debug.csv', sep =';', index=False)

# debug_2 = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/debug/smallexp_debug_debug.csv', sep =';', na_values='-9999')
# #debug_2 = debug_2.fillna(pd.NA)
# print(debug_2.dtypes)
