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
                occs[col] = occs[col].apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated values within a cell
                occs[col] = occs[col].str.strip()
                occs[col] = occs[col].str.strip(',')
    return occs