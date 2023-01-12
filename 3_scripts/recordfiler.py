#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your CLEANED records, compares them to a masterdatabase,
and integrates the new data in to said masterdatabase.

PREFIX "Y_" for scripts
'''

import z_functions_a as stepA
import z_functions_b as stepB
import z_nomenclature as stepC
import z_merging as stepD

import z_dependencies

#import dependencies

import argparse, os, pathlib, codecs
import pandas as pd
import numpy as np


print(os.getcwd())



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='RECORD FILER',
                                     description='RECORD FILER takes your CLEANED data and integrates it into a specified database. It is intended as step 2 in the process, i.e. following rigorous cleaning of your data.',
                                     epilog='If it doesn\'t work, or you would like to chat, feel free to contact me at serafin.streiff<at>ird.fr')
    parser.add_argument('input_file',
                        help='(Cleaned) input file path',
                        type = pathlib.Path)
    parser.add_argument('MasterDB',
                        help='The location of the master database file.',
                        type=str)
    parser.add_argument('database',
                        help='Location of database csv (temporary. This might change to a SQL at some point)'
                        type=pathlib.Path)
    parser.add_argument('-v', '--verbose',
                        help = 'If true (default), I will print a lot of stuff that might or might not help...',
                        default = True)
    args = parser.parse_args()
    # optional, but for debugging. Maybe make it prettier
    print('Arguments:', args)

"""
In record filer we want to implement the following steps.

- download database subset that we need to integrate into (i.e. subset by genera or region?)

- check input data for valid barcodes and other details?

- see which are duplicates of already existing data.
    reference to s.n. collection
    reference to 'missing-coordinate' collection

- integrate data.

-reupload data into server.

"""



    #---------------------------------------------------------------------------
    print('Now I would like to \n')
    print('\t - Check your new records against indets and for duplicates \n')
    print('\t - Merge them into the master database')

    mdb_op = codecs.open(args.MasterDB,'r','utf-8')
    mdb = pd.read_csv(mdb_op, sep = ';',  dtype = z_dependencies.final_col_for_import_type)
    mdb = mdb.replace('nan', pd.NA)
    #tmp_occs_5 = tmp_occs_5.replace('nan', pd.NA)

    print('Master database read successfully!', len(mdb))

    mn_db = stepD.check_premerge(mdb, tmp_occs_5, verbose=True)
    mn_db = mn_db.replace('nan', pd.NA)
    mn_db = mn_db.replace(np.nan, pd.NA)


    #print(mn_db)
    pref = args.prefix + 'main_C_'
    print(pref, 'TEST')
    print(mn_db)
    print(tmp_occs_5)
    mn_db_2 = stepB.duplicate_cleaner(mn_db, args.working_directory, pref, verbose=True, step='Master')

    # Type error: my columns are different to the standard we used before. I need t change the reference type lib, so that it works i think.
    # this is annoying and will be attempted tomorrow.
