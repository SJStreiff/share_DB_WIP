#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your CLEANED records, compares them to a masterdatabase,
and integrates the new data in to said masterdatabase.

PREFIX "Y_" for scripts
'''


import y_sql_functions as SQL
import z_merging as pre_merge

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
    # parser.add_argument('MasterDB',
    #                     help='The location of the master database file.',
    #                     type=str)
    parser.add_argument('database_name',
                        help='The name of the SQL database to access',
                        type = str)
    parser.add_argument('hostname',
                        help= 'The hostname for the SQL database',
                        type=str)
    parser.add_argument('tablename',
                        help='The table to extract from the master database (SQL)',
                        type=str)
    parser.add_argument('schema',
                        help='the schema name in which the table resides',
                        type=str)
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

    # check input data variation: is it just one genus? just one country?
    imp = codecs.open(args.input_file,'r','utf-8') #open for reading with "universal" type set
    occs = pd.read_csv(imp, sep = ';',  dtype = z_dependencies.final_col_for_import_type) # read the data
    print(occs)
    # if just one small group, then do

    
    print('I am just going to upload the current state of the data into GLOBAL (schema: \"serafin_test\")')
    SQL.send_to_sql(occs ,args.database_name, args.hostname, args.tablename, args.schema)



    # Step D1
    # - get database extract from the GLOBAL database.
    print('Please check if you are connected to the VPN. If not this will not work.')
    SUBSET_GENERA = '???'
    SUBSET_COUNTRY = '???'
    m_DB = SQL.fetch_master_db(args.database_name, args.hostname, args.tablename, args.schema)
    print('The downloaded DB is the following:', m_DB)
    m_DB.to_csv('./4_DB/sql_data_transf_test.csv', index=False, sep=';')
    # m_DB =pd.read_csv('./4_DB/sql_data_transf_test.csv')
    # This works
    try:
        m_DB = m_DB.replace('nan', pd.NA)
    except Exception as e:
        print('NA as nan replacement problematic')
    try:
        m_DB = m_DB.replace(np.nan, pd.NA)
    except Exception as e:
        print('NA as np replacement problematic')

    print('Master database read successfully!', len(m_DB), 'records downloaded')
    #
    test_upd_DB = pre_merge.check_premerge(m_DB, occs, verbose=True)
    # something really weird happening. Should not be as many duplicates as it gives me.














# needs more finetuning.
