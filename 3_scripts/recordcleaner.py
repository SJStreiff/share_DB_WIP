#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your raw records and compares them to existing data if you have it,
cleaning up column names, removing duplicates and making it more pleasing in general
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
    parser = argparse.ArgumentParser(prog='RECORDCLEANER',
                                     description='RECORDCLEANER takes your raw occurence records and compares them to an existing database if you have it, cleaning up column names, removing duplicates and making it more pleasing in general',
                                     epilog='If it doesn\'t work, or you would like to chat, feel free to contact me at serafin.streiff<at>ird.fr')
    parser.add_argument('input_file',
                        help='Raw input file path',
                        type = pathlib.Path)
    parser.add_argument('data_type',
                        help = 'File format. So far I can only handle Darwin core (GBIF) or herbonautes (P)',
                        type = str, choices=['GBIF', 'P']) # modify if anything else becomes available.
    parser.add_argument('working_directory',
                        help = 'the directory in which to deposit all intermediate working files. Files that need reviewing start with "TO_CHECK_"',
                        type = str)
    parser.add_argument('output_directory',
                        help = 'the wished output directory',
                        type = str)
    parser.add_argument('prefix',
                        help = 'prefix for ouput filenames',
                        type = str)
    parser.add_argument('MasterDB',
                        help='The location of the master database file.',
                        type=str)
    parser.add_argument('--nonamecln',
                        help = 'if specified, collector names are expected in the format <Surname>, <F>irstname, or if deviating standardised to be identical across all datasets. The collector names will not be standardised! Use with caution!',
                        type = int)
    parser.add_argument('-v', '--verbose',
                        help = 'If true (default), I will print a lot of stuff that might or might not help...',
                        default = True)
    args = parser.parse_args()
    print('Arguments:', args)

    #---------------------------------------------------------------------------
    # step 1:
    tmp_occs = stepA.column_standardiser(args.input_file, args.data_type, verbose = False) # verbose by default true



    tmp_occs_2 = stepA.column_cleaning(tmp_occs, args.data_type, args.working_directory, args.prefix, verbose=False)


    print(tmp_occs_2.columns)

    # do we check the nomenclature here?

    # Here we check if the user wants to check the collector names, and if yes, the user can reinsert checked non-conforming names into the workflow
    if args.nonamecln == None:
        tmp_occs_3 = stepA.collector_names(tmp_occs_2, args.working_directory, args.prefix, verbose=False, debugging=False)
        # should we reinsert the names we threw out?
        print('\n ................................\n',
        'Would you like to reinsert the names I couldn\'t handle?',
        'Please take care of encoding (usually best is UTF-8) when opening (especially in Microsoft Excel!!)',
        'If you would like to reinsert your checked names, please indicate the path to your modified file. Otherwise type "n" or "no".')
        reinsert='n' # make back to input()

        if reinsert == 'n' or 'no':
            print('Ok I will continue without any reinsertion')
        else:
            # check for fileending etc
            print('reinserting the file', reinsert)
            try:
                tmp_occs_3 = stepA.reinsertion(tmp_occs_3, reinsert)
            except:
                print('ERROR: I couldn\'t read the file from the path you provided. Try again.')

    # If this step is not wished for, we just continue as if nothing happened
    else:
        tmp_occs_3 = tmp_occs_2

    # # option of including a fuzzy matching step here. I haven't implemented this yet...
    print('STEP A complete.')
    print(tmp_occs_3.columns)


    #---------------------------------------------------------------------------
    # step B:

    #make this optional? No probably just force people to read this mess.
    stepB.duplicate_stats(tmp_occs_3)

    tmp_occs_4 = stepB.duplicate_cleaner(tmp_occs_3, args.working_directory, args.prefix, verbose=False)

    print(len(tmp_occs_4))

    stepB.duplicate_stats(tmp_occs_4)

    print(tmp_occs_4.columns)

    # write csv to file, next step is time intensive
    #tmp_occs_4.to_csv(args.working_directory + 'pre_taxon_check.csv', index=False, sep=';')

    #---------------------------------------------------------------------------

    # step C?, nomenclature check??
    print('\n.........................................\n')
    print('Checking the taxonomy now. This takes a moment!')
    print('Do you want to do this now? [y]/[n]')
    goahead=input()
    if goahead == 'y':
        tmp_occs_5 = stepC.kew_query(tmp_occs_4, args.working_directory, verbose=True)

    else:
        print('Nomenclature remains unchecked!!')
        miss_col = [i for i in z_dependencies.final_cols_for_import if i not in tmp_occs_4.columns]
        if args.verbose:
            print('These columns are missing as a result, I will fill them with <NA>:', miss_col)
        tmp_occs_4[miss_col] = pd.NA
        tmp_occs_4 = tmp_occs_4.astype(dtype = z_dependencies.final_col_for_import_type)
        tmp_occs_5 = tmp_occs_4
        # print(tmp_occs_5.dtypes)

    tmp_occs_5.to_csv(args.output_directory+args.prefix+'cleaned.csv', index=False, sep=';')
    #depends.1a_columns()
    # check coordinates --> R package gridder????

    #coordinate checks???

    # would be nice to have an option to again merge in some data that is known as clean, e.g. other data that was cleaned manually

    #print(tmp_occs_5.columns)
    print('Thanks for cleaning your records ;-)')

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


    print('so far so good??')








# done
