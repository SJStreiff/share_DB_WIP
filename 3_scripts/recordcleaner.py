#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your raw records and compares them to existing data if you have it,
cleaning up column names, removing duplicates and making it more pleasing in general
'''

import z_functions_a as stepA
import z_functions_b as stepB
import z_nomenclature as stepC

import z_dependencies

#import dependencies

import argparse, os, pathlib



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

    tmp_occs_2 = stepA.column_cleaning(tmp_occs, args.data_type, args.working_directory, args.prefix)

    # do we check the nomenclature here?

    # Here we check if the user wants to check the collector names, and if yes, the user can reinsert checked non-conforming names into the workflow
    if args.nonamecln == None:
        tmp_occs_3 = stepA.collector_names(tmp_occs_2, args.working_directory, args.prefix, debugging=False)
        # should we reinsert the names we threw out?
        print('\n ................................\n',
        'Would you like to reinsert the names I couldn\'t handle?',
        'Please take care of encoding (usually best is UTF-8) when opening (especially in Microsoft Excel!!)',
        'If you would like to reinsert your checked names, please indicate the path to your modified file. Otherwise type "n" or "no".')
        reinsert=input()
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

    #---------------------------------------------------------------------------
    # step B:

    #make this optional? No probably just force people to read this mess.
    stepB.duplicate_stats(tmp_occs_3)

    tmp_occs_4 = stepB.duplicate_cleaner(tmp_occs_3, args.working_directory, args.prefix, verbose=True)

    print(tmp_occs_4)

    #---------------------------------------------------------------------------

    # step C?, nomenclature check??
    print('\n.........................................\n')
    print('Checking the taxonomy now. This takes a moment!')

    tmp_occs_5 = stepC.kew_query(tmp_occs_4, args.working_directory, verbose=False)

    #depends.1a_columns()
    # check coordinates --> R package gridder????

    #coordinate checks???


    # would be nice to have an option to again merge in some data that is known as clean, e.g. other data that was cleaned manually


    print('Data should be somewhat useable now ;-)')



# done
