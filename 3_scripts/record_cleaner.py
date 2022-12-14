#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your raw records and compares them to existing data if you have it,
cleaning up column names, removing duplicates and making it more pleasing in general
'''

import z_functions_a as step1
import z_functions_b as step2

import z_dependencies

#import dependencies

import argparse, os, pathlib



print(os.getcwd())



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='RECORDCLEANER',
                                     description='RECORDCLEANER takes your raw occurence records and compares them to an existing database if you have it, cleaning up column names, removing duplicates and making it more pleasing in general',
                                     epilog='If it doesn\'t work, or you would like to chat, feel free to contact serafin.streiff@ird.fr')
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
    parser.add_argument('-nc','--nocollectors',
                        help = 'if specified, collector names are expected in the format <Surname>, <F>irstname, or if deviating standardised to be identical across all datasets.',
                        type = int)
    #parser.add_argument('-v', '--verbose',
    #                    help = 'If true (default), I will print a lot of stuff that might or might not help...',
    #                    default = True)
    args = parser.parse_args()
    print('Arguments:', args)

    stop
    # step 1:
    tmp_occs = step1.column_standardiser(args.input_file, args.data_type, verbose = False) # verbose by default true

    tmp_occs_2 = step1.column_cleaning(tmp_occs, args.data_type, args.working_directory, args.prefix)

    # do we check the nomenclature here?


    if args.nocollectors == 0:
        tmp_occs_3 = step1.collector_names(tmp_occs_2, args.working_directory, args.prefix, debugging=False)
    else:
        tmp_occs_3 = tmp_occs_2

    # # option of including a fuzzy matching step here. I haven't implemented this yet...
    print('STEP 1 complete.')

    # step 2:
    step2.duplicate_stats(tmp_occs_3)

    tmp_occs_4 = step2.duplicate_cleaner(tmp_occs_3, args.working_directory, args.prefix, verbose=True)

    print(tmp_occs_4)

    # step 3, nomenclature check??




    #depends.1a_columns()
