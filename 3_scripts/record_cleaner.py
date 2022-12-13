#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your raw records and compares them to existing data if you have it,
cleaning up column names, removing duplicates and making it more pleasing in general
'''

import z_functions_a as step1
import z_dependencies

#import dependencies

import argparse, os, pathlib



print(os.getcwd())



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file',
                        help='Raw input file path',
                        type = pathlib.Path)
    parser.add_argument('--data_type',
                        help = 'File format. So far I can only handle Darwin core (GBIF) or herbonautes (P)',
                        type = str)
    parser.add_argument('working_directory',
                        help = 'the directory in which to deposit all working files and output',
                        type = str)
    #parser.add_argument('-v', '--verbose',
    #                    help = 'If true (default), I will print a lot of stuff that might or might not help...',
    #                    default = True)
    args = parser.parse_args()
    print('Arguments:', args)

    # step 1:
    tmp_occs = step1.col_standardiser(args.input_file, args.data_type, verbose = False) # verbose by default true

    tmp_occs_2 = step1.column_cleaning(tmp_occs, args.data_type, args.working_directory)

    tmp_occs_3 = step1.collector_names(tmp_occs_2, args.working_directory, debugging=False)
    # option of including a fuzzy matching step here. I haven't implemented this yet...
    print('STEP 1 complete.')

    print(tmp_occs_3)




    #depends.1a_columns()
