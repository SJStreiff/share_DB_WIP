#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your raw records and compares them to existing data if you have it,
cleaning up column names, removing duplicates and making it more pleasing in general

PREFIX "Z_" for scripts
'''

import z_functions_a as stepA
import HUH_query as huh_query
import z_functions_b as stepB
import z_nomenclature as stepC
import z_functions_c as stepB2
#import z_merging as stepD

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



    tmp_occs_2 = stepA.column_cleaning(tmp_occs, args.data_type, args.working_directory, args.prefix, verbose=True)


    #print(tmp_occs_2.columns)

    # do we check the nomenclature here?

    # Here we check if the user wants to check the collector names, and if yes, the user can reinsert checked non-conforming names into the workflow
    if args.nonamecln == None:
        tmp_occs_3, frame_to_check = stepA.collector_names(tmp_occs_2, args.working_directory, args.prefix, verbose=False, debugging=True)
        # should we reinsert the names we threw out?
        print('\n ................................\n',
        'Would you like to reinsert the collector names I couldn\'t handle?',
        'Please take care of encoding (usually best is UTF-8) when opening (especially in Microsoft Excel!!)',
        'If you would like to reinsert your checked names, please indicate the path to your modified file. Otherwise type "n"')
        reinsert=input() #'n' # make back to input()
        print(reinsert)
        if reinsert == 'n':
            print('TRUE')
            print('Ok I will continue without any reinsertion')
        else:
            # check for fileending etc
            print('reinserting the file', reinsert)
            #print('How is it separated? (e.g. ";" or ","...)')
            #separ = input()
            try:
                tmp_occs_3 = stepA.reinsertion(tmp_occs_3, frame_to_check, reinsert)
                print('Reintegration successful!')
            except:
                print('ERROR: I couldn\'t read the file from the path you provided. Try again.')
                
                #tmp_occs_3 = stepA.collector_names(tmp_occs_2, args.working_directory, args.prefix, verbose=False, debugging=True)
                # should we reinsert the names we threw out?
        print('\n ................................\n')
        # dets combined with
        # 'Would you like to also reinsert the determiner names I couldn\'t handle?',
        # 'If you would like to reinsertthis too, please indicate the path to your modified file. Otherwise type "n" or "no".')
        # reinsert=input() #'n' # make back to input()
        #
        # if reinsert == 'n' or 'no':
        #     print('Ok I will continue without any reinsertion')
        # else:
        #     # check for fileending etc
        #     print('reinserting the file', reinsert)
        #     try:
        #         tmp_occs_3 = stepA.reinsertion(tmp_occs_3, reinsert)
        #     except:
        #         print('ERROR: I couldn\'t read the file from the path you provided. Try again.')


    # If this step is not wished for, we just continue as if nothing happened
    else:
        tmp_occs_3 = tmp_occs_2
    #stop
    # # option of including a fuzzy matching step here. I haven't implemented this yet...
    print('STEP A complete.')
    print(tmp_occs_3.columns)
    #---------------------------------------------------------------------------
    # Here I am blacklisting some herbaria, as I cannot work with their data. (no proper barcodes, mixed up columns)
    # For now this is not much data loss
    HERB_TO_RM = [['AAU']]
    print('Before removing dataproblematic institutions:', tmp_occs_3.shape)
    drop_ind = tmp_occs_3[(tmp_occs_3['institute'] == 'AAU')].index
    tmp_occs_3.drop(drop_ind, inplace = True)
    print('After removing dataproblematic institutions:',tmp_occs_3.shape)

   

    # HUH name query
    tmp_occs_3 = huh_query.huh_wrapper(tmp_occs_3, verbose = True, debugging = False)

    #---------------------------------------------------------------------------
    # step B:

    #make this optional? No probably just force people to read this mess.
    tmp_s_n = stepB.duplicate_stats(tmp_occs_3, args.working_directory, args.prefix)
    tmp_occs_4 = stepB.duplicate_cleaner(tmp_occs_3, args.working_directory, args.prefix, verbose=False, debugging=False)
    print(len(tmp_occs_4))
    stepB.duplicate_stats(tmp_occs_4, args.working_directory, args.prefix)
    print(tmp_occs_4.columns)

    #---------------------------------------------------------------------------
    # do the same with s.n.
    tmp_s_n_1 = stepB.duplicate_cleaner_s_n(tmp_s_n, args.working_directory, args.prefix, verbose=True)
    print('S.N.:', len(tmp_s_n_1))

    # now recombine numbered and s.n. data
    tmp_occs_5 = pd.concat([tmp_occs_4, tmp_s_n_1])

    #---------------------------------------------------------------------------

    # crossfill country names
    tmp_occs_5 = stepB2.country_crossfill(tmp_occs_5, verbose=True)


    # step C1, nomenclature check
    print('\n.........................................\n')
    print('Checking the taxonomy now. This takes a moment!')
    print('Do you want to do this now? [y]/[n]')
    goahead=input()
    if goahead == 'y':
        tmp_occs_6, indets = stepC.kew_query(tmp_occs_5, args.working_directory, verbose=True)

    else:
        print('Nomenclature remains unchecked!!')
        miss_col = [i for i in z_dependencies.final_cols_for_import if i not in tmp_occs_4.columns]
        if args.verbose:
            print('As you do not want to chack your taxonomy (although this is strongly recommended), these columns are missing: \n',
            miss_col, '\n I will fill them with <NA>')
        tmp_occs_5[miss_col] = pd.NA
        tmp_occs_5 = tmp_occs_5.astype(dtype = z_dependencies.final_col_for_import_type)
        tmp_occs_6 = tmp_occs_5
        # print(tmp_occs_5.dtypes)

    tmp_occs_6.to_csv(args.output_directory+args.prefix+'cleaned.csv', index=False, sep=';')
    print("\n\n--------------------------------------\n",
    "The output of this first processing is saved to:",
    args.output_directory+args.prefix+'cleaned.csv',
    '\n---------------------------------------------\n')

    # check coordinates --> R package gridder????

    #coordinate checks
    #---------------------------------------------------------------------------
    # this happens in R, as I have not found an alternative in python. There are known reliable (albeit somewhat problematic) packages available in R.
    print('First cleaning steps completed. Next is Coordinate validation. ')

    # write indets to backlog
    indets.to_csv(args.output_directory+args.prefix+'indet.csv', index=False, sep=';')
    print("\n\n--------------------------------------\n",
    "The indets file is saved to:",
    args.output_directory+args.prefix+'indets.csv',
    '\n---------------------------------------------\n')


    print('Thanks for cleaning your records ;-)')





# done
###-------- END OF RECORDCLEANER ------###
