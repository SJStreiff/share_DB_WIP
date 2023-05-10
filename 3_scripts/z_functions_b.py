#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for duplicate detection and tratment

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created


CONTAINS:
    duplicate_stats():
        does some stats on duplicates found with different combinations
    duplicate_cleaner():
        actually goes in and removes, merges and cleans duplicates
'''


import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import swifter
from datetime import datetime


import z_dependencies







def duplicate_stats(occs, working_directory, prefix, out=True, verbose=True, debugging=False):
    '''
    Function that prints a load of stats about potential duplicates
    '''
    #-------------------------------------------------------------------------------
    # these columns are used to identify duplicates (i.e. if a value of both these column is shared
    # for a row, then we flag the records as duplicates)
    dup_cols = ['coll_surname', 'colnum', 'sufix', 'col_year'] # the columns by which duplicates are identified
    #-------------------------------------------------------------------------------


    # data types can be annoying
    occs = occs.astype(z_dependencies.final_col_type) # double checking
    print(occs.dtypes)
    #occs[['recorded_by', 'colnum_full']] = occs[['recorded_by', 'colnum_full']].replace('nan', pd.NA)


    #-------------------------------------------------------------------------------
    # MISSING collector information and number
    # remove empty collector and coll num
    occs1 = occs.dropna(how='all', subset = ['recorded_by']) # no collector information is impossible.
    #occs1 = occs1[occs1.recorded_by != 'nan']
    print('Deleted', len(occs.index) - len(occs1.index),
     'rows with no collector and no number; \n',
     len(occs1.index), 'records left')
    print(occs1)

    #-------------------------------------------------------------------------------
    # MISSING col num
    # these are removed, and added later after processing(?)
    subset_col = ['colnum_full']

    occs_colNum = occs1.dropna(how='all', subset=subset_col)
    occs_nocolNum = occs1[occs1['colnum_full'].isna()]

    occs_colNum.ddlong.astype(float)
    occs_nocolNum['ddlong'].astype(float)
    occs_colNum.ddlat.astype(float)
    occs_nocolNum['ddlat'].astype(float)

    print('1TEST', occs_colNum)
    occs_colNum['coll_surname'] = occs_colNum['recorded_by'].str.split(',', expand=True)[0]
    print(occs_colNum['coll_surname'])

    #if len(occs_nocolNum)>0:
    #    occs_nocolNum.to_csv(working_directory+prefix+'s_n.csv', index = False, sep = ';' )
    #    print('\n The data with no collector number was saved to', working_directory+prefix+'s_n.csv', '\n')

    #-------------------------------------------------------------------------------
    # Perform some nice stats, just to get an idea what we have

    occs_colNum.colnum_full.astype(str)
    print('Total records:', len(occs),';\n Records with colNum_full:', len(occs_colNum),';\n Records with no colNum_full:', len(occs_nocolNum))

    print('\n \n Some stats about potential duplicates: \n .................................................\n')
    print('\n By Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['recorded_by', 'colnum_full'], keep=False)].shape)
    print('\n By NON-STANDARD Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['orig_recby', 'colnum_full'], keep=False)].shape)
    print('\n By Collector-name and FULL collector number, and coordinates', occs_colNum.duplicated(['recorded_by', 'colnum_full', 'ddlat', 'ddlong'], keep=False).sum())
    print('\n By Collector-name and FULL collector number, genus and specific epithet', occs_colNum.duplicated(['recorded_by', 'colnum_full', 'genus' , 'specific_epithet'], keep=False).sum())
    print('\n By FULL collector number, genus and specific epithet', occs_colNum.duplicated([ 'colnum_full', 'genus' , 'specific_epithet'], keep=False).sum())
    print('\n By FULL collector number and genus', occs_colNum.duplicated([ 'colnum_full', 'genus' ], keep=False).sum())
    print('\n By FULL collector number, collection Year and country', occs_colNum.duplicated([ 'colnum_full', 'col_year' , 'country'], keep=False).sum())
    print('\n By collection Year and FULL collection number (checking for directionality)', occs_colNum.duplicated([ 'col_year' , 'colnum_full'], keep=False).sum())
    print('\n By REDUCED collection number and collection Yeear', occs_colNum.duplicated([ 'colnum', 'col_year' ], keep=False).sum())
    print('\n By locality, REDUCED collection number and collection Year', occs_colNum.duplicated([ 'locality', 'colnum', 'col_year' ], keep=False).sum())
    print('\n By SURNAME and COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colnum' ], keep=False).sum())
    print('\n By SURNAME and FULL COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colnum_full' ], keep=False).sum())
    print('\n By SURNAME and COLLECTION NUMBER and YEAR', occs_colNum.duplicated([ 'coll_surname', 'colnum', 'col_year' ], keep=False).sum())
    print('\n By SURNAME and COLLECTION NUMBER, SUFIX and YEAR', occs_colNum.duplicated([ 'coll_surname', 'colnum', 'sufix', 'col_year' ], keep=False).sum())
    print('\n By HUH-NAME and COLLECTION NUMBER, SUFIX and YEAR', occs_colNum.duplicated([ 'huh_name', 'colnum', 'sufix', 'col_year' ], keep=False).sum())

    print('\n ................................................. \n ')



    # this function only returns records with no collection number!
    if out:
        return occs_colNum, occs_nocolNum




def duplicate_cleaner(occs, dupli, working_directory, prefix, expert_file, User, step='Raw', verbose=True, debugging=False):
    '''
    This one actually goes and cleans/merges duplicates.
        > occs = occurrence data to de-duplicate
        > dupli: the columns by which we identify duplicates
        > working_directory = path to directory to output intermediate files
        > prefix = filename prefix
        > User = username used to fill the 'modified' field
        > step = {raw, master} = reference for datatype checks
        > expert_file = if 'EXP' skips certain parts in within dataset cleaning and gives priority as below when integrating in master step.
            # IF expert_status = 'EXP', then dets and coordinates get priority over others
                               = 'NO', then dets and coordinates consolidated normally.
    '''

    if step=='Master':
        occs=occs.astype(z_dependencies.final_col_for_import_type)
    else:
        occs = occs.astype(z_dependencies.final_col_type) # double checking
    print(occs.dtypes)
    #occs = occs.replace(np.nan, pd.NA)

    dup_cols = dupli #['recorded_by', 'colnum', 'sufix', 'col_year'] # the columns by which duplicates are identified

    #-------------------------------------------------------------------------------
    # MISSING collector information and number
    # remove empty collector 
    occs1 = occs.dropna(how='all', subset = ['recorded_by']) #, 'colnum', 'colnum_full'


    #-------------------------------------------------------------------------------
    # find duplicated barcodes before we do anything
    # this step only takes place in master step.
    if step=='Master':
        duplic_barcodes = occs1[occs1.duplicated(subset=['barcode'], keep=False)] # gets us all same barcodes
        print('BARCODES DUPLICATED:', duplic_barcodes.shape)
    # print(occs1.barcode)
        cl_barcodes = occs1.drop_duplicates(subset=['barcode'], keep=False) # throws out all duplicated rows
        print('NO BARCODES DUPLICATED:', cl_barcodes.shape)

        # ----------- only duplicate barcodes being merged!
        # all other duplicates follow below.
        if expert_file == 'EXP':
            # if expert we need to take care we integrate the expert data properly
            if verbose:
                print('Merging duplicated barcodes, EXPERT file')
            barcode_merged = duplic_barcodes.groupby(['barcode'], as_index = False).agg(
                    scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                    genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                    specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                    species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                    collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                    recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                    colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                    prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                    colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                    sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                    col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
                    col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
                    col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
                    col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
                    det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
                    det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                    det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                    det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                    det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                    country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
                    country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
                    continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
                    locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
                    coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
                    ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                    ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                    institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
                    herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
                    barcode = pd.NamedAgg(column = 'barcode', aggfunc='first'), # as we have premerged all barcodes above, it doesn't matter which one we take
                    orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
                    coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                    huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                    geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                    wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'))

            if step == 'master':
                # add master columns to the mix
                if verbose:
                    print('Merging duplicated barcodes, EXPERT file and master step.')
                master_add_cols = duplic_barcodes.groupby(['barcode'], as_index = False).agg(
                        expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                        status = 'ACCEPTED',
                        accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                        ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                        ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first')
                        )
                # merge in master columns to the rest.
                barcode_merged = pd.concat([barcode_merged, master_add_cols], axis=1) # add master columns.


        else:
            # if not EXP, just bung the data together....
            if verbose:
                print('Merging duplicated barcodes')
            barcode_merged = duplic_barcodes.groupby(['barcode'], as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc='first'),
                prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
                col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
                col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
                col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
                country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
                continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
                locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
                coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
                ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
                herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
                barcode = pd.NamedAgg(column = 'barcode', aggfunc='first'),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first')
                )

            if step == 'master':
                if verbose:
                    print('Merging duplicated barcodes, master step')
                # add master columns to the mix
                master_add_cols = duplic_barcodes.groupby(subset = ['barcode'], as_index = False).agg(
                        expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                        status = 'ACCEPTED',
                        accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                        ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                        ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first')
                        )
                # merge in master columns to the rest.
                barcode_merged = pd.concat([barcode_merged, master_add_cols], axis=1) # add master columns.



        ### and re merge deduplicated barcodes and unique barcodes
        occs1 = pd.concat([cl_barcodes, barcode_merged], axis=0) ###CHECK axis assignments

        

    #-------------------------------------------------------------------------------
    occs_dup_col =  occs1.loc[occs1.duplicated(subset=dup_cols, keep=False)]
    print('Run1', occs_dup_col)
    # get the NON-duplicated records
    occs_unique = occs1.drop_duplicates(subset=dup_cols, keep=False)

    if verbose:
        print('\n First filtering. \n Total records: ', len(occs1),
         ';\n records with no duplicates (occs_unique): ',len(occs_unique), 
         ';\n records with duplicates (occs_dup_col): ',len(occs_dup_col))

    if len(occs_dup_col) == 0:
        if verbose:
            print('Nothing to deduplicate. Returning without any modifications')
        return occs_unique


    #-------------------------------------------------------------------------------
    # Duplicates part 1:
    #---------------------------------
    ## A  DIFFERENT COORDINATES
    # Coordinates not identical:
        # small difference --> MEAN
        # big difference --> exctract and either discard or check manually

    # double-check type of coordinate columns
    convert_dict = {'ddlat': float,
                    'ddlong': float}
    occs_dup_col = occs_dup_col.astype(convert_dict)
    print('HERE',occs_dup_col)
    # data in sn step still here TODO

    if verbose:
        print('\n The duplicates subset, before cleaning dups has the shape: ', occs_dup_col.shape)
    # in this aggregation step we calculate the variance between the duplicates.
    
    if expert_file ==  'EXP':
        # as it is expert file, we expect coordinates to match within the file. We check coordinates when integrating into master:
        # > Do nothing, and then take expert value at final merge.
        if verbose:
            print('We have expert file!')

    else:
        # no expert file, so just do as normal....
        test = occs_dup_col.groupby(dup_cols, as_index = False).agg(
            ddlong = pd.NamedAgg(column = 'ddlong', aggfunc='var'),
            ddlat = pd.NamedAgg(column = 'ddlat', aggfunc='var'))

        print('TEST', test)
        # if this variance is above 0.1 degrees
        if len(test)>0:
            test.loc[test['ddlong'] >= 0.1, 'long bigger than 0.1'] = 'True'
            test.loc[test['ddlong'] < 0.1, 'long bigger than 0.1'] = 'False'

            test.loc[test['ddlat'] >= 0.1, 'lat bigger than 0.1'] = 'True'
            test.loc[test['ddlat'] < 0.1, 'lat bigger than 0.1'] = 'False'

            # filter by large variance.
            true = test[(test["long bigger than 0.1"] == 'True') | (test["lat bigger than 0.1"] == 'True')]

            # write these records to csv for correcting or discarding.
            if debugging:
                true.to_csv(working_directory + 'TO_CHECK_'+prefix+'_coordinates_to_combine.csv', index = False, sep = ';')
                print('Wrote a dataframe of coordinate duplicates to check., ending "_coordinates_to_combine.csv". ')
            # remove the offending rows and spit them out for manual checking or just discarding
            coord_to_check = occs_dup_col[occs_dup_col.index.isin(true.index)]
            occs_dup_col = occs_dup_col[~ occs_dup_col.index.isin(true.index)]
            if debugging:
                coord_to_check.to_csv(working_directory + 'TO_CHECK_'+prefix+'coordinate_issues.csv', index = False, sep = ';')
            #print(occs_dup_col.shape)

             # print summary
            if verbose:
                print('\n Input records: ', len(occs1),
                ';\n records with no duplicates (occs_unique): ', len(occs_unique),
                ';\n duplicate records with very disparate coordinates removed:', len(coord_to_check),
                '\n If you want to check them, I saved them to', working_directory + 'TO_CHECK_'+prefix+ 'coordinate_issues.csv')

        # fo r smaller differences, take the mean value...
        occs_dup_col['ddlat'] = occs_dup_col.groupby(['col_year', 'colnum_full'])['ddlat'].transform('mean')
        occs_dup_col['ddlong'] = occs_dup_col.groupby(['col_year', 'colnum_full'])['ddlong'].transform('mean')
        #print(occs_dup_col.shape)


    #---------------------------------
    ## B DIFFERENT IDENTIFICATIONS
    # Different identification between duplicates
    # update by newest det, or remove spp./indet or string 'Annonaceae'

    no_go = 'Annonaceae'

    # expert_list ??? it might be a nice idea to gather a list of somewhat recent identifiers
    # to  use them to force through dets? Does that make sense? We do update the
    # taxonomy at a later date, so dets to earlier concepts might not be a massive issue?

    occs_dup_col['specific_epithet'] = occs_dup_col['specific_epithet'].str.replace('sp.', '')
    occs_dup_col['specific_epithet'] = occs_dup_col['specific_epithet'].str.replace('indet.', '') 

    dups_diff_species = occs_dup_col[occs_dup_col.duplicated(['col_year','colnum_full', 'country'],keep=False)&~occs_dup_col.duplicated(['recorded_by','colnum_full','specific_epithet','genus'],keep=False)]
    dups_diff_species = dups_diff_species.sort_values(['col_year','colnum_full'], ascending = (True, True))

    if verbose:
        print('\n We have', len(dups_diff_species),
        'duplicate specimens with diverging identification.')

    #-------------------------------------------------------------------------------
    # i think this just is nice for visual confirmation we didn't reverse an identification
    # by replacing it with 'sp.'
    # check which speciimens have no specific_epithet, or 'sp./indet.'
    # # occs_dup_col['sE-sp'] = occs_dup_col['specific_epithet']
    # occs_dup_col['sE-sp'] = occs_dup_col['sE-sp'].str.replace(r'^(.(?<!sp\.))*?$', '')
    # #[dups_diff_species['specific_epithet'] == 'sp.']
    # print('HA')
    # occs_dup_col['sE-indet'] = occs_dup_col['specific_epithet']
    # #.str.replace(r"^(.(?<!Grass))*?$", "Turf")
    # occs_dup_col['sE-indet'] = occs_dup_col['sE-indet'].str.replace(r'^(.(?<!indet.))*?$', '')
    # # thinking about it, no sense in seperating out empty values, as these will not merge back ...
    # print(occs_dup_col.columns)
    #
    # # combine the sp and indet cols
    #
    # occs_dup_col = occs_dup_col.assign(indets=occs_dup_col[['sE-indet', 'sE-sp']].sum(1)).drop(['sE-indet', 'sE-sp'], 1)
    #-------------------------------------------------------------------------------

    # backup the old dets
    occs_dup_col['genus_old'] = occs_dup_col['genus']
    occs_dup_col['specific_epithet_old'] = occs_dup_col['specific_epithet']

    # cols we to change
    # cols = ['genus','specific_epithet','detBy', 'det_year']


    #groupby col and num, and sort more recent det #swifter apply should do the apply as efficiently as possible based on the resources available on your machine.
    occs_dup_col = occs_dup_col.groupby(dup_cols, group_keys=False, sort=True).apply(lambda x: x.sort_values('det_year', ascending=False))


    #groupby col and num, and transform the rest of the columns
    #we shall create a new column just to keep a trace

    occs_dup_col.reset_index(drop=True, inplace=True)
    occs_dup_col['genus'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['genus'].transform('first')
    ###### HERE no data left in sn step... TODO
    print('HERE', occs_dup_col)
    occs_dup_col['specific_epithet'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['specific_epithet'].transform('first')

    #save a csv with all duplicates beside each other but otherwise cleaned, allegedly.
    if debugging:
        occs_dup_col.to_csv(working_directory + 'TO_CHECK_' + prefix + 'dupli_dets_cln.csv', index = False, sep = ';')
        print('\n I have saved a checkpoint file of all cleaned and processed duplicates, nicely beside each other, to:',
        working_directory + 'TO_CHECK_' + prefix + 'dupli_dets_cln.csv')



    #-------------------------------------------------------------------------------
    # DE-DUPLICATE AND THEN MERGE

    # check type (again)
    occs_dup_col = occs_dup_col.astype(z_dependencies.final_col_type)
    #print(occs_dup_col.dtypes)


    # Here we can still modify which columns we take further, and how they are merged,
    #   i.e. is it record1, record1duplicate or do we discard duplicate data and just take the first record.


    ############################################ Here integrate expert flag
    if step == 'master':
        # Expert level deduplication only between datasets, not in single dataset!
        if expert_file == 'EXP':
            # We have an expert dataset being integrated into the database.

            # first handle duplicates with all expert values (expert duplicates)
            expert_rows = occs_dup_col[occs_dup_col['expert_det'].isin('EXP')]

            expert_merged = expert_rows.groupby(dup_cols, as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
                col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
                col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
                col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
                country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
                continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
                locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
                coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
                ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
                herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
                barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = 'ACCEPTED',
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                geo_issues = pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                modified = np.nan # is filled wioth new value at the end of deduplication
                )
            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            expert_merged = expert_merged.sort_values(dup_cols, ascending = (True, True, False, False))

            if verbose:
                print('EXPERT-EXPERT deduplicated')     
                print('\n There were', len(expert_rows), 'duplicated specimens')
                print('\n There are', len(expert_merged), 'unique records after merging.')


            # then normally handle duplicates with no expert values at all
            non_exp_rows = occs_dup_col[occs_dup_col['expert_det'].notin('EXP') ]

            non_exp_merged = expert_rows.groupby(dup_cols, as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
                col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
                col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
                col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
                country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
                continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
                locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
                coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
                ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
                herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
                barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                geo_issues = pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                modified = np.nan # is filled wioth new value at the end of deduplication
            )

            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            non_exp_merged = non_exp_merged.sort_values(dup_cols, ascending = (True, True, False, False))

            if verbose:
                print('EXPERT-EXPERT deduplicated')     
                print('\n There were', len(non_exp_rows), 'duplicated specimens')
                print('\n There are', len(non_exp_merged), 'unique records after merging.')

            # SANITY CHECK
            if (len(expert_rows) + len(non_exp_rows)) != len(occs_dup_col):
                print('SOMETHINGS WRONG HERE!!')

            # now merge expert-only and non-expert-only parts together and deduplicate

            # once deduplicated dataframe
            one_dd_done = pd.concat(expert_merged, non_exp_merged)

            # now merge duplicates with expert & non-expert  values (only one of each left if everything above went to plan..)
            occs_merged = one_dd_done.groupby(dup_cols, as_index = False).agg(
                scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc = 'first' ),
                prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
                col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
                col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
                col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
                country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
                continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
                locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
                coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
                ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
                herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
                barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
                orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
                coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                geo_issues = pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                modified = np.nan # is filled wioth new value at the end of deduplication
                )
            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            occs_merged = occs_merged.sort_values(dup_cols, ascending = (True, True, False, False))

            print('\n There were', len(occs_dup_col), 'duplicated specimens')
            print('\n There are', len(occs_merged), 'unique records after merging.')

            ########################## end of expert non expert mess

        else:
            # master but not expert. proceed as normal.
                occs_merged = occs_dup_col.groupby(dup_cols, as_index = False).agg(
                    scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
                    genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
                    specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
                    species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
                    collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
                    recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
                    colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
                    prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
                    colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
                    sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
                    col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
                    col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
                    col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
                    col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
                    det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
                    det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                    det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                    det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                    det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                    country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
                    country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
                    continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
                    locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
                    coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
                    ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
                    ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
                    institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
                    herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
                    barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
                    orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
                    coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
                    huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
                    geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
                    wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                    expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                    status = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                    accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                    ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                    ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                    geo_issues = pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                    modified = np.nan # is filled wioth new value at the end of deduplication
                    )
                # here quite some data might get lost, so we need to check where we want to just join first,
                # and where we add all values, and then decide on the columns we really want in the final
                # database!!!

                # de-duplicated duplicates sorting to get them ready for merging
                occs_merged = occs_merged.sort_values(dup_cols, ascending = (True, True, False, False))

                print('\n There were', len(occs_dup_col), 'duplicated specimens')
                print('\n There are', len(occs_merged), 'unique records after merging.')

        ########### END of expert/non expert if/else

    else:
        occs_dup_col['colnum_full'] = occs_dup_col['colnum_full'].fillna('')
        # not master
        occs_merged = occs_dup_col.groupby(dup_cols, as_index = False).agg(
            scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
            genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
            specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
            species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
            collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
            recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
            colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc=lambda x: ', '.join(x)),
            prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
            colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
            sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
            col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
            col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
            col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
            col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
            det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
            det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
            det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
            det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
            det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
            country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
            country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
            continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
            locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
            coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
            ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
            ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
            institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
            herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
            barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
            orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
            coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'),
            huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
            geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
            wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first')
            )
        # here quite some data might get lost, so we need to check where we want to just join first,
        # and where we add all values, and then decide on the columns we really want in the final
        # database!!!

    # de-duplicated duplicates sorting to get them ready for merging
    print(len(dupli))
    length = len(dupli)-2
    order_vec = tuple([True, True] + [False]*length)
    print(order_vec)
    occs_merged = occs_merged.sort_values(dup_cols, ascending = order_vec)

    print('\n There were', len(occs_dup_col), 'duplicated specimens')
    print('\n There are', len(occs_merged), 'unique records after merging.')

    ######### END of if/else master 

    print('\n \n FINAL DUPLICATE STATS:')
    print('-------------------------------------------------------------------------')
    print('\n Input data:', len(occs), '; \n De-duplicated duplicates:', len(occs_merged),
    '; \n Non-duplicate data:', len(occs_unique),
     #'; \n No collection number. (This is not included in output for now!) :', len(occs_nocolNum),
     ';\n total data written:', len(occs_merged) + len(occs_unique) ,
     '; \n Datapoints removed: ',
    len(occs) - (len(occs_merged) + len(occs_unique)))
    print('-------------------------------------------------------------------------')

    if step=='master':
        date = date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        occs_merged['modified'] = User + '_' + date
        
    #    occs_cleaned = pd.merge(pd.merge(occs_merged,occs_unique,how='outer'))#,occs_nocolNum ,how='outer')
    occs_cleaned = pd.concat([occs_merged, occs_unique])
    occs_cleaned = occs_cleaned.sort_values(dup_cols, ascending = (True, True, False, False))
    #print(occs_cleaned.head(50))

    if debugging:
        occs_cleaned.to_csv(working_directory+prefix+'deduplicated.csv', index = False, sep = ';', )
        print('\n The output was saved to', working_directory+prefix+'deduplicated.csv', '\n')

    return occs_cleaned





# DEPRECATED!
# s-n cleaner integrated in duplicate_cleaner()
# remove when ready.

# def duplicate_cleaner_s_n(occs, working_directory, prefix, step='Raw', verbose=True, debugging=False):
#     '''
#     This one actually goes and cleans/merges duplicates with no collection number
#         > occs = occurrence data to de-duplicate
#         > working_directory = path to directory to output intermediate files
#         > prefix = filename prefix
#         > step = {raw, master} = reference for datatype checks
#     '''

#     if step=='Master':
#         occs=occs.astype(z_dependencies.final_col_for_import_type)
#     else:
#         occs = occs.astype(z_dependencies.final_col_type) # double checking
#     print(occs.dtypes)
#     #occs.replace('nan', pd.NA, inplace=True)

#     # for the s.n. we have to be quite specific, as otherwise we deduplicate too much!
#     dup_cols = ['huh_name', 'col_year', 'col_month', 'col_day', 'genus', 'specific_epithet'] # the columns by which duplicates are identified

#     #-------------------------------------------------------------------------------
#     # MISSING collector information r
#     # remove empty collector
#     occs1 = occs.dropna(how='all', subset = ['recorded_by'])

#     #-------------------------------------------------------------------------------
#     occs_dup_col =  occs1.loc[occs1.duplicated(subset=dup_cols, keep=False)]
#     print('Run1', occs_dup_col)
#     # get the NON-duplicated records
#     occs_unique = occs1.drop_duplicates(subset=dup_cols, keep=False)

#     if verbose:
#         print('\n First filtering. \n Total records: ',
#         len(occs), ';\n records with no duplicates (occs_unique): ',
#         len(occs_unique), ';\n records with duplicates (occs_dup_col): ',
#         len(occs_dup_col))

#     #-------------------------------------------------------------------------------
#     # Duplicates part 1:
#     #---------------------------------
#     ## A  DIFFERENT COORDINATES
#     # Coordinates not identical:
#         # small difference --> MEAN
#         # big difference --> exctract and either discard or check manually

#     # double-check type of coordinate columns
#     convert_dict = {'ddlat': float,
#                     'ddlong': float}
#     occs_dup_col = occs_dup_col.astype(convert_dict)
#     print(occs_dup_col)


#     if verbose:
#         print('\n The duplicates subset, before cleaning dups has the shape: ', occs_dup_col.shape)
#     # in this aggregation step we calculate the variance between the duplicates.
#     test = occs_dup_col.groupby(dup_cols, as_index = False).agg(
#         ddlong = pd.NamedAgg(column = 'ddlong', aggfunc='var'),
#         ddlat = pd.NamedAgg(column = 'ddlat', aggfunc='var'))

#     print(test)
#     # if this variance is above 0.1 degrees
#     if len(test)>0:
#         test.loc[test['ddlong'] >= 0.1, 'long bigger than 0.1'] = 'True'
#         test.loc[test['ddlong'] < 0.1, 'long bigger than 0.1'] = 'False'

#         test.loc[test['ddlat'] >= 0.1, 'lat bigger than 0.1'] = 'True'
#         test.loc[test['ddlat'] < 0.1, 'lat bigger than 0.1'] = 'False'

#         # filter by large variance.
#         true = test[(test["long bigger than 0.1"] == 'True') | (test["lat bigger than 0.1"] == 'True')]

#         # write these records to csv for correcting or discarding.
#         true.to_csv(working_directory + 's_n_TO_CHECK_'+prefix+'coordinates_to_combine.csv', index = False, sep = ';')

#         # remove the offending rows and spit them out for manual checking or just discarding
#         coord_to_check = occs_dup_col[occs_dup_col.index.isin(true.index)]
#         occs_dup_col = occs_dup_col[~ occs_dup_col.index.isin(true.index)]
#         coord_to_check.to_csv(working_directory + 's_n_TO_CHECK_'+prefix+'coordinate_issues.csv', index = False, sep = ';')
#         #print(occs_dup_col.shape)

#     # print summary
#         if verbose:
#             print('\n Input records: ', len(occs),
#             ';\n records with no duplicates (occs_unique): ', len(occs_unique),
#             ';\n duplicate records with very disparate coordinates removed:', len(coord_to_check),
#             '\n If you want to check them, I saved them to', working_directory + 'TO_CHECK_'+prefix+ 'coordinate_issues.csv')

#     print('CHECKING:', occs_dup_col.shape)
#     # for smaller differences, take the mean value...
#     occs_dup_col['ddlat'] = occs_dup_col.groupby(['col_year'])['ddlat'].transform('mean')
#     occs_dup_col['ddlong'] = occs_dup_col.groupby(['col_year'])['ddlong'].transform('mean')
#     #print(occs_dup_col.shape)


#     #---------------------------------
#     ## B DIFFERENT IDENTIFICATIONS
#     # Different identification between duplicates
#     # update by newest det, or remove spp./indet or string 'Annonaceae'

#     no_go = 'Annonaceae'

#     # expert_list ??? it might be a nice idea to gather a list of somewhat recent identifiers
#     # to  use them to force through dets? Does that make sense? We do update the
#     # taxonomy at a later date, so dets to earlier concepts might not be a massive issue?
#     print('CHECKING2:', occs_dup_col.shape)

#     dups_diff_species = occs_dup_col[occs_dup_col.duplicated(['col_year', 'country'],keep=False)&~occs_dup_col.duplicated(['recorded_by','colnum_full','specific_epithet','genus'],keep=False)]
#     try:
#         dups_diff_species = dups_diff_species.sort_values(['col_year'], ascending = (True, True))
#     except:
#         do_nothing=True

#     if verbose:
#         print('\n We have', len(dups_diff_species),
#         'duplicate specimens with diverging identification.')

#     #-------------------------------------------------------------------------------
#     # i think this just is nice for visual confirmation we didn't reverse an identification
#     # by replacing it with 'sp.'
#     # check which speciimens have no specificEpithet, or 'sp./indet.'
#     # # occs_dup_col['sE-sp'] = occs_dup_col['specificEpithet']
#     # occs_dup_col['sE-sp'] = occs_dup_col['sE-sp'].str.replace(r'^(.(?<!sp\.))*?$', '')
#     # #[dups_diff_species['specificEpithet'] == 'sp.']
#     # print('HA')
#     # occs_dup_col['sE-indet'] = occs_dup_col['specificEpithet']
#     # #.str.replace(r"^(.(?<!Grass))*?$", "Turf")
#     # occs_dup_col['sE-indet'] = occs_dup_col['sE-indet'].str.replace(r'^(.(?<!indet.))*?$', '')
#     # # thinking about it, no sense in seperating out empty values, as these will not merge back ...
#     # print(occs_dup_col.columns)
#     #
#     # # combine the sp and indet cols
#     #
#     # occs_dup_col = occs_dup_col.assign(indets=occs_dup_col[['sE-indet', 'sE-sp']].sum(1)).drop(['sE-indet', 'sE-sp'], 1)
#     #-------------------------------------------------------------------------------

#     # backup the old dets
#     occs_dup_col['genus_old'] = occs_dup_col['genus']
#     occs_dup_col['specificEpithet_old'] = occs_dup_col['specificEpithet']

#     # cols we to change
#     # cols = ['genus','specificEpithet','detBy', 'det_year']

#     # https://stackoverflow.com/questions/59697994/what-does-transformfirst-do
#     #groupby col and num, and sort more recent det
#     print('CHECKING3:', occs_dup_col.shape)
#     #PROBLEM IS HERE:



#     occs_dup_col = occs_dup_col.groupby(dup_cols, group_keys=False, sort=True).apply(lambda x: x.sort_values('det_year', ascending=False))
#     #print('Intermediate Check: ', occs_dup_col.shape)
#     #occs_dup_col = occs_dup_col
#     print('CHECKING3:', occs_dup_col.shape)


#     #groupby col and num, and transform the rest of the columns
#     #we shall create a new column just to keep a trace
#     # if the df is empty, there's issues
#     try:
#         occs_dup_col['genus'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['genus'].transform('first')
#         occs_dup_col['specific_epithet'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['specific_epithet'].transform('first')
#     except:
#         do_nothing=True
#         print(len(occs_dup_col))
#     #save a csv with all duplicates beside each other but otherwise cleaned, allegedly.
#     occs_dup_col.to_csv(working_directory + 's_n_TO_CHECK_' + prefix + 'dupli_dets_cln.csv', index = False, sep = ';')
#     print('\n I have saved a checkpoint file of all cleaned and processed duplicates, nicely beside each other, to:',
#     working_directory + 's_n_TO_CHECK_' + prefix + 'dupli_dets_cln.csv')

#     #-------------------------------------------------------------------------------
#     # not sure how to integrate revisions just yet... this might be something
#     # for when integrating new data into the established database, when we already
#     # have revision datasets present and we can compare these...
#     #-------------------------------------------------------------------------------

#     #-------------------------------------------------------------------------------
#     # DE-DUPLICATE AND THEN MERGE

#     # check type (again)
#     occs_dup_col = occs_dup_col.astype(z_dependencies.final_col_type)
#     #print(occs_dup_col.dtypes)


#     # Here we can still modify which columns we take further, and how they are merged,
#     #   i.e. is it record1, record1duplicate or do we discard duplicate data and just take the first record.
#     if len(occs_dup_col)>0:
#         occs_merged = occs_dup_col.groupby(dup_cols, as_index = False).agg(
#             scientific_name = pd.NamedAgg(column = 'scientific_name', aggfunc = 'first'),
#         	genus = pd.NamedAgg(column = 'genus', aggfunc =  'first'),
#         	specific_epithet = pd.NamedAgg(column = 'specific_epithet', aggfunc = 'first' ),
#         	species_author = pd.NamedAgg(column = 'species_author', aggfunc = 'first' ),
#         	collector_id = pd.NamedAgg(column = 'collector_id', aggfunc = 'first' ),
#             recorded_by = pd.NamedAgg(column = 'recorded_by', aggfunc = 'first' ),
#             colnum_full = pd.NamedAgg(column = 'colnum_full', aggfunc= 'first'), # as both are by definition NA, it really doesn't matter how we merge this...
#         	prefix = pd.NamedAgg(column = 'prefix', aggfunc = 'first' ),
#         	colnum = pd.NamedAgg(column = 'colnum', aggfunc = 'first' ),
#         	sufix = pd.NamedAgg(column = 'sufix', aggfunc =  'first'),
#             col_date = pd.NamedAgg(column = 'col_date', aggfunc = 'first' ),
#             col_day = pd.NamedAgg(column = 'col_day', aggfunc = 'first' ),
#             col_month = pd.NamedAgg(column = 'col_month', aggfunc = 'first' ),
#             col_year = pd.NamedAgg(column = 'col_year', aggfunc = 'first' ),
#         	det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ', '.join(x) ),
#         	det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
#             det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
#             det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
#             det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
#         	country_id = pd.NamedAgg(column = 'country_id', aggfunc = 'first' ),
#         	country = pd.NamedAgg(column = 'country', aggfunc = 'first' ),
#         	continent = pd.NamedAgg(column = 'continent', aggfunc = 'first' ),
#         	locality = pd.NamedAgg(column = 'locality', aggfunc = 'first' ),
#         	coordinate_id = pd.NamedAgg(column = 'coordinate_id', aggfunc = 'first' ),
#         	ddlong = pd.NamedAgg(column = 'ddlong', aggfunc = 'first' ),
#             ddlat = pd.NamedAgg(column = 'ddlat', aggfunc = 'first' ),
#         	institute = pd.NamedAgg(column = 'institute', aggfunc = lambda x: ', '.join(x)),
#             herbarium_code = pd.NamedAgg(column = 'herbarium_code', aggfunc = lambda x: ', '.join(x)),
#             barcode = pd.NamedAgg(column = 'barcode', aggfunc=lambda x: ', '.join(x)),
#             orig_bc = pd.NamedAgg(column = 'orig_bc', aggfunc=lambda x: ', '.join(x)),
#             coll_surname = pd.NamedAgg(column = 'coll_surname', aggfunc = 'first'))

#     # here quite some data might get lost, so we need to check where we want to just join first,
#     # and where we add all values, and then decide on the columns we really want in the final
#     # database!!!

#     # de-duplicated duplicates sorting to get them ready for merging
#     if len(occs_dup_col)>0:
#         occs_merged = occs_merged.sort_values(dup_cols, ascending = (True, True, False, False, True, True))
#     else:
#         occs_merged = None
#     print('\n There were', len(occs_dup_col), 'duplicated specimens')
#     try:
#         print('\n There are', len(occs_merged), 'unique records after merging.')
#     except:
#         print('\n There are no unique records after merging.')

#     # print('\n \n S.N. FINAL DUPLICATE STATS:')
#     # print('-------------------------------------------------------------------------')
#     # print('\n Input data:', len(occs), '; \n De-duplicated duplicates:', len(occs_merged),
#     # '; \n Non-duplicate data:', len(occs_unique),
#     #  #'; \n No collection number. (This is not included in output for now!) :', len(occs_nocolNum),
#     #  ';\n total data written:', len(occs_merged) + len(occs_unique) ,
#     #  '; \n Datapoints removed: ',
#     # len(occs) - (len(occs_merged) + len(occs_unique)))
#     # print('-------------------------------------------------------------------------')


#     #    occs_cleaned = pd.merge(pd.merge(occs_merged,occs_unique,how='outer'))#,occs_nocolNum ,how='outer')
#     occs_cleaned = pd.concat([occs_merged, occs_unique])
#     occs_cleaned = occs_cleaned.sort_values(dup_cols, ascending = (True, True, False, False, True, True))
#     #print(occs_cleaned.head(50))

#     occs_cleaned.to_csv(working_directory+prefix+'deduplicated.csv', index = False, sep = ';', )
#     print('\n The output was saved to', working_directory+prefix+'deduplicated.csv', '\n')

#     return occs_cleaned


#
