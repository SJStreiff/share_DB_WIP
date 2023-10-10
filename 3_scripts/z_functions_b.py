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
import datetime 
import logging

import z_dependencies
import z_functions_c as cc_functions







def duplicate_stats(occs, working_directory, prefix, out=True, verbose=True, debugging=False):
    '''
    Function that prints a load of stats about potential duplicates
    '''
    #-------------------------------------------------------------------------------
    # these columns are used to identify duplicates (i.e. if a value of both these column is shared
    # for a row, then we flag the records as duplicates)
    dup_cols = ['coll_surname', 'colnum', 'sufix', 'col_year'] # the columns by which duplicates are identified
    #-------------------------------------------------------------------------------

    # cleaning up a bit

    # data types can be annoying
    occs = occs.astype(z_dependencies.final_col_type) # double checking
    #print(occs.dtypes)
    #occs[['recorded_by', 'colnum_full']] = occs[['recorded_by', 'colnum_full']].replace('nan', pd.NA)

    try:
        occs = occs.drop(['to_check', 'to_check_det'], axis = 'columns')
    except:
        logging.debug('No tmp_check to drop')

    #-------------------------------------------------------------------------------
    # MISSING collector information and number
    # remove empty collector and coll num
    occs1 = occs #.dropna(how='all', subset = ['recorded_by']) # no collector information is impossible.
    #occs1 = occs1[occs1.recorded_by != 'nan']
    logging.debug(f'Deleted {len(occs.index) - len(occs1.index)} rows with no collector and no number; {len(occs1.index)} records left')
    
    #-------------------------------------------------------------------------------
    # MISSING col num
    # these are removed, and added later after processing(?)
    subset_col = ['colnum_full']

    occs1 = occs1.replace({'NaN': pd.NA}, regex=False)
    occs1 = occs1.replace({'nan': pd.NA}, regex=False)

    occs_colNum = occs1.dropna(how='all', subset=['colnum'])
    print('I dropped', len(occs1)-len(occs_colNum), 'NA colnum records')
    occs_nocolNum = occs1[occs1['colnum'].isna()]
    print(occs1.colnum.dtypes, occs1['colnum'].isna())

    occs_colNum.ddlong.astype(float)
    occs_nocolNum['ddlong'].astype(float)
    occs_colNum.ddlat.astype(float)
    occs_nocolNum['ddlat'].astype(float)

    occs_colNum['coll_surname'] = occs_colNum['recorded_by'].str.split(',', expand=True)[0]
    logging.info(f'{occs_colNum.coll_surname}')

    #if len(occs_nocolNum)>0:
    #    occs_nocolNum.to_csv(working_directory+prefix+'s_n.csv', index = False, sep = ';' )
    #    print('\n The data with no collector number was saved to', working_directory+prefix+'s_n.csv', '\n')

    #-------------------------------------------------------------------------------
    # Perform some nice stats, just to get an idea what we have

    occs_colNum.colnum_full.astype(str)
    
    print('Total records:', len(occs),';\n Records with colNum_full:', len(occs_colNum),';\n Records with no colNum_full:', len(occs_nocolNum),
                            '\n \n Some stats about potential duplicates: \n .................................................\n',
                            '\n By Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['recorded_by', 'colnum_full'], keep=False)].shape,
                            '\n By NON-STANDARD Collector-name and FULL collector number', occs_colNum[occs_colNum.duplicated(subset=['orig_recby', 'colnum_full'], keep=False)].shape,
                            '\n By Collector-name and FULL collector number, and coordinates', occs_colNum.duplicated(['recorded_by', 'colnum_full', 'ddlat', 'ddlong'], keep=False).sum(),
                            '\n By Collector-name and FULL collector number, genus and specific epithet', occs_colNum.duplicated(['recorded_by', 'colnum_full', 'genus' , 'specific_epithet'], keep=False).sum(),
                            '\n By FULL collector number, genus and specific epithet', occs_colNum.duplicated([ 'colnum_full', 'genus' , 'specific_epithet'], keep=False).sum(),
                            '\n By FULL collector number and genus', occs_colNum.duplicated([ 'colnum_full', 'genus' ], keep=False).sum(),
                            '\n By FULL collector number, collection Year and country', occs_colNum.duplicated([ 'colnum_full', 'col_year' , 'country'], keep=False).sum(),
                            '\n By collection Year and FULL collection number (checking for directionality)', occs_colNum.duplicated([ 'col_year' , 'colnum_full'], keep=False).sum(),
                            '\n By REDUCED collection number and collection Yeear', occs_colNum.duplicated([ 'colnum', 'col_year' ], keep=False).sum(),
                            '\n By locality, REDUCED collection number and collection Year', occs_colNum.duplicated([ 'locality', 'colnum' , 'col_year' ], keep=False).sum(),
                            '\n By SURNAME and COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colnum' ], keep=False).sum(),
                            '\n By SURNAME and FULL COLLECTION NUMBER', occs_colNum.duplicated([ 'coll_surname', 'colnum_full' ], keep=False).sum(),
                            '\n By SURNAME and COLLECTION NUMBER and YEAR', occs_colNum.duplicated([ 'coll_surname', 'colnum', 'col_year' ], keep=False).sum(),
                            '\n By SURNAME and COLLECTION NUMBER, SUFIX and YEAR', occs_colNum.duplicated([ 'coll_surname', 'colnum', 'sufix', 'col_year' ], keep=False).sum(),
                            '\n By HUH-NAME and COLLECTION NUMBER, SUFIX and YEAR', occs_colNum.duplicated([ 'huh_name', 'colnum', 'sufix', 'col_year' ], keep=False).sum(),
                            '\n ................................................. \n ')
    


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
    #print(occs.dtypes)
    #occs = occs.replace(np.nan, pd.NA)

    dup_cols = dupli #['recorded_by', 'colnum', 'sufix', 'col_year'] # the columns by which duplicates are identified

    #-------------------------------------------------------------------------------
   # Housekeeping. Clean up problematic types and values...

    # occs = occs.str.strip()
    occs1 = occs.replace(['', ' '], pd.NA)

    
    #-------------------------------------------------------------------------------
    # find duplicated BARCODES before we do anything
    # this step only takes place in master step.
    if step=='Master':
        logging.info('Deduplication: MASTER-1')
        # ONLY RETAIN DUPLICATED BARCODES 
        duplic_barcodes = occs1[occs1.duplicated(subset=['barcode'], keep=False)] # gets us all same barcodes
        logging.info(f'BARCODES DUPLICATED: {duplic_barcodes.shape}')

        # KEEP UNIQUE BARCODES
        cl_barcodes = occs1.drop_duplicates(subset=['barcode'], keep=False) # throws out all duplicated rows,
        logging.info(f'NO BARCODES DUPLICATED: {cl_barcodes.shape}')

        # ----------- only duplicated
        # all other duplicates follow below.
        if expert_file == 'EXP':
            # if expert we need to take care we integrate the expert data properly
            logging.info('Deduplication: MASTER-1, expert')
            logging.info('Merging duplicated barcodes, EXPERT file')
            duplic_barcodes = duplic_barcodes.sort_values(['barcode', 'expert_det'], ascending = [False, False])
            logging.info(f'{duplic_barcodes.barcode}{duplic_barcodes.recorded_by}{duplic_barcodes.colnum}{duplic_barcodes.expert_det}')

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
                    det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                    det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                    det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                    det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                    det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                    country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
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
                    source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                    wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                    expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                    status = pd.NamedAgg(column = 'status',  aggfunc = 'first'),
                    accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                    ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                    link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                    ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first')
                    )
            logging.info(f'{barcode_merged.barcode}{barcode_merged.recorded_by}{barcode_merged.colnum}{barcode_merged.expert_det}')
            
            logging.info('********* Adding timestamp *******')
            date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
            barcode_merged['modified'] = User + '_' + date
            logging.info(f'{barcode_merged.modified}')


            logging.info(f'deduplocated barcodes: {barcode_merged.shape} {barcode_merged.modified}')
            
                # merge in master columns to the rest.
            


        else:
            # if not EXP, just bung the data together....
            logging.info('Deduplication: MASTER-1 non-expert')
            logging.info('Merging duplicated barcodes')
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
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
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
                wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first'),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                expert_det = pd.NamedAgg(column = 'expert_det', aggfunc = 'first'),
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first')
                )
        # END of if/else Expert file
        # merge in master columns to the rest.
       



        ### and re merge deduplicated barcodes and unique barcodes
        occs1 = pd.concat([cl_barcodes, barcode_merged], axis=0) ###CHECK axis assignments
        logging.info(f'After deduplocated barcodes entire data: {occs1.shape}')
    # END of  if/else step=MASTER
        
    #-------------------------------------------------------------------------------
    #NOW start with actual deduplication

    occs_dup_col =  occs1.loc[occs1.duplicated(subset=dup_cols, keep=False)]
    #print('Run1', occs_dup_col)
    # get the NON-duplicated records
    occs_unique = occs1.drop_duplicates(subset=dup_cols, keep=False)

    logging.info(f'\n First filtering. \n Total records: {len(occs1)} ;\n records with no duplicates (occs_unique): {len(occs_unique)} \n records with duplicates (occs_dup_col): {len(occs_dup_col)}')

    if len(occs_dup_col) == 0:
        logging.info('Nothing to deduplicate anymore. Either barcodes took care of it, or there were no duplicates.')
         
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

    logging.info(f'\n The duplicates subset, before cleaning dups has the shape: {occs_dup_col.shape}')
    # in this aggregation step we calculate the variance between the duplicates.
    
    if expert_file ==  'EXP':
        # as it is expert file, we expect coordinates to match within the file. We check coordinates when integrating into master:
        # > Do nothing, and then take expert value at final merge.
        logging.info('We have expert file!')

    else:
        # no expert file, so just do as normal....
        occs_dup_col = occs_dup_col.reset_index(drop = True)

   #     print('PROBLEM:\n', occs_dup_col[dup_cols])
        occs_dup_col[dup_cols] = occs_dup_col[dup_cols].fillna(-9999)
        # print('PROBLEM:\n', occs_dup_col[dup_cols], occs_dup_col.ddlat)

        # calculate variance between grouped objects
        occs_dup_col['ddlat_var'] = occs_dup_col.groupby(dup_cols)['ddlat'].transform('var')
        occs_dup_col['ddlong_var'] = occs_dup_col.groupby(dup_cols)['ddlong'].transform('var')
    
        # subset problematic large variance!
        occs_large_var = occs_dup_col[(occs_dup_col['ddlat_var'] > 0.1) | (occs_dup_col['ddlong_var'] > 0.1)]

        # and drop the large variance data here.
        occs_ok = occs_dup_col[(occs_dup_col['ddlat_var'] <= 0.1) & (occs_dup_col['ddlong_var'] <= 0.1)]

   

# # CLEASNUP!!!!!
#         print('DEBUG\n', test )


#         if len(test)>0:
#             # if this variance is above 0.1 mark as true an d subset just the values that have large variance
#             test.loc[test['ddlong'] >= 0.1, 'long bigger than 0.1'] = 'True'
#             test.loc[test['ddlong'] < 0.1, 'long bigger than 0.1'] = 'False'

#             test.loc[test['ddlat'] >= 0.1, 'lat bigger than 0.1'] = 'True'
#             test.loc[test['ddlat'] < 0.1, 'lat bigger than 0.1'] = 'False'

#             # filter by large variance.
#             true = test[(test["long bigger than 0.1"] == 'True') | (test["lat bigger than 0.1"] == 'True')]
#             # print('true', true)
#             # print(true.columns)
#             if len(true)>0:
#                 print('true is larger 0', true.shape)
#                 print(true)
#                 # if we have records with large variance, merge with all duplicates and then subset just problems
                
#                 #now get data from occs_dup_col which is in true to do some test and modifications
#                 print(true[dup_cols].values)
#                 print(occs_dup_col[dup_cols].values)


#                 # print('# # # TESTING FOR LOOP')
#                 # for col in dup_cols:
#                 #     print(col)
#                 #     print(occs_dup_col[col].values)
#                 #     print(true[col].values)
#                 #     print(true[col].values == occs_dup_col[col].values)
#                 #     col_bool = pd.DataFrame(occs_dup_col[col].values == true[col].values)
#                 #     print(col_bool)


#                 print(np.where(occs_dup_col[dup_cols].values == true[dup_cols].values))
#                 res_df = pd.DataFrame(occs_dup_col[dup_cols].values == true[dup_cols].values)
#                # TODO: persisting error here, when true and test are not the same dimensions!



#                 res_df['sum'] = res_df.apply(lambda row: sum(row), axis=1)
               
#                 res_df['to_subset'] = res_df[res_df.sum == len(dup_cols)]
#                 # print(res_df)
#                 # make df for good and bad coordiantes
#                 occs_prob_coords = occs_dup_col[res_df.to_subset]
#                 #good
#                 occs_dup_col = occs_dup_col[~res_df.to_subset]

#                 print('NOW GOOD?\n', occs_prob_coords)
#         # now check for coordinate - country


        occs_large_var['coordinate_country'] = occs_large_var.apply(lambda row: cc_functions.get_cc(row['ddlat'], row['ddlong']), axis = 1, result_type = 'reduce')
                #occs_large_var['cc_discrepancy'] = (occs_prob_coords['country_id'] != occs_prob_coords['coordinate_country'])
        
        if len(occs_large_var) > 0:
        # if we have records that have excessive variance 
            # read the old problematic values
            try:
                # if file exists then this shold work
                coord_prob_bl = pd.read_csv(working_directory + '0_'+'coordinate_discrepancy.csv', sep = ';')
                # append the new ones
                merged_out = pd.concat([coord_prob_bl, occs_large_var])
            except: 
                merged_out = occs_large_var

            # and write back to file...
            merged_out.to_csv(working_directory + '0_'+'coordinate_discrepancy.csv', index = False, sep = ';')


        
        occs_dup_col = occs_ok

        logging.info(f'\n Input records: {len(occs1)} \n records with no duplicates (occs_unique): {len(occs_unique)}')
        logging.info(f'duplicate records with very disparate coordinates removed: {len(occs_large_var)} If you want to check them, I saved them to {working_directory}TO_CHECK_{prefix}coordinate_issues.csv')

        # fo r smaller differences, take the mean value...
        occs_dup_col['ddlat'] = occs_dup_col.groupby(['col_year', 'colnum_full'])['ddlat'].transform('mean')
        occs_dup_col['ddlong'] = occs_dup_col.groupby(['col_year', 'colnum_full'])['ddlong'].transform('mean')
        logging.info(f'after coordinate consolidation:{occs_dup_col.shape}')


    #---------------------------------
    ## B DIFFERENT IDENTIFICATIONS
    # Different identification between duplicates
    # update by newest det, or remove spp./indet or string 'Annonaceae'

    # expert_list ??? it might be a nice idea to gather a list of somewhat recent identifiers
    # to  use them to force through dets? Does that make sense? We do update the
    # taxonomy at a later date, so dets to earlier concepts might not be a massive issue?

    if step =='Master':
        # if Master, we merge the accepted name field.
        dups_diff_species = occs_dup_col[occs_dup_col.duplicated(['col_year','colnum_full', 'country'],keep=False)&~occs_dup_col.duplicated(['recorded_by','colnum_full','accepted_name'],keep=False)]
        # print('DUPLICATED SPECIES?\n', dups_diff_species[['recorded_by', 'barcode', 'accepted_name', 'det_year']])
        dups_diff_species = dups_diff_species.sort_values(['col_year','colnum_full'], ascending = (True, True))

        logging.info(f'\n We have {len(dups_diff_species)} duplicate specimens with diverging identification.')

        # # backup the old dets
        # occs_dup_col['genus_old'] = occs_dup_col['genus']
        # occs_dup_col['specific_epithet_old'] = occs_dup_col['specific_epithet']
        
        # needs this to be able to group by and work below
        occs_dup_col[dup_cols] = occs_dup_col[dup_cols].fillna(0)
        #occs_dup_col = occs_dup_col.reset_index(drop = True)
        
        #groupby and sort more recent det 
        occs_dup_col = occs_dup_col.groupby(dup_cols, group_keys=False, sort=True).apply(lambda x: x.sort_values(['det_year'], ascending=False))
        occs_dup_col.reset_index(drop=True, inplace=True)

        occs_dup_col['accepted_name'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['accepted_name'].transform('first')
        occs_dup_col['genus'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['genus'].transform('first')
        occs_dup_col['specific_epithet'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['specific_epithet'].transform('first')
        # and values 0 back to NA
        occs_dup_col[dup_cols] = occs_dup_col[dup_cols].replace(0, pd.NA)

        
        ###### HERE no data left in sn step... 
        #print('HERE', occs_dup_col)

        



    else:
        logging.info(f'test1 {occs_dup_col.shape}')
        logging.info(f'occs_dup_col.specific_epithet')
        occs_dup_col['specific_epithet'] = occs_dup_col['specific_epithet'].str.replace('sp.', '')
        occs_dup_col['specific_epithet'] = occs_dup_col['specific_epithet'].str.replace('indet.', '') 
        logging.info(f'test2 {occs_dup_col.shape}')

        dups_diff_species = occs_dup_col[occs_dup_col.duplicated(['col_year','colnum_full', 'country'],keep=False)&~occs_dup_col.duplicated(['recorded_by','colnum_full','specific_epithet','genus'],keep=False)]
        dups_diff_species = dups_diff_species.sort_values(['col_year','colnum_full'], ascending = (True, True))

        logging.info(f'\n We have {len(dups_diff_species)} duplicate specimens with diverging identification.')

        # backup the old dets
        occs_dup_col['genus_old'] = occs_dup_col['genus']
        occs_dup_col['specific_epithet_old'] = occs_dup_col['specific_epithet']
        logging.info(f'test3 {occs_dup_col.shape}')
        
        # replace zeroes with NA, better for downstream sorting
        occs_dup_col['det_year'].replace(0, pd.NA, inplace=True)
                # print(occs_dup_col.det_year)
                # print(occs_dup_col[['recorded_by', 'colnum']])

        # Issues if NA values here in following groupby step
        occs_dup_col.sufix = occs_dup_col.sufix.fillna(-9999) 
       
       # groupby the deduplication columns,  sort by increasing (low to high) det_year
        occs_dup_col = occs_dup_col.groupby(dup_cols, group_keys=False, sort=True).apply(lambda x: x.sort_values('det_year', ascending=False))
        # double checking
        logging.info(f'There used to be errors here,  {occs_dup_col.shape}')

        # return the -9999 value to NA
        occs_dup_col['sufix'].replace(-9999, 0, inplace =True)
        occs_dup_col['sufix'].replace(0, np.nan, inplace =True)
        # for some unknown reason, values of NaN are isna()=True, but does not do fillna(pd.NA) (i.e. it remains at NaN...)
        occs_dup_col.sufix.fillna(pd.NA, inplace=True)
        #        occs_dup_col.sufix = occs_dup_col.sufix.replace({'NaN', pd.NA}, regex=False) # not working, weird error.
        # print(occs_dup_col.sufix.isna())
        # print(occs_dup_col.sufix)

        occs_dup_col.reset_index(drop=True, inplace=True)
        occs_dup_col['genus'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['genus'].transform('first')
        occs_dup_col['specific_epithet'] = occs_dup_col.groupby(dup_cols, group_keys=False, sort=False)['specific_epithet'].transform('first')
        logging.info(f'test5 {occs_dup_col.shape}')

        #save a csv with all duplicates beside each other but otherwise cleaned, allegedly.
        
        occs_dup_col.to_csv(working_directory + 'TO_CHECK_' + prefix + 'dupli_dets_cln.csv', index = False, sep = ';')
        logging.debug(f'\n I have saved a checkpoint file of all cleaned and processed duplicates, nicely beside each other, to: {working_directory}TO_CHECK_{prefix}dupli_dets_cln.csv')

    #-------------------------------------------------------------------------------
    # DE-DUPLICATE AND THEN MERGE
    # check type (again)
    occs_dup_col = occs_dup_col.astype(z_dependencies.final_col_type)
    #print(occs_dup_col.dtypes)

    # any empty strings need to be set NA, otherwise sorting gets messed up ('' is at beginning)
    occs_dup_col = occs_dup_col.replace(['', ' '], pd.NA)

    # s.n. needed for deduplication and error reduction. removed later...
    occs_dup_col.colnum_full = occs_dup_col.colnum_full.fillna('s.n.')
    ############################################ Here integrate expert flag
    if step == 'Master':
        logging.info('Deduplication: MASTER-2')
        # Expert level deduplication only between datasets, not in single dataset!
        if expert_file == 'EXP':
            
            logging.info('Deduplication: MASTER-2 expert')
        # We have an expert dataset being integrated into the database.

            # first handle duplicates with all expert values (expert duplicates)
            expert_rows = occs_dup_col[occs_dup_col['expert_det'] == 'expert_det_file']
            expert_rows = expert_rows.sort_values(['status', 'expert_det'], ascending = [True, True])
            
            expert_rows.modified = expert_rows.modified.fillna('')
            expert_rows.geo_issues = expert_rows.geo_issues.fillna('')
            
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
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
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
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                #status = pd.NamedAgg(column = 'status', aggfunc=lambda x: 'ACCEPTED'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled with new value at the end of deduplication
                )
            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            expert_merged = expert_merged.sort_values(dup_cols, ascending = order_vec)

            logging.info('EXPERT-EXPERT deduplicated')     
            logging.info(f'\n There were {len(expert_rows)} duplicated specimens')
            logging.info(f'\n There are {len(expert_merged)} unique records after merging.')


            # then normally handle duplicates with no expert values at all
            non_exp_rows = occs_dup_col[occs_dup_col['expert_det'] != 'expert_det_file' ]
            non_exp_rows = non_exp_rows.sort_values(['status', 'det_year'], ascending = [True, False])

            non_exp_rows.modified = non_exp_rows.modified.fillna('')
            non_exp_rows.geo_issues = non_exp_rows.geo_issues.fillna('NONE')

            non_exp_merged = non_exp_rows.groupby(dup_cols, as_index = False).agg(
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
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
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
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
            )

            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            non_exp_merged = non_exp_merged.sort_values(dup_cols, ascending = order_vec)

            logging.info('EXPERT-EXPERT deduplicated')         
            logging.info(f'\n There were {len(non_exp_rows)} duplicated specimens')
            logging.info(f'\n There are {len(non_exp_merged)} unique records after merging.')

            # SANITY CHECK
            if (len(expert_rows) + len(non_exp_rows)) != len(occs_dup_col):
                logging.warning('SOMETHINGS WRONG HERE!!')

            # now merge expert-only and non-expert-only parts together and deduplicate

            # once deduplicated dataframe
            one_dd_done = pd.concat([expert_merged, non_exp_merged])

            one_dd_done = one_dd_done.sort_values(['status', 'expert_det'], ascending = [True, True])

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
                det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
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
                status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
                )
            # here quite some data might get lost, so we need to check where we want to just join first,
            # and where we add all values, and then decide on the columns we really want in the final
            # database!!!

            # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            occs_merged = occs_merged.sort_values(dup_cols, ascending = order_vec)

            logging.info(f'\n There were {len(occs_dup_col)} duplicated specimens')
            logging.info(f'\n There are {len(occs_merged)} unique records after merging.')

            ########################## end of expert non expert mess

        else:
            logging.info('Deduplication: MASTER-2 no expert')
                #print(occs_dup_col.status)
            
            occs_dup_col = occs_dup_col.sort_values(['status', 'expert_det'], ascending = [True, True])
            logging.info(f'{occs_dup_col.accepted_name}')
            # master but not expert. proceed as normal.
            print(occs_dup_col.modified.isna())
            occs_dup_col.modified = occs_dup_col.modified.fillna('nan')
            occs_dup_col.geo_issues = occs_dup_col.geo_issues.fillna('NONE')
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
                    det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
                    det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
                    det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
                    det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
                    det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
                    country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
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
                    status = pd.NamedAgg(column = 'status', aggfunc = 'first'),
                    accepted_name = pd.NamedAgg(column = 'accepted_name', aggfunc = 'first'),
                    link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
                    ipni_no =  pd.NamedAgg(column = 'ipni_no', aggfunc = 'first'),
                    ipni_species_author =  pd.NamedAgg(column = 'ipni_species_author', aggfunc = 'first'),
                    geo_issues = pd.NamedAgg(column = 'geo_issues', aggfunc=lambda x: ', '.join(x)),
                    source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
                    modified = pd.NamedAgg(column = 'modified',  aggfunc=lambda x: ', '.join(x)) # is filled wioth new value at the end of deduplication
                    )
                # here quite some data might get lost, so we need to check where we want to just join first,
                # and where we add all values, and then decide on the columns we really want in the final
                # database!!!
                # de-duplicated duplicates sorting to get them ready for merging
            length = len(dup_cols)-2
            order_vec = tuple([True, True] + [False]*length)
            occs_merged = occs_merged.sort_values(dup_cols, ascending = order_vec)
            logging.info(f'\n There were {len(occs_dup_col)} duplicated specimens')
            logging.info(f'\n There are {len(occs_merged)} unique records after merging.')
            logging.info(f'The  deduplicated one is: {occs_merged}')

        ########### END of expert/non expert if/else
        # end of if master

    else:
        logging.info('Deduplication: RAW-1')
        occs_dup_col['colnum_full'] = occs_dup_col['colnum_full'].fillna('')
        occs_dup_col['orig_bc'] = occs_dup_col['orig_bc'].fillna('')

        # not master
        occs_dup_col = occs_dup_col.sort_values(['expert_det'], ascending = True)
        logging.info(occs_dup_col[['recorded_by', 'colnum', 'ddlat']])
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
            det_by = pd.NamedAgg(column = 'det_by', aggfunc = lambda x: ' / '.join(x) ),
            det_date = pd.NamedAgg(column = 'det_date', aggfunc = 'first' ),
            det_day = pd.NamedAgg(column = 'det_day', aggfunc = 'first' ),
            det_month = pd.NamedAgg(column = 'det_month', aggfunc = 'first' ),
            det_year = pd.NamedAgg(column = 'det_year', aggfunc = 'first' ),
            country_iso3 = pd.NamedAgg(column = 'country_iso3', aggfunc = 'first' ),
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
            link =  pd.NamedAgg(column = 'link',  aggfunc=lambda x: ' - '.join(x)),
            huh_name = pd.NamedAgg(column = 'huh_name', aggfunc = 'first'),
            geo_col = pd.NamedAgg(column = 'geo_col', aggfunc = 'first'),
            source_id = pd.NamedAgg(column = 'source_id',  aggfunc=lambda x: ', '.join(x)),
            wiki_url = pd.NamedAgg(column = 'wiki_url', aggfunc = 'first')
            )
        # here quite some data might get lost, so we need to check where we want to just join first,
        # and where we add all values, and then decide on the columns we really want in the final
        # database!!!

    #  END of if/else MASTER
    # REMOVE the s.n. used for deduplicatin s.n. values...    
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n.', pd.NA)
        logging.info('1 s.n. found')
    except:
        logging.info('1 s.n. NOT found')
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n., s.n.', pd.NA)
        logging.info('2 s.n. found')
    except:
        logging.info('2 s.n. NOT found')
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n., s.n., s.n.', pd.NA)
        logging.info('3 s.n. found')
    except:
        logging.info('3 s.n. NOT found')
    try:
        occs_dup_col.colnum_full = occs_dup_col.colnum_full.replace('s.n., s.n., s.n., s.n.', pd.NA)
        logging.info('4 s.n. found')
    except:
        logging.info('4 s.n. NOT found')

    # de-duplicated duplicates sorting to get them ready for merging
    #print(len(dupli))
    length = len(dupli)-2
    order_vec = tuple([True, True] + [False]*length)
    #print(order_vec, len(order_vec))

    occs_merged = occs_merged.sort_values(dup_cols, ascending = order_vec)

    logging.info(f'\n There were {len(occs_dup_col)} duplicated specimens')
    logging.info(f'\n There are {len(occs_merged)} unique records after merging.')
    logging.info(f'The deduplicated one is: {occs_merged}')


    logging.info('\n \n FINAL DUPLICATE STATS:')
    logging.info('-------------------------------------------------------------------------')
    logging.info(f'\n Input data: {len(occs)} ; \n De-duplicated duplicates: {len(occs_merged)}')
    logging.info(f'Non-duplicate data:{len(occs_unique)}')
     #'; \n No collection number. (This is not included in output for now!) :', len(occs_nocolNum),
    logging.info(f';\n total data written: {len(occs_merged) + len(occs_unique)} \n Datapoints removed: {len(occs) - (len(occs_merged) + len(occs_unique))}')
    

    if step=='Master':
        logging.info('********* Adding timestamp *******')
        date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        occs_merged['modified'] = User + '_' + date
        logging.info(f'{occs_merged.modified}')
        occs_unique['modified']
    #    occs_cleaned = pd.merge(pd.merge(occs_merged,occs_unique,how='outer'))#,occs_nocolNum ,how='outer')
    occs_cleaned = pd.concat([occs_merged, occs_unique])
    occs_cleaned = occs_cleaned.sort_values(dup_cols, ascending = order_vec)
    #print(occs_cleaned.head(50))

    if debugging:
        occs_cleaned.to_csv(working_directory+prefix+'deduplicated.csv', index = False, sep = ';', )
        logging.debug(f'\n The output was saved to {working_directory+prefix}deduplicated.csv', '\n')



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
