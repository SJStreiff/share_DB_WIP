#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for basic format changes, subsetting, ...

2022-12-13 sjs

CHANGELOG:
    2022-12-13: created


CONTAINS:
    column_standardiser():
        reads dataframe and subsets columns, adds empty columns for standardised format
    column_cleaning():
        takes columns and puts the content into standard/exchangeable format
    collector_names():
        takes collector names column ("recorded_by") and rearranges it into the format
        Surname, F (firstname just as initials)
'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import logging

#custom dependencies
import z_dependencies # can be replaced at some point, but later...




def column_standardiser(importfile, data_source_type, verbose=True, debugging = False):
    ''' reads a file, checks the columns and subsets and adds columns where
    necessary to be workable later down the line.'''


    imp = codecs.open(importfile,'r','utf-8') #open for reading with "universal" type set

    #-------------------------------------------------------------------------------
    # different sources have different columns, here we treat the most common source types,
    # rename the odd columns, and subset the wished ones
    # (dictionary and list of wanted columns in auxilary variable files, can be easily added and modified)

    if(data_source_type == 'P'):
        # for data in the format of herbonautes from P
        logging.info('data type P')
        occs = pd.read_csv(imp, sep = ';',  dtype = str) # read the data
        occs = occs.fillna(pd.NA)
        occs = occs.rename(columns = z_dependencies.herbo_key) # rename columns
        occs = occs[z_dependencies.herbo_subset_cols] # subset just required columns
        occs['source_id'] = 'P_herbo'
        # if verbose:
 	    #       print('Just taking the Philippines for now!')
        # occs = occs[occs['country'] == 'Philippines']

    elif(data_source_type == 'GBIF'):
        # for all data in the darwin core format!!
        logging.info('data type GBIF')
        occs = pd.read_csv(imp, sep = '\t',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        occs = occs[occs['basisOfRecord'] == "PRESERVED_SPECIMEN"] # remove potential iNaturalist data....
        occs = occs[occs['occurrenceStatus'] == 'PRESENT'] # loose absence data from surveys
        try:
            occs['references'] = occs['references'].fillna(occs['bibliographicCitation'])
        except:
            try:
                occs['references'] = occs['references'].fillna('')
            except:
                occs['references'] = ''    
        # here we a column species-tobesplit, as there is no single species columns with only epithet
        occs = occs.rename(columns = z_dependencies.gbif_key) # rename

        occs = occs[z_dependencies.gbif_subset_cols] # and subset
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'gbif'

        print(occs.link)

    elif(data_source_type == 'BRAHMS'):
        # for data from BRAHMS extracts
        logging.info('data type BRAHMS')
        occs = pd.read_csv(imp, sep = ';',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        occs = occs.rename(columns = z_dependencies.brahms_key) # rename
        occs = occs[z_dependencies.brahms_cols] # and subset
        print('READ3',occs.columns)
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'brahms'

    elif(data_source_type == 'MO'):
        # for data from BRAHMS extracts
        logging.info('data type MO')
        occs = pd.read_csv(imp, sep = ',',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        
        occs = occs.rename(columns = z_dependencies.brahms_key) # rename

        occs = occs[z_dependencies.brahms_cols] # and subset
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'MO_tropicos'


    elif(data_source_type == 'RAINBIO'):
        # for data from BRAHMS extracts
        logging.info('data type RAINBIO')
        occs = pd.read_csv(imp, sep = ';',  dtype = str, na_values=pd.NA, quotechar='"') # read data
        
        occs = occs.rename(columns = z_dependencies.rainbio_key) # rename

        occs = occs[z_dependencies.rainbio_cols] # and subset
        #occs = occs.fillna(pd.NA) # problems with this NA
        occs['source_id'] = 'RAINBIO'




    else:
        logging.warning('datatype not found')
        # maybe think if we want to somehow merge and conserve the plant description
        # for future interest (as in just one column 'plantdesc'???)
    
    logging.info(f'The columns now are: {occs.columns}')

    #occs = occs.replace(';', ',')
    logging.info(f'{occs}')
    #-------------------------------------------------------------------------------
    # add all final columns as empty columns
    # check for missing columns, and then add these, as well as some specific trimming
    # splitting etc...
    miss_col = [i for i in z_dependencies.final_cols if i not in occs.columns]


    logging.debug(f'These columns are missing in the data from source: {miss_col} Empty columns will be added and can later be modified.')
    logging.debug(f'{occs.dtypes}')
    occs[miss_col] = '0'
    #occs = occs.apply(lambda col: pd.to_numeric(col, errors='coerce').astype(float))
#        test_upd_DB.colnum = pd.to_numeric(test_upd_DB.colnum, errors='coerce').astype(pd.Int64Dtype())
    logging.debug(f'Testing new type implementation: {occs.dtypes}')
    logging.info(f'{occs}')

    print(occs[['col_day', 'col_month', 'col_year']])
    for column in occs.columns:
        # Iterate over each row in the column
        for index, value in occs[column].items():
            try:
                # Try converting the value to float
                float(value)
            except ValueError:
                a =1 
            except TypeError:
                # Print the column, index, and value where the error occurred
                #print(f"Error in column '{column}', index '{index}': {value}")
                b=1
 
    occs = occs.astype(dtype = z_dependencies.final_col_type, errors='raise')
    print(occs.dtypes)
    #_bc
    return occs


def column_cleaning(occs, data_source_type, working_directory, prefix, verbose=True, debugging = False):
    '''
    Cleaning up the columns to all be the same...
    '''
    logging.info('Standardising column names.')
    if(data_source_type == 'P'):
        """things that need to be done in the herbonautes data:
            - coordinate split (is ['lat, long'], want ['lat'], ['long'])
            - date split coll
            - date split det
            - split ColNum into pre-, main and sufix!
        """
        # COORDINATES
        occs[['ddlat', 'ddlong']] = occs.coordinates.str.split(",", expand = True)
        occs['ddlat'] = occs['ddlat'].str.strip() # trim any trailing spaces
        occs['ddlong'] = occs['ddlong'].str.strip()
        occs.drop(['coordinates'], axis = 'columns', inplace=True)

        # DATES

        # herbonautes data has 2 dates, start and end. We have decided to just take the first.
        # sometimes these are identical, but sometimes these are ranges.

        occs[['col_date_1', 'col_date_2']] = occs.col_date.str.split("-", expand=True,)
        occs[['det_date_1', 'det_date_2']] = occs.det_date.str.split("-", expand=True,)
        #delete the coldate_2 colomn, doesn't have info we need
        occs.drop(['col_date_2'], axis='columns', inplace=True)
        occs.drop(['det_date_2'], axis='columns', inplace=True)

        #split colDate_1 on '/' into three new fields (dd/mm/yyyy). Let's just hope no american ever turns up in that data
        # occs[['col_day', 'col_month', 'col_year']] = occs.col_date_1.str.split("/", expand=True,)
        # occs[['det_day', 'det_month', 'det_year']] = occs.det_date_1.str.split("/", expand=True,)
        # occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']]  = occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype('<NA>', 0)
        # print(occs.col_date_1)
        # # change datatype to pd Int64 (integer that can handle NAs)
        # occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']] = occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype(pd.Int64Dtype())


        #split colDate_1 on '/' into three new fields
        occs.col_date_1 = occs.col_date_1.replace('<NA>', '0/0/0')
        occs.det_date_1 = occs.det_date_1.replace('<NA>', '0/0/0')

        occs[['col_day', 'col_month', 'col_year']] = occs.col_date_1.str.split("/", expand=True,).astype(pd.Int64Dtype())
        occs[['det_day', 'det_month', 'det_year']] = occs.det_date_1.str.split("/", expand=True,).astype(pd.Int64Dtype())
        #occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']] = occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype(str).replace('<NA>', '0')
        #occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype(pd.Int64Dtype())
        print('HERHEREHEEREREREFEE:', occs.dtypes)
        #occs[['col_day', 'col_month', 'col_year', 'det_day', 'det_month', 'det_year']].astype(pd.Int64Dtype())



        # not sure which is best, but if T, then need to rename cols for standardisation
        # T: drop original col and det date comumns, but keep colDate_1 and DetDAte_1
        # S: keep the very original col and det date cols, remove the colDate_1...

        # for the time being I'm just doing it my way...
        occs.drop(['col_date_1'], axis='columns', inplace=True)
        occs.drop(['det_date_1'], axis='columns', inplace=True)

        
        logging.debug(f'Collection date formatting: {occs.col_date}')
        logging.debug('The collection date has now been split into separate (int) columns for day, month and year')


        # COLLECTION NUMBERS

        # keep the original colnum column
        #occs.rename(columns={'colnum': 'colnum_full'}, inplace=True)
        #create prefix, extract text before the number
        print(occs.colnum_full)

   
        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()


        # occs['prefix'] = occs.colnum_full.astype(str).str.extract(r'^([A-Z]+)\d+')
        # print(sum(pd.isna(occs.prefix)))
        
        # tmp_1 = pd.Series(occs.colnum_full.astype(str).str.extract(r'^([A-Z]+)\d+'))
        # print('TMP', type(tmp_1))
        # occs.prefix = occs.prefix.fillna(pd.Series(occs.colnum_full.astype(str).str.extract(r'^([A-Z]+)\d+')))
        print(occs.prefix)
        ##this code deletes spaces at start or end
        #print(occs.dtypes)

        #create sufix , extract text or pattern after the number
        #you can create different regex expressions here. The point of this code is to have the extract into a single and same colum
        #otherwise it creates different coloumns for each pattern.
        # see here: https://toltman.medium.com/matching-multiple-regex-patterns-in-pandas-121d6127dd47

        # going from most specific to most general regex, this list takes all together in the end
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only digits without associated stuff, but including some characters (colNam)
        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs.colnum_full.str.extract('(' + '|'.join(regex_list_digits) + ')')
        print(occs[['colnum_full', 'colnum']])
        occs['colnum'] = occs['colnum'].str.strip()


        print(sum(pd.isna(occs.ddlat)))
        occs.ddlat[pd.isna(occs.ddlat)] = 0
        occs.ddlong[pd.isna(occs.ddlong)] = 0

        # for completeness we need this
        occs['orig_bc'] = occs['barcode']
        occs = occs.astype(dtype = z_dependencies.final_col_type)

        logging.debug(f'{occs.dtypes}')

        occs = occs.replace({'nan': pd.NA}, regex=False) # remove NAs that aren't proper

        #print(occs.head(5))

    #-------------------------------------------------------------------------------
    if(data_source_type == 'GBIF'):
        """things that need to be done in the GBIF data:
            - BARCODE (just number or Herbcode + number)
            - specific epithet!
            - record number (includes collectors regularly...)
        """
    # -----------------------------------------------------------------------
    # split dates into format we can work with
        logging.info(f'{occs}')
        occs = occs.astype(dtype = z_dependencies.final_col_type)

        # format det date into separate int values yead-month-day
        occs['tmp_det_date'] = occs['det_date'].str.split('T', expand=True)[0]
        try:
            occs[['det_year', 'det_month', 'det_day']] = occs['tmp_det_date'].str.split("-", expand=True)
            occs = occs.drop(['tmp_det_date'], axis='columns')
        except:
            logging.debug('no det dates available...')

        logging.debug(f'{occs.dtypes}')

        occs = occs.replace({'nan': pd.NA}, regex=False)
        logging.debug(occs.det_year)


    # -----------------------------------------------------------------------
    # Barcode issues:
    # sonetimes the herbarium is in the column institute, sometimes herbarium_code, sometimes before the actual barcode number...
            # check for different barcode formats:
            # either just numeric : herb missing
            #     --> merge herbarium and code

        logging.info('Reformatting problematic barcodes and standardising them.')
        logging.debug(f'{occs.barcode}')


        # if there is a nondigit, take it away from the digits, and modify it to contain only letters
        barcode_regex = [
            r'^(\w+)$', # digit and letters
            r'(\d+\-\d+\/\d+)$', # digit separated by - and /
            r'(\d+\:\d+\:\d+)$', # digit separated by :
            r'(\d+\-\d+)$', # digit separated by - 
            r'(\d+\/\d+)$', # digit separated by /
            r'(\d+\.\d+)$', # digit separated by .
            r'(\d+\s\d+)$', # digit separated by space
            r'(\d+)$', # digit
        ]
        # extract the numeric part of the barcode
        bc_extract = occs['barcode'].astype(str).str.extract('(' + '|'.join(barcode_regex) + ')')
        #occs['prel_bc'] = occs['prel_bc'].str.strip()

        logging.debug(f'Numeric part of barcodes extracted {bc_extract}')

        i=0
        while(len(bc_extract.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
            i = i+1
            bc_extract.iloc[:,-1] = bc_extract.iloc[:,-1].fillna(bc_extract.iloc[:,-2])
            bc_extract = bc_extract.drop(bc_extract.columns[-2], axis = 1)
            #print(names_WIP) # for debugging, makes a lot of output
            #print('So many columns:', len(names_WIP.columns), '\n')
        logging.debug(f'barcodes extracted: {bc_extract}')
        # reassign into dataframe
        occs['prel_bc'] = bc_extract

        logging.debug(f'Prelim. barcode: {occs.prel_bc}')
        # now get the herbarium code. First if it was correct to start with, extract from barcode.
        bc = pd.Series(occs['barcode'])
        insti = pd.Series(occs['institute'])

        prel_herbCode = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)') # gets most issues... ABC-DE-000000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+\s[A-Z]+)')) # ABC DE00000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+\-)')) # ABC-00000
        prel_herbCode = prel_herbCode.fillna(bc.str.extract(r'(^[A-Z]+)')) #ABC000
        # If still no luck, take the capital letters in 'Institute'
        prel_herbCode = prel_herbCode.fillna(occs['institute'].str.extract(r'(^[A-Z]+)')) # desperately scrape whatever is in the 'institute' column if we still have no indication to where the barcode belongs

        occs = occs.assign(prel_herbCode = prel_herbCode)
        occs['prel_code'] = occs['barcode'].astype(str).str.extract(r'(\D+)')
        occs['prel_code_X'] = occs['barcode'].astype(str).str.extract(r'(\d+\.\d)') # this is just one entry and really f@#$R%g annoying
        #occs.to_csv('debug.csv', sep = ';')

        logging.debug(f'Prelim. barcode Type: {type(occs.prel_code)}')
        logging.debug(f'Institute would be: {occs.institute}')
        
        logging.debug(f'Prel. Herbarium code: {occs.prel_herbCode}')

        # if the barcode column was purely numericm integrate
        occs['tmp_hc'] = occs['institute'].str.extract(r'(^[A-Z]+\Z)')
        occs['tmp_hc'] = occs['barcode'].str.extract(r'(^[A-Z]+\-[A-Z]+\-)')
        #occs['hc_tmp_tmp'] = occs['herbarium_code'].str.extract(r'(^[A-Z]+\Z)')
        #occs['tmp_hc'].fillna(occs.hc_tmp_tmp, inplace = True)
        occs['tmp_hc'] = occs['tmp_hc'].replace({'PLANT': 'TAIF'})
        #occs['tmp_hc'] = occs['tmp_hc'].str.replace('PLANT', '')
        occs.prel_herbCode.fillna(occs['tmp_hc'], inplace = True)
        occs.prel_herbCode.fillna('', inplace = True)
        #occs[occs['herbarium_code'] == r'([A-Z]+)', 'tmp_test'] = 'True'
        
        logging.debug(f'TMP herb code: {occs.tmp_hc}')
        # this now works, but,
            # we have an issue with very few institutions messing up the order in which stuff is supposed to be...
            # (TAIF)

        #occs.institute.fillna(occs.prel_herbCode, inplace=True)
        print('DEBUG:', pd.isna(occs.prel_bc.str.extract(r'([A-Z])')))
        occs['sel_col_bc'] = pd.isna(occs['prel_bc'].str.extract(r'([A-Z])'))
        occs.loc[occs['sel_col_bc'] == False, 'prel_herbCode'] = ''
        logging.debug(f'prel_code: {occs.prel_herbCode}')
        logging.debug(f'{occs.prel_bc}')
        occs.drop



        occs['st_barcode'] = occs['prel_herbCode'] + occs['prel_bc']
        occs['st_barcode'] = occs.st_barcode.astype(str)
        
        # for i in occs.st_barcode:

        #     j = re.findall(r'([A-Z])', i)
        #     print(len(i), i, len(j))
        # indexbc = pd.isna(occs.st_barcode.str.extract(r'([A-Z])'))
        # logging.debug(indexbc)
        # print('HERE', indexbc, type(indexbc))
        # print('HERE again', indexbc.value_counts())
        

        logging.debug(f'{occs.st_barcode}')
    #    prel_herbCode trumps all others,
    #        then comes herbarium_code, IF (!) it isn't a word (caps and non-caps) and if it is not (!!) 'PLANT'
    #           then comes institute


        logging.debug(f'Now these columns: {occs.columns}')

        if occs.st_barcode.isna().sum() > 0:
        
            logging.info('I couldn\'t standardise the barcodes of some records. This includes many records (if from GBIF) with barcode = NA')
            na_bc = occs[occs['st_barcode'].isna()]
            na_bc.to_csv(working_directory + prefix + 'NA_barcodes.csv', index = False, sep = ';', )
            logging.info(f'I have saved {len(na_bc)} occurences to the file {working_directory+prefix}NA_barcodes.csv for corrections')

            logging.info('I am continuing without these.')
            occs = occs[occs['st_barcode'].notna()]
            
            logging.debug(f'Occs: {occs}')

        # and clean up now
        occs = occs.drop(['prel_bc', 'prel_herbCode', 'prel_code', 'prel_code_X', 'tmp_hc', 'sel_col_bc'], axis = 1)
        occs = occs.rename(columns = {'barcode': 'orig_bc'})
        occs = occs.rename(columns = {'st_barcode': 'barcode'})
        
        logging.debug(f'{occs.columns}')
        # -----------------------------------------------------------------------
        # COLLECTION NUMBERS
        # keep the original colnum column
        # split to get a purely numeric colnum, plus a prefix for any preceding characters,
        # and sufix for trailing characters

        #occs.rename(columns={'colnum': 'colnum_full'}, inplace=True)
        #try:
        #    occs.colnum_full = occs.colnum_full.replace('s.n.', pd.NA) # keep s.n. for later?
        #except:
        #    logging.info('No plain s.n. values found in the full collection number fields.')

        #occs.colnum_full.replace("s. n.", pd.NA, inplace=True)
        
        logging.debug(f'{occs.colnum_full}')

        #create prefix, extract text before the number
        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()

        #create sufix , extract text or pattern after the number
        #you can create different regex expressions here. The point of this code is to have the extract into a single and same colum
        #otherwise it creates different coloumns for each pattern.
        # see here: https://toltman.medium.com/matching-multiple-regex-patterns-in-pandas-121d6127dd47

        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only number (colNam)

        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_digits) + ')')
        print(occs[['colnum_full', 'colnum']])

        occs['colnum'] = occs['colnum'].str.strip()

        occs['colnum'].replace('nan', pd.NA, inplace=True)

        #print(occs)
        # ok this looks good for the moment.
        """ Still open here: TODO
        remove collector name in prefix (partial match could take care of that)
        """

        # SPECIFIC EPITHET

        # darwin core specific...

        occs[['tmp', 'specific_epithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
        #print(occs.tmp)


        occs[['tmp', 'specific_epithet']] = occs['species-tobesplit'].str.split(' ', expand = True)
        logging.debug(f'{occs.tmp}')

        occs.drop(['tmp'], axis='columns', inplace=True)
        occs.drop(['species-tobesplit'], axis='columns', inplace=True)

        #occs['barcode'] = occs['prel_herbCode'].fillna('') + occs['prel_BC']

        #
        # else:
        #     print('nay')
        # \w.\d+ then remove punctuation and fill in herbarium code
            # split barcode into herbarium and barcodes



    #-------------------------------------------------------------------------------
    if(data_source_type == 'BRAHMS'):
        """things that need to be done in the BRAHMS data:
            - colnum_full for completeness (= prefix+colnum+sufix)
        """
    # -------------------------------------
        #remove odd na values
        occs.prefix = occs.prefix.str.replace('nan', '')
        occs.sufix = occs.sufix.str.replace('nan', '')
        # make colnum_full
        occs.colnum_full = occs.prefix + occs.colnum + occs.sufix
        # for completeness we need this
        occs['orig_bc'] = occs['barcode']
        occs['country_id'] = pd.NA

    #----------------------------------------------------------------------------------
    if(data_source_type == 'RAINBIO'):
        """ things needed:
         - colnum splitting
         """
        # COLLECTION NUMBERS

        # keep the original colnum column
        print(occs.colnum_full)
   
        occs['prefix'] = occs.colnum_full.str.extract('^([a-zA-Z]*)')
        ##this code deletes spaces at start or end
        occs['prefix'] = occs['prefix'].str.strip()
        print(occs.prefix)
    
        # going from most specific to most general regex, this list takes all together in the end
        regex_list_sufix = [
          r'(?:[a-zA-Z ]*)$', ## any charcter at the end
          r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
          r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
        ]

        occs['sufix'] = occs['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
        occs['sufix'] = occs['sufix'].str.strip()

        # extract only digits without associated stuff, but including some characters (colNam)
        regex_list_digits = [
            r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
            r'(?:\d+\-\d+)', # of structure 00-00
            r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
            r'(?:\d+\.\d+)', # 00.00
            r'(?:\d+)', # 00000
        ]
        occs['colnum']  = occs.colnum_full.str.extract('(' + '|'.join(regex_list_digits) + ')')
        print(occs[['colnum_full', 'colnum']])
        occs['colnum'] = occs['colnum'].str.strip()

        print(sum(pd.isna(occs.ddlat)))
        occs.ddlat[pd.isna(occs.ddlat)] = 0
        occs.ddlong[pd.isna(occs.ddlong)] = 0

        occs['orig_bc'] = occs['barcode']
        occs['country_id'] = pd.NA
        occs = occs.astype(dtype = z_dependencies.final_col_type)

        logging.debug(f'{occs.dtypes}')

        occs = occs.replace({'nan': pd.NA}, regex=False) # remove NAs that aren't proper





        #MO split species-tobesplit


    occs = occs.astype(dtype = z_dependencies.final_col_type) # check data type
    #print(occs.dtypes)
    logging.info('Data has been standardised to conform to the columns we need later on.') #\n It has been saved to: ', out_file)
    logging.info(f'Shape of the final file is: {occs.shape}' )
    logging.debug(f'{occs.columns}')


    occs = occs.astype(dtype = z_dependencies.final_col_type)
    logging.info(f'Shape of the final file is: {occs.shape}')

    #occs.to_csv(out_file, sep = ';')
    
    logging.info('Column standardisation completed successfully.')

    return occs

def collector_names(occs, working_directory, prefix, verbose=True, debugging=False):
    """
    With an elaborate regex query, match all name formats I have been able to come up with.
    This is applied to both recorded_by and det_by columns. All those records that failed are
    written into a debugging file, to be manually cleaned.
    """
    print('NAMES FORMAT:', occs.columns)
    pd.options.mode.chained_assignment = None  # default='warn'
    # this removes this warning. I am aware that we are overwriting stuff with this function.
    # the column in question is backed up


    #print(occs.dtypes) # if you want to double check types again
    occs = occs.astype(dtype = z_dependencies.final_col_type)
    occs['recorded_by'] = occs['recorded_by'].replace('nan', pd.NA)
    occs['det_by'] = occs['det_by'].replace('nan', pd.NA)
    #print(occs.head)
    # -------------------------------------------------------------------------------
    # Clean some obvious error sources, back up original column
    occs['orig_recby'] = occs['recorded_by'] # keep original column...
    occs['orig_detby'] = occs['det_by']
    # remove the introductory string before double point :
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Collector(s):', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Unknown', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('&', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace(' y ', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace(' and ', ';', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Jr.', 'JUNIOR', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('et al.', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('et al', '', regex=False)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('etal', '', regex=False)
    #occs['recorded_by'] = occs['recorded_by'].astype(str).str.replace('Philippine Plant Inventory (PPI)', 'Philippines, Philippines Plant Inventory', regex=False)
    #we will need to find a way of taking out all the recorded_by with (Dr) Someone's Collector

    #isolate just the first collector (before a semicolon)
    occs['recorded_by'] = occs['recorded_by'].astype(str).str.split(';').str[0]

    occs['recorded_by'] = occs['recorded_by'].str.strip()

    print('---------------- \n There are still weird exceptions which I do not catch. These have to be handled manually and reintegrated.',
        '\n -------------------')

    # sep collectors by ';', multiple collectors separated like this
    # 'Collector(s):' is also something that pops up in GBIF....

    # now we need to account for many different options
    # 1 Meade, C.     --> ^[A-Z][a-z]*\s\[A-Z]   ^([A-Z][a-z]*)\,
    # 2 Meade, Chris  -->
    # 3 Meade C       -->
    # 4 C. Meade      -->
    # 5 Chris Meade   -->


    #-------------------------------------------------------------------------------
    # regex queries: going from most specific to least specific.

   
    extr_list = {
            #r'^([A-ZÀ-Ÿ][a-zà-ÿ]\-[A-ZÀ-Ÿ][a-zà-ÿ]\W+[A-ZÀ-Ÿ][a-zà-ÿ])' : r'\1', # a name with Name-Name Name
            #r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ]{2,5})' : r'\1, \2', #Surname FMN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([a-zà-ÿ]{0,3})' : r'\1, \2\3\4\5 \6',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+' : r'\1, \2\3\4\5',  # all full full names with sep = ' '

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([a-zà-ÿ]{0,3})' : r'\1, \2\3\4 \5',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+' : r'\1, \2\3\4',  # all full full names with sep = ' '

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([a-zà-ÿ]{0,3})': r'\1, \2\3 \4',  # all full names: 2 given names # + VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]+\s+([A-ZÀ-Ÿ])[a-zà-ÿ]+': r'\1, \2\3',  # all full names: 2 given names

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]{2,20}\s+([a-zà-ÿ]{0,3})': r'\1, \2 \3',  # just SURNAME, Firstname  # + VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])[a-zà-ÿ]{2,20}': r'\1, \2',  # just SURNAME, Firstname

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3\4\5 \6',  # Surname, F(.) M(.) M(.)M(.) # VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.)M(.)


            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.) # VAN
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W*': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.)

            r'(^[A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W\s+([a-zà-ÿ]{0,3})\,.+': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.), other collectors
            r'(^[A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W\,.+': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.), other collectors

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3 \4',  # Surname, F(.) M(.)
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+\Z': r'\1, \2\3',  # Surname, F(.) M(.)

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})': r'\1, \2\3\4\5 \6',  # Surname FMMM
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])': r'\1, \2\3\4\5',  # Surname FMMM

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})': r'\1, \2\3\4 \5',  # Surname FMM
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])': r'\1, \2\3\4',  # Surname FMM

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2\3 \4',  # Surname FM
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\Z': r'\1, \2\3',  # Surname FM

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])\s+([a-zà-ÿ]{0,3})\Z': r'\1, \2 \3',  # Surname F
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ])\Z': r'\1, \2',  # Surname F

            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W*\Z': r'\1',  # Surname without anything
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+\s+([a-zà-ÿ]{0,3})': r'\1, \2 \3',  # Surname, F(.)
            r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\,\W+([A-ZÀ-Ÿ])\W+': r'\1, \2',  # Surname, F(.)

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\6, \1\2\3\4 \5', # Firstname Mid Nid Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3\4', # Firstname Mid Nid Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3 \4', # Firstname Mid Nid Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2\3', # Firstname Mid Nid Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', # Firstname Mid Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', # Firstname Mid Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', # Firstname M. Surname ...
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', # Firstname M. Surname ...

            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1 \2', # Firstname Surname
            r'^([A-ZÀ-Ÿ])[a-zà-ÿ]+\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', # Firstname Surname

            r'^([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', # F. Surname ...
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', # F. Surname ...

            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', #F. M. van  Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', #F. M. Surname
            
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3 \4', #F. M. M. van Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2\3', #F. M. M. van Surname

            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\6, \1\2\3\4 \5', #F. M. M. M. van Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3\4', #F. M. M. M. van Surname

            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\6, \1\2\3\4 \5', #FMMM Surname
            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3\4', #FMM Surname


            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\5, \1\2\3 \4', #FMM Surname
            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2\3', #FMM Surname

            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\4, \1\2 \3', #FM Surname
            r'^([A-ZÀ-Ÿ])([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1\2', #FM Surname

            r'^([A-ZÀ-Ÿ])\W+([a-zà-ÿ]{0,3})\s([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\3, \1 \2', #F Surname
            r'^([A-ZÀ-Ÿ])\W+([A-ZÀ-Ÿ][a-zà-ÿ]+)': r'\2, \1', #F Surname
            #r'^([A-ZÀ-Ÿ][a-zà-ÿ]+)\W+([A-ZÀ-Ÿ]{2,5})' : r'\1, \2', #Surname FMN

        }


    # The row within the extr_list corresponds to the column in the debugging dataframe printed below


    names_WIP = occs[['recorded_by']] #.astype(str)

    #print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.recorded_by.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'recorded_by'] = names_WIP.loc[:,'recorded_by'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
    if debugging:
        names_WIP.to_csv(working_directory + prefix + 'DEBUG_regex.csv', index = False, sep =';', )
        logging.debug(f'debugging dataframe printed to {working_directory + prefix}DEBUG_regex.csv')

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually
    TC_occs = occs.copy()
    TC_occs['to_check'] = names_WIP['recorded_by']

    # #-------------------------------------------------------------------------------


    # mask all values that didn't match for whatever reason in the dataframe (results in NaN)
    names_WIP = names_WIP.mask(names_WIP.recorded_by.notna())

    # now merge all columns into one
    while(len(names_WIP.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
        i = i+1
        names_WIP.iloc[:,-1] = names_WIP.iloc[:,-1].fillna(names_WIP.iloc[:,-2])
        names_WIP = names_WIP.drop(names_WIP.columns[-2], axis = 1)
        #print(names_WIP) # for debugging, makes a lot of output
        #print('So many columns:', len(names_WIP.columns), '\n')
    #print(type(names_WIP))


    #print('----------------------\n', names_WIP, '----------------------\n')
    # just to be sure to know where it didn't match
    names_WIP.columns = ['corrnames']
    names_WIP = names_WIP.astype(str)

    # now merge these cleaned names into the output dataframe
    occs_newnames = occs.assign(recorded_by = names_WIP['corrnames'])



    occs_newnames['recorded_by'] = occs_newnames['recorded_by'].replace('nan', 'ZZZ_THIS_NAME_FAILED')
    occs_newnames['recorded_by'] = occs_newnames['recorded_by'].replace('<NA>', 'ZZZ_THIS_NAME_FAILED')

    #print(occs_newnames.recorded_by)
    # remove records I cannot work with...
    occs_newnames = occs_newnames[occs_newnames['recorded_by'] != 'ZZZ_THIS_NAME_FAILED']


    # ------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------
    # repeat the story with the det names
    logging.debug(f'The cleaned name format: {occs_newnames.recorded_by}')
    names_WIP = occs_newnames[['det_by']] #.astype(str)

    #print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.det_by.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'det_by'] = names_WIP.loc[:,'det_by'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()

    # debugging dataframe: every column corresponds to a regex query
    if debugging:
        names_WIP.to_csv(working_directory + prefix + 'DEBUG_detby_regex.csv', index = False, sep =';', )
        logging.debug(f'debugging dataframe for det bys printed to {working_directory + prefix}DEBUG_detby_regex.csv')

    #####
    # Now i need to just merge the columns from the right and bobs-your-uncle we have beautiful collector names...
    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')

    #-------------------------------------------------------------------------------
    # For all names that didn't match anything:
    # extract and then check manually

    TC_occs1 = occs_newnames.copy()
    TC_occs1['to_check_det'] = names_WIP['det_by']
    print('CHECK', TC_occs1.to_check_det)

    print('TEST', TC_occs.colnum_full)
    print(TC_occs.to_check)
    print('NAs:',sum(pd.isna(TC_occs.to_check)))
    try:
        TC_occs1['to_check_det'] = TC_occs1['to_check_det'].str.replace('NaN', pd.NA)
    #     print('THIS TIME IT WORKED')
    except:
        logging.debug(f'TC_occs no NA transformation possible')

    print('det NAs:',sum(pd.isna(TC_occs1.to_check_det)))
    # ###




    #-------------------------------------------------------------------------------


    # mask all values that didn't match for whatever reason in the dataframe (results in NaN)
    names_WIP = names_WIP.mask(names_WIP.det_by.notna())

    # now merge all columns into one
    while(len(names_WIP.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
        i = i+1
        names_WIP.iloc[:,-1] = names_WIP.iloc[:,-1].fillna(names_WIP.iloc[:,-2])
        names_WIP = names_WIP.drop(names_WIP.columns[-2], axis = 1)
        #print(names_WIP) # for debugging, makes a lot of output
        #print('So many columns:', len(names_WIP.columns), '\n')
    #print(type(names_WIP))

    # just to be sure to know where it didn't match
    names_WIP.columns = ['corrnames']
    names_WIP = names_WIP.astype(str)

    # now merge these cleaned names into the output dataframe
    occs_newnames = occs_newnames.assign(det_by = names_WIP['corrnames'])

    #leave just problematic names (drop NA)
    TC_occs.dropna(subset= ['to_check'], inplace = True)
    print('HERE 1', TC_occs.to_check)
    TC_occs.det_by = occs_newnames.det_by

    print(TC_occs1.to_check_det)
    # some datasets have problem with NA strings (vs NA type)
    try:
        TC_occs1['to_check_det'] = TC_occs1['to_check_det'].replace('<NA>', pd.NA)
        print('<NA> handled')
    except:
        print('<NA> issues')
    TC_occs1.dropna(subset= ['to_check_det'], inplace = True)
    print('HERE 2', TC_occs1.to_check_det)
    print('NAs:',sum(pd.isna(TC_occs1.to_check_det)))

    #print(TC_occs.to_check)
    TC_occs['to_check_det'] = 'recby_problem'
    TC_occs1['to_check'] = 'detby_problem'

    TC_occs = pd.concat([TC_occs, TC_occs1])#, on = 'orig_recby', how='inner')
    logging.debug(f'To check: {TC_occs.shape}')
    TC_occs = TC_occs.drop_duplicates(subset = ['barcode'], keep = 'first')
    
    logging.debug(f'To check (deduplicated by barcode) {TC_occs.shape}')
    print(TC_occs.columns)
    TC_occs_write = TC_occs[['recorded_by', 'orig_recby', 'colnum_full', 'det_by', 'orig_detby', 'to_check', 'to_check_det']]

    # output so I can go through and check manually
    if len(TC_occs)!= 0:
        TC_occs_write.to_csv(working_directory +'TO_CHECK_' + prefix + 'probl_names.csv', index = True, sep = ';', )
        print(len(TC_occs), ' records couldn\'t be matched to the known formats.',
        '\n Please double check these in the separate file saved at: \n', working_directory+'TO_CHECK_'+prefix+'probl_names.csv')



    # ------------------------------------------------------------------------------#
    # ------------------------------------------------------------------------------

    logging.info(f'It used to look like this: {occs.recorded_by}')
    logging.info('---------------------------------------------------')
    logging.info(f'Now it looks like this: {occs_newnames.recorded_by}')

    logging.info(f'DETS: It used to look like this: {occs.det_by}')
    logging.info(f'---------------------------------------------------')
    logging.info(f'DETS: Now it looks like this: {occs_newnames.det_by}')

    logging.info(f'I removed {len(occs) - len(occs_newnames)} records because I could not handle the name.')
   
    occs_newnames = occs_newnames.astype(z_dependencies.final_col_type)
    # save to file
    #occs_newnames.to_csv(working_directory+prefix+'names_standardised.csv', index = False, sep = ';', )
    occs_newnames['coll_surname'] = occs_newnames['recorded_by'].str.split(',', expand=True)[0]

    if debugging:
        unique_names = occs_newnames['recorded_by'].unique()
        unique_names = pd.DataFrame(unique_names)
        unique_names.to_csv(working_directory + prefix+'collectors_unique.csv', index = False, sep =';', )
        logging.debug(f'I have saved {len(unique_names)} Collector names to the file {working_directory + prefix}collectors_unique.csv.')

    # done
    return occs_newnames, TC_occs



def reinsertion(occs_already_in_program, frame_to_be_better, names_to_reinsert, verbose=True, debugging=False):
    '''
    Quickly read in data for reinsertion, test that nothing went too wrong, and append to the data already in the system.
    occs_already_in_program: Data not flagged in previous steps
      frame_to_be_better: Data flagged in previous step
      names_to_reinsert: subset of frame the above, but corrected by user (hopefully!)
    '''
    logging.info('REINSERTING...')
    logging.info(f'To reinsert: {names_to_reinsert}')
    imp = codecs.open(names_to_reinsert,'r','utf-8') #open for reading with "universal" type set
    re_occs = pd.read_csv(imp, sep = ';',  dtype = str, index_col=0) # read the data
    logging.debug(f'The read dataframe: {re_occs}')
    logging.debug(f'The read dataframe columns: {re_occs.columns}')
    
    re_occs = re_occs.drop(['to_check', 'to_check_det'], axis = 1)
    re_occs.sort_index(inplace=True)
    re_occs = re_occs.replace({'NaN': pd.NA}, regex=False)
    
    
    logging.debug(f'Reordered read data: {re_occs.recorded_by}')
    frame_to_be_better.sort_index(inplace=True)
    logging.debug(f'The problem data before user correction: {frame_to_be_better.orig_recby}')
    frame_to_be_better['recorded_by'] = re_occs['recorded_by']
    frame_to_be_better['det_by'] = re_occs['det_by']
    logging.debug(f'The problem data after user correction: {frame_to_be_better.recorded_by}') #[['recordedBy', 'orig_recby']])
    logging.debug(f'Original problematic values{frame_to_be_better.orig_recby}')
    
    frame_to_be_better = frame_to_be_better.astype(z_dependencies.final_col_type)

    
    logging.info('Reinsertion data read successfully')

    occs_merged = pd.concat([occs_already_in_program, frame_to_be_better])
    logging.info(f'TOTAL {len(occs_merged)} in Prog {len(occs_already_in_program)} reintegrated {len(re_occs)}')
    if len(occs_merged) == len(occs_already_in_program) + len(re_occs):
        logging.info('Data reinserted successfully.')
        print('Data integrated successfully!')
    else:
        #raise Exception("Something weird happened, please check input and code.")
        logging.info('data integration anomalous, either error or discrepancy')
        logging.info('This might for example be if the integrated data is not exactly the same size of the data i wrote in previous steps')
    return occs_merged







#
