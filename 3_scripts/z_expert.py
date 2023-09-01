#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Functions for expert det

2023-08-23 sjrs


- format input data

- check for missing data etc.

- integrate with master/find duplicated BC
"""

import codecs
import pandas as pd
import logging
import pykew.ipni as ipni
import swifter


from pykew.ipni_terms import Filters as ipni_filter
from pykew.ipni_terms import Name as ipni_name
from tqdm import tqdm

def read_expert(importfile, verbose=True):
    """
    read file, check columns
    """
    print('EXPERT file integration. \n',
          'Please assure that your columns are the following:',
          'ddlat, ddlong, locality, country or ISO2, recorded_by, colnum_full, det_by, det_date, barcode')
    imp = codecs.open(importfile,'r','utf-8')
    exp_dat = pd.read_csv(imp, sep = ';',  dtype = str)
    exp_dat['source_id'] = 'specialist'

    # make prefix from colnum
    exp_dat['prefix'] = exp_dat.colnum_full.str.extract('^([a-zA-Z]*)')
    exp_dat['prefix'] = exp_dat['prefix'].str.strip()

    # make sufix from colnum
    # going from most specific to most general regex, this list takes all together in the end
    regex_list_sufix = [
        r'(?:[a-zA-Z ]*)$', ## any charcter at the end
        r'(?:SR_\d{1,})', ## extract 'SR_' followed by 1 to 3 digits
        r'(?:R_\d{1,})', ## extract 'R_' followed by 1 to 3 digits
    ]
    exp_dat['sufix'] = exp_dat['colnum_full'].astype(str).str.extract('(' + '|'.join(regex_list_sufix) + ')')
    exp_dat['sufix'] = exp_dat['sufix'].str.strip()

    # extract only digits without associated stuff, but including some characters (colNam)
    regex_list_digits = [
        r'(?:\d+\-\d+\-\d+)', # of structure 00-00-00
        r'(?:\d+\s\d+\s\d+)', # 00 00 00 or so
        r'(?:\d+\.\d+)', # 00.00
        r'(?:\d+)', # 00000
    ]
    exp_dat['colnum']  = exp_dat.colnum_full.str.extract('(' + '|'.join(regex_list_digits) + ')')
    exp_dat['colnum'] = exp_dat['colnum'].str.strip()



# det date
    exp_dat[['det_year', 'det_month', 'det_day']] = exp_dat['det_date'].str.split("-", expand=True)



    exp_dat[['huh_name', 'geo_col', 'wiki_url']] = '0'
    exp_dat['orig_recby'] = exp_dat['recorded_by']
    exp_dat['col_year'] = pd.NA

    return exp_dat


# do HUH
# get ipni numbers?




# deduplication....
# we will do this by barcodes and or recorded_by + colnum + ??

def deduplicate_small_experts(master_db, exp_dat, verbose=True):
    """
    Find duplicates based on barcode, and collector name,

    Any values in these records found are overwritten by 'expert' data. This is assuned to be of (much) better quality.


    """


    # first some housekeeping: remove duplicated barcodes in input i.e. [barcode1, barcode2, barcode1] becomes [barcode1, barcode2]
    exp_dat.barcode = exp_dat.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell
    master_db.barcode = master_db.barcode.apply(lambda x: ', '.join(set(x.split(', '))))    # this combines all duplicated barcodes within a cell

    #----- prep barcodes (i.e. split multiple barcodes into seperate values <BC001, BC002> --> <BC001>, <BC002>)
    # split new record barcode fields (just to check if there are multiple barcodes there)
    bc_dupli_split = exp_dat['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    bc_dupli_split.columns = [f'bc_{i}' for i in range(bc_dupli_split.shape[1])] # give the columns names..
    bc_dupli_split = bc_dupli_split.apply(lambda x: x.str.strip())
     # some information if there are issues
    logging.debug(f'NEW OCCS:\n {bc_dupli_split}')
    logging.debug(f'NEW OCCS:\n {type(bc_dupli_split)}')
    master_bc_split = master_db['barcode'].str.split(',', expand = True) # split potential barcodes separated by ','
    master_bc_split.columns = [f'bc_{i}' for i in range(master_bc_split.shape[1])]
    master_bc_split = master_bc_split.apply(lambda x: x.str.strip())  #important to strip all leading/trailing white spaces!
    
    logging.debug(f'master OCCS:\n {master_bc_split}')

    exceptions = master_db.head(1)

    total_iterations = len(exp_dat)
    print('Crosschecking barcodes...\n', total_iterations, 'iterations to do')
    for i in tqdm(range(total_iterations), desc = 'Processing', unit= 'iteration'):
        # the tqdm should in theory have a progress bar...
            
    #####todo
        barcode = list(bc_dupli_split.loc[i].astype(str))
            # logging.info(f'BARCODE1: {barcode}')
            # if multiple barcodes in the barcode field, iterate across them
        for x in  range(len(barcode)):
            bar = barcode[x]

            # logging.info(f'working on row {i}')
            # logging.info(f'BC to test:{bc_dupli_split.iloc[i]}') # TODO

            if bar == 'None':
                # this happens a lot. skip if this is the case.
                a = 'skip'
                #logging.info('Values <None> are skipped.')
            else:
                selection_frame = pd.DataFrame()  # df to hold resulting True/False mask  
                # now iterate over columns to find any matches
                for col in master_bc_split.columns:
                    # iterate through rows. the 'in' function doesn't work otherwise
                    #logging.info('checking master columns')
                    f1 = master_bc_split[col] == bar # get true/false column
                    selection_frame = pd.concat([selection_frame, f1], axis=1) # and merge with previos columns
                    # end of loop over columns

                # when selection frame finished, get out the rows we need including master value
                sel_sum = selection_frame.sum(axis = 1)
                sel_sum = sel_sum >= 1 # any value >1 is a True => match 
            
                if sel_sum.sum() == 0:

                    # logging.info('NO MATCHES FOUND!')
                    out_barcode = pd.DataFrame([bar])

                # in this case we do not modify anything!

                else:
                    out_barcode = pd.Series(master_db.barcode[sel_sum]).astype(str)
                    out_barcode.reset_index(drop = True, inplace = True)


                # replace i-th element of the new barcodes with the matched complete range of barcodes from master
            
                input = str(exp_dat.at[i, 'barcode'])
                master = str(out_barcode[0])
                new = input + ', ' + master

                # reduce duplicated values
                new = ', '.join(set(new.split(', ')))

                # QUICK CHECK if recorded by and colnum are identical. Flag and save to special file if not
                print('1', master_db.loc[sel_sum, 'recorded_by'].item())
                print('2', exp_dat.loc[i, 'recorded_by'])
                if master_db.loc[sel_sum, 'recorded_by'].item() == (exp_dat.loc[i, 'recorded_by']):

                    master_db.loc[sel_sum, 'barcode'] = new
                    master_db.loc[sel_sum, 'accepted_name'] = exp_dat.at[i, 'accepted_name'] 
                    master_db.loc[sel_sum, 'det_by'] = exp_dat.at[i, 'det_by'] 
                    master_db.loc[sel_sum, 'det_year'] = exp_dat.at[i, 'det_year'] 
                    master_db.loc[sel_sum, 'ddlat'] = exp_dat.at[i, 'ddlat'] 
                    master_db.loc[sel_sum, 'ddlong'] = exp_dat.at[i, 'ddlong'] 
                    master_db.loc[sel_sum, 'status'] = 'ACCEPTED'
                    master_db.loc[sel_sum, 'expert_det'] = 'A_expert_det_file'
                    master_db.loc[sel_sum, 'prefix'] = exp_dat.at[i, 'prefix'] 

# TODO:
    # add more details, prefix etc all better here as handcurated!!


                else:
                    # exception where 'recorded_by' do not match
                    new_except = pd.concat([master_db.loc[sel_sum], pd.DataFrame(exp_dat.loc[i]).transpose()]) # see if this works...
                    # drop the offending rows for manual editing and adding later
                    master_db = master_db.loc[~sel_sum]
                    try:
                        # if exception already exists add concat
                        exceptions  = pd.concat([exceptions, new_except])
                    except:
                        # otherwise new 
                        exceptions = new_except
                # at the end of for loop print exceptions to file, let me manually redo it.



    return master_db, exceptions


def integrate_exp_exceptions(integration_file, exp_dat):
    """
    read and concatenate data manually edited, chekc data lengths 
    """
    imp = codecs.open(integration_file,'r','utf-8') #open for reading with "universal" type set
    re_exp = pd.read_csv(imp, sep = ';',  dtype = str, index_col=0) # read the data
    try:
        new_exp_dat = pd.concat([re_exp, exp_dat])
        if len(new_exp_dat) == (len(re_exp) + len(exp_dat)):
            print('Reintegration successful.')
            logging.info('reintroduction successful')
        else:
            print('reintegration not successful.')
            logging.info('reintegration unsuccessful')
    
    except:
        new_exp_dat = exp_dat
        print('reintegration not successful.')
        logging.info('reintegration unsuccessful')

    return new_exp_dat




def expert_ipni(species_name):
    ''' 
    Check species names against IPNI to get publication year, author name and IPNI link

    INPUT: 'genus'{string}, 'specific_epithet'{string} and 'distribution'{bool}
    OUTPUT: 'ipni_no' the IPNI number assigned to the input name
            'ipni_yr' the publication year of the species

    '''
    #print('Checking uptodate-ness of nomenclature in your dataset...')

    query = species_name
        #print(query)
    res = ipni.search(query, filters = ipni_filter.specific) # so we don't get a mess with infraspecific names
        #res = ipni.search(query, filters=Filters.species)  # , filters = [Filters.accepted])
    try:
        for r in res:
            #print(r)
            if 'name' in r:
                r['name']
        ipni_pubYr = r['publicationYear']
        ipni_no = 'https://ipni.org/n/' + r['url']
        ipni_author = r['authors']
        #print(ipni_pubYr)
        #logging.debug('IPNI publication year found.')
    except:
        ipni_pubYr = pd.NA

    return ipni_pubYr, ipni_no, ipni_author




def exp_run_ipni(exp_data):
        """
        wrapper for swifter apply of above function 'expert_ipni()'
        """
        print(exp_data.columns)
        exp_data[['ipni_species_author', 'ipni_no', 'ipni_pub']] = exp_data.swifter.apply(lambda row: expert_ipni(row['accepted_name']), axis = 1, result_type='expand')

        return exp_data






# # # # # # # --- DEBUGGING LINES ----- # # # # # # # 

# # debug_master = pd.read_csv('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/debug/smallexp_debug.csv', sep =';')
# debug_exp_file = '/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/debug/exp.csv'

# # print(debug_exp_file)
# # print(debug_master)
# # print('---WORKING---')
# debug_exp = read_expert(debug_exp_file)
# print(debug_exp)

# # print(debug_exp)
# debug_exp = exp_run_ipni(debug_exp)

# print(debug_exp)
# # final, exception = deduplicate_experts_minimal(debug_master, debug_exp)

# # print(final.accepted_name)
# # print('EXCEPTIONS', exception)
# spec = 'Cananga odorata'
# test_Y, test_N, testA = expert_ipni(spec)
    # print(test_Y, test_N, testA)