#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''This program takes your CLEANED records, compares them to a masterdatabase,
and integrates the new data in to said masterdatabase.

PREFIX "Y_" for scripts
'''


#import y_sql_functions as sql
import z_merging as pre_merge
import z_functions_b as dupli
import z_cleanup as cleanup
import z_expert as expert

import z_dependencies

#import dependencies

import argparse, os, pathlib, codecs
import pandas as pd
import numpy as np
import datetime 
from getpass import getpass
import logging


print(os.getcwd())



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='RECORD FILER',
                                     description='RECORD FILER takes your CLEANED data and integrates it into a specified database. It is intended as step 2 in the process, i.e. following rigorous cleaning of your data.',
                                     epilog='If it doesn\'t work, or you would like to chat, feel free to contact me at serafin.streiff<at>ird.fr')
    parser.add_argument('input_file',
                        help='(Cleaned) input file path',
                        type = pathlib.Path),
    # parser.add_argument('MasterDB',
    #                     help='The location of the master database file.',
    #                     type=str)
    parser.add_argument('expert_file',
                         help = 'Specify if input file is of expert source (i.e. all determinations and coordinates etc. are to have priority over other/previous data)',
                         type = str,
                         choices = ['EXP', 'NO', 'SMALLEXP'] ),
    parser.add_argument('db_local',
                         choices=['local','remote'],
                        help='Is the database saved locally or on a server??',
                        type = str)
    parser.add_argument('database_name',
                        help='The name of the SQL database to access, or if db_local = \'local\', then the directory of database',
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
    parser.add_argument('working_directory',
                        help='the working directory for debugging output',
                        type=str)
    parser.add_argument('na_value',
                        help = 'Value used for NA/NaN/<NA>',
                        type = str)
    
    # parser.add_argument('indets_backlog',
    #                     help='The location of the indets backlog database for crosschecking and indets-rescuing',
    #                     type=str)
    # parser.add_argument('no_coords_backlog',
    #                     help='The location of the backlog database with records of missing data for crosschecking and duplicate-rescuing',
    #                     type=str)
    parser.add_argument('-v', '--verbose',
                        help = 'If true (default), I will print a lot of stuff that might or might not help...',
                        default = True)
    parser.add_argument('-l', '--log_file',
                        help = 'path specifying a location for the output logfile.',
                        type = str)
    args = parser.parse_args()
    # optional, but for debugging. Maybe make it prettier
    #print('Arguments:', args)

    print('-----------------------------------------------------------\n')
    print('#> This is the RECORD FILER step of the pipeline\n',
          'Arguments supplied are:\n',
          'INPUT FILE:', args.input_file,
          '\n Expert status:', args.expert_file,
          '\n Database location (local or remote):', args.db_local,
          '\n Database name:', args.database_name,
          '\n Hostname:', args.hostname,
          '\n Table name:', args.tablename,
          '\n Schema name:', args.schema,
          '\n Working directory:', args.working_directory,
          '\n verbose:', args.verbose)
    print('-----------------------------------------------------------\n')


    logging.basicConfig(filename=args.log_file, encoding='utf-8', level=logging.INFO)
    logging.info('-----------------------------------------------------------\n')
    logging.info('#> This is the RECORD FILER step of the pipeline\n')
    logging.info('Arguments supplied are:')
    logging.info(f'INPUT FILE: {args.input_file}')
    logging.info(f'Expert status: {args.expert_file}')
    logging.info(f'Database location (local or remote): {args.db_local}')
    logging.info(f'Database name: {args.database_name}')
    logging.info(f'\n Hostname: {args.hostname}')
    logging.info(f'Table name: {args.tablename}')
    logging.info(f'Schema name: {args.schema}')
    logging.info(f' Working directory: {args.working_directory}')
    logging.info(f' verbose: {args.verbose}')
    logging.info('-----------------------------------------------------------')


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
    logging.info('\n#> Now we \n')
    logging.info('\t - Check your new records against indets and for duplicates \n')
    logging.info('\t - Merge them into the master database\n---------------------------------------------\n')

    # check input data variation: is it just one genus? just one country?
    imp = codecs.open(args.input_file,'r','utf-8') #open for reading with "universal" type set
    occs = pd.read_csv(imp, sep = ';',  dtype = z_dependencies.final_col_for_import_type, na_values=pd.NA, quotechar='"') # read the data
    # #print(occs)
    occs = occs.fillna(pd.NA)
    logging.info('NA filled!')
    # print('\n ................................\n',
    # 'NOTE that for the GLOBAL database you must be connected to the VPN...\n'
    print('Please type the USERNAME used to annotate changes in the records:')
    username=input() 
    # print('\n ................................\n',
    # 'Please type the PASSWORD used to connect to the database for user', username)
    # password=getpass() #'n' # make back to input()
    # print('\n ................................\n',
    # 'Please type the PORT required to connect to the database:')
    # port=input() #'n' # make back to input()


    # give data a (time??)stamp
    date = date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    occs['modified'] = username + '_' + date

    countries_new = occs.country_iso3.unique()


    # if just one small group, then do some sort of subsetting
    # genus? country? ...

    # IF I WANT TO UPLOAD A STARTING POINT AT SOME POINT...
    # print('I am just going to upload the current state of the data into GLOBAL (schema: \"serafin_test\")')
    # SQL.send_to_sql(occs ,args.database_name, args.hostname, args.tablename, args.schema)

    if args.db_local == 'remote':
         logging.info("Trying to read remote database...")
         logging.info('Not today')
        # TEMPORARILY NOT HAPPENING AS I JUST WANT OT DEBUG THE MERGING OF DATASETS.
        # Step D1
        # # - get database extract from the GLOBAL database.
        # print('Please check if you are connected to the VPN. If not this will not work.')

        # if EXP then only subset by country!

        # SUBSET_GENERA = '???'
        
        # SUBSET_COUNTRY = '???'
        
        # m_DB = SQL.fetch_master_db(args.database_name, args.hostname, args.tablename, args.schema)
        # print('The downloaded DB is the following:', m_DB)
        # m_DB.to_csv('./4_DB/sql_data_transf_test.csv', index=False, sep=';')
        # # m_DB =pd.read_csv('./4_DB/sql_data_transf_test.csv')
        # # This works
        # try:
        #     m_DB = m_DB.replace('nan', pd.NA)
        # except Exception as e:
        #     print('NA as nan replacement problematic')
        # try:
        #     m_DB = m_DB.replace(np.nan, pd.NA)
        # except Exception as e:
        #     print('NA as np replacement problematic')
        # 
        # print('Master database read successfully!', len(m_DB), 'records downloaded')
        # #
    # working with a local database...    
    elif args.db_local == 'local':
        logging.info('Reading local database')
        mdb_dir = args.database_name

        BL_indets = pd.read_csv(mdb_dir+'/indet_backlog.csv', sep=';', dtype= z_dependencies.final_col_for_import_type, na_values=pd.NA)
        BL_indets = BL_indets.fillna(pd.NA)
    
        BL_indets_cc = BL_indets[BL_indets.country_iso3.isin(countries_new)]
        BL_indets_non = BL_indets[~BL_indets.country_iso3.isin(countries_new)]

        logging.info('NA filled!')
        #print(BL_indets)
        try:
            no_coord_bl = pd.read_csv(mdb_dir + '/coord_backlog.csv', sep=';', dtype= z_dependencies.final_col_for_import_type)
            no_coord_bl = no_coord_bl.fillna(pd.NA)

            no_coord_bl_cc = no_coord_bl[no_coord_bl.country_iso3.isin(countries_new)]
            no_coord_bl_non = no_coord_bl[~no_coord_bl.country_iso3.isin(countries_new)]

        except:
            #nothing
            a = 1


        #m_DB = pd.read_csv(mdb_dir + '/20230918_master_db.csv', sep =';', dtype= z_dependencies.final_col_for_import_type, na_values=pd.NA)
        m_DB = pd.read_csv(mdb_dir + '/master_db.csv', sep =';', dtype= z_dependencies.final_col_for_import_type, na_values=pd.NA)
        m_DB = m_DB.fillna(pd.NA)
           # to make it look like the masterdb I will add all the final columns
        miss_col = [i for i in z_dependencies.final_cols_for_import if i not in m_DB.columns]
        m_DB[miss_col] = '0'
        m_DB = m_DB.astype(dtype = z_dependencies.final_col_for_import_type)
 
        m_DB_cc = m_DB[m_DB.country_iso3.isin(countries_new)]
        m_DB_non = m_DB[~m_DB.country_iso3.isin(countries_new)]

    masters = pd.concat([no_coord_bl_cc, BL_indets_cc, m_DB_cc])
    # sanity check
    if len(masters) == (len(no_coord_bl_cc) + len(BL_indets_cc) + len(m_DB_cc)):
        print('YAY, debug good 1')

    # subset by countries
    

########################### TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO # TODO ``

    if args.expert_file == 'SMALLEXP':
        logging.info('#> SMALL EXPERT file. separate step')

        exp_occs = occs

        master_exp_occs, exceptions = expert.deduplicate_small_experts(masters, exp_occs)
        # by way the exceptions df is created we need the tail(-1)
        exceptions = exceptions.tail(-1)
        # then go through exceptions manually and reintegrate
        if len(exceptions) > 1:
            #let user modify exceptions
            exceptions.to_csv(args.working_directory+args.prefix+'expert_exceptions.csv', index=False, sep =';')
            print('I have written exceptions to',
                  args.working_directory+args.prefix+'expert_exceptions.csv', 
                  '\n for you to check. Please do so and save the file for me to read it again once finished.')
            disexceptions = input()
            file=args.output_dir+args.prefix+'expert_exceptions.csv'

            exp_occs_1 = expert.integrate_exp_exceptions(file, master_exp_occs)
        else:
            exp_occs_1 = master_exp_occs

        # sort out indet and no_coord dat
        exp_occs_final = cleanup.clean_up_nas(exp_occs_1, '-9999')
        
        indet_to_backlog = exp_occs_final[exp_occs_final.accepted_name == args.na_value] # ==NA !!
        occs = occs[occs.accepted_name != args.na_value] # NOT NA!
        no_coords_to_backlog = occs[occs.ddlat == args.na_value] # ==NA !!
        deduplid = occs[occs.ddlat != args.na_value] # NOT NA!
    
        logging.info('SMALLXP handling completed')
        # final data is written at end


    else:
        # 'normal' data handling (i.e. expert = EXP / NO)
        ###---------------------- First test against indets backlog --------------------------------###
        logging.info('\n#> INDET consolidation')
        logging.info('------------------------------------------------')

        # check all occs against indet backlog
        test_upd_DB = pre_merge.check_premerge(mdb = BL_indets_cc, occs = occs, verbose=True)

        test_upd_DB=test_upd_DB.astype(z_dependencies.final_col_for_import_type)

        test_upd_DB.colnum = test_upd_DB.colnum.replace('nan', pd.NA)

        # separate data with colNum and no colNum
        occs_s_n = test_upd_DB[test_upd_DB.colnum.isna()]
        occs_num = test_upd_DB.dropna(how='all', subset=['colnum'])
        
        # deduplicate (x2)
        occs_num_dd = dupli.duplicate_cleaner(occs_num, dupli = ['recorded_by', 'colnum', 'sufix', 'col_year'], 
                                        working_directory = args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                        expert_file = args.expert_file, verbose=True, debugging=False)
        occs_s_n_dd = dupli.duplicate_cleaner(occs_s_n, dupli = ['recorded_by', 'col_year', 'col_month', 'col_day', 'genus', 'specific_epithet'], 
                                    working_directory =  args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                    expert_file = args.expert_file, verbose=True, debugging=False)
        # recombine data 
        occs = pd.concat([occs_s_n_dd, occs_num_dd], axis=0)
        #occs = occs.replace('\'nan\'', pd.NA)

        ###--------------testing if filter by 'accepted_name' ----------------###
                # #print(occs.status)
                # occs.status = occs.status.replace('nan', None)
                # occs.status = occs.status.replace('<NA>', None)
                # # check nomencl. status
                # logging.debug(f'INDET:::::\n {occs[~occs.status.notna()]}')
                # indet_to_backlog = occs[occs.status.isna()] # ==NA !!
                # occs = occs[occs.status.notna() ] # NOT NA!
                # logging.info(f'{occs.status.notna()}')
                # logging.info(f'{indet_to_backlog.status.notna()}')
        ###-------------------------------------------------------------------###

        occs.accepted_name = occs.accepted_name.replace('nan', None)
        occs.accepted_name = occs.accepted_name.replace('<NA>', None)
        # check nomencl. status
        logging.debug(f'INDET:::::\n {occs[~occs.accepted_name.notna()]}')
        indet_to_backlog = occs[occs.accepted_name.isna()] # ==NA !!
        occs = occs[occs.accepted_name.notna() ] # NOT NA!
        logging.info(f'{occs.accepted_name.notna()}')

        # merge modified BL with untouched BL
        indet_to_backlog = pd.concat([indet_to_backlog, BL_indets_non])
        
        #indet_to_backlog = pd.concat([indet_to_backlog])
        # keep indet_to_backlog and send back into server
        indet_to_backlog.to_csv(mdb_dir + '/indet_backlog.csv', sep=';', index=False)
        logging.info(f'{occs.status}')
        logging.info(f'{occs.accepted_name}')


        ###---------------------- Then test against coordinate-less data backlog --------------------------------###
        logging.info('\n#> COORDINATE consolidation\n------------------------------------------------')
            # geo_issues is the column name for georeferencing issues



        #no_coord_bl = pd.read_csv('/Users/Serafin/Sync/1_Annonaceae/share_DB_WIP/4_DB_tmp/coord_backlog.csv', sep=';')
        # check all occs against indet backlog
        no_coord_check = pre_merge.check_premerge(mdb=no_coord_bl_cc, occs=occs, verbose=True)

        #print(no_coord_check.dtypes)
        no_coord_check=no_coord_check.astype(z_dependencies.final_col_for_import_type)
        #print(no_coord_check.dtypes)
        no_coord_check = no_coord_check.reset_index(drop=True)

        occs_s_n = no_coord_check[no_coord_check['colnum_full'].isna()]
        occs_num = no_coord_check.dropna(how='all', subset=['colnum_full'])

        # deduplicate (x2)
        occs_num_dd = dupli.duplicate_cleaner(occs_num, dupli = ['recorded_by', 'colnum', 'sufix', 'col_year'], 
                                        working_directory = args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                    expert_file = args.expert_file, verbose=True, debugging=False)
        occs_s_n_dd = dupli.duplicate_cleaner(occs_s_n, dupli = ['recorded_by', 'col_year', 'col_month', 'col_day', 'genus', 'specific_epithet'], 
                                        working_directory = args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                    expert_file = args.expert_file, verbose=True, debugging=False)
        # recombine data 
        occs = pd.concat([occs_s_n_dd, occs_num_dd], axis=0)


        no_coords_to_backlog = occs[occs.ddlat.isna() ] # ==NA !!
        occs = occs[occs.ddlat.notna() ] # NOT NA!
        
        no_coords_to_backlog = pd.concat([no_coords_to_backlog, no_coord_bl_non])

        no_coords_to_backlog.to_csv(mdb_dir + 'coord_backlog.csv', sep=';', index=False)

    
        #print('No coordinate-less records found.')
            # occs remains unchanged
        logging.info(f'{occs.accepted_name}')

        ###---------------------- Then merge all with master database. Make backup of previous version. --------------------------------###

        logging.info('\n#> FINAL master database consolidation')
        logging.info('------------------------------------------------')

        #print(' size:', len(occs), 'With columns:', occs.columns)




        upd_DB = pre_merge.check_premerge(mdb = m_DB_cc, occs = occs, verbose=True)
        # something really weird happening. Should not be as many duplicates as it gives me.

        upd_DB=upd_DB.astype(z_dependencies.final_col_for_import_type)
        #print(upd_DB.dtypes)

        occs_s_n = upd_DB[upd_DB['colnum_full'].isna()]
        occs_num = upd_DB.dropna(how='all', subset=['colnum_full'])
        
        
        # deduplicate (x2)
        occs_num_dd = dupli.duplicate_cleaner(occs_num, dupli = ['recorded_by', 'colnum', 'sufix', 'col_year'], 
                                    working_directory =  args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                    expert_file = args.expert_file, verbose=True, debugging=False)
        
        
        if len(occs_s_n) != 0:
            occs_s_n_dd = dupli.duplicate_cleaner(occs_s_n, dupli = ['recorded_by', 'col_year', 'col_month', 'col_day', 'genus', 'specific_epithet'], 
                                                working_directory = args.working_directory, prefix = 'Integrating_', User = username, step='Master',
                                                expert_file = args.expert_file, verbose=True, debugging=False)
        
        

            # recombine data 
            deduplid = pd.concat([occs_s_n_dd, occs_num_dd], axis=0)
        
        else:
            deduplid = occs_num_dd
        #print(deduplid.status)

        #print(deduplid)

        date = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

        #print(date)

        # print the old database back into new backup table

        deduplid = pd.concat([deduplid, m_DB_non])


        for col in deduplid.columns:
            deduplid[col] = deduplid[col].astype(str).str.replace('nan', '')
            deduplid[col] = deduplid[col].fillna('')

    no_coord_bl.to_csv(mdb_dir + '/backups/coord/'+date+'_coord_backlog.csv', sep=';')
    BL_indets.to_csv(mdb_dir + '/backups/indet/'+date+'_indet_backlog.csv', sep=';')
    m_DB.to_csv(mdb_dir + '/backups/'+date+'_master_backup.csv', sep = ';', index = False)#, mode='x')
    # the mode=x prevents overwriting an existing file...

    # reduce duplicated information within cells     
    deduplid = cleanup.cleanup(deduplid, cols_to_clean=['source_id', 'colnum_full', 'institute', 'herbarium_code', 'barcode', 'orig_bc', 'geo_issues', 'det_by', 'link'], verbose=True)
    
    logging.info('\n#> Merging steps complete.\n------------------------------------------------')

    logging.info(f'Trimming master database before writing: {len(deduplid)}')
    deduplid = deduplid[z_dependencies.final_cols_for_import]
    print('Final size:', len(deduplid))#, 'With columns:', deduplid.columns)
    print('Indet backlog size:', len(indet_to_backlog))
    print('No-Coord backlog size:', len(no_coords_to_backlog))
    # this is now the new master database...
    deduplid.to_csv(mdb_dir + 'master_db.csv', sep=';', index=False)
    logging.info(f'------------------------------------------------\n#> {len(deduplid)} Records filed away into master database.\n')
    


    #upload = False


# needs more finetuning.
