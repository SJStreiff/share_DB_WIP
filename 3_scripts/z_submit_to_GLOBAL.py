#!/usr/bin/env/python3
# -*- coding: utf-8 -*-

import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import MetaData, Table, Column, Integer, String
from getpass import getpass
import z_dependencies

# def connect_test(banana):

inputfile='/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/20230824_mdb_for_upload.csv'

print('password?')
passw = getpass()
print('port?')
port = getpass()

url_obj = URL.create('postgresql',
                    username='streiff',
                    password=passw,
                    host='10.4.91.57',
                    port=port,
                    database='Global_ss')
print(url_obj)
engine = create_engine(url_obj, connect_args={'options': '-csearch_path={}'.format('species_collection')})
print(engine)

df = pd.read_sql_table('species_collected', engine)
print(df)
schema='species_collection'

#imp = codecs.open(args.input_file,'r','utf-8') #open for reading with "universal" type set
occs = pd.read_csv(inputfile, sep = ';',  dtype = z_dependencies.final_col_for_import_type) 
#data = pd.read_csv()
print(occs)

occs.to_sql('Asia_v1_20230824', engine, schema)



# # connection
# conn = psycopg2.connect(database="Global_ss", options="-c search_path=dbo,species_collection",
#                         user='streiff', password='serafin', 
#                         host='10.4.91.57', port='5432')

# conn.autocommit = True
# cursor = conn.cursor()

# # change the path of your file

# with open('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/20230821_mdb_for_upload.csv', 'r') as f:
    
# # Skip the header row.
#     next(f)
#     cursor.copy_from(f,'species_collected', sep=';')

# conn.commit()
# conn.close()
