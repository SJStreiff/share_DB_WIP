#!/usr/bin/env/python3
# -*- coding: utf-8 -*-

import os
import psycopg2



# connection
conn = psycopg2.connect(database="Global_ss", options="-c search_path=dbo,species_collection",
                        user='streiff', password='serafin', 
                        host='10.4.91.57', port='5432')
  
conn.autocommit = True
cursor = conn.cursor()

# change the path of your file

with open('/Users/serafin/Sync/1_Annonaceae/G_GLOBAL_distr_DB/X_GLOBAL/master.csv', 'r') as f:
     
# Skip the header row.
    next(f)
    cursor.copy_from(f,'species_collected', sep=';')

conn.commit()
conn.close()
