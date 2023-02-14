#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
All the functions called from the main script for namechecking the Harvard University Herbarium collectors database

2023-01-10 sjs

CHANGELOG:
    2023-01-10: created
    2023-01-12: it works. BUT it doesn't give what i want. I do not get access to the full name of the collector or ID I need to access more deta
    2023-02-13: added webscraping. getting names, now differentiate between different name options with collection year (and region?)
    

CONTAINS:
    harvard_reference():
      queries the HUH database of collectors for the correct name format, using their api followed by webscraping to get to the full details of each collector.
   
'''

import pandas as pd
import numpy as np
import codecs
import os
import regex as re
import requests

from bs4 import BeautifulSoup
# get the 
#beautiful soup?
# scrapy




# for debugging. Full name is "de Wilde, Willem Jan Jacobus Oswald", and W. J. de Wilde links to this in HUH in theory
recordedBy = "Wilde, WJ de"










def get_HUH_names(recordedBy, colyear, country, orig_recby, verbose=True):
    """ Query the HUH database on botanists/collectors names.
    In: Collector name cleaned with regex
    Out: HUH botanist name, COllector geography and wiki link
    """

    if verbose:
        print('HUH name checker \n  \n .........................\n')
        print("Checking the botanist", recordedBy)

    recby_length = len(recordedBy.split())

    # split recorded by into Firstnames and Surnames. If just one word, assume it is the surname and proceed with just this...    
    if recby_length > 1:
        lastname, firstnames = recordedBy.split(',')
             # sometimes we have a complex mid name insert (de/van/...etc)
        mid_insert = re.sub(r'([A-Z])', '', firstnames).strip()
        firstnames = re.sub(r'([a-z]{0,3})', '', firstnames)
        if verbose:
            print(firstnames, lastname, 'MID=', mid_insert)

    else:
        lastname = recordedBy.split()[0]
        mid_insert = ''
        firstnames = ''

    # add points and plus into firstnames string for the query link
    if len(firstnames) > 0:
        s = firstnames[0]
        firstnames=firstnames.strip()
        for i in range(4):
            try:
                s = s + firstnames[i] + '.' + '+'
                s.replace('.+.', '.')
            except:
                if verbose:
                    print('Exception at link generation (~ line 80)')
        Firstname_query = s.strip() # strip empty spaces
    else:
        Firstname_query = pd.NA    
    if verbose:
        print(Firstname_query)

    # create name=<string> for insertion into url for query.
    lastname=lastname.strip()
    name_string = lastname # for now i have found just querying with surname yields best results
    print(name_string)
    name_string=name_string.strip() # just to make sure no leadin/trailing whitespace again
    
    # do query
    url = "https://kiki.huh.harvard.edu/databases/botanist_search.php?name="+name_string+"&individual=on"
    if verbose:
        print('The URL is:', url)
    response = requests.get(url)
    # important: re-encode into utf-8 to have all non-english characters and accents work
    response.encoding = 'UTF-8'


    # now feed the html document into beautiful soup package
    soup = BeautifulSoup(response.text, "html.parser")

    # print(soup)
    #if verbose:
    #    print(soup.findAll(href = re.compile("botanist_search.php")))

    # we now get all possibilities found on the webpage
    pot_names = soup.findAll(href = re.compile("botanist_search.php"))

    """ structure is:
     <a href="botanist_search.php?mode=details&amp;id=28447">P. A. W. J. de Wilde</a>     
    """

    # now format it in a way we can work with it
    # to integrate into a dataframe afterwards make tmp dataframe container.
    tmp = ('link_id', 'name')
    pot_df = pd.DataFrame(tmp).transpose()
    #

    for i in pot_names:
       
        i1 = i.attrs # this gets us the href which has the botanistsearch id...
        i1 = pd.Series(str(i1))

        i2 = pd.DataFrame(i).astype(str)
        i2 = i2.astype(str).replace('<strong>','')
        i2 = i2.replace('</strong>','')
             
        i3 = ''
        for j in range(0,len(i2)):
            j_tmp = i2.iloc[j]

            j_tmp = j_tmp.astype(str).str.replace('</strong>','')           
            j_tmp = j_tmp.astype(str).str.replace('<strong>','') 
            i3 = i3+j_tmp


        i3 = i3.str.replace('.', '. ', regex=False) # remove any problematic characters

        i1 = i1.str.split(':', expand=True)[1]
        i1 = i1.str.strip('}')
        i1 = i1.str.replace('\'','')
        i1 = i1.str.replace(' ','')

        I_out = (i1[0], i3[0])

        # I_out is now a vector with one element of links to detail page, and other element the full name as offered as choice at HUH

        new_row =  pd.DataFrame(I_out).transpose()
        pot_df = pd.concat([pot_df, new_row], axis=0)

    pot_df.reset_index(inplace=True) # reset index
    pot_df.columns = pot_df.iloc[0] # so we can rename columns
    pot_df.drop(index = 0, axis = 0, inplace=True) # and drop the row with the column names
    if verbose:
        print('Here the pot_df after cleaning up:\n',pot_df)

    pot_df.name = pot_df.name.str.replace('De ', 'de ') # sometimes the caps mid-name-inserts can cause issues, in our data we have only lower case mid-name-inserts
    pot_df.name = pot_df.name.str.replace('.', '')
    pot_df.name = pot_df.name.str.replace('&', ',') # we split by ',' later, so if botanist team we catch that there

    ###---------
    # Now with the power of regex, format resulting names to the same standard as used in previous steps.
    # Then check for similarity 

    # this regex query list is identical to our query in the name-standardising step...
    extr_list = {
            #r'^([A-Z][a-z]\-[A-Z][a-z]\W+[A-Z][a-z])' : r'\1', # a name with Name-Name Name
            #r'^([A-Z][a-z]+)\W+([A-Z]{2,5})' : r'\1, \2', #Surname FMN
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([a-z]{0,3})' : r'\1, \2\3\4\5 \6',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+.*' : r'\1, \2\3\4\5',  # all full full names with sep = ' '

            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([a-z]{0,3})' : r'\1, \2\3\4 \5',  # all full full names with sep = ' ' plus Surname F van
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([A-Z])[a-z]+.*' : r'\1, \2\3\4',  # all full full names with sep = ' '

            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+\s+([a-z]{0,3})': r'\1, \2\3 \4',  # all full names: 2 given names # + VAN
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]+\s+([A-Z])[a-z]+': r'\1, \2\3',  # all full names: 2 given names

            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]{2,20}\s+([a-z]{0,3})': r'\1, \2 \3',  # just SURNAME, Firstname  # + VAN
            r'^([A-Z][a-z]+)\,\W+([A-Z])[a-z]{2,20}': r'\1, \2',  # just SURNAME, Firstname

            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W*\s+([a-z]{0,3})\Z': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.) # VAN
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W*': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.)

            r'(^[A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W\s+([a-z]{0,3})\,.+': r'\1, \2\3\4 \5',  # Surname, F(.) M(.) M(.), other collectors
            r'(^[A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+([A-Z])\W\,.+': r'\1, \2\3\4',  # Surname, F(.) M(.) M(.), other collectors

            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+\s+([a-z]{0,3})\Z': r'\1, \2\3 \4',  # Surname, F(.) M(.)
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+([A-Z])\W+\Z': r'\1, \2\3',  # Surname, F(.) M(.)

            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])([A-Z])\s+([a-z]{0,3})': r'\1, \2\3\4\5 \6',  # Surname FMMM
            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])([A-Z])': r'\1, \2\3\4\5',  # Surname FMMM

            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])\s+([a-z]{0,3})': r'\1, \2\3\4 \5',  # Surname FMM
            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])([A-Z])': r'\1, \2\3\4',  # Surname FMM

            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])\s+([a-z]{0,3})\Z': r'\1, \2\3 \4',  # Surname FM
            r'^([A-Z][a-z]+)\W+([A-Z])([A-Z])\Z': r'\1, \2\3',  # Surname FM

            r'^([A-Z][a-z]+)\W+([A-Z])\s+([a-z]{0,3})\Z': r'\1, \2 \3',  # Surname F
            r'^([A-Z][a-z]+)\W+([A-Z])\Z': r'\1, \2',  # Surname F

            r'^([A-Z][a-z]+)\W*\Z': r'\1',  # Surname without anything
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+\s+([a-z]{0,3})': r'\1, \2 \3',  # Surname, F(.)
            r'^([A-Z][a-z]+)\,\W+([A-Z])\W+': r'\1, \2',  # Surname, F(.)

            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\6, \1\2\3\4 \5', # Firstname Mid Nid Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\5, \1\2\3\4', # Firstname Mid Nid Surname ...

            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\5, \1\2\3 \4', # Firstname Mid Nid Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\4, \1\2\3', # Firstname Mid Nid Surname ...

            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', # Firstname Mid Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\3, \1\2', # Firstname Mid Surname ...

            r'^([A-Z])[a-z]+\s([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', # Firstname M. Surname ...
            r'^([A-Z])[a-z]+\s([A-Z])\W+([A-Z][a-z]+)': r'\3, \1\2', # Firstname M. Surname ...

            r'^([A-Z])[a-z]+\s([a-z]{0,3})\s([A-Z][a-z]+)': r'\3, \1 \2', # Firstname Surname
            r'^([A-Z])[a-z]+\s([A-Z][a-z]+)': r'\2, \1', # Firstname Surname

            r'^([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\2, \1', # F. Surname ...
            r'^([A-Z])\W+([A-Z][a-z]+)': r'\2, \1', # F. Surname ...

            r'^([A-Z])\W+([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', #F. M. van  Surname
            r'^([A-Z])\W+([A-Z])\W+([A-Z][a-z]+)': r'\3, \1\2', #F. M. Surname
            
            r'^([A-Z])\W+([A-Z])\W+([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\5, \1\2\3 \4', #F. M. M. van Surname
            r'^([A-Z])\W+([A-Z])\W+([A-Z])\W+([A-Z][a-z]+)': r'\4, \1\2\3', #F. M. M. van Surname


            r'^([A-Z])([A-Z])([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\5, \1\2\3 \4', #FMM Surname
            r'^([A-Z])([A-Z])([A-Z])\W+([A-Z][a-z]+)': r'\4, \1\2\3', #FMM Surname

            r'^([A-Z])([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\4, \1\2 \3', #FM Surname
            r'^([A-Z])([A-Z])\W+([A-Z][a-z]+)': r'\3, \1\2', #FM Surname

            r'^([A-Z])\W+([a-z]{0,3})\s([A-Z][a-z]+)': r'\3, \1 \2', #F Surname
            r'^([A-Z])\W+([A-Z][a-z]+)': r'\2, \1', #F Surname
            #r'^([A-Z][a-z]+)\W+([A-Z]{2,5})' : r'\1, \2', #Surname FMN

        }
        # The row within the extr_list corresponds to the column in the debugging dataframe printed below


    names_WIP = pd.DataFrame(pot_df.name) #.astype(str)
    if verbose:
        print(names_WIP)
    i = 0
    for key, value in extr_list.items():
        i = i+1
        # make a new panda series with the regex matches/replacements
        X1 = names_WIP.name.str.replace(key, value, regex = True)
        # replace any fields that matched with empty string, so following regex cannot match.
        names_WIP.loc[:,'name'] = names_WIP.loc[:,'name'].str.replace(key, '', regex = True)
        # make new columns for every iteration
        names_WIP.loc[:,i] = X1 #.copy()
    ### no debugging dataframe here. if required uncomment the lines below
    # # debugging dataframe: every column corresponds to a regex query

    # names_WIP.to_csv('DEBUG_regex.csv', index = False, sep =';', )
    # print('debugging dataframe printed to','DEBUG_regex.csv')

    names_WIP = names_WIP.mask(names_WIP == '') # mask all empty values to overwrite them potentially
    names_WIP = names_WIP.mask(names_WIP == ' ')
    names_WIP = names_WIP.mask(names_WIP.name.notna())

    # now merge all columns into one
    while(len(names_WIP.columns) > 1): # while there are more than one column, merge the last two, with the one on the right having priority
        i = i+1
        names_WIP.iloc[:,-1] = names_WIP.iloc[:,-1].fillna(names_WIP.iloc[:,-2])
        names_WIP = names_WIP.drop(names_WIP.columns[-2], axis = 1)
        #print(names_WIP) # for debugging, makes a lot of output
        #print('So many columns:', len(names_WIP.columns), '\n')

    names_WIP.columns = ['corrnames']
    names_WIP = names_WIP.astype(str)

    # now merge these cleaned names into the dataframe for further processing
    pot_df_ext = pot_df.assign(regexed_nm = names_WIP['corrnames'])
    if verbose:
        print(pot_df_ext)

    pot_df_ext[['surname', 'givname']] = pot_df_ext.regexed_nm.str.split(',', expand = True)
    # now we have a column with surname, which has to match exactly (identical) 
    
    pot_df_ext['givname'] =  pot_df_ext['givname'].str.strip()
    pot_df_ext = pot_df_ext[pot_df_ext.surname == lastname]

    # and then we  try to see which initials best match to the query
    if len(firstnames)==0:
        subs1 = pot_df_ext
        if verbose:
            print('No first name provided')
    elif len(firstnames)==1:
        subs1 = pot_df_ext[pot_df_ext.givname.str[0:(len(firstnames))] == firstnames[0:(len(firstnames))]]
        if verbose:
            print('Length of firstnames 1')
    else:
        subs1 = pot_df_ext[pot_df_ext.givname.str[0:(len(firstnames)-1)] == firstnames[0:(len(firstnames)-1)]]
        if verbose:
            print('length of firstnames laregr 1')
    
    subs1 = subs1.drop_duplicates(subset = ['link_id']) # get rid of all records pointing to the identical database entry

    if verbose:
        print('\n','We have', len(subs1.name), 'candidate names.' )
        print(subs1)
    # now we have a shortlist, we can go and check     

    ###------- Now go and check out the results from the first query --------###

    pot_df = pd.DataFrame(tmp).transpose()
    print(pot_df)
 
    for i in range(len(subs1.name)):
        print(i)
        bot_str = subs1.iloc[i,1]
        print(bot_str)
        url = "https://kiki.huh.harvard.edu/databases/"+bot_str
        
       # print('The URL is:', url)
        response = requests.get(url)
        # # important: re-encode into utf-8 to have all non-english characters and accents work
        response.encoding = 'UTF-8'

        # # now feed the html document into beautiful soup
        soup = BeautifulSoup(response.text, "html.parser")
        # this time around, we can transform the output to a table
        df_soup = pd.read_html(str(soup.find('table')))[0].transpose()
        if verbose:
            print('For this record, the following details are available:', df_soup.columns)
    
        # we want Name, Date of birth, Date of death, URL, Geography Collector
        df_soup.columns = df_soup.iloc[0] # set col names
        df_soup.drop(df_soup.index[0], inplace=True) # and remove col name row
        # print(df_soup)
        
        # Name always exist by definitioin
        name = df_soup.Name   
        # the following do not always exist
        try: # date of birth might not always be there, so except with NA
            dobirth = df_soup['Date of birth']
        except:
            dobirth = pd.Series(pd.NA)        
        try: # date of death might not always be there, so except with NA
            dodeath = df_soup['Date of death']
        except:
            dodeath = pd.Series(pd.NA)       
        try: # URL might not always be there, so except with NA
            wiki_url = df_soup['URL']
        except:
            wiki_url = pd.Series(pd.NA)       
        try: # Collector geography might not always be there, so except with NA
            geo_col = df_soup['Geography Collector']
        except:
            geo_col = pd.Series(pd.NA)

        df_out = pd.concat([name, dobirth, dodeath, wiki_url, geo_col], axis=1)
        if verbose:
            print(df_out)

        try:
            df_tocheck = pd.concat([df_tocheck,df_out])
        except: # if the concat doesn't work, it means the tocheck doesn't yet exist...
            df_tocheck = df_out

        df_tocheck = df_tocheck.dropna(subset='Name')
        df_tocheck = df_tocheck.filter(regex='^\D')
        df_tocheck.replace('NaN', '0', inplace=True)
        print('CHECK', df_tocheck)
        # change colnames to tractable names...
       # df_tocheck.columns =
    cols_names = ('Name', 'Date of birth', 'Date of death', 'Geography Collector', 'URL')
    miss_col = [i for i in cols_names if i not in df_tocheck.columns]
    df_tocheck[miss_col] = '0' # and fill columns with nothing

    # rename columns
    df_tocheck = df_tocheck.rename(columns = {'Name': 'name', 'Date of birth':'dobirth', 'Date of death':'dodeath', 'Geography Collector':'geo_col', 'URL':'wiki_url'})
     
     # count commas to separate out entries with more than one name...
    df_tocheck = df_tocheck[df_tocheck.name.str.count(',')< 2] # records with more than 2 commas in the name are made up of multiple individuals.
    # print('REDUCED?', df_tocheck)

    if len(df_tocheck.name) > 1: # if still more than one name in the dataframe

        # the df_tocheck we can take away and do further checks with...
        colyear = float(colyear)
        #colyear = pd.Int64Dtype
        #colyear.astype('Int64')
        df_tocheck[['dobirth', 'dodeath']] = df_tocheck[['dobirth', 'dodeath']].astype(float)

        if pd.isna(colyear) == False: # if we have a collection year to work with, we can try to match to the life of botanist. However, usually these are NA...
            if any(df_tocheck.dobirth == '0'):
                df_checked = df_tocheck[df_tocheck.dobirth <= colyear]
            elif any(df_tocheck.dodeath == '0'):
                    df_checked = df_tocheck[df_tocheck.dodeath >= colyear]
            else:
                df_checked = df_tocheck   
        else:
            df_checked = df_tocheck   

        print('CHECKED2',df_checked)

        #df_checked['dobirth'] = df_checked['dobirth'].astype(str).str.replace('NaN', '0')
  
  ### TODO: finish this effing error
  
        df_checked[['dobirth', 'dodeath']] = df_checked[['dobirth', 'dodeath']].astype(float).astype(int)
        df_checked[['surname', 'othernames']] = df_checked['name'].str.split(',', expand=True) 
        df_checked['fname1'] = df_checked['othernames'].str.strip().str.split(' ', expand=True)[0]   

        #print(df_checked)
        # query using the first name of the original unmodified collector name (if by chance the first name was in there...) 
        orig_recby = orig_recby.replace(',', ' ')
        orig_recby = orig_recby.replace('.', ' ')
        orig_recby_list = orig_recby.split(' ')
        #print(orig_recby_list)
        
        if recby_length > 1:
            for i in df_checked.fname1:
                test = any(firstname in i for firstname in orig_recby_list)         
                try:
                    finalvote = (finalvote,test)
                except: # if the concat doesn't work, it means the tocheck doesn't yet exist...
                    finalvote = test
        else:
            # if only one option, take it
            if len(df_checked.name) == 1:
                finalvote = True
            elif len(df_checked.name) > 1:
                finalvote = False

    else:
        df_tocheck[['dobirth', 'dodeath']] = df_tocheck[['dobirth', 'dodeath']].astype(float)
        print(df_tocheck.dtypes)
        df_checked = df_tocheck
        print(df_checked)
        df_checked[['dobirth', 'dodeath']] = df_checked[['dobirth', 'dodeath']].astype(int)
        df_checked[['surname', 'othernames']] = df_checked['name'].str.split(',', expand=True) 
        df_checked['fname1'] = df_checked['othernames'].str.strip().str.split(' ', expand=True)[0]   
    
        finalvote = True

    df_checked.reset_index(inplace=True)
    
    # If still more than 1 name in the dataframe, do geography check
    if len(df_checked.name) > 1:
        if verbose:
            print("resorting to geography")

        df_checked = df_checked[df_checked.geo_col == country]
        if len(df_checked.name) == 1:
            finalvote = True
    
    try:
        df_checked = df_checked.assign(finalvote = pd.DataFrame(finalvote))
    except:
        if verbose:
            print('Only one FINALVOTE remaining:', finalvote)
        df_checked = df_checked.assign(finalvote = (finalvote))


    df_out = df_checked[df_checked.finalvote == True] # only get those that are chose based on above checks
    df_out = df_out[['name', 'geo_col', 'wiki_url']] # and subset cols we're interested in

    # get the final output variables 
    if verbose:
        print('OUTPUT', df_out)
    name = (df_out.name.values)
    geo_col = (df_out.geo_col.values)
    wiki_url = (df_out.wiki_url.values)

    if len(df_out.name) == 0: # if no result make all NA
        name = pd.NA
        geo_col = pd.NA
        wiki_url = pd.NA

    return name, geo_col, wiki_url # and return vars of interest.
         
 ##########---------- END OF FUNCTION ----------##########    


test_data = pd.read_csv('/Users/Serafin/Sync/1_Annonaceae/share_DB_WIP/2_data_out/G_Phil_cleaned.csv', sep =';')
print(test_data)

test_data = test_data.head(50)
print(test_data.recorded_by)
#test_data[['huh_name','geo_col', 'wiki_url']] = test_data.apply(lambda row: get_HUH_names(row['recorded_by'], row['col_year'], row['country'], 
 #                                                                                           row['orig_recby'], verbose=False), axis = 1, result_type='expand')

print(test_data)


# debugging, but other names are more suitable.
asagray = "Barnes, P"
test1, test2, test3 = get_HUH_names(asagray, 1899, 'Philippines', 'Barnes, P', verbose = True)
print(test1, test2, test3)








#
