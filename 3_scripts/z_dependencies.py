#!/usr/bin/env 	'python',
# coding: utf-'8':

""" Here are some variables that account for the weird colnames we find.

"""

import pandas as pd
# final columns wished in the final database

# columns required to import data into the final database
final_cols_for_import = list(['globalID',
    'childDataID',
	'accSpeciesID',
    'scientificName',
	'genus',
	'specificEpithet',
	'speciesAuthor',
	'collectorID',
    'recordedBy',
    'colNum_full'
	'prefix',
	'colNum',
	'sufix',
    'colDate',
    'colDay',
    'colMonth',
    'colYear',
	'detBy',
	'detDate',
    'detDay',
    'detMonth',
    'detYear',
	'countryID',
	'country',
	'continent',
	'locality',
	'coordinateID',
	'ddlong',
    'ddlat',
	'institute',
    'herbarium_code',
    'barcode',
    'orig_BC',
    'coll_surname',
    'ORIG_recBy',
    'status',
    'accepted_name',
    'ipni_no'])

final_col_for_import_type = {'globalID': str,
    'childDataID': str,
	'accSpeciesID': str,
    'scientificName': str,
	'genus': str,
	'specificEpithet': str,
	'speciesAuthor': str,
	'collectorID': str,
    'recordedBy': str,
    'colNum_full': str,
	'prefix': str,
	'colNum': str,
	'sufix': str,
    'colDate': str,
    'colDay': pd.Int64Dtype(), # this allows nan within int
    'colMonth': pd.Int64Dtype(),
    'colYear': pd.Int64Dtype(),
	'detBy': str,
	'detDate': str,
    'detDay': pd.Int64Dtype(),
    'detMonth': pd.Int64Dtype(),
    'detYear': pd.Int64Dtype(),
	'countryID': str,
	'country': str,
	'continent': str,
	'locality': str,
	'coordinateID': str,
	'ddlong': float,
    'ddlat': float,
	'institute': str,
    'herbarium_code': str,
    'barcode': str,
    'orig_BC': str,
    'coll_surname': str,
    'ORIG_recBy': str,
    'status': str,
    'accepted_name': str,
    'ipni_no': str}



final_cols = list(['scientificName',
	'genus',
	'specificEpithet',
	'speciesAuthor',
	'collectorID',
    'recordedBy',
	'prefix',
	'colNum',
	'sufix',
    'colDate',
    'colDay',
    'colMonth',
    'colYear',
	'detBy',
	'detDate',
    'detDay',
    'detMonth',
    'detYear',
	'countryID',
	'country',
	'continent',
	'locality',
	'coordinateID',
	'ddlong',
    'ddlat',
	'institute',
    'herbarium_code',
    'barcode'])

final_col_type = {'scientificName': str,
	'genus': str,
	'specificEpithet': str,
	'speciesAuthor': str,
	'collectorID': str,
    'recordedBy': str,
	'prefix': str,
	'colNum': str,
	'sufix': str,
    'colDate': str,
    'colDay': pd.Int64Dtype(), # this allows nan within int
    'colMonth': pd.Int64Dtype(),
    'colYear': pd.Int64Dtype(),
	'detBy': str,
	'detDate': str,
    'detDay': pd.Int64Dtype(),
    'detMonth': pd.Int64Dtype(),
    'detYear': pd.Int64Dtype(),
	'countryID': str,
	'country': str,
	'continent': str,
	'locality': str,
	'coordinateID': str,
	'ddlong': float,
    'ddlat': float,
	'institute': str,
    'herbarium_code': str,
    'barcode': str}


# herbonautes data set columns present and wished in database
herbo_key = {
    'specimen - code': 'barcode',
    'collector - collector':'recordedBy',
    #'collector - other_collectors': 'addCol',
    #'prefix': 'prefix',
    'collect_number - number': 'colNum',
    #'sufix': 'sufix',
    'identifier - identifier': 'detBy',
    'Date deter - collect_date': 'detDate',
    'collect_date - collect_date': 'colDate',
    'country - country': 'country',
    'region1 - region': 'region',
    'locality - locality': 'locality',
    'geo - position': 'coordinates',
    #'ddlat': 'ddlat',
    # 'ddlong': 'ddlong',
    'specimen - genus': 'genus',
    'specimen - specific_epithet': 'specificEpithet',
    'scientificName': 'scientificName'}

#
# herbo_cols_to_add = [[plantdesc: 'plantdesc']]
#
herbo_subset_cols = (['recordedBy',
	#'prefix',
	'colNum',
	#'sufix',
	#'addCol',
	'detBy',
	'detDate',
	'colDate',
	'region',
	'locality',
	#'ddlat',
	#'ddlong',
    'coordinates',
    'barcode',
	'country',
	'genus',
	'specificEpithet'])

gbif_key = {'genus': 'genus',
    'species': 'species-tobesplit',
    'countryCode': 'countryID',
    'stateProvince': 'region',
    'decimalLatitude': 'ddlat',
    'decimalLongitude': 'ddlong',
    'day':'colDay',
    'month':'colMonth',
    'year':'colYear',
    'institutionCode':'institute',
    'collectionCode':'herbarium_code',
    'catalogNumber':'barcode',
    'recordNumber':'colNum',
    'identifiedBy':'detBy',
    'dateIdentified':'detDate',
    'recordedBy':'recordedBy'}


gbif_subset_cols =([
    'scientificName',
    'genus',
    'species-tobesplit',
    'countryID',
    'locality',
    'ddlat',
    'ddlong',
    #'collectorID',
    'recordedBy',
    #'collectorNumberPrefix',
    'colNum',
    #'collectorNumberSuffix',
    #'colDate',
    'colDay',
    'colMonth',
    'colYear',
    'detBy',
    'detDate',
    #'country',
    #'continent',
    #'coordinateID',
    'institute',
    'herbarium_code',
    'barcode'])
#
#
# MO_cols = {
#     # barcode: 'barcode',
#     recordedBy:'recordedBy',
#     AddCol: 'AddCol',
#     ColNum: 'ColNum',
#     detBy: 'detBy',
#     DetYear: 'DetYear',
#     ColYear: 'ColYear',
#     ColDay: 'ColDay',
#     ColMonth: 'ColMonth',
#     country: 'country',
#     region: 'region',
#     locality: 'locality',
#     ddlat: 'ddlat',
#     ddlong: 'ddlong'}
