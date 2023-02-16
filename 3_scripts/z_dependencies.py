#!/usr/bin/env 	'python',
# coding: utf-'8':

""" Here are some variables that account for the weird colnames we find.

"""

import pandas as pd
# final columns wished in the final database

# columns required to import data into the final database
final_cols_for_import = list(['global_id',
    'childData_id',
	'accSpecies_id',
    'scientific_name',
	'genus',
	'specific_epithet',
	'species_author',
	'collector_id',
    'recorded_by',
    'colnum_full'
	'prefix',
	'colnum',
	'sufix',
    'col_date',
    'col_day',
    'col_month',
    'col_year',
	'det_by',
	'det_date',
    'det_day',
    'det_month',
    'det_year',
	'country_id',
	'country',
	'continent',
	'locality',
	'coordinate_id',
	'ddlong',
    'ddlat',
	'institute',
    'herbarium_code',
    'barcode',
    'orig_bc',
    'coll_surname',
    'orig_recby',
    'status',
    'accepted_name',
    'ipni_no',
    'huh_name',
    'geo_col',
    'wiki_url'])

final_col_for_import_type = {'global_id': str,
    'childData_id': str,
	'accSpecies_id': str,
    'scientific_name': str,
	'genus': str,
	'specific_epithet': str,
	'species_author': str,
	'collector_id': str,
    'recorded_by': str,
    'colnum_full': str,
	'prefix': str,
	'colnum': str,
	'sufix': str,
    'col_date': str,
    'col_day': pd.Int64Dtype(), # this allows nan within int
    'col_month': pd.Int64Dtype(),
    'col_year': pd.Int64Dtype(),
	'det_by': str,
	'det_date': str,
    'det_day': pd.Int64Dtype(),
    'det_month': pd.Int64Dtype(),
    'det_year': pd.Int64Dtype(),
	'country_id': str,
	'country': str,
	'continent': str,
	'locality': str,
	'coordinate_id': str,
	'ddlong': float,
    'ddlat': float,
	'institute': str,
    'herbarium_code': str,
    'barcode': str,
    'orig_bc': str,
    'coll_surname': str,
    'orig_recby': str,
    'status': str,
    'accepted_name': str,
    'ipni_no': str,
    'huh_name': str,
    'geo_col': str,
    'wiki_url': str}



final_cols = list(['scientific_name',
	'genus',
	'specificEpithet',
	'species_author',
	'collector_id',
    'recorded_by',
	'prefix',
	'colnum',
	'sufix',
    'col_date',
    'col_day',
    'col_month',
    'col_year',
	'det_by',
	'det_date',
    'det_day',
    'det_month',
    'det_year',
	'country_id',
	'country',
	'continent',
	'locality',
	'coordinate_id',
	'ddlong',
    'ddlat',
	'institute',
    'herbarium_code',
    'barcode',
    'huh_name',
    'geo_col',
    'wiki_url'])

final_col_type = {'scientific_name': str,
	'genus': str,
	'specificEpithet': str,
	'species_author': str,
	'collector_id': str,
    'recorded_by': str,
	'prefix': str,
	'colnum': str,
	'sufix': str,
    'col_date': str,
    'col_day': pd.Int64Dtype(), # this allows nan within int
    'col_month': pd.Int64Dtype(),
    'col_year': pd.Int64Dtype(),
	'det_by': str,
	'det_date': str,
    'det_day': pd.Int64Dtype(),
    'det_month': pd.Int64Dtype(),
    'det_year': pd.Int64Dtype(),
	'country_id': str,
	'country': str,
	'continent': str,
	'locality': str,
	'coordinate_id': str,
	'ddlong': float,
    'ddlat': float,
	'institute': str,
    'herbarium_code': str,
    'barcode': str,
    'huh_name': str,
    'geo_col': str,
    'wiki_url': str}


# herbonautes data set columns present and wished in database
herbo_key = {
    'specimen - code': 'barcode',
    'collector - collector':'recorded_by',
    #'collector - other_collectors': 'addCol',
    #'prefix': 'prefix',
    'collect_number - number': 'colnum',
    #'sufix': 'sufix',
    'identifier - identifier': 'det_by',
    'Date deter - collect_date': 'det_date',
    'collect_date - collect_date': 'col_date',
    'country - country': 'country',
    'region1 - region': 'region',
    'locality - locality': 'locality',
    'geo - position': 'coordinates',
    #'ddlat': 'ddlat',
    # 'ddlong': 'ddlong',
    'specimen - genus': 'genus',
    'specimen - specific_epithet': 'specific_epithet',
    'scientificName': 'scientific_name'}

#
# herbo_cols_to_add = [[plantdesc: 'plantdesc']]
#
herbo_subset_cols = (['recorded_by',
	#'prefix',
	'colnum',
	#'sufix',
	#'addCol',
	'det_by',
	'det_date',
	'col_date',
	'region',
	'locality',
	#'ddlat',
	#'ddlong',
    'coordinates',
    'barcode',
	'country',
	'genus',
	'specific_epithet'])

gbif_key = {'genus': 'genus',
    'species': 'species-tobesplit',
    'countryCode': 'country_id',
    'stateProvince': 'region',
    'decimalLatitude': 'ddlat',
    'decimalLongitude': 'ddlong',
    'day':'col_day',
    'month':'col_month',
    'year':'col_year',
    'institutionCode':'institute',
    'collectionCode':'herbarium_code',
    'catalogNumber':'barcode',
    'recordNumber':'colnum',
    'identifiedBy':'det_by',
    'dateIdentified':'det_date',
    'recordedBy':'recorded_by',
    'scientificName':'scientific_name'}


gbif_subset_cols =([
    'scientific_name',
    'genus',
    'species-tobesplit',
    'country_id',
    'locality',
    'ddlat',
    'ddlong',
    #'collector_id',
    'recorded_by',
    #'collectorNumberPrefix',
    'colnum',
    #'collectorNumberSuffix',
    #'colDate',
    'col_day',
    'col_month',
    'col_year',
    'det_by',
    'det_date',
    #'country',
    #'continent',
    #'coordinate_id',
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
