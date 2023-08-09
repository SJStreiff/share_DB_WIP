# changelog RECORDCLEANER


# 2023-08-01:
    - switched from iso2 to iso3 in country_id (works directly with coordinate_cleaner)...

# 2023-08-02:
    - changed the barcode formatting to include alphanumeric repeating formats of 00000A000A000  (to be validated)
    - changed indet_backlog criteria from 'status' to 'accepted_name': if NA, then in backlog.

# 2023-08-08:
    - added 'link' column, where urls leading to the image can be integrated (remains hit-and-miss for GBIF, as there data entered quite randomly sometimes)
    
# 2023-08-09:
    - added 'BRAHMS' flag, allows integration of BRAHMS extract data
        included slight modifications to name standardisation, recurring NA vs NA-string 
