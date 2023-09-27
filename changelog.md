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

# 2023-08-22:
    - added basic scripts for SQL integration
    - finalised brahms integration

# 2023-08-23:
    - merged branch brahms integration
    - new branch: speed up master merging

# 2023-09-13:
    - new branch deduplication error solving (coordinates);
        * [solved] discovered bug in deduplication leading to data getting lost in groupby (groupby on a column including only NA make data go away)
        * [solved]bug in (pure) colnum not extracting correctly for the format 00-123 or similar...
        * [BUG] coordinate filter (difference between 0.1 mean, otherwise throw out, not working)

# 2023-09-18:
    - coordinate problems worked out. now coordinates more than 0.1 variance are tested for country-coordinate match and if match then kept...

# 2023-09-25:
    - [SOLVED] coordinates now filter eventual duplicates with coordinates with a variance > 0.1

# 2023-09-26/27:
    - cleaned up superfluous files, deprecated scripts...
    - merging branch coordinate debug into master