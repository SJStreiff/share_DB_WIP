---

---

# Recordcleaner and Filer

This pipeline consists (at the moment) of two more or less independent steps:
* RECORDCLEANER: This step takes raw occurrence records and cleans them to a somewhat acceptable standard. The main feature here is that we merge duplicate collections, while retaining all the information of the different iso-specimens.
* RECORD-FILER: This step integrates the data cleaned in RECORDCLEANER into a specified database (might at some point be automatically into a postgres/SQL database).

## Installation

In your terminal window navigate to your folder with the enclosed *'environment.yml'* file. Once there, execute the command *conda env create -f environment.yml*.

More details to come.

## The/A PIPELINE for standardising occurrence records: RECORDCLEANER

<!-- ![There would be a funny picture here normally](TMP_titleimage.png "") -->


To launch recordcleaner, you can use the useful bash launcher, in terminal in your directory:

```
# chmod +x launch.sh
./launch.sh
```

This handy script let's you specify your input and output options, as well as other important parameters.
Alternatively, the script can be called directly in the command line with the '-h' flag, which lets a user know exactly which parameters are required and which are optional.

```
> python ./3_scripts/recordcleaner.py -h

RECORDCLEANER takes your raw occurence records and compares them to an existing database if you have it, cleaning up column names, removing duplicates and making it more pleasing in general

positional arguments:
  input_file            Raw input file path
  {GBIF,P}              File format. So far I can only handle Darwin core (GBIF) or herbonautes (P)
  {EXP, NO}             Input file is expert file or not (only arguments {EXP, NO} are accepted)
  working_directory     the directory in which to deposit all intermediate working files. Files that need reviewing start with "TO_CHECK_"
  output_directory      the wished output directory
  prefix                prefix for ouput filenames

optional arguments:
  -h, --help            show this help message and exit
--nonamecln NONAMECLN
                       if specified, collector names are expected in the format <Surname>, <F>irstname, or if deviating standardised to be identical across all datasets.

If it doesn't work, or you would like to chat, feel free to contact serafin.streiff<at>ird.fr
```

## Working with RECORDCLEANER

### Input files

So far, the pipeline only takes input file in either the "darwin core format", as for example found in GBIF data, or the format used at P for their herbonautes metadata collection projects.
GBIF raw data downloads are in tab-separated tables (csv), and herbonautes data is separated by semicolons ';'. For advanced users, or maybe for me further down the line, one might change the columns to subset. These are specified in the file "./3_scripts/z_dependencies.py". **It is crucial that both the columns and their associated datatype are correctly specified there.** Failure to do so will create problems when working with the data.

**more to come**

### Working directory

The working directory is specified, as some data might be non-conforming to the standardisation steps, and therefore not filterable with some steps of the pipeline. However, a lot of this might be still useable. Therefore it is written into the working directory for manual editing of the steps that cannot be done automatically. In this case, I will try to implement an automatic pause in the program that allows the user to edit the problematic data manually and then reinsert it into the pipeline.

For example, when standardising collector names (which is crucial for detecting duplicates in subsequent steps), I cannot handle names that are not in the format of some firstname, any middle names and some surname. E.g. if collections are filed under a program name (i.e. in SE-Asia, herbarium specimens are frequently labelled and numbered as something similar to "Forest Research Institute (FRI)", which I haven't so far been able to cleanly filter). Therefore it is faster to manually cross check these for consistency within dataset, and then I  reinsert them with no problem).
During the name standardisation step I output the concerned records to a temporary file, and after this step I plan to let the user know than one can edit the records before reinserting them and continuing...


## What does record cleaner really do?

RECORDCLEANER goes through a few iterative step, which I briefly expain here.

* Step A:
  * A1: Standardise column names, remove unwanted columns and add empty columns we need later
    Note that as the postgreSQL database columns make all capital letters to small, I have changed this accordingly in the preprocessing
  * A2: Standardise data within some columns, e.g. separate all dates into separate columns day, month and year, make sure all barcodes have the institution leading before the number, have the first collector in a separate column,
  * A3: Standardise collector names to  *Streiff, SJR*, instead of *Serafin J. R. Streiff* in one record and *Streiff, Serafin* in another record
  * A4:  Standardise collector names even more by querying the Harvard Univ. Herbarium botanists [https://kiki.huh.harvard.edu/databases/botanist_index.html] database to get full or normalised names with a link to the record in that database (and wikipedia links for very famous botanists...). Names that are not found in that database are returned in the same format as the regex names (i.e. *Streiff, SJR*), whereas successfully found names are either returned as full names (*Surname, Firstname Any Middlename*) or abbreviated names (*Surname, F. A. M.*). The HUH database query is performed only with the surname, and results filtered with the original label data provided by the input data.

* Step B:
  * B1: run some statistics on duplicates in the dataset. Select and remove all records where the collection number is a variation of <<s.n.>>. These are then treated separately to the others with other criteria to find duplicated specimens.
  * B2: remove said duplicates. The duplicates are at the moment identified by the following criteria: If the collector name (mix of HUH where possible, and regex where not), number, number sufix and year are identical, they are flagged as duplicates.
  Note that records with no collection number (i.e. *s.n.*) are treated separately (B3) 
  * B3: remove duplicates from separated *s.n.* -records. Here more other values are taken into consideration,  the combination of Surname, Collection Year, Month and Day and Genus + specific_epithet are used to identify duplicates. This leads to errors, but in my humble opinion it's better than nothing.
  * B4: Crossfill country full name and ISO codes (needed later on, and not always complete.)

* Step C:
  * Check taxonomy for accurracy, and update if necessary. At the moment this is done by cross checking with POWO (powo.kew.org), which for Annonaceae we can update relatively easily within the framework of our project collaborators. With other families the situation might be different, but changes can always be pushed by making the curators of POWO aware of published taxonomic changes that aren't up to date.
  * Check coordinates. Probably we just check for country centroids and points in the water. This will be done with already available packages, and issues flagged for potential correction in e.g. QGIS (qgis.org)
  This process is invoked as a separate step in R, as the packages available there are more used and robust (and I am more familiar with them). For the moment we implemented an automatic CoordinateCleaner (https://ropensci.github.io/CoordinateCleaner/index.html)

The resulting data of these cleaning steps are hte following files:
* FILENAME_cleaned.csv: final output from python processing (up to, including Step C1)
* FILENAME_spatialvalid.csv: final output with spatialvalid tag from *CoordinateCleaner*. Final data for import into database.
* FILENAME_indet.csv: final cleaned data with indet at any level. This is later added to the indet section of database.
* FILENAME_nocoords.csv: final cleaned data without coordinates (or spatial invalid??). This is added to Non-spatialvalid section of database.

RECORD-FILER then goes and takes freshly (or even old) cleaned data and tries to integrate it into a pre-existing database

* RECORD-FILER:
  * Merge newly cleaned data with the database. Before the actual merging, I check for duplicates and merge these 
  * First all databases are read. These include two backlog datasets, the *indet*-backlog and the *no-coordinate*-backlog, as well as the master database. Backups of these are saved with a timestamp, so changes can be reverted.
  * With barcode priority, duplicates are checked and merged identically to the cleaning steps outlined above. If duplicates are found to with new data and the backlogs, these records are integrated into the new master version.
  * Finally the newly modified backlogs and master database are saved.




## Options

### Expert flag

CAUTION: this setting gives new data more priority than previous data. Therefore, make sure this data is clean, validated and doesn't include missing data!

If the EXP=EXP in the config file, then the pipeline expects this dataset to be completely expert determined and curated. The values here are given precedence over duplicates found in the master database. The main utility of using this flag is to integrate this data to update any determinations and georeferencing.




## TODO

* Keep readme uptodate with new developments and changes
* **add overwrite protection in case script is called multiple times**, at least for time intensive steps (removed for debugging!!) --> done as mode='x' for example within pd.to_csv()
* **DONE**: RETURN interactive input(), removed for scripting...
* **DONE**: Barcodes error in GBIF input! (issue dealing with "-" within barcode.)
* **DONE** - implement problem name output and reinsertion, optionally pausing execution
* **DONE** when do we query POWO/IPNI?? irrelevant in my opinion. We do it before inserting a new bunch of data into the master database. 
* quantify fast and slow steps and make backup files between, so we can restart at that step (maybe integrate variable to call analysis from step XYZ)
* **DONE** make distinction between non-spatialvalid and no coordinate data!! then we can revise non-spatialvalid points in QGIS

* **Implement background files**:
  * **DONE**  indets and similar: before integrating data, check for data previosly set aside because we had no conclusive data...?

  * **DONE**  master distribution database for integration.




#
