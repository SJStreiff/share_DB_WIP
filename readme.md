---

---

# Recordcleaner

## The/A PIPELINE for standardising occurrence records

![There would be a funny picture here normally](TMP_titleimage.png "")


To launch recordcleaner, you can use the useful bash launcher, in terminal in your directory:

```
chmod +x recordcleaner.sh
./recordcleaner.sh
```

This handy script let's you specify your input and output options, as well as other important parameters.
Alternatively, the script can be called directly in the command line with the '-h' flag, which lets a user know exactly which parameters are required and which are optional.

```
python ./3_scripts/recordcleaner.py -h

RECORDCLEANER takes your raw occurence records and compares them to an existing database if you have it, cleaning up column names, removing duplicates and making it more pleasing in general

positional arguments:
  input_file            Raw input file path
  {GBIF,P}              File format. So far I can only handle Darwin core (GBIF) or herbonautes (P)
  working_directory     the directory in which to deposit all intermediate working files. Files that need reviewing start with "TO_CHECK_"
  output_directory      the wished output directory
  prefix                prefix for ouput filenames

optional arguments:
  -h, --help            show this help message and exit
--nonamecln NONAMECLN
                       if specified, collector names are expected in the format <Surname>, <F>irstname, or if deviating standardised to be identical across all datasets.

If it doesn't work, or you would like to chat, feel free to contact serafin.streiff@ird.fr
```

## Working with RECORDCLEANER

### Input files

So far, the pipeline only takes input file in either the "darwin core format", as for example found in GBIF data, or the format used at P for their herbonautes metadata collection projects.
GBIF raw data downloads are in tab-separated tables (csv), and herbonautes data is separated by semicolons ';'. For advanced users, or maybe for me further down the line, one might change the columns to subset. These are specified in the file "./3_scripts/z_dependencies.py".

**more to come**

### Working directory

The working directory is specified, as some data might be non-conforming to the standardisation steps, and therefore not filterable with some steps of the pipeline. However, a lot of this might be still useable. Therefore it is written into the working directory for manual editing of the steps that cannot be done automatically. In this case, I will try to implement an automatic pause in the program that allows the user to edit the problematic data manually and then reinsert it into the pipeline.

For example, when standardising collector names (which is crucial for detecting duplicates in subsequent steps), I cannot handle names that are not in the format of some firstname, any middle names and some surname. E.g. if collections are filed under a program name (i.e. in SE-Asia, herbarium specimens are frequently labelled and numbered as something like "Forest Research Institute (FRI)", which I haven't so far been able to cleanly filter). Therefore it is faster to manually cross check these for consistency within dataset, and then I can reinsert them with no problem).
During the name standardisation step I output the concerned records to a temporary file, and after this step I plan to let the user know than one can edit the records before reinserting them and continuing...


## What does record cleaner really do?

RECORDCLEANER goes through a few iterative step, which I briefly expain here.

* Step A:
  * A1: Standardise column names, remove unwanted columns and add empty columns we need later
  * A2: Standardise data within some columns, e.g. separate all dates into separate columns day, month and year, make sure all barcodes have the institution leading before the number, have the first collector in a separate column,
  * A3: Standardise collector names to e.g. *Streiff, SJR*, instead of *Serafin J. R. Streiff* in one record and *Streiff, Serafin* in another record.

* Step B:
  * B1: run some statistics on duplicates in the dataset.
  * B2 remove said duplicates.

* Somewhere Step X:
  * Check taxonomy for accuracy, and update if necessary. At the moment this is done by cross checking with POWO (powo.kew.org), which for Annonaceae we can update relatively easily. With other families this is not guaranteed, but changes can always be pushed by making the curators of powo aware of taxonomic changes that were published.
  * Check coordinates. Probably we just check for country centroids and points in the water. This will be done with already available packages, and issues flagged for potential correction in e.g. QGIS (qgis.org)


## TODO

* Keep readme uptodate with new developments and changes
* add overwrite protection in case script is called multiple times, at least for time intensive steps (removed for debugging!!) --> done as mode='x' for example within pd.to_csv()
* Barcodes error in GBIF input! (issue dealing with "-" within barcode.)
* implement problem name output and reinsertion, optionally pausing execution
* when do we query POWO/IPNI??
* quantify fast and slow steps?
* Implement background files:
  * indets and similar
  * master distribution database for integration.








#
