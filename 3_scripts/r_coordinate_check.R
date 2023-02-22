# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Clean coordinates, part of RECORDCLEANER
# 2022-01-18: sjs
#
# using options parsed from the bash launch script
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

rm(list = ls()) # just making sure the environment is clean

package_list <- c('optparse',
                  'rnaturalearthdata',
                  'CoordinateCleaner') # or geocodeR etc.; ShinyCCleaner is not findable yet... Unpublished

# If for any reason the required packages are not installed, this nifty few lines will do this automatically
not_installed <- package_list[!(package_list %in% installed.packages()[ , "Package"])]    # Extract not installed packages
if(length(not_installed)) install.packages(not_installed)                               # Install not installed packages

# and load the packages
sapply(package_list, require, character.only=TRUE) # load all packages

# argument parsing to launch from command line or bash launch script
option_list <- list(
  make_option(c("-i", "--input"), type="character", default=FALSE,
              dest="input_name", help="Filepath of input file"),
  make_option(c("-o", "--output"),type="character", default=FALSE,
              dest = "output_file", help="Output file path including prefix")
              )

# opt is the variable holding the arguments
opt <- parse_args(OptionParser(option_list=option_list), positional_arguments = TRUE)

# in and outfiles
inputfile  <- opt$options$input_name
out_file <- opt$options$output_file


#print('R working!')
# some stats for the log file
print('Working directory:')
getwd()
print('Inputfile:')
print(opt$options$input)

#debugging
# dat <- read.csv('~/Sync/1_Annonaceae/share_DB_WIP/2_data_out/G_ID2_cleaned.csv', sep =';', head=T)

# read the csv data
dat <- read.csv(inputfile, header = TRUE, sep = ';')
dat <- data.frame(dat)  # checking

dat <- dat[!is.na(dat$ddlong),] # checking that all records have coordinates...
no_coord_dat <- dat[is.na(dat$ddlong),] # subsetting coords with no coordinate value



flags <- clean_coordinates(x = dat,
                           lon = "ddlong",
                           lat = "ddlat",
                           species = 'scientific_name',
                           countries = "country_iso3",
                           tests = c("capitals", "centroids", "equal","gbif", "institutions",
                                     "zeros", "countries" ))
# and send the file out again for integrating into database
'%notin%' <-  Negate('%in%')
newcols <- which(colnames(flags) %notin% colnames(dat))
# print(newcols)
# keep the last column (that is << .summary >>)
newcols <- newcols[-length(newcols)]
#flags[,newcols] <- !flags[,newcols] # so that all the problematic values are 

for(j in newcols){
  print(names(flags[j])) # working
  w <- which(flags[,j]=="FALSE")
  print(w)
  flags[,j] <- as.character(NA)
  flags[w, j] <- names(flags[j])
}


# make a new column with all issues collated together... in one cell.
geo_issues <- tidyr::unite(flags, geo_issues, any_of(newcols), sep = ',', na.rm = TRUE)
geo_issues <- geo_issues[,-geo_issues$.summary] # drop the summary col as we have all the info we need in the geo_issue column

write.table(geo_issues, file = out_file, row.names = FALSE, sep=';')

print(paste('Annotated coordinates are written to', out_file))

# end
