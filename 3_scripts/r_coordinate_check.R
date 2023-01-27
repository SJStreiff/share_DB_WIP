# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Clean coordinates, part of RECORDCLEANER
# 2022-01-18: sjs
#
# using options parsed from the bash launch script
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

# read the csv data
dat <- read.csv(inputfile, header = TRUE, sep = ';')
dat <- data.frame(dat)  # checking

dat <- dat[!is.na(dat$ddlong),]
flags <- clean_coordinates(x = dat,
                           lon = "ddlong",
                           lat = "ddlat",
                           species = 'scientific_name',
                           countries = "country_iso3",
                           tests = c("capitals", "centroids", "equal","gbif", "institutions",
                                     "zeros", "countries" ))
# and send the file out again for integrating into database
write.table(flags, file = out_file, row.names = FALSE, sep=';')

print(paste('Annotated coordinates are written to', out_file))

# end
