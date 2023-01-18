# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Clean coordinates, part of RECORDCLEANER
# 2022-01-18: sjs
#
# using options parsed from the bash launch script
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

rm(list = ls())
package_list <- c('optparse',
                  'CoordinateCleaner') # or geocodeR etc.
not_installed <- package_list[!(package_list %in% installed.packages()[ , "Package"])]    # Extract not installed packages
if(length(not_installed)) install.packages(not_installed)                               # Install not installed packages
sapply(package_list, require, character.only=TRUE) # load all packages


parser <- OptionParser()
parser <- add_option(parser, c("-v", "--verbose"), action="store_true", 
                     default=TRUE, help="Print extra output [default]")
parser <- add_option(parser, c("--input"), type="string", 
                     help="Input file for coordinate check")
parser <- add_option(parser, "--output", type='string',
                     help='Output file name')
parse_args(parser)




print('R working!')
q()



optparse

and call as > Rscript script.R options
