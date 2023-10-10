####################################################################################################
# Clean coordinates, part of RECORDCLEANER                                                         #
#                                                                                                  #
# Author: Serafin J R Streiff                                                                      #
# Date: 2023-01-18                                                                                 #    
# ------------------------------------------------------------------------------------------------ #
#     Modified 2023-06-26: added coastline point snapping below threshhold                         #    
#                                                                                                  #    
####################################################################################################
rm(list = ls()) # just making sure the environment is clean


print('#> C: Coordinate checking')
package_list <- c('optparse',
                  'rnaturalearthdata',
                  'rworldxtra',
                  'CoordinateCleaner',
                  'sf') # or geocodeR etc.; ShinyCCleaner is not findable yet... Unpublished

# If for any reason the required packages are not installed, this nifty few lines will do this automatically
not_installed <- package_list[!(package_list %in% installed.packages()[ , 
                                                            "Package"])]    # Extract not installed packages
if(length(not_installed)) install.packages(not_installed)                   # Install not installed packages

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


####################################################################################################
###---------------------- Some functions we need later on ---------------------------------------###

`%notin%` <- Negate(`%in%`) # handy function

get_dist <- function(x, y){
  # function to get distance to coastline for every point in df
  n <- nrow(x) # length of df
  a <- do.call(c,
               lapply(seq(n), function(i){
                 testvalues <- st_as_sf(x[i,]$geometry) # formatting to be on the safe side
                 test <- st_distance(testvalues, y) #get distance to all coastline points
                 return(min(test)) # minimum distance 
          }))
  return(a)
}


get_closest_coast = function(x, y){
  # get the points on y that are closest to points x
  n <- nrow(x)
  a <- do.call(c,
              lapply(seq(n), function(i){
              nrst <- st_nearest_points(st_geometry(x)[i], y)
              nrst_len <- st_length(nrst)
              nrst_mn <- which.min(nrst_len)
              ab <- st_cast(nrst[nrst_mn], "POINT")[2]
              return(ab)}))
  b <- st_coordinates(a)
  colnames(b) <- c('ddlong', 'ddlat')
  out_frame <- cbind(x, b)
  return(out_frame)
}

####################################################################################################
###---------------------- Read data and do coordinate check -------------------------------------###

#debugging test dataframe
 dat <- read.csv('~/Sync/1_Annonaceae/G_GLOBAL_distr_DB/2_final_data/20231009_rainbio_cleaned.csv', sep =';', head=T)

# read the csv data
dat <- read.csv(inputfile, header = TRUE, sep = ';')
dat <- data.frame(dat)  # checking


# keep all data in same dataframe. Sorted/filtered in database integration step

no_coord_dat <- dat[is.na(dat$ddlong),] # subsetting coords with no coordinate value
no_coord_dat <- rbind(no_coord_dat, dat[is.na(dat$ddlat),])
no_coord_dat <- unique(no_coord_dat)
if(length(no_coord_dat[,1]) != 0){
  no_coord_dat$geo_issues <- 'NA_coord'
}
dat <- dat[!is.na(dat$ddlong),] # checking that all records have coordinates...

# NA combine to same values
dat[dat == 'nan'] <- ''
dat[dat == '<NA>'] <- ''
dat[dat == '<NA> nan'] <- ''
#dat[dat == '0'] <- ''
dat[is.na(dat)] <- ''

#path to shapefile
# clines <- rgdal::readOGR(('/Users/serafin/Sync/1_Annonaceae/Y_DATA/2_land-map_rasters/ne_50m_coastline/ne_50m_coastline.shp') )
# clines <- clines %>% st_set_crs('WGS84') #double checking, should actually already be


test_coords <- clean_coordinates(x = dat,
                                 lon = "ddlong",
                                 lat = "ddlat",
                                 species = 'accepted_name',
                                 countries = "country_iso3",
                                 tests = c('equal', 'zeros'))

# coordinate cleaner 
flags <- clean_coordinates(x = dat,
                           lon = "ddlong",
                           lat = "ddlat",
                           species = 'accepted_name',
                           countries = "country_iso3",
                           tests = c("capitals", "centroids", "equal","gbif", "institutions", "seas",
                                     "zeros", "countries" ))#,
                           #seas_ref = clines)


####################################################################################################
###--------------------- START of coordinate saving from the seas -------------------------------###


clines <- read_sf('/Users/serafin/Sync/1_Annonaceae/Y_DATA/2_land-map_rasters/ne_50m_coastline/ne_50m_coastline.shp')
clines <- clines %>% st_set_crs('WGS84') #double checking, should actually already be

# subset data to correct, and data to flag as incorrect
flags_tt <- flags[flags$.sea == FALSE,]
# good data
flags_no_sea <-flags[flags$.sea == TRUE,]

if(length(flags_tt$.sea) > 0){

  # spatialise data
  dat_sf_tt <- flags_tt %>% st_as_sf(coords = c('ddlong','ddlat')) %>% 
    st_set_crs(4326)



  ####################################################################################################
  ###--------------------------------- Visualise problematic values -------------------------------###
  # x_max <- round(max(flags_tt$ddlong) + 3)
  # x_min <- round(min(flags_tt$ddlong) - 3)
  # y_max <- round(max(flags_tt$ddlat) + 3)
  # y_min <- round(min(flags_tt$ddlat) - 3)
  # 
  # data("countriesHigh")
  # mapdat    <- sf::st_as_sf(countriesHigh)
  # p1 <-  ggplot() +
  #   geom_sf(data = mapdat) +#, aes(x = long, y = lat)) +
  #   geom_sf(data = dat_sf_tt, colour = 'red')+
  #   coord_sf(xlim = c(x_min, x_max), ylim = c(y_min, y_max)) +
  #   theme(plot.background = element_rect(fill = 'white'),
  #         panel.background = element_rect(fill = "lightblue"),
  #         panel.grid = element_blank(),
  #         line = element_blank(),
  #         rect = element_blank(),
  #         axis.text.x = element_text(size = 7)) +
  #   labs(x='Longitude',y='Latitude')
  # p1
  ####################################################################################################



  ####################################################################################################
  ###--------------------------------- Calculate distance and points ------------------------------###

  # get minimum distance to coastline...
  dat_sf_tt$coast_2 <- as.numeric(get_dist(dat_sf_tt, clines))

  print(paste('This many points are in the sea:', length(dat_sf_tt$scientific_name)))
  # error margin, i.e. points less than this from the coast get a new point assigned
  error_margin <- 5000
  dat_tobesaved <- dat_sf_tt[dat_sf_tt$coast_2 <= error_margin,]
  print(paste('This many points were moved to the coastline:', length(dat_tobesaved$scientific_name),
              ', with the error margin of', error_margin, '[m]'))

  # save points closer to coast than <error_margin>
  if(length(dat_tobesaved$recorded_by) > 0){
    # to avoid errors when no data in sea
    dat_tobesaved <- get_closest_coast(dat_tobesaved, clines)
    old_coords <- st_coordinates(dat_tobesaved)
    colnames(old_coords) <- c('old_ddlong', 'old_ddlat')
    dat_tobesaved <- cbind(st_drop_geometry(dat_tobesaved), old_coords)
  
    dat_tobesaved$.sea <- TRUE #update values, no further action needed there later
  }else{
    dat_tobesaved <- st_drop_geometry(dat_tobesaved)
    }
  dat_inseas <- dat_sf_tt[dat_sf_tt$coast_2 > error_margin,]
  print(paste('The other', length(dat_inseas$recorded_by), 'values remain flagged as problematic coordinates.'))

  # add empty col for the old coordinates

  coords <- st_coordinates(dat_inseas)
  colnames(coords) <- c('ddlong', 'ddlat')
  dat_inseas <- cbind(st_drop_geometry(dat_inseas), coords)
  dat_inseas$old_ddlong <- rep(NA, length(dat_inseas$recorded_by))
  dat_inseas$old_ddlat <- rep(NA, length(dat_inseas$recorded_by))

  flags_no_sea$old_ddlong <- rep(NA, length(flags_no_sea$recorded_by))
  flags_no_sea$old_ddlat  <- rep(NA, length(flags_no_sea$recorded_by))

  # merge dataframes again
  dat_to_int <- rbind(dat_tobesaved, dat_inseas)
  col_to_drop <- which(colnames(dat_to_int) %notin% colnames(flags_no_sea))
  #print(colnames(dat_to_int)[col_to_drop])
  dat_to_int <- dat_to_int[,-col_to_drop]

  # final data from coordinate check
  flags_final <- rbind(flags_no_sea, dat_to_int)

  }else{
  flags_final <- flags_no_sea
  }
###--------------------- END of coordinate saving -----------------------------------------------###
####################################################################################################

# drop summary column
col_to_drop <- which(colnames(flags_final) == '.summary')
flags_final <- flags_final[,-col_to_drop]

# send the file out again for integrating into database
ref_colnames <- append(colnames(dat), c('old_ddlong', 'old_ddlat'))
newcols <- which(colnames(flags_final) %notin% ref_colnames)


####################################################################################################
###-------------------- make a column for the geo_issues flag -----------------------------------###

# TRUE/FALSE into str of what the issue is in each CoordinateCleaner output
for(j in newcols){
  print(names(flags_final[j])) # working
  w <- which(flags_final[,j]=="FALSE")
  print(w)
  flags_final[,j] <- as.character(NA)
  flags_final[w, j] <- names(flags_final[j])
}

# make a new column with all issues together in one cell.
geo_issues <- tidyr::unite(flags_final, geo_issues, any_of(newcols), sep = '-', na.rm = TRUE) 
# sep with '-' to keep ',' reserved for separating duplicates

no_coord_dat$old_ddlong <- rep(NA, length(no_coord_dat$scientific_name))
no_coord_dat$old_ddlat <- rep(NA, length(no_coord_dat$scientific_name))

# add empty coordinates back in. These are sorted later
geo_issues <- rbind(geo_issues, no_coord_dat)


####################################################################################################
###-------------------- some final issues being resolved from other steps -----------------------###

# remove nomenclatural expert info in indet spp
geo_issues[geo_issues$specific_epithet == '', "accepted_name"] <- ''
geo_issues[geo_issues$specific_epithet == '', "status"] <- ''
geo_issues[geo_issues$specific_epithet == '', "expert_det"] <- ''


####################################################################################################
###-------------------- Write ourput to file for furter processing ------------------------------###


print('writing file now')
write.table(geo_issues, file = out_file, row.names = FALSE, sep=';')

print(paste('Annotated coordinates are written to', out_file))
print('#> C: Coordinate checking - complete')


###------------------------------------- R section done -----------------------------------------###
####################################################################################################