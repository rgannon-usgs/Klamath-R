#
# STARTUP FILE FOR KLAMATH DATA PROCESSING
#  - loads required packages, sets variables, pulls global shapefiles and datasets
#
print("Loading required packages and source files")
# load these packages
require(terra)
require(tidyterra)
require(plyr)
require(tidyverse)
require(sf)
require(data.table)
require(lubridate)
require(plotly)
require(elevatr)
require(progress)

# DEFINE VARIABLES AND ABSOLUTE PATHS:
print("Defining variables, paths, and column names for data tables")
  # Set global crs
crsnad <- 4269
crsutm <- 32610

  # set paths for general data loading
model_path <- "../KlamathARC/GIS_Files/model_grid/klamath_mf_grid_epsg26910_nad83_utm_10n.shp"
bound_path <- "../KlamathARC/GIS_Files/Domain/MODFLOW_domain_active.shp"
fields_shp_path <- "../KlamathARC/GIS_Files/Subsetted_shps/Phase1_2_Subsetted_fields.shp"
fields_path <- "../KlamathARC/GIS_Files/FieldScale_20250620/"
fields_zips <- paste0(fields_path, list.files(path = fields_path, full.names = F, recursive = F,
                                              pattern = "*.zip"))
subset_path <- "../KlamathARC/GIS_Files/SubsetScale_20250620/"
subset_zips <- paste0(subset_path, list.files(path = subset_path, full.names = F, recursive = F,
                                              pattern = "*.zip"))
intersect_path <- "../KlamathARC/Field_Grid_Intersects.shp"

# set root output directory if I want to change where MF input is sent to
rootz <- paste0("C:/Users/rgannon/OneDrive - DOI/Klamath-RG/MF_input/",Sys.Date(), "/")
rootzC <- paste0(rootz, "/.code/")
if(!dir.exists(rootzC)) {dir.create(rootzC, recursive = T, showWarnings = F)}
figpath <- paste0(rootz, "_Figures/")

# loads functions
source("klamath_functions.R")

  # set column names for various datasets for robustness
grid_names <- c(SEQNUM = "SEQNUM",
                ROW = "ROW",
                COLUMN = "COLUMN_")

dat_names <- c(ET = "et_irr_AF",
               IRR = "Applied_irrigation_AF",
               RO = "Irr_runoff_AF",
               Q = "Qin_AF",
               SW = "Estimated_SWdeliv_AF",
               SEEP = "Canal_seepage_AF",
               PUMP = "Estimated_pumping_AF")

# LOAD GIS/DATA AND SOME MINOR DATA MODIFICATIONS
print("Loading GIS coverages")

if (exists("bounds")) {
  print("GIS coverages already imported. Skipping.")
} else {
  # model boundary
  bounds <- st_read(bound_path, quiet = T) %>%
    st_transform(crs = crsnad)

  # ag field extents
  fields <- st_read(fields_shp_path, quiet = T) %>%
    st_transform(crs = crsnad)

  # model grid
  grid <- st_read(model_path, quiet = T) %>% 
    st_transform(crs = crsnad) %>% 
    filter(!st_is_empty(geometry)) %>% 
    rename(!!grid_names)

  # ag field intersections with model grid (too slow to run in R via sf)
  field_intersects <- st_read(intersect_path, quiet = T) %>% 
    st_transform(crs = crsnad)
}

  # Load all csv data into a single table
options(datatable.fread.input.cmd.message=FALSE)

  # define data import function
datprocessor <- function(zip_vector) {
  f <- function(zipfile) {
    fls <- unzip(zipfile, list = TRUE)$Name
    lapply(fls, \(f) fread(unzip(zipfile, files = f, exdir = "temp"))) %>% 
      rbindlist(fill = TRUE) %>% 
    mutate(HUC8 = substr(str_extract(zipfile, "HUC8_20\\d"),6,8),
           chunk = chunkz(Date))
  }
  lapply(zip_vector, f) %>% 
    rbindlist(fill = TRUE)
}

#  process data import on vector of zip file names and output amount of processing time

if (exists("dat")) {
  print("SubsetScale already imported. Skipping.")
  } else {
    print("Compiling SubsetScale csv data from the 6 HUC8 zip files")
    begin <- round(Sys.time())
    dat <- datprocessor(subset_zips) %>% 
      rename(!!dat_names)
    end <- round(Sys.time())
    
    unlink("temp", recursive = T)

    print("Processing time:")
    print(end-begin)
  }

if (exists("datf")) {
  print("FieldScale already imported. Skipping.")
} else {
  print("Compiling FieldScale csv data from the 6 HUC8 zip files (this can take 30 min)")
  begin <- round(Sys.time())
  datf <- datprocessor(fields_zips) #%>% 
  #  rename(!!dat_names)
  end <- round(Sys.time())
  
  unlink("temp", recursive = T)
  
  print("Processing time:")
  print(end-begin)
}

  # this pulls all possible timeslices from existing data
chunker <- arrange(ungroup(dat),chunk)$chunk %>% 
  unique()  

if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/data_defs.R", to = rootzC, overwrite = T)
}

print("Complete")
