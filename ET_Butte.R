#
# BUTTE RASTER ET PROCESSOR
# this is a separate script because the Butte ET data is a completely different raster/TIF format
#

# load functions and data from sources
a <- round(Sys.time())
print(paste("EVAPOTRANSPIRATION *BUTTE* PROCESSING. Loading source files. Start:", a))

source("data_defs.R")

print("Processing ETB raster data")
ETB_path <- "../KlamathARC/GIS_Files/ET/2025-07/Butte/update_2025/seasonal_etg/"
ETBdatfiles <-  list.files(ETB_path, full.names = T)

# output paths
title1 <- "butte_et"
folder <- "ET/"
tfrpath <- paste0(rootz,folder,title1,".tfr")
ETB_figpath <- paste0(figpath, "ET_butte_coverage.png")

dir.create(paste0(rootz,folder), recursive = T, showWarnings = F)
dir.create(paste0(rootz,folder,title1,"_input"), recursive = T, showWarnings = F)

# make dummy datasets for missing 1980-1983 data
#
# CANNOT RUN THIS MORE THAN ONCE, OR ELSE.
#  SERIOUSLY, ONLY RUN THIS IF YOU ARE RE-PULLING FILES FROM DRI/USBR SERVER. THIS MAKES DUMMY FILES 
#   TO FILL OUT MISSING DATA FOR 1980 TO 1983 THAT DOES NOT EXIST. IF THIS IS RUN ON EXISTING FILES, IT
#    WILL BE BAD.
#
# copier <- ETBdatfiles[1:4]
# 
# file.copy(from = copier[4], to = paste0(ETB_path, "butte_valley_18010205_1980_water_year_Oct_Dec_etg_mean_mm.tif"))
# 
# for (i in 1:4) {
#   months <- substr(copier[i], 105,111)
#   for (year in 1981:1983) {
#     file.copy(from = copier[i], to = paste0(ETB_path, "butte_valley_18010205_",year,"_water_year_",months,"_etg_mean_mm.tif"))
#   }
# }
#
#RERUN ETBdatfiles to get new file list, and forget 161
ETBdatfiles <- list.files(ETB_path, full.names = T)[-161]

# Read in Butte ET data from files
# First, conditionally set up dated column names for each time chunk
ETBnames <- data.frame(year = as.numeric(substr(list.files(ETB_path),23,26)),
                      quarter = substr(list.files(ETB_path),39,45)) %>% 
  mutate(qval = ifelse(substr(quarter,1,3) == "Jan", 1, ifelse(
    substr(quarter,1,3) == "Apr", 2, ifelse(
      substr(quarter,1,3) == "Jul", 3, ifelse(
        substr(quarter,1,3) == "Oct", 4, NA))))) %>% 
  select(-quarter) %>% 
  mutate(chunk = paste0(year, "-", qval),
         days = ifelse(qval == 2, 91, ifelse(
           qval == 3 | qval == 4, 92, ifelse(
             qval == 1 & year %% 4 == 0, 91, 90)))) %>% 
  filter(chunk != "2020-4")

# Then pull in raw raster TIF data using rast package
ETBrast <- lapply(ETBdatfiles, rast)

# And filter model grid so it matches Butte area to minimize computing time of raster conversion
ETBgrid <- filter(grid, between(ROW, 210, 227) & between(COLUMN, 39, 63)) %>% 
  #mutate(SEQNUM = SEQNUM - 43928) %>% 
  select(-Shape_Leng)

# Now, convert raster data to numeric using 'extract' from 'terra' package. Still very slow even
#  with minimizing grid

b <- round(Sys.time())

print(paste("Converting ET Butte raster info to numerical data (slow); start:", b))

ETBdat <- ETBrast %>% 
  lapply(function(x) {
    terra::extract(x, ETBgrid, fun = "mean", na.rm = TRUE) %>% 
      select(ID, VAL = 2) %>% 
      mutate(across(everything(), ~ifelse(is.nan(.), 0, .))) %>% 
      cbind(ETBgrid) %>% 
      mutate(VAL_ft = VAL/304.8) 
  })

print(paste("Complete.", format(round(Sys.time() - b, digits = 1))))

# Rename numeric dataframe with date steps
names(ETBdat) <- ETBnames$chunk
ETBdat = ETBdat[order(names(ETBdat))]
ETBnames <- ETBnames %>% 
  arrange(chunk)

# Write a function specifically for Butte raster data to convert resultant numeric data to 
# MODFLOW readable input

butter <- function(data, namer, days, folder, title, count) {
  print(paste("Generated",count,"of",length(ETBdat),"direct recharge files"))
  
  trinum <- str_pad(count, 3, pad = "0")
  filenm <- paste0(title, "_", trinum, ".txt")
  newpath <- paste0(rootz, folder, title, "_input/", filenm)
  tfr_pathnm <- paste0(title, "_input/", filenm)
  
  results <- grid %>% 
    st_drop_geometry() %>% 
    select(SEQNUM, ROW, COLUMN) %>% 
    left_join(select(data, SEQNUM, VAL_ft), by = join_by(SEQNUM)) %>% 
    mutate(VAL_ft = VAL_ft/days/86400) %>% 
    mutate(value = ifelse(is.na(VAL_ft), 0, VAL_ft)) %>% 
    as.data.table() %>% 
    dcast(ROW ~ COLUMN, value.var = "value") %>%
    select(-ROW)

  # write empty first line comment, and then subsequent data by model cell
  write(x = paste0("# SP ", trinum, "    ", namer), append = F, file = newpath, sep = " ")
  write.table(results, file = newpath, append = T, sep = " ", row.names = F, col.names = F,
              quote = F)
  
  fileConn <- file(tfrpath, "a")
  
  if(count == 1) {writeLines(paste0("# Klamath Basin FMP Transient File Reader from ",
                                   title,
                                   " in feet per second with model cell area 6250000.0 ft2"),
                        fileConn)}
  
  writeLines(paste0("OPEN/CLOSE  ", tfr_pathnm, "  SF 1.0   # SP ", trinum, "    ", namer),
              fileConn)

  close(fileConn)
  return(results)
}

# And now, process data using function above, first deleting any existing .TFR file for Butte
unlink(tfrpath)

ETBlist <- lapply(seq_along(ETBdat), function(i) {
  butter(
    ETBdat[[i]],
    ETBnames$chunk[i],
    ETBnames$days[i],
    folder,
    title1,
    i
  )
})

# Plot up Butte data, zooming into area, and save to file
ETB_plot <- ggplot() +
  geom_sf(data = bounds, fill = "gray95", color = "black", linewidth = 0.5) +
  geom_spatraster(data = ETBrast[[1]]) +
  geom_sf(data = fields, aes(color = "Ag field boundaries"), fill = NA) +
  scale_color_manual(values = "red") +
  scale_fill_continuous("Evapotranspiration (ft/s)") +
  theme_bw() +
  coord_sf(xlim = c(-122.3, -121.75), ylim = c(41.75, 42), expand = F) 
ETB_plot

ggsave(plot = ETB_plot, filename = ETB_figpath, width = 10, height = 10, create.dir = T)

# Save script to file
if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/ET_Butte.R", to = rootzC, overwrite = T)
}
