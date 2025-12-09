#
# SURFACE WATER DELIVERY/DIVERSION PROCESSOR
#

# load functions and data from sources
a <- round(Sys.time())
print(paste("SURFACE WATER DELIVERY/DIVERSION PROCESSING. Loading source files. Start:", a))

source("data_defs.R")

print("Processing surface water diversion subset data")
# Set paths for requisite delivery shapefiles and output file
PODs_shp <- "../KlamathARC/GIS_Files/Subsetted_shps/Phase1_2_Subsetted_pods.shp"
streams_shp <- "../KlamathARC/GIS_Files/SW_diversion_shapes/Stream Network Clean V6.shp"
modoc_path <- "../KlamathARC/GIS_Files/SubsetScale_20250620/Modoc_diversion_estimates.csv"

# output paths
title <- "sw_diversions"
folder <- "sw_diversions/"
#dir_rch_path <- paste0(rootz,folder, subf) 
swd_figpath <- paste0(figpath, "SW_diversion_points-snapped.png")
swd_forcing <- paste0(rootz,folder,title,".csv")
shppath <- paste0(rootz,folder, "/shapefiles/")
dir.create(shppath, recursive = T, showWarnings = F)

# Read POD shapefile and major stream shapefile

PODs <- st_read(PODs_shp, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  filter(!st_is_empty(geometry)) %>% 
  filter(POD_ID == "131681" & Subset == "SouthMarsh" |
           POD_ID == "56328" & Subset == "WoodRiver3" |
           POD_ID == "61352" & Subset == "SouthForkSprague" |
           POD_ID %out% c("131681", "56328", "61352")) %>% 
  select(Subset, POD_ID, geometry)

streams <- st_read(streams_shp, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  filter(!st_is_empty(geometry))

# Snap PODs to nearest stream point
PODsnap <- PODs %>% 
  cbind(streams[st_nearest_feature(PODs, streams),]) %>% 
  rowwise() %>% 
  mutate(line2point = st_nearest_points(geometry, geometry.1),
         closest_pt = st_cast(line2point, 'POINT')[2],
         distance = st_distance(geometry, closest_pt)[,1]) %>% 
  mutate(orig_pt = geometry) %>% 
  st_drop_geometry() %>% 
  select(Subset, POD_ID, StreamName, orig_pt, closest_pt, line2point, distance) %>% 
  st_as_sf(sf_column_name = "closest_pt")

# Transform data into 3 month chunks of diversion values by Subset
PODsum <- st_drop_geometry(PODs) %>%
  filter(POD_ID != "Modoc1" & POD_ID != "Modoc2") %>% 
  group_by(Subset) %>% 
  summarise(n = n())

swdat <- dat %>% 
  mutate(days = days_in_month(Date)) %>% 
  group_by(Subset, chunk, HUC8) %>% 
  summarise(Qtot = sum(Q), # OR SUM OF SW; DETERMINE BEST OPTION; SW IS 75% OF Q.
            days = sum(days)) %>% 
  arrange(HUC8, Subset, chunk) %>% 
  left_join(PODsum) %>% 
  mutate(Q_CFS = AFconv(Qtot,days),
         Q_frac = Q_CFS/n) %>% 
  ungroup()

# Calculate SW diversion volumes for each diversion point by evenly divvying Subset totals

swdat_wide <- data.table(SP = swdat$chunk) %>% 
  distinct(SP)

for (i in PODs$POD_ID) {
  
  POD_dat <- st_drop_geometry(PODs) %>% 
    as.data.table() %>% 
    filter(POD_ID == i) %>% 
    select(Subset, POD_ID) %>% 
    left_join(select(swdat, Subset, chunk, Q_frac), by = "Subset") %>% 
    select(Q_frac) %>% 
    setnames("Q_frac", i)
    
  swdat_wide <- swdat_wide %>% 
    cbind(POD_dat)
  
}

rm(POD_dat, i)

# Bind manually-loaded Modoc1 and Modoc2 data to swdat
modoc_dat <- read.csv(modoc_path) %>% 
  select(date = Date, POD_ID, Q = POD_div_est_AF) %>% 
  mutate(date = as.Date(date),
         days = days_in_month(date),
         chunk = chunkz(date)) %>% 
  group_by(chunk, POD_ID) %>% 
  summarise(Qtot = sum(Q),
            days = sum(days)) %>% 
  mutate(Q_CFS = AFconv(Qtot,days)) %>% 
  ungroup() %>% 
  as.data.table() %>% 
  dcast(chunk ~ POD_ID, value.var = "Q_CFS") %>% 
  mutate(across(everything(), ~replace(., is.na(.), 0))) %>% 
  select(-1)
  
swdat_wide <- swdat_wide %>% 
  select(-Modoc1, -Modoc2) %>% 
  cbind(modoc_dat)

# deletes any POD that is 0 for all periods
swdat_wide <- swdat_wide %>%
  select(where(~ !all(. == 0)))

PODs <- PODs %>% 
  filter(POD_ID %in% names(swdat_wide))

PODsnap <- PODsnap %>% 
  filter(POD_ID %in% names(swdat_wide))

# write everything to file including figure
write.csv(swdat_wide, paste0(rootz,folder,"diversion_data.csv"), row.names = F)

st_write(PODs, paste0(shppath, "PODs_original.shp"), delete_layer = T)
Zip_Files <- list.files(path = shppath, pattern = "PODs_original*", full.names = T)
zip(zipfile = paste0(shppath, 'PODs_original'), files = Zip_Files)

st_write(PODsnap, paste0(shppath, "PODs_snapped.shp"), delete_layer = T)
Zip_Files <- list.files(path = shppath, pattern = "PODs_snapped*", full.names = T)
zip(zipfile = paste0(shppath, 'PODs_snapped'), files = Zip_Files)

rm(Zip_Files)

swd_plot <- ggplot() +
  geom_sf(data = bounds, aes(fill = "Model boundary"), color = "black", linewidth = 0.5) +
  geom_sf(data = streams, aes(color = "Streams")) +
  geom_sf(data = PODs, aes(color = "PODs - original"), size = 3) +
  geom_sf(data = PODsnap, aes(color = "PODs - snapped"), shape = 1, size = 3) +
  geom_sf(data = PODsnap$line2point, linetype = 'dotted') +
  scale_fill_manual(name = NULL, values = "gray95") +
  scale_color_manual(name = NULL, values = c("brown1", "black", "cyan4")) +
  theme_bw() #+
  #coord_sf(xlim = c(-122.55, -120.5), ylim = c(41.45, 43.015), expand = F) 
swd_plot
ggsave(plot = swd_plot, filename = swd_figpath, width = 10, height = 10, create.dir = T)

# Save R script to folder
if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/SW_diversions.R", to = rootzC, overwrite = T)
}

print("SW diversions complete. Total runtime:")
print(round(Sys.time()) - a)
