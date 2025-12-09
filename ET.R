#
# ET PROCESSOR
#

# load functions and data from sources
a <- round(Sys.time())
print(paste("EVAPOTRANSPIRATION PROCESSING. Loading source files. Start:", a))

source("data_defs.R")

print("Processing ET field data")
# Set paths for ET shapefile and seepage output file
ET_root <- "../KlamathARC/GIS_Files/ET/2025-07/"

ET_shp_path <- paste0(ET_root,"Shapefiles/shallow_gw_areas_addition_klamath_upper_basin_2025.shp")
ET_shp_path2 <- paste0(ET_root,"Shapefiles/wetlands_addition_to_field_boundaries.shp")
ET_shp_intersect <- paste0(ET_root,"../shallow_GW_grid_intersect.shp")
ET_shp_intersect2 <- paste0(ET_root,"../wetlands_intersect.shp")
ET_path <- paste0(ET_root,"UKRB/shallow_gw_areas/")
ET_path2 <- paste0(ET_root,"UKRB/wetlands/")
ETdatfiles <- list.files(c(ET_path, ET_path2), full.names = T)

# output paths
title <- "gw_et"
title1 <- "shallow_gw_et"
title2 <- "wetlands_et"
folder <- "ET/"
#dir_rch_path <- paste0(rootz,folder, subf) 
ET_figpath <- paste0(figpath, "ET_coverage.png")

dir.create(paste0(rootz,folder), recursive = T, showWarnings = F)

# Read ET dat
ETdat <- lapply(ETdatfiles, read.csv) %>%
  rbindlist(fill = TRUE) %>% 
  mutate(ET = `ET.Volume..ac.ft.`,
         date = as.Date(date)) %>% 
  filter(date < "2020-10-01")

# Read ET shallow gw shapefile 
ET_area <- st_read(ET_shp_path, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  filter(!st_is_empty(geometry)) %>% 
  select(OPENET_ID, geometry) %>% 
  mutate(field_length = st_perimeter(.),
         field_area = st_area(.),
         type = "shallowGW")

ET_area2 <- st_read(ET_shp_path2, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  filter(!st_is_empty(geometry)) %>% 
  select(OPENET_ID, geometry) %>% 
  mutate(field_length = st_perimeter(.),
         field_area = st_area(.),
         type = "wetlands")

ET_areaC <- rbind(ET_area, ET_area2)

ET_int <- st_read(ET_shp_intersect, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  filter(!st_is_empty(geometry)) %>% 
  mutate(grid_length = Shape_Leng,
         grid_area = Shape_Area,
         intersect_length = Length2,
         intersect_area = Area2,
         type = "shallowGW") %>% 
  select(OPENET_ID, SEQNUM, ROW, COLUMN_, grid_length, grid_area, intersect_length, intersect_area, type, geometry) %>% 
  left_join(st_drop_geometry(ET_area)) %>% 
  mutate(field_frac = intersect_area/field_area,
         cell_frac = intersect_area/grid_area)

ET_int2 <- st_read(ET_shp_intersect2, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  filter(!st_is_empty(geometry)) %>% 
  mutate(grid_length = Shape_Leng,
         grid_area = median(ET_int$grid_area),
         intersect_length = Shape_Le_1,
         intersect_area = Shape_Area,
         type = "wetlands") %>% 
  select(OPENET_ID, SEQNUM, ROW, COLUMN_, grid_length, grid_area, intersect_length, intersect_area, type, geometry) %>% 
  left_join(st_drop_geometry(ET_area)) %>% 
  mutate(field_frac = intersect_area/field_area,
         cell_frac = intersect_area/grid_area)

ET_intC <- rbind(ET_int, ET_int2)

#rm(ET_area2, ET_int2)
# Transform data into 3 month chunks of ET by field area; join with intersection shapefile data,
# and modify down so each grid cell has an ET value from a given "field"

ETchunks <- ETdat %>% 
  select(Date = date, OPENET_ID, ET) %>% 
  mutate(days = days_in_month(Date),
         chunk = chunkz(Date)) %>% 
  group_by(OPENET_ID, chunk) %>% 
  summarise(ET_AF = sum(ET),
            days = sum(days)) %>% 
  arrange(OPENET_ID, chunk) %>% 
  left_join(st_drop_geometry(ET_intC), relationship = "many-to-many") %>% 
  mutate(ET_AF_frac = as.numeric(ET_AF*field_frac),
         ET_CFS_frac = AFconv(ET_AF_frac, days)) %>% 
  ungroup() %>% 
  group_by(SEQNUM, chunk) %>% 
  summarise(ET_input = sum(ET_CFS_frac)/6250000)

ETchunks1 <- ETdat %>% 
  filter(OPENET_ID %in% unique(ET_area$OPENET_ID)) %>% 
  select(Date = date, OPENET_ID, ET) %>% 
  mutate(days = days_in_month(Date),
         chunk = chunkz(Date)) %>% 
  group_by(OPENET_ID, chunk) %>% 
  summarise(ET_AF = sum(ET),
            days = sum(days)) %>% 
  arrange(OPENET_ID, chunk) %>% 
  left_join(st_drop_geometry(ET_int), relationship = "many-to-many") %>% 
  mutate(ET_AF_frac = as.numeric(ET_AF*field_frac),
         ET_CFS_frac = AFconv(ET_AF_frac, days)) %>% 
  ungroup() %>% 
  group_by(SEQNUM, chunk) %>% 
  summarise(ET_input = sum(ET_CFS_frac)/6250000)

ETchunks2 <- ETdat %>% 
  filter(OPENET_ID %in% unique(ET_area2$OPENET_ID)) %>% 
  select(Date = date, OPENET_ID, ET) %>% 
  mutate(days = days_in_month(Date),
         chunk = chunkz(Date)) %>% 
  group_by(OPENET_ID, chunk) %>% 
  summarise(ET_AF = sum(ET),
            days = sum(days)) %>% 
  arrange(OPENET_ID, chunk) %>% 
  left_join(st_drop_geometry(ET_int2), relationship = "many-to-many") %>% 
  mutate(ET_AF_frac = as.numeric(ET_AF*field_frac),
         ET_CFS_frac = AFconv(ET_AF_frac, days)) %>% 
  ungroup() %>% 
  group_by(SEQNUM, chunk) %>% 
  summarise(ET_input = sum(ET_CFS_frac)/6250000)


# Now run function to generate gridded recharge files and TFR call file

print("Writing ET TFR and direct recharge files")

# Generate "gw_et" for compilation purposes, NOT analysis
ETlist <- dr_grid(ETchunks, grid, folder, title)
unlink(paste0(rootz,"ET/gw_et.tfr"))
unlink(paste0(rootz,"ET/gw_et_input"), recursive = T)

# Or, comment out these 2 if we want combined wetlands/shallow_gw
ETlist1 <- dr_grid(ETchunks1, grid, folder, title1)
ETlist2 <- dr_grid(ETchunks2, grid, folder, title2)


# make a plot of ET grid locations and shapefile overlay

print("Generating plot of shallow GW ET")

unlink(ET_figpath)

ET_fig <- ETchunks %>% 
  select(SEQNUM) %>% 
  distinct() %>% 
  left_join(grid) %>% 
  st_as_sf(sf_column_name = "geometry")

ET_plot <- ggplot() +
  geom_sf(data = bounds, fill = "gray95", aes(color = "Model boundary"), linewidth = 0.5) +
  geom_hline(yintercept = 42, color = "gray20", linewidth = 0.1) +
  geom_sf(data = ET_fig, aes(color = "ET model cells"), fill = NA) +
  geom_sf(data = ET_area, aes(color = type, fill = type), alpha = 1/3) +
  scale_fill_manual(name = NULL, values = c("blue", "green3")) +
  scale_color_manual(name = NULL, values = c("black", "gray20", "blue", "green3"),
                     labels = c("ET model cells", "Model boundary", "Shallow GW", "Wetlands")) +
  theme_bw() +
  guides(fill = "none") +
  coord_sf(xlim = c(-122.5, -120.5), ylim = c(41.45, 43.35), expand = F) 

ET_plot

ggsave(plot = ET_plot, filename = ET_figpath, width = 10, height = 10, create.dir = T)

if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/ET.R", to = rootzC, overwrite = T)
}

print("ET complete. Total runtime:")
print(round(Sys.time()) - a)


