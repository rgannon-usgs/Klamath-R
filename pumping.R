#
# PUMPING DATA PROCESSOR
# WE JUST WANT TO PUMP -- YOU UP
#

# load functions and data from sources
a <- round(Sys.time())
print(paste("PUMPAGE PROCESSING. Loading source files. Start:", a))

source("data_defs.R")

#input paths

dcmipath <- "../KlamathARC/GIS_Files/Pumping/ProcessedData_20241105/"
cadcmi_gis_path <- paste0(dcmipath, "/Wells/CA_wells_NAD83.shp")
ordcmi_gis_path <- paste0(dcmipath, "/Wells/OR_DCMI_Wells_NAD83.shp")
cadcmi_dat_path <- paste0(dcmipath, "CA_DCMI_Production_revised_20241105.csv")
ordcmi_dat_path <- paste0(dcmipath, "OR_DCMI_Production_revised20241105.csv")
cadcmi_depths_path <- paste0(dcmipath, "CA_DCMI_depths.csv")

CASGEM_path <- paste0(dcmipath, "CASGEM_elevs.csv")
DWR_path <- paste0(dcmipath, "wellcompletionreports.csv")

ag_wells_path <- "../KlamathARC/GIS_Files/Subsetted_shps/Phase1_2_Subsetted_wells.shp"

TID_path <- "../KlamathARC/GIS_Files/Pumping/Supplemental_Irrigation_Pumping/TID_district_wells/"
TID_well_path <- paste0(TID_path,"/GIS/CA_PumpingWells_toCanals.shp")
TID_data_path <- paste0(TID_path,"TID_district_wells.csv")

# output paths
pumppath <- paste0(rootz, "pumpage/")
fffolder <- "feedfiles"
ffpath <- paste0(pumppath,fffolder,"/")
pump_figpath <- paste0(figpath, "Pumping_coverage.png")
dir.create(ffpath, recursive = T, showWarnings = T)

# "GLOBAL" pumping variables for ease of setting:
LAYp = 2
LOSSTYPEp = "SKIN"
PUMPLOCp = 0
Qlimitp = 0
PPFLAGp = 0
PUMPCAPp = 0
Rwp = 0.75
Rskinp = 3
Kskinp = 1.12
Hlimp = 200
QCUTp = 0

print("Processing well pumpage data")
print("Loading DCMI shapefiles and pumpage values")

# Starting workflow on DCMI wells

# load CA and OR DCMI shapefiles
cadcmi_gis <- st_read(cadcmi_gis_path, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  distinct(Well_ID, .keep_all = T)
ordcmi_gis <- st_read(ordcmi_gis_path, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  mutate(Well_ID = paste0(wl_county_, wl_nbr)) %>% 
  distinct(Well_ID, .keep_all = T)

# load CA and OR DCMI data
cadcmi_dat <- read.csv(cadcmi_dat_path) %>% 
  select(-1)
ordcmi_dat <- read.csv(ordcmi_dat_path) %>% 
  select(-1)

print("Loading CASGEM and DWR data for well construction attribution")
# import all CA sites from CASGEM
CASGEM <- read.csv(CASGEM_path) %>% 
  filter(LONGITUDE < -121) %>% 
  st_as_sf(crs = crsnad, coords = c("LONGITUDE", "LATITUDE"))

# also import DWR sites to check for more depth data
DWR <- read.csv(DWR_path)
KDWR <- DWR %>% 
  mutate(lat = as.numeric(DECIMALLATITUDE),
         lon = as.numeric(DECIMALLONGITUDE)) %>% 
  filter(lat > 41.5, between(lon, -123, -120.5)) %>% 
  st_as_sf(crs = crsnad, coords = c("lon", "lat"))

KDWR_simple <- KDWR %>% 
  mutate(src = "DWR",
         bop = ifelse(!is.na(BOTTOMOFPERFORATEDINTERVAL), BOTTOMOFPERFORATEDINTERVAL,
                      ifelse(!is.na(TOTALCOMPLETEDDEPTH), TOTALCOMPLETEDDEPTH, 
                             ifelse(!is.na(TOTALDRILLDEPTH), TOTALDRILLDEPTH, NA))),
         top = TOPOFPERFORATEDINTERVAL,
         diam = ifelse(CASINGDIAMETER < 1, NA, CASINGDIAMETER)) %>% 
  select(src, top, bop, diam, geometry)

# combine CASGEM & DWR top/bop data for DCMI depth "attribution"
perfz <- CASGEM %>% 
  mutate(src = "CASGEM",
         bop = ifelse(!is.na(PERFORATION_BOTTOM_MSRMNT), PERFORATION_BOTTOM_MSRMNT,
                      ifelse(!is.na(TOTAL_DEPTH_FT), TOTAL_DEPTH_FT, NA)),
         top = PERFORATION_TOP_MSRMNT,
         diam = NA) %>% 
  select(src, top, bop, diam, geometry) %>% 
  rbind(KDWR_simple) %>% 
  distinct(bop, geometry, .keep_all = T)

rm(DWR, KDWR, KDWR_simple, CASGEM)

# Automatically and manually and adding depths for DCMI wells of interest

print("Drawing buffers around DCMI sites and getting min Ztop and max Zbot from CASGEM/DWR within 1 km radius (slow)")
# AUTO: draw buffers around DCMI sites to try and find min/max top/bot perfs from CASGEM & DWR dbs ----
perf_buffs <- st_buffer(perfz, dist = 1000) %>% # this is slow
  st_geometry() %>%
  st_transform(crs = crsnad)

overlaps <- st_within(x = st_transform(cadcmi_gis, crs = crsnad), y = perf_buffs)
counts <- data.frame(count = sapply(overlaps, length))

bots <- data.frame(bop = sapply(overlaps, function(index) find_max_bop(perfz, index))) %>% 
  mutate(bop = ifelse(bop %in% c(Inf, NA, -Inf), NA, bop))
tops <- data.frame(top = sapply(overlaps, function(index) find_min_top(perfz, index))) %>% 
  mutate(top = ifelse(top %in% c(Inf, NA, -Inf), 0, top))
diams<- data.frame(diam = sapply(overlaps, function(index) find_avg_diam(perfz, index))) %>% 
  mutate(diam = ifelse(diam %in% c(Inf, NA, -Inf), NA, diam))

final_result <- cadcmi_gis %>% bind_cols(counts) %>% bind_cols(tops) %>% bind_cols(bots) %>% bind_cols(diams)

rm(perfz, perf_buffs, overlaps, counts, bots, tops, diams)

# merge with manually attributed CADCMI depth data ----
print("Merging manually-attributed wells from CA_DCMI_depths.csv")
depths <- read.csv(cadcmi_depths_path)

cadcmi_depths_low <- final_result %>% 
  filter(Well_ID %out% c(depths$Well_ID)) %>% 
  select(-count) %>% 
  st_transform(crs = crsnad)

cadcmi_depths_hi <- cadcmi_gis %>% 
  filter(Well_ID %in% c(depths$Well_ID)) %>% 
  left_join(depths) %>% 
  st_transform(crs = crsnad)
  
cadcmi_depth <- cadcmi_depths_hi %>% 
  rbind(cadcmi_depths_low) 

rm(depths, cadcmi_depths_low, cadcmi_depths_hi)

# now, clean up CA coverage and OR coverage and combine into one well file for MNW2
# merge with model grid to get ROW/COL cells

print("Cleaning up,adding elevations, and intersecting with model grid for final MNW2 file (slow)")

cadcmi_wells <- cadcmi_depth %>% 
  filter(!is.na(bop)) %>% 
  filter(Well_ID %out% c("MODOC1", "SISKIYOU2", "SISKIYOU3", "SISKIYOU67")) %>%  # these wells are bad
  select(ID = Well_ID, Ztop = top, Zbot = bop, diam) %>% 
  get_elev_point(units = "feet", src = "aws", z = 10) %>%  # this is a convenient way to pull elevs but it is slow
  mutate(Ztop = elevation - Ztop, 
         Zbot = elevation - Zbot,
         Rw = ifelse(!is.na(diam),diam/24, Rwp)) %>% 
  select(ID, Ztop, Zbot, Rw)
 
ordcmi_wells <- ordcmi_gis %>% 
  get_elev_point(units = "feet", src = "aws", z = 10) %>%  # this is a convenient way to pull elevs but it is slow
  mutate(Ztop = ifelse(completed_ == 0, NA, elevation),    # decrease zoom (< 12) for faster but less accurate
         Zbot = ifelse(completed_ == 0, NA, elevation - completed_),
         Rw = ifelse(casing_dia == 0, Rwp, casing_dia/24)) %>% 
select(ID = Well_ID, Ztop, Zbot, Rw)

dcmi_intersubset <- st_intersects(rbind(cadcmi_wells, ordcmi_wells), grid) 

dcmi_wells <- cadcmi_wells %>% 
  rbind(ordcmi_wells) %>% 
  st_drop_geometry() %>% 
  distinct(ID, .keep_all = T) %>% 
  mutate(SEQNUM = as.numeric(dcmi_intersubset)) %>% 
  left_join(select(st_drop_geometry(grid), SEQNUM, ROW, COL = COLUMN)) %>% 
  select(ID, Ztop, Zbot, ROW, COL, Rx = Rw) %>% 
  mutate(LAY = ifelse(is.na(Zbot), LAYp, NA),
         NNODES = ifelse(is.na(Zbot), 1, -1),
         LOSSTYPE = LOSSTYPEp,
         PUMPLOC = PUMPLOCp,
         Qlimit = Qlimitp, #ifelse(is.na(Zbot), 1, 0),
         PPFLAG = ifelse(is.na(Zbot), 0, 1),
         PUMPCAP = PUMPCAPp,
         Rw = round(Rx, 2),
         Rskin = Rw*2,
         Kskin = Kskinp,
         Hlim = ifelse(is.na(Zbot), Hlimp, NA),
         QCUT = QCUTp,
         budget_group = "DCMI_wells",
         ID = str_trunc(ID, width = 15, side = "center", ellipsis = "_")) %>% 
  select(-Rx)
  
rm(cadcmi_wells, cadcmi_gis, cadcmi_depth, ordcmi_wells, ordcmi_gis, dcmi_intersubset)

print("Generating DCMI pumpage for feed files")

# generate DCMI feedfile
DCMI_dat <- cadcmi_dat %>% 
  rbind(ordcmi_dat) %>% 
  select(Date, Days, Well_ID, Projected_production_AF) %>% 
  mutate(chunk = chunkz2(Date)) %>% 
  group_by(Well_ID, chunk) %>% 
  summarise(pumpage_AF = sum(Projected_production_AF),
            days = sum(Days)) %>% 
  arrange(Well_ID, chunk) %>% 
  mutate(pumpage_CFS = AFconv(pumpage_AF,days)) %>% 
  ungroup() %>% 
  group_by(Well_ID) %>% 
  filter(sum(pumpage_AF) > 1) %>% 
  ungroup() %>% 
  mutate(chunk = chunkrev(chunk))

DCMI_filter <- DCMI_dat %>% 
  group_by(Well_ID) %>% 
  summarise(pumpage_AF = sum(pumpage_AF))

DCMI <- as.data.table(DCMI_dat) %>% 
  select(Well_ID, chunk, pumpage_CFS) %>% 
  mutate(pumpage_CFS = formatC(pumpage_CFS, format = "e", digits = 10)) %>% 
  dcast(chunk ~ Well_ID, value.var = "pumpage_CFS") %>% 
  mutate(rownum = str_pad(1:160,3,pad = "0"),
         TimeChunk = paste0("# SP ", rownum,": ", chunk)) %>% 
  relocate(1, .after = last_col()) %>% 
  select(-rownum, -chunk)

rm(DCMI_dat)

# ----
# PHEW. Next, work on AG data. Hopefully simpler
# so for a given "subset", we have x wells (mostly 0-2)
# This is WELL ORIENTED, so focus on the wells first
# I need to load all wells
#
# load subsetted wells (OR only); 'dat' is the subsetted data

print("Moving on to ag wells; loading shapefiles and transforming data")

wells_sub <- st_read(ag_wells_path, quiet = T) %>%
  group_by(Well_ID) %>% 
  mutate(Well_ID = ifelse(row_number() == 1, Well_ID, paste0(Well_ID,"b")))

wells_sub_cnt <- wells_sub %>% 
  st_drop_geometry() %>% 
  group_by(Subset) %>% 
  summarise(n = n()) %>% 
  ungroup()

pump_ag <- dat %>% 
  filter(!is.na(PUMP)) %>% 
  mutate(days = days_in_month(Date)) %>% 
  group_by(Subset, chunk, HUC8) %>% 
  summarise(pumpage_AF = sum(PUMP),
            days = sum(days)) %>% 
  arrange(HUC8, Subset, chunk) %>% 
  left_join(wells_sub_cnt) %>% 
  mutate(pumpage_CFS = AFconv(pumpage_AF,days)) %>% 
  ungroup() %>% 
  group_by(Subset) %>% 
  filter(sum(pumpage_AF) > 1) %>% 
  ungroup()

# For subsets with wells; easy. Need to add column for fraction of pumpage per well
pump_ag_real <- pump_ag %>% 
  filter(Subset %in% wells_sub_cnt$Subset) %>% 
  mutate(pump_per_well_CFS = pumpage_CFS/n)

print("Focusing on subsets with wells; pulling elevations and intersecting (slow)")

# Filter out wells that don't have pumpage data
wells_sub_viable <- wells_sub %>% 
  filter(Subset %in% pump_ag$Subset) %>% 
  get_elev_point(units = "feet", src = "aws", z = 10) %>% 
  mutate(ID = Well_ID,
         Subset,
         completed_dt = as.Date(complete_d),
         Ztop = ifelse(completed_ == 0, NA, elevation),
         Zbot = ifelse(completed_ == 0, NA, elevation - completed_),
         Rw = ifelse(casing_dia == 0, Rwp, casing_dia/24),
         geometry,
         .keep = "none") 

viable_intersubset <- st_intersects(st_transform(wells_sub_viable, crs = 4269), grid) 

ag_wells <- wells_sub_viable %>% 
  st_drop_geometry() %>% 
  distinct(ID, .keep_all = T) %>% 
  ungroup() %>% 
  mutate(SEQNUM = as.numeric(viable_intersubset)) %>% 
  left_join(select(st_drop_geometry(grid), SEQNUM, ROW, COL = COLUMN)) %>% 
  select(ID, Ztop, Zbot, ROW, COL, Rx = Rw) %>% 
  mutate(LAY = ifelse(is.na(Zbot), LAYp, NA),
         NNODES = ifelse(is.na(Zbot), 1, -1),
         LOSSTYPE = LOSSTYPEp,
         PUMPLOC = PUMPLOCp,
         Qlimit = Qlimitp, #ifelse(is.na(Zbot), 1, 0),
         PPFLAG = ifelse(is.na(Zbot), 0, 1),
         PUMPCAP = PUMPCAPp,
         Rw = round(Rx, 2),
         Rskin = Rw*2,
         Kskin = Kskinp,
         Hlim = ifelse(is.na(Zbot), Hlimp, NA),
         QCUT = QCUTp,
         budget_group = "ag_wells",
         ID = str_trunc(ID, width = 15, side = "center", ellipsis = "_")) %>% 
  select(-Rx)

rm(viable_intersubset) 

# merge pumpage data to wells; recalculate pumpage per well based on well construction date so it
# is more accurately counted (ppw_new)

print("Processing pumpage data for wells")

wells_sub_pump <- wells_sub_viable %>% 
  left_join(pump_ag_real, relationship = "many-to-many") %>% 
  mutate(chunk1 = as.Date(substr(chunk,  0, 10)),
         chunk2 = as.Date(substr(chunk, 15, 24))) %>% 
  mutate(countme = ifelse(is.na(completed_dt), 1,
                          ifelse(completed_dt < chunk2, 1, 0))) %>% 
  group_by(Subset, chunk) %>% 
  mutate(newcount = sum(countme)) %>%   
  ungroup() %>% 
  mutate(ppw_new = ifelse(countme == 1, pumpage_CFS/newcount, 0)) %>% # break here if I need to edit
  select(Well_ID, chunk, ppw_new) %>% 
  st_drop_geometry()

# transform ag well pumpage to be processed by feed files
ag_well_input <- as.data.table(wells_sub_pump) %>% 
  mutate(ppw_new = formatC(ppw_new, format = "e", digits = 10)) %>% 
  dcast(chunk ~ Well_ID, value.var = "ppw_new") %>% 
  mutate(rownum = str_pad(1:nrow(.),3,pad = "0"),
         TimeChunk = paste0("# SP ", rownum,": ", chunk)) %>% 
  relocate(1, .after = last_col()) %>% 
  select(-rownum, -chunk)

rm(wells_sub_pump, pump_ag_real)

# For subsets with no wells identified; harder. Need to properly distribute a field across the grid,
# add columns for n fields per Subset, add fraction for pumpage per field

print("Moving on to ag data that does not have well coverage to create virtual wells")
print("Starting with intersecting model grid cell coverage")

WBS_intersect <- field_intersects %>% 
  select(SEQNUM, ROW, COLUMN_, Subset, Shape_Area) %>% # OPENET_ID, STATE,
  st_drop_geometry() %>% 
  group_by(Subset) %>% 
  mutate(COL = COLUMN_,
         area = Shape_Area*10.7639, #convert from sq m to sq ft
         sub_area = sum(Shape_Area)*10.7639,
         cell_area = 6250000,
         size = ifelse(sub_area < 6250000*2, "small", "big")) %>% # any subset smaller than 2 grid cell areas
  ungroup() %>% 
  group_by(SEQNUM, ROW, COL, Subset, size) %>% 
  mutate(sub_cell_occup = area/cell_area) %>% 
  summarise(area_of_sub_in_cell = sum(area),
            total_sub_area = first(sub_area),
            fraction_total_sub = area_of_sub_in_cell/total_sub_area,
            fraction_sub_occupying_cell = sum(sub_cell_occup)) %>% 
  ungroup() %>% 
  group_by(Subset) %>% 
  mutate(n = n()) %>% 
  filter(fraction_sub_occupying_cell > 0.25 | fraction_total_sub == max(fraction_total_sub)) %>% 
         #(n < 5 & (fraction_sub_occupying_cell > 0.5 | fraction_total_sub == max(fraction_total_sub))) |
         #   (n > 4 & (fraction_sub_occupying_cell > 0.25 | fraction_total_sub == max(fraction_total_sub)))) %>% 
  mutate(NNODES = n()) 

virtuwells <- WBS_intersect %>% 
  filter(Subset %out% wells_sub_cnt$Subset) %>% 
  filter(Subset %in% pump_ag$Subset) %>% 
  select(Subset, NNODES, ROW, COL) %>% 
  mutate(Ztop = NA,
         Zbot = NA,
         LAY = LAYp,
         LOSSTYPE = LOSSTYPEp,
         PUMPLOC = PUMPLOCp,
         Qlimit = Qlimitp,
         PPFLAG = PPFLAGp,
         PUMPCAP = PUMPCAPp,
         Rw = round(Rwp,2),
         Rskin = Rskinp,
         Kskin = Kskinp,
         Hlim = Hlimp,
         QCUT = QCUTp,
         budget_group = "virtual_wells",
         virtuwellcount = NNODES) %>% 
  ungroup() %>% 
  arrange(Subset) %>% 
  group_by(Subset) %>% 
  mutate(ID = str_trunc(paste0(Subset,"-",str_pad(1:virtuwellcount,nchar(max(virtuwellcount)), pad = "0")),
                        width = 15, side = "center", ellipsis = "_"),
         NNODES = 1) %>% 
  ungroup()
  
# generate feed file for virtual wells

print("Processing ag pumpage data for virtual wells")

pump_ag_virtual <- as.data.table(pump_ag) %>% 
  filter(Subset %in% virtuwells$Subset) %>% 
  select(Subset, chunk, pumpage_CFS) %>% 
  left_join(distinct(virtuwells, Subset, virtuwellcount)) %>% 
  mutate(pumpage_CFS = formatC(pumpage_CFS/virtuwellcount, format = "e", digits = 10)) %>% 
  full_join(select(virtuwells, Subset, ID), by = c("Subset" = "Subset"),
            relationship = "many-to-many") %>% 
  dcast(chunk ~ ID, value.var = "pumpage_CFS") %>% 
  mutate(rownum = str_pad(row_number(),3,pad = "0"),
         TimeChunk = paste0("# SP ", rownum,": ", chunk)) %>% 
  relocate(1, .after = last_col()) %>% 
  select(-rownum, -chunk)

# clean up virtuwells MNW feed to be parallel to others
virtuwells <- virtuwells %>% select(-Subset, -virtuwellcount)

rm(pump_ag, WBS_intersect)

# TID wells ----

print("Final dataset: TID wells. Loading shapefile and pumpage data")

TID_wells <- st_read(TID_well_path, quiet = T) %>% 
  mutate(Well_ID = paste0("TIDWell", substr(Label, 6,8))) %>% 
  mutate(ID = Well_ID,
         Ztop = NA,
         Zbot = NA,
         Rw = Rwp,
         geometry,
         .keep = "none") 

print("Intersecting TID wells with model grid and transforming for MNW2 input")

TID_intersubset <- st_intersects(st_transform(TID_wells, crs = 4269), grid) 

# creates MNW2 formatted well list
TID <- TID_wells %>% 
  st_drop_geometry() %>% 
  mutate(SEQNUM = as.numeric(TID_intersubset)) %>% 
  left_join(select(st_drop_geometry(grid), SEQNUM, ROW, COL = COLUMN)) %>% 
  select(ID, Ztop, Zbot, ROW, COL, Rx = Rw) %>% 
  mutate(LAY = ifelse(is.na(Zbot), LAYp, NA),
         NNODES = ifelse(is.na(Zbot), 1, -1),
         LOSSTYPE = LOSSTYPEp,
         PUMPLOC = PUMPLOCp,
         Qlimit = Qlimitp, #ifelse(is.na(Zbot), 1, 0),
         PPFLAG = ifelse(is.na(Zbot), 0, 1),
         PUMPCAP = PUMPCAPp,
         Rw = round(Rx, 2),
         Rskin = Rw*2,
         Kskin = Kskinp,
         Hlim = ifelse(is.na(Zbot), Hlimp, NA),
         QCUT = QCUTp,
         budget_group = "TID_wells",
         ID = str_trunc(ID, width = 15, side = "center", ellipsis = "_")) %>% 
  select(-Rx)

rm(TID_wells, TID_intersubset) 

# creates MNW2 formatted feedfile

print("Processing pumpage data for TID wells") 

TID_data  <- read.csv(TID_data_path) %>% 
  rename(X = X, date = Date, a689 = WELL.SITES_6.8.AND.9, b12345 = TO.J.LATERALS_FROM.WELL.SITES_1.2.3.4.AND.5, c14 = TO.N.SYSTEM_FROM.WELL_SITE.14) %>% 
  mutate(date = as.Date(date),
         TIDWell1 = b12345/5,
         TIDWell2 = b12345/5,
         TIDWell3 = b12345/5,
         TIDWell4 = b12345/5,
         TIDWell5 = b12345/5,
         TIDWell6 = a689/3,
         TIDWell8 = a689/3,
         TIDWell9 = a689/3,
         TIDWell14 = c14) %>% 
  select(-1, -3, -4, -5) %>% 
  as.data.table() %>% 
  filter(between(as.Date(date), "1980-09-29", "2020-09-02")) %>% 
  melt(id.vars = "date", variable.name = "Wells", value.name = "pumpage_AF") %>% 
  mutate(chunk = chunkz(date),
         days = days_in_month(date)) %>% 
  group_by(Wells, chunk) %>% 
  summarise(pumpage_AF = sum(pumpage_AF),
            days = sum(days)) %>% 
  ungroup() %>% 
  as.data.table() %>% 
  rbind(data.table(chunk = chunker, Wells = "Dummy", days = NA, pumpage_AF = NA)) %>% 
  mutate(pumpage_CFS = AFconv(pumpage_AF,days)) %>% 
  mutate(pumpage_CFS = formatC(pumpage_CFS, format = "e", digits = 10)) %>% 
  dcast(chunk ~ Wells, value.var = "pumpage_CFS") %>% 
  mutate_all(~replace(., is.na(.), formatC(0, format = "e", digits = 10))) %>% 
  mutate(rownum = str_pad(row_number(),3,pad = "0"),
         TimeChunk = paste0("# SP ", rownum,": ", chunk)) %>% 
  relocate(1, .after = last_col()) %>% 
  select(-rownum, -chunk, -Dummy) 

print("Now outputting everything as MF input files")
# merge all MNW2 input files ----

pumping_input <- dcmi_wells %>% 
  rbind(virtuwells) %>% 
  rbind(ag_wells) %>% 
  rbind(TID) %>% 
  plyr::arrange(budget_group, NNODES, ID)

print("Creating feed files")

feedfiler("DCMI_wells", DCMI)
feedfiler("ag_wells", ag_well_input)
feedfiler("TID_wells", TID_data)
feedfiler("virtual_wells", pump_ag_virtual)

print(paste("Creating MNW2 file (slow, ~2 min;", length(unique(pumping_input$ID)), "wells)"))

start.time <- round(Sys.time())
mnw2_writer("Klamath_pumping", pumping_input)
end.time <- round(Sys.time())
time.taken <- end.time - start.time
print(time.taken)

# make a plot of well locations.

print("Generating plots of MF cell coverage by budget group")

unlink(pump_figpath)

location_fig <- pumping_input %>% 
 # filter(budget_group != "DCMI_wells" |
#           (budget_group == "DCMI_wells" &
#              ID %in% DCMI_filter$Well_ID[DCMI_filter$pumpage_AF == 14.74456])) %>% 
  select(ROW, COL, budget_group) %>% 
  distinct() %>% 
  group_by(ROW, COL, budget_group) %>% 
  summarise(groups = paste(budget_group, collapse = ", ")) %>% 
  left_join(grid, by = c("COL" = "COLUMN", "ROW" = "ROW")) %>% 
  st_as_sf(sf_column_name = "geometry")

# 
# some plot manipulations to look at over-selection of data
# THIS INCLUDES THE FILTER IN location_fig ABOVE
#
# pump_over <- pumping_input %>% 
#   left_join(st_drop_geometry(select(grid, SEQNUM, ROW, COLUMN)),
#             by = c("ROW" = "ROW", "COL" = "COLUMN")) %>% 
#   group_by(SEQNUM, ROW, COL, budget_group) %>% 
#   summarise(n = n(),
#             Rw_mean = mean(Rw, na.rm = T),
#             Rw_sd = sd(Rw, na.rm = T),
#             Ztop_mean = mean(Ztop, na.rm = T),
#             Ztop_sd = sd(Ztop, na.rm = T),
#             Zbot_mean = mean(Zbot, na.rm = T),
#             Zbot_sd = sd(Zbot, na.rm = T)) %>% 
#   filter(n > 1 & budget_group != "virtual_wells")
# 
# pumpmelt <- as.data.table(ungroup(pump_over)) %>% 
#   select(-ROW, -COL) %>% 
#   melt(id.vars = c("SEQNUM", "budget_group", "n")) %>% 
#   mutate(group = sub("_.*", "", variable),
#          maths = sub(".*_", "", variable)) %>% 
#   select(-variable) %>% 
#   dcast(SEQNUM + budget_group + n + group ~ maths, value.var = "value")
# 
#   
# ggplot(data = filter(pump_over, n > 1)) +
#   geom_histogram(aes(x = n, fill = budget_group)) +
#   facet_wrap(vars(budget_group), nrow = 1) +
#   labs(x = "Number of wells in a single grid cell\n(only showing cells with n > 1)") +
#   theme_bw()
#   
# ggplot(data = filter(pumpmelt, group != "Rw" & n > 4)) +
#   geom_ribbon(aes(x = SEQNUM, ymin = mean-sd, ymax = mean+sd, group = group, fill = group)) +
#   labs(y = "mean (+/- sd)") +
#   geom_line(aes(x = SEQNUM, y = mean, group = group)) #+
#   facet_wrap(vars(group), ncol = 1, scales = "free_y")

pump_plot <- ggplot() +
  geom_sf(data = bounds, fill = "gray90") +
  #geom_sf(data = grid, linewidth = 0.1, fill = NA, color = "gray80") +
  geom_hline(yintercept = 42, color = "gray20", linewidth = 0.1) +
  geom_sf(data = location_fig, aes(fill = budget_group), color = NA) +
  scale_fill_discrete("Budget group") +
  theme_bw() +
  coord_sf(xlim = c(-122.5, -120.5), ylim = c(41.45, 43.35), expand = F) +
  facet_wrap(vars(budget_group), nrow = 2)

ggsave(plot = pump_plot, filename = pump_figpath, width = 10, height = 11, create.dir = T)
  
if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/pumping.R", to = rootzC, overwrite = T)
}

print("Pumpage complete. Total runtime:")
print(round(Sys.time()) - a)
