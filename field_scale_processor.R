#
# Process all Field-scale data (gw_et, irrigation runoff, deep perc recharge)
#

# load functions and data from sources
a <- round(Sys.time())
print(paste("FIELD-SCALE PROCESSING. Loading source files. Start:", a))

source("data_defs.R")

# output paths
title1 <- "ag_runoff"
folder1 <- "ag_estimated_runoff/"

title2 <- "ag_pet_gw"
folder2 <- "ag_pet_gw/"

title3 <- "ag_recharge"
folder3 <- "ag_estimated_direct_rch/"

FS_figpath <- paste0(figpath, "field-scale_coverage.png")

dir.create(paste0(rootz,folder1), recursive = T, showWarnings = F)
dir.create(paste0(rootz,folder2), recursive = T, showWarnings = F)
dir.create(paste0(rootz,folder3), recursive = T, showWarnings = F)

# use 'fields' and 'field_intersects' for previously imported shapefiles
# use 'datf' for previously imported field-scale data
# use 'Irr_runoff_AF', 'et_gw_AF', and 'Deep_perc_AF' within 'datf' for data
# UNITS NOTE: ET_GW IS *feet per second* AND IRR & DEEP PERC ARE *cubic feet per second*

# Transform data into 3 month chunks 
print("Processing field-scale data down to 3 month chunks and converting to CFS")

field_dat <- datf %>% 
  mutate(days = days_in_month(Date)) %>% 
  group_by(OPENET_ID, chunk, HUC8) %>% 
  summarise(IRR = sum(Irr_runoff_AF),
            ET = sum(et_gw_mm),
            DP = sum(Deep_perc_AF),
            days = sum(days)) %>% 
  arrange(HUC8, OPENET_ID, chunk) %>% 
  mutate(IRR_CFS = AFconv(IRR,days),
         ET_FS = ET/304.8/days/86400,
         DP_CFS = AFconv(DP,days))

# Distribute values across model cells and intersect data onto model grid
print("Distributing field-scale data onto model grid cells")

field_scale <- field_intersects %>% 
  select(OPENET_ID, ACRES, SEQNUM, Shape_Area) %>% 
  mutate(area_frac = (Shape_Area/4046.86)/ACRES,
         cell_frac = ifelse(Shape_Area < 580644, Shape_Area/580644, 580644)) %>% 
  left_join(field_dat, by = c("OPENET_ID" = "OPENET_ID"), relationship = "many-to-many") %>% 
  mutate(IRR = IRR_CFS * area_frac,
         ET = ET_FS * cell_frac,
         DP = DP_CFS * area_frac) %>% 
  select(OPENET_ID, SEQNUM, chunk, IRR, ET, DP)

field_scale2 <- field_scale %>% 
  st_drop_geometry() %>% 
  group_by(SEQNUM, chunk) %>% 
  summarise(IRR = sum(IRR),
            ET = sum(ET),
            DP = sum(DP)) %>% 
  ungroup()

# Minimize number of grid cells to speed up later computing
print("Creating smaller model grid for computational purposes")
grid_f <- grid %>% filter(SEQNUM %in% field_scale$SEQNUM)

print("Writing TFR and direct recharge files for irrigation runoff, ET, and deep perc recharge.")

dr_grid(select(field_scale2, 1, 2, 3), grid, folder1, title1, "CFS")
ETGWlist <- dr_grid(select(field_scale2, 1, 2, 4), grid, folder2, title2, "FPS")
dr_grid(select(field_scale2, 1, 2, 5), grid, folder3, title3, "CFS")

# Figs, and trouble shooting. Why doesn't my data match Scott's?
print("Updating data for plots and generating plot")

ALL_fig <- field_scale %>% 
  st_drop_geometry() %>% 
  select(SEQNUM) %>% 
  distinct() %>% 
  left_join(grid_f, by = c("SEQNUM" = "SEQNUM")) %>% 
  st_as_sf(sf_column_name = "geometry")

IRR_fig <- field_scale %>% 
  st_drop_geometry() %>% 
  filter(IRR > 0) %>% 
  select(SEQNUM) %>% 
  distinct() %>% 
  left_join(grid_f, by = c("SEQNUM" = "SEQNUM")) %>% 
  st_as_sf(sf_column_name = "geometry")

GW_ET_fig <- field_scale %>% 
  st_drop_geometry() %>% 
  filter(ET > 0) %>% 
  select(SEQNUM) %>% 
  distinct() %>% 
  left_join(grid_f, by = c("SEQNUM" = "SEQNUM")) %>% 
  st_as_sf(sf_column_name = "geometry")

DP_fig <- field_scale %>% 
  st_drop_geometry() %>% 
  filter(DP > 0) %>% 
  select(SEQNUM) %>% 
  distinct() %>% 
  left_join(grid_f, by = c("SEQNUM" = "SEQNUM")) %>% 
  st_as_sf(sf_column_name = "geometry")

field_plot <- ggplot() +
  geom_sf(data = bounds, fill = "gray95", aes(color = "Model boundary"), linewidth = 0.5) +
  geom_sf(data = fields, aes(color = "Ag field boundaries"), fill = "green3", linewidth = 0.15) +
  geom_hline(yintercept = 42, color = "gray20", linewidth = 0.1) +
  geom_sf(data = ALL_fig, aes(color = "Field-scale model cells"), fill = NA) +
  #geom_sf(data = IRR_fig, aes(color = "IRR model cells"), fill = NA) +
  #geom_sf(data = GW_ET_fig, aes(color = "GW_ET model cells"), fill = NA) +
  #geom_sf(data = DP_fig, aes(color = "DP model cells"), fill = NA) +
  #scale_fill_manual(name = NULL, values = c("blue", "green3")) +
  scale_color_manual(name = NULL, values = c("black", "gray30", "black")) +
  theme_bw() +
  guides(fill = "none") #+
  #coord_sf(xlim = c(-122.5, -120.5), ylim = c(41.45, 43.35), expand = F) 

ggsave(plot = field_plot, filename = FS_figpath, width = 10, height = 10, create.dir = T)

if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/field_scale_processor.R", to = rootzC, overwrite = T)
}

print(paste("Field-scale (gw_et, irrigation runoff, deep perc recharge) processing complete. Total runtime:",
            round(Sys.time() - a, 1), "min"))
