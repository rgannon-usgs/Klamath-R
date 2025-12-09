#
# CANAL SEEPAGE PROCESSOR
#

# load functions and data from sources
a <- round(Sys.time())
print(paste("CANAL SEEPAGE PROCESSING. Loading source files. Start:", a))

source("data_defs.R")

print("Processing canal seepage subset data")
# Set paths for canal shapefile and seepage output file
canal_path <- "../KlamathARC/GIS_Files/Subsetted_shps/Phase1_2_Subsetted_canals.shp"

# output paths
title <- "canal_seepage"
folder <- "seepage/"
#dir_rch_path <- paste0(rootz,folder, subf) 
canal_figpath <- paste0(figpath, "Canal_seepage_coverage.png")

dir.create(paste0(rootz,folder), recursive = T, showWarnings = F)

# Read canal shapefile
canal <- st_read(canal_path, quiet = T) %>% 
  st_transform(crs = crsnad) %>% 
  filter(!st_is_empty(geometry))

# Transform canal shapefile down to have total lengths of area Subset and merge with seepage data
canal_lengths <- st_drop_geometry(canal) %>% 
  group_by(Subset) %>% 
  summarise(total_lengthFT = sum(LengthFT))

# Transform data into 3 month chunks of seepage by area Subset
seepage <- dat %>% 
  filter(!is.na(SEEP)) %>% 
  mutate(days = days_in_month(Date)) %>% 
  group_by(Subset, chunk, HUC8) %>% 
  summarise(seepage_AF = sum(SEEP),
            days = sum(days)) %>% 
  arrange(HUC8, Subset, chunk) %>% 
  left_join(canal_lengths) %>% 
  mutate(seepage_CFS = AFconv(seepage_AF,days))

# Intersect canal file onto model grid
# This results in canal lines being broken up onto model grid, and calculates length of each canal
#   in each grid cell

begin <- round(round(Sys.time()))

print(paste0("Intersecting canal shapefile onto grid (slow; ~5 min). Began at ", begin))

ints <- lengths(st_intersects(grid, canal)) > 0

grid_c <- grid[ints,]

canal_intersectz <- st_intersection(canal, grid_c) 

print(round(Sys.time()))

canal_intersect <- canal_intersectz %>% 
  mutate(intersect_length = st_length(.)) %>% 
  filter(!st_is_empty(geometry)) %>% 
  st_cast("MULTILINESTRING") %>% 
  mutate(lenFT = as.numeric(intersect_length)/.3048)

end <- round(Sys.time())

print("Processing canal/grid intersection took:")
print(end - begin)

# simplify to a couple columns and run the direct recharge gridding function 
seepage_input2 <- canal_intersect %>% # separating this part out for later plotting purposes
  left_join(seepage, by = c("Subset" = "Subset"), relationship = "many-to-many") %>% 
  mutate(len_frac = lenFT/total_lengthFT) %>% 
  mutate(seep_frac_CFS = seepage_CFS * len_frac) %>% 
  filter(seep_frac_CFS > 0) 

seepage_input <- seepage_input2 %>%
  st_drop_geometry() %>% 
  group_by(SEQNUM, chunk) %>% 
  summarise(seepage_CFS = sum(seep_frac_CFS)) %>% 
  ungroup()


print("Writing canal seepage TFR and direct recharge files")

dr_grid(seepage_input, grid, folder, title)

# make a plot of canal grid locations and canal overlay

print("Generating plot of canal seepage")

unlink(canal_figpath)

seepage_fig <- seepage_input %>% 
  select(SEQNUM) %>% 
  distinct() %>% 
  left_join(grid) %>% 
  st_as_sf(sf_column_name = "geometry")

seep_plot <- ggplot() +
  #ggtitle("hey") +
  geom_sf(data = bounds, fill = "gray95", aes(color = "Model boundary"), linewidth = 0.5) +
  #geom_sf(data = grid, linewidth = 0.1, fill = NA, color = "gray80") +
  geom_hline(yintercept = 42, color = "gray20", linewidth = 0.1) +
  geom_sf(data = fields, aes(color = "Ag field boundaries"), fill = "gray90") +
  geom_sf(data = seepage_fig, aes(fill = "Seepage model cells"), color = NA) +
  geom_sf(data = canal, aes(color = "Canal paths (no seepage data)"), linewidth = 0.25) +
  geom_sf(data = seepage_input2, aes(color = "Canal paths (with data)"), linewidth = 0.5) +
  scale_fill_manual(name = NULL, values = "cyan3") +
  scale_color_manual(name = NULL, values = c("gray50", "red4", "blue1", "black")) +
  theme_bw() +
  coord_sf(xlim = c(-122.5, -120.5), ylim = c(41.45, 43.35), expand = F) 

ggsave(plot = seep_plot, filename = canal_figpath, width = 10, height = 10, create.dir = T)

if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/canal_seepage.R", to = rootzC, overwrite = T)
}

print("Canal seepage complete. Total runtime:")
print(round(Sys.time()) - a)


