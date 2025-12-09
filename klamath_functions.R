#
# FUNCTION FILE FOR KLAMATH DATA PROCESSING
#
print("Loading required functions for data processing")

# all functions defined for MF data processing

# convert monthly AF to CFS
# IMPORTANT that this has already been aggregated to quarterly, otherwise it calculates incorrectly
AFconv <- function(AF, days) {
  result = 43559.9*AF/days/86400
  result
}

# %out% function to check for values NOT WITHIN (%in%) a group
'%out%' <- function(x,y)!('%in%'(x,y))

# time chunker function to add a column where dates are modified to quarterly groups
chunkz <- function(Date) {
  result = ifelse(month(Date) %in% c(1,2,3),
                  paste0(year(Date), "-01-01 to ",year(Date), "-03-31"),
                  ifelse(month(Date) %in% c(4,5,6),
                         paste0(year(Date), "-04-01 to ",year(Date), "-06-30"),
                         ifelse(month(Date) %in% c(7,8,9),
                                paste0(year(Date), "-07-01 to ",year(Date), "-09-30"),
                                ifelse(month(Date) %in% c(10,11,12),
                                       paste0(year(Date), "-10-01 to ",year(Date), "-12-31"),""))))
  result
}

# time chunker2; faster for bigger datasets. Need to mutate later via 'chunkrev' to add formatting back
chunkz2 <- function(Date) {
  
  year = year(Date)
  quarter = (month(Date)+2)%/%3
  
  paste(year, "-", quarter)
}

# convert chunkz2 back to chunkz format
chunkrev <- function(chunk) {
  result = ifelse(substr(chunk,8,8) == 1,
                  paste0(substr(chunk,1,4), "-01-01 to ",substr(chunk,1,4), "-03-31"),
                  ifelse(substr(chunk,8,8) == 2,
                         paste0(substr(chunk,1,4), "-04-01 to ",substr(chunk,1,4), "-06-30"),
                         ifelse(substr(chunk,8,8) == 3,
                                paste0(substr(chunk,1,4), "-07-01 to ",substr(chunk,1,4), "-09-30"),
                                paste0(substr(chunk,1,4), "-10-01 to ",substr(chunk,1,4), "-12-31"))))
  result
}

# The following function prints out the MNW2 file from a data table with the following fields:
# "ID"      "NNODES"   "ROW"  "COL"      "LAY"      "LOSSTYPE" "PUMPLOC"  "Qlimit"    
# "PPFLAG"  "PUMPCAP"  "Rw"   "Rskin"    "Kskin"    "Hlim"     "QCUT"     "Ztop"    "Zbot"

mnw2_writer <- function(title,dt) {
  
  subs <- unique(dt$ID)
  dat  <- dt %>% distinct(ID, .keep_all = TRUE)
  MNWMAX = length(subs)
  filename = paste0(pumppath,title,".mnw2")
  grps <- unique(dt$budget_group)
  widths <- dt %>%
    group_by(budget_group) %>%
    summarise(IDw = max(nchar(ID)),
              NODEw = max(nchar(NNODES)),
              GRPw = max(nchar(budget_group))) %>% 
    mutate(COLw = IDw+NODEw+GRPw+6) %>% 
    arrange(budget_group, .locale="en")
  
  colwidth = 40 #first(widths$COLw)
  
  # write header portion of MNW2 file:
  write(x = paste0("# Klamath Basin MNW2 package for ",title,
                   "\n#\nBEGIN LINEFEED\n",
                   paste0("  ./",fffolder,"/",grps, "_FeedFile.txt", collapse = "\n"),"\nEND\n#\n",
                   "BEGIN BUDGET_GROUPS\n",
                   paste0("  ",grps, collapse = "\n"),"\nEND\n#\n",
                   format(paste0(MNWMAX,"  37  1"), width = colwidth), "# 1.    Data: MNWMAX IWL2CB MNWPRNT {OPTION}"),
        append = F,
        file = filename)
  
  fileConn <- file(filename, "a")
  
  # Next, write individual site metadata (well ID, model input setup, budget group, etc etc):
  for (z in 1:MNWMAX) {
    
    #colwidth  = widths$COLw[widths$budget_group  == dat[z,]$budget_group]
    IDwidth   = widths$IDw[widths$budget_group   == dat[z,]$budget_group]
    nodewidth = widths$NODEw[widths$budget_group == dat[z,]$budget_group]
    
    Line1  = paste0(with(dat[z,], format(paste(str_pad(ID, IDwidth, side = "right"),
                                               str_pad(NNODES, nodewidth, side = "right"),
                                               budget_group), width = colwidth)),
                    "# 2a.   Data: WELLID NNODES BUDGET_GROUP")
    Line2  = paste0(with(dat[z,], format(paste(" ", LOSSTYPE, " ", PUMPLOC, Qlimit, PPFLAG, PUMPCAP),
                                         width = colwidth)),
                    "# 2b.   Data: LOSSTYPE PUMPLOC Qlimit PPFLAG PUMPCAP")
    Line3  = paste0(with(dat[z,], format(paste(" ",
                                               format(signif(Rw,2), nsmall = 2), " ",
                                               format(signif(Rskin,2), nsmall = 2), " ",
                                               format(signif(Kskin,2), nsmall = 2)),
                                         width = colwidth)),
                    "# 2c.   Data: Rw Rskin Kskin {B C P CWC}")
    
    writeLines(c(Line1, Line2, Line3), fileConn)
    
    # Then, write individual layer data, either multiple row/cols at layer 2 for virtual wells,
    # or Ztop/Zbot for real wells:
    wellz <- dt[dt$ID == subs[z],]
    
    for (q in 1:nrow(wellz)) {
      
      LineX  = ifelse(wellz[q,]$NNODES > 0,
                      with(wellz[q,], format(paste("   ",
                                                   str_pad(LAY, 3, side = "right"), " ",
                                                   str_pad(ROW, 3, side = "right"), " ",
                                                   COL),
                                             width = colwidth)),
                      with(wellz[q,], format(paste(" ", round(Ztop,0), round(Zbot,0), ROW, COL),
                                             width = colwidth)))
      
      LineXc = ifelse(wellz[q,]$NNODES > 0,
                      "# 2d-1. Data: LAY ROW COL {Rw Rskin Kskin B C P CWC}",
                      "# 2d-2. Data: Ztop Zbot ROW COL {Rw Rskin Kskin}")
      
      if (q == 1) {
        writeLines(paste0(LineX, LineXc), fileConn)
      } else {
        writeLines(LineX, fileConn)
      }
    }
    
    # Finish with Hlim info for wells/areas without depth data    
    if (with(dat[z,],Qlimit) > 0) { 
      LineEnd = paste0(with(dat[z,],  format(paste(" ", Hlim, " ", QCUT), width = colwidth)),
                       "# 2f.   Data: Hlim QCUT {Qfrcmn Qfrcmx}")
      writeLines(LineEnd, fileConn) 
    }
  }
  close(fileConn)
}

# The following function takes a pumpage dataset and transforms it into a feed file.
# Structure must be Well Names as column names, no row names, and each row is for
# the same forcing period. Each cell is pumping in CFS for a given well at a given time chunk.
# Last column must be the time chunk comment
feedfiler <- function(title, dt) {
  
  filename = paste0(ffpath,title,"_FeedFile.txt")
  
  write(x = "# WELLID -- STATIC INPUT\n#", file = filename, append = F, sep = " ")
  write.table(x = data.table(colnames(select(dt, -ncol(dt)))), file = filename, append = T,sep = " ",
              row.names = F, col.names = F,quote = F)
  write(x = "#\n# FLAG TO INDICATE STRESS PERIOD INPUT\n#", file = filename, append = T, sep = " ")
  write(x = "TEMPORAL INPUT\n#", file = filename, append = T, sep = " ")
  cat(paste0(c("#",paste0(str_pad(str_trunc(colnames(dt), width = 15, side = "center", ellipsis = "_"),
                           width = 15, side = "left"),
                  collapse = "  "), "\n"), collapse = ""),
      file = filename, append = T, sep = " ")
  write.table(dt, file = filename, append = T, sep = " ", row.names = F, col.names = F,
              quote = F)
  
}

# The following function takes a direct recharge dataset (ET, canal seepage) and converts to a grid file,
# outputting one for each time chunk. It also generates the TFR call file for the direct recharge.
#    'data' is any dataset in form SEQNUM, chunk, output
#    'grid' is model grid
dr_grid <- function(data,grid,folder,title) {
  
  ETlist <- list()
  tfrpath <- paste0(rootz,folder,title,".tfr")
  
  write(x = paste0("# Klamath Basin FMP Transient File Reader from ",
                   title,
                   " in CFS with model cell area 6250000.0 ft2"),
        append = F, file = tfrpath)
  
  chunker <- arrange(ungroup(data),chunk)$chunk %>% 
    unique()  
  
  dir.create(paste0(rootz,folder,title,"_input"), recursive = T, showWarnings = F)
  
  for (i in 1:length(chunker)) {
    
    print(paste("Generated",i,"of",length(chunker),"direct recharge files"))
    
    trinum <- str_pad(i, 3, pad = "0")
    filenm <- paste0(title, "_", trinum, ".txt")
    newpath <- paste0(rootz, folder, title, "_input/", filenm)
    tfr_pathnm <- paste0(title, "_input/", filenm)
    
    data <- data %>% 
      select(SEQNUM = 1, chunk = 2, value = 3)
    
    # mounts all above intersection data onto the full model grid
    grid_input <- grid %>% 
      st_drop_geometry() %>% 
      select(SEQNUM, ROW, COLUMN) %>% 
      left_join(filter(data, chunk == chunker[i]), by = join_by(SEQNUM)) %>% 
      mutate(value = ifelse(is.na(value), 0, value)) %>% 
      as.data.table() %>% 
      dcast(ROW ~ COLUMN, value.var = "value") %>%
      select(-ROW)
    
    ETlist[[i]] <- grid_input
    
    # write empty first line comment, and then subsequent data by model cell
    write(x = paste0("# SP ", trinum, "    ", chunker[i]), append = F, file = newpath, sep = " ")
    write.table(grid_input, file = newpath, append = T, sep = " ", row.names = F, col.names = F,
                quote = F)
    
    write.table(x = paste0("OPEN/CLOSE  ", tfr_pathnm, "  SF 1.0   # SP ", trinum, "    ", chunker[i]),
                file = tfrpath, append = T, row.names = F, col.names = F, quote = F)
  }
  return(ETlist)
}


# functions to determine min/max/mean of TOP/BOP/well diams
find_min_top <- function(sf_df, indices) {
  entries <- sf_df[indices, ]
  result <- ifelse(min(as.numeric(entries$top), na.rm = T) == Inf, NA,
                   min(as.numeric(entries$top), na.rm = T))
  result
}
find_max_bop <- function(sf_df, indices) {
  entries <- sf_df[indices, ]
  result <- ifelse(max(as.numeric(entries$bop), na.rm = T) == Inf, NA,
                   max(as.numeric(entries$bop), na.rm = T))
  result
}
find_avg_diam <- function(sf_df, indices) {
  entries <- sf_df[indices, ]
  result <- ifelse(mean(as.numeric(entries$diam), na.rm = T) == Inf, NA,
                   mean(as.numeric(entries$diam), na.rm = T))
  result
}

# save file
if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/klamath_functions.R", to = rootzC, overwrite = T)
}

print("Complete")
