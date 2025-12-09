# need to import ag ET from scott... or do it myself D:
# then need to probably just combine all text files and add them up
# should be a couple clever for loops, or lapply's
# generate a new tfr file and done

# i should already have ETlist and ETBlist loaded
# so I just need to import scott's ag data 

ag_path <- "../KlamathARC/GIS_Files/ET/2025-07/ag_pet_gw/"
#gw_et_path <- paste0(rootz, "ET/", "gw_et_input/")
titleC <- "ET_all"
folder <- "ET/"
tfrpathC <- paste0(rootz,folder,titleC,".tfr")
dir.create(paste0(rootz,folder,titleC,"_input"), recursive = T, showWarnings = F)
#gw_et <- lapply(list.files(gw_et_path, full.names = T), read.table)

ag_et <- list.files(ag_path, full.names = T) %>% 
  lapply(function(x) {
    read.table(x) %>% 
      as.data.table(x) %>% 
      select(-1)
  })

#all_arrays <- list(gw_et, ag_et, ETlist, ETBlist)

sum_et <- lapply(seq_along(ag_et), function(i){
  ag_et[[i]] + ETlist[[i]] + ETBlist[[i]]
})

#View(sum_et[[1]])


butterz <- function(data, namer, days, folder, title, count) {
  print(paste("Generated", count, "of", length(ETBdat), "direct recharge files"))
  
  trinum <- str_pad(count, 3, pad = "0")
  filenm <- paste0(title, "_", trinum, ".txt")
  newpath <- paste0(rootz, folder, title, "_input/", filenm)
  tfr_pathnm <- paste0(title, "_input/", filenm)
  
  # write empty first line comment, and then subsequent data by model cell
  write(x = paste0("# SP ", trinum, "    ", namer), append = F, file = newpath, sep = " ")
  write.table(data, file = newpath, append = T, sep = " ", row.names = F, col.names = F,
              quote = F)
  
  fileConn <- file(tfrpathC, "a")
  
  if(count == 1) {writeLines(paste0("# Klamath Basin FMP Transient File Reader from ",
                                    title,
                                    " in feet per second with model cell area 6250000.0 ft2"),
                             fileConn)}
  
  writeLines(paste0("OPEN/CLOSE  ", tfr_pathnm, "  SF 1.0   # SP ", trinum, "    ", namer),
             fileConn)
  
  close(fileConn)
}

unlink(tfrpathC)

sum_list <- lapply(seq_along(sum_et), function(i) {
  butterz(
    sum_et[[i]],
    ETBnames$chunk[i],
    ETBnames$days[i],
    folder,
    titleC,
    i
  )
})


if (Sys.getenv("RSTUDIO") == "1") {
  file.copy(from = "~/USGS/Klamath/Klamath-R/ET_compiler.R", to = rootzC, overwrite = T)
}


