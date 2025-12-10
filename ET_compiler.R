# combine all ET list data and add them up
# should be a couple clever for loops, or lapply's
# generate a new tfr file and done

# Must already have ETlist, ETGWlist, and ETBlist loaded

#gw_et_path <- paste0(rootz, "ET/", "gw_et_input/")
titleC <- "ET_all"
folder <- "ET/"
tfrpathC <- paste0(rootz,folder,titleC,".tfr")
dir.create(paste0(rootz,folder,titleC,"_input"), recursive = T, showWarnings = F)
#gw_et <- lapply(list.files(gw_et_path, full.names = T), read.table)

sum_et <- lapply(seq_along(ag_et), function(i){
  ETGWlist[[i]] + ETlist[[i]] + ETBlist[[i]]
})

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
                                    " in FPS with model cell area 6250000.0 ft2"),
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


