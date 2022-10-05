# This script will produce a dataset of monthly counts per atc stratified by
# age (in 10 year blocks), sex, and pharmacy state.
# The resulting dataset should not be used for analyses yet as it does not 
# account for the different quantities/ammounts of different item codes.
# We will likely need to convert dispensings into DDD's, and aggregate these
# counts.

library(tidyr)
library(tidyverse)
library(haven)
library(dplyr)
library(progress)
library(lubridate)
library(magrittr)
options(dplyr.summarise.inform=F)
options(dplyr.summarise.inform=F)

get.age.breaks <- function(){
  # Used to make sure consistent age breaks are used across functions
  #seq(0,110,by=10)
  seq(-2,118,by=10)
}
reformat.age.range <- function(x){
  # Used to make sure formatting of age ranges are consistent across functions
    x %>% mutate(AGE_RANGE=ifelse(AGE_RANGE=='[-2,8)','0-8',
                     ifelse(AGE_RANGE=='[8,18)','8-17',
                     ifelse(AGE_RANGE=='[18,28)','18-27',
                     ifelse(AGE_RANGE=='[28,38)','28-37',
                     ifelse(AGE_RANGE=='[38,48)','38-47',
                     ifelse(AGE_RANGE=='[48,58)','48-57',
                     ifelse(AGE_RANGE=='[58,68)','58-67',
                     ifelse(AGE_RANGE=='[68,78)','68-77',
                     ifelse(AGE_RANGE=='[78,88)','78-87',
                     ifelse(AGE_RANGE=='[88,98)','88-97',
                     ifelse(AGE_RANGE=='[98,108)','98-107',NA)
    )))))))))))
}
filter.unusable.data <- function(x){
  # Filter out those under 18 and over 97
  # Also filtering out unknown age/sex patients
    x %>% filter(!is.na(AGE_RANGE),
           !is.na(PATIENT_SEX), 
           (AGE_RANGE != '0-8') & (AGE_RANGE != '8-17') & (AGE_RANGE != '98-107'))
}

# create 1 dataframe for each year/atc code
split.by.atc <- function(year.list=c(2016:2021)){
#*
  # Split the main data files into smaller, more manageable subfiles.
  # Each subfile contains raw entries for just one year/atc code
  
  #for(year in c(2017:2020)){
  for(year in year.list){
    pbs <- read_sas(paste0('../data/original-data/y',year,'supply.sas7bdat'))
    print(length(unique(pbs$ATC_CODE)))
    for(atc in unique(pbs$ATC_CODE)){
      out.file <- paste0('../data/modified-data/by-atc/raw-entries/',atc,'_',year,'.rds')
      #if(!file.exists(out.file)){
      print(paste0("Writing: ",out.file))
      pbs %>% filter(ATC_CODE==atc) %>% 
        write_rds(file=out.file, compress="gz", version=3)
      gc()
    }
    rm(pbs)
    gc()
  }
}


# Stratify each of the datasets created by split.by.atc, and get the daily dispensing 
# count (by atc code, not item code)
stratify.daily.atc.count<- function(){
  pat <- read_sas('../data/original-data/patient_ids.sas7bdat') %>% 
    dplyr::select(PAT_ID,PATIENT_SEX,YEAR_BIRTH)
  #for(in.file in list.files('../data/modified-data/by-atc/raw-entries/')){
  for(atc in unique(sapply(strsplit(list.files('../data/modified-data/by-atc/raw-entries/'),'_'),'[[',1))){
    out.file <- paste0('../data/modified-data/by-atc/stratified-entries/',atc,'.rds')
    print(paste0("Writing: ",out.file))
    data <- bind_rows(lapply(
      paste0('../data/modified-data/by-atc/raw-entries/',atc,'_',
             c(2016,2017,2018,2019,2020,2021),'.rds'),
      function(x){if(file.exists(x)){readRDS(x)}}
      )
    )
    # to group by age range, replace YEAR_BIRTH with YR=cut(YEAR_BIRTH,breaks=seq(1900,2020,by=5))
    data %>% left_join(pat) %>% group_by(PHARMACY_STATE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH, REPEATS > 0, PREV_SUPP_NUM > 0) %>%
        tally() %>% write_rds(file=out.file, compress='gz', version=3)
    rm(data)
    gc()
  }
}

# Take the dataframes created by stratify.daily.atc.count, and merge into
# monthly counts for each atc code
# apply filtering to remove unwanted patients/data
merge.monthly.atc.count <- function(){
  age.breaks <- get.age.breaks()
  lapply(list.files('../data/modified-data/by-atc/stratified-entries'), function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      group_by(year(SUPP_DATE),month(SUPP_DATE),PHARMACY_STATE,PATIENT_SEX,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
      summarize(!!paste0(strsplit(x,'\\.')[[1]][1]) := sum(n, na.rm=T))
    }) %>% reduce(full_join,by=c('PHARMACY_STATE', 'year(SUPP_DATE)','month(SUPP_DATE)', 'PATIENT_SEX', 'AGE_RANGE')) %>% 
    mutate_if(is.numeric,coalesce,0) %>% 
    reformat.age.range() %>%
    filter.unusable.data() %>%
    write_rds(file='../data/modified-data/stratified-monthly-dispensings-all-atc.rds', compress='gz', version=3)
  gc()
}

# Take the dataframes created by stratify.daily.atc.count, and merge into
# daily counts for each atc code
# apply filtering to remove unwanted patients/data
merge.daily.atc.count <- function(){
  age.breaks <- get.age.breaks()
  lapply(list.files('../data/modified-data/by-atc/stratified-entries'), function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      group_by(SUPP_DATE,PHARMACY_STATE,PATIENT_SEX,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
      summarize(!!paste0(strsplit(x,'\\.')[[1]][1]) := sum(n, na.rm=T))
    }) %>% reduce(full_join,by=c('PHARMACY_STATE', 'SUPP_DATE', 'PATIENT_SEX', 'AGE_RANGE')) %>% 
    mutate_if(is.numeric,coalesce,0) %>% 
    reformat.age.range() %>%
    filter.unusable.data() %>%
    write_rds(file='../data/modified-data/stratified-daily-dispensings-all-atc.rds', compress='gz', version=3)
  gc()
}

# Stratify the datasets created by split.by.atc, but this time keeping item codes separate
stratify.daily.item.qty <- function(){
  pat <- read_sas('../data/original-data/patient_ids.sas7bdat') %>% 
    dplyr::select(PAT_ID,PATIENT_SEX,YEAR_BIRTH)
  files <- list.files('../data/modified-data/by-atc/raw-entries/')
  opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  for(atc in unique(sapply(strsplit(opioid.files,'_'),'[[',1))){
    out.file <- paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',atc,'.rds')
    print(paste0("Writing: ",out.file))
    data <- bind_rows(lapply(
      paste0('../data/modified-data/by-atc/raw-entries/',atc,'_',
             c(2016,2017,2018,2019,2020, 2021),'.rds'),
      function(x){if(file.exists(x)){readRDS(x)}}
      )
    )
    # to group by age range, replace YEAR_BIRTH with YR=cut(YEAR_BIRTH,breaks=seq(1900,2020,by=5))
    data %>% left_join(pat) %>% group_by(ITEM_CODE, PHARMACY_STATE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH, REPEATS > 0, PREV_SUPP_NUM > 0) %>%
        summarize(QTY=sum(QTY, na.rm=T)) %>% write_rds(file=out.file, compress='gz', version=3)
    
    rm(data)
    gc()
  }
}

# Take the dataframes created above, and merge into one dataframe
#  NOT WORKING
merge.monthly.item.qty.1 <- function(){
  # Monthly
  stop('do not use this function')
  # age.breaks <- get.age.breaks()
  # lapply(list.files('../data/modified-data/by-atc/stratified-entries_by-item-code'), function(x){
  #   readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
  #     mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
  #     filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
  #            !is.na(PATIENT_SEX)) %>%
  #     group_by(ITEM_CODE, year(SUPP_DATE),month(SUPP_DATE),PHARMACY_STATE,PATIENT_SEX,
  #              AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
  #              ) %>% 
  #     summarize(QTY = sum(QTY))
  #   }) %>% 
  #   write_rds(file='../data/modified-data/INTERMEDIATE-LIST_stratified-monthly-dispensings-all-item-code.rds', compress='gz', version=3)
}
#  NOT WORKING
merge.monthly.item.qty.2 <- function(){
  stop('do not use this function')
    # data.list <- readRDS(file='../data/modified-data/INTERMEDIATE-LIST_stratified-monthly-dispensings-all-item-code.rds')
    # list.range <- floor(seq(1,length(data.list),length.out=10))
    # for(i in c(1:9)){
    #   data.list[list.range[i]:list.range[i+1]] %>%
    #   reduce(full_join) %>%
    #   mutate_if(is.numeric,coalesce,0) %>% 
    #   reformat.age.range() %>%
    #   filter(!is.na(AGE_RANGE),
    #          !is.na(PATIENT_SEX),
    #          (AGE_RANGE != '100-109') & (AGE_RANGE != '0-9') & (AGE_RANGE != '10-19')) %>%
    #   write_rds(file=paste0('../data/modified-data/stratified-monthly-dispensings-all-item-code_part',
    #                         i,'.rds'), compress='gz', version=3)
    # gc()
    # }
}
#  NOT WORKING
merge.daily.item.qty <- function(){
  stop('do not use this function')
  # Take the dataframes created above, and merge into one dataframe
  # Daily
  # age.breaks <- get.age.breaks()
  # lapply(list.files('../data/modified-data/by-atc/stratified-entries_by-item-code'), function(x){
  #   readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
  #     mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
  #     filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
  #            !is.na(PATIENT_SEX)) %>%
  #     group_by(ITEM_CODE, SUPP_DATE,PHARMACY_STATE,PATIENT_SEX,
  #              AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
  #              ) %>% 
  #     summarize(QTY = sum(QTY))
  #   }) %>% reduce(full_join) %>%
  #   mutate_if(is.numeric,coalesce,0) %>% 
  #   reformat.age.range() %>%
  #   filter(!is.na(AGE_RANGE),
  #          !is.na(PATIENT_SEX)
  #          (AGE_RANGE != '100-109') & (AGE_RANGE != '0-9') & (AGE_RANGE != '10-19')) %>%
  #   write_rds(file='../data/modified-data/stratified-daily-dispensings-all-item-code.rds', compress='gz', version=3)
  # gc()
}

merge.monthly.opioid.items <- function(){
  # Creates a dataset with the quantity of each item code dispensed per month
  # stratified by state/sex/age-range.
  # Age range filtering has been done a little strangly here 
  # -- the first cut-off of 100 is further reduced by the call to 'filter.unusable.data'
  files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  age.breaks <- get.age.breaks()
  x <- lapply(opioid.files, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
             !is.na(PATIENT_SEX)) %>%
      group_by(ITEM_CODE, year(SUPP_DATE),month(SUPP_DATE),PATIENT_SEX,PHARMACY_STATE,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
      dplyr::summarize(QTY = sum(QTY, na.rm=T))
    }) %>% reduce(full_join, by = c("ITEM_CODE","year(SUPP_DATE)", "month(SUPP_DATE)", "PHARMACY_STATE", "PATIENT_SEX", "AGE_RANGE", "QTY")) %>%
    mutate_if(is.numeric,coalesce,0) %>% 
    reformat.age.range() %>%
    filter.unusable.data() %>%
    pivot_wider(names_from='ITEM_CODE',values_from='QTY', values_fill=0, values_fn=sum) %>%
    write_rds(file='../data/modified-data/stratified-monthly-opioid-dispensings-all-item-code.rds', compress='gz', version=3)
  gc()
}

merge.daily.opioid.items <- function(){
  # Creates a dataset with the quantity of each item code dispensed per day
  # stratified by state/sex/age-range.
  files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  age.breaks <- get.age.breaks()
  lapply(opioid.files, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
             !is.na(PATIENT_SEX)) %>%
      group_by(ITEM_CODE,SUPP_DATE,PHARMACY_STATE,PATIENT_SEX,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
      dplyr::summarize(QTY = sum(QTY, na.rm=T))
    }) %>% reduce(full_join,by=c("ITEM_CODE", "SUPP_DATE", "PHARMACY_STATE", "PATIENT_SEX", "AGE_RANGE", "QTY")) %>%
    mutate_if(is.numeric,coalesce,0) %>% 
    reformat.age.range() %>%
    filter.unusable.data() %>%
    pivot_wider(names_from='ITEM_CODE',values_from='QTY', values_fill=0, values_fn=sum) %>%
    write_rds(file='../data/modified-data/stratified-daily-opioid-dispensings-all-item-code.rds', compress='gz', version=3)
  gc()
}

merge.monthly.opioid.ome <- function(){
  # Creates a dataset with the total OME dispensed per month for each item code
  # stratified by state/sex/age-range
  ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  age.breaks <- get.age.breaks()
  lapply(opioid.files, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
             !is.na(PATIENT_SEX)) %>%
      left_join(.,ome.conv,by='ITEM_CODE') %>% 
      group_by(ITEM_CODE, year(SUPP_DATE),month(SUPP_DATE),PHARMACY_STATE,PATIENT_SEX,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
      dplyr::summarize(OME = sum(QTY*OME_CONV))
    }) %>% reduce(full_join, by = c("ITEM_CODE", "year(SUPP_DATE)", "month(SUPP_DATE)", "PHARMACY_STATE", "PATIENT_SEX", "AGE_RANGE", "OME")) %>%
    mutate_if(is.numeric,coalesce,0) %>% 
    reformat.age.range() %>%
    filter.unusable.data() %>%
    pivot_wider(names_from='ITEM_CODE',values_from='OME', values_fill=0, values_fn=sum) %>%
    write_rds(file='../data/modified-data/stratified-monthly-ome.rds', compress='gz', version=3)
  gc()
}

merge.daily.opioid.ome <- function(){
  # Creates a dataset with the total OME dispensed per day for each item code
  # stratified by state/sex/age-range
  files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  age.breaks <- get.age.breaks()
  lapply(opioid.files, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
             !is.na(PATIENT_SEX)) %>%
      left_join(.,ome.conv,by='ITEM_CODE') %>% 
      group_by(ITEM_CODE,SUPP_DATE,PHARMACY_STATE,PATIENT_SEX,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
      summarize(OME = sum(QTY*OME_CONV))
    }) %>% reduce(full_join,by=c("ITEM_CODE", "SUPP_DATE", "PHARMACY_STATE", "PATIENT_SEX", "AGE_RANGE", "OME")) %>%
    mutate_if(is.numeric,coalesce,0) %>% 
    reformat.age.range() %>%
    filter.unusable.data() %>%
    pivot_wider(names_from='ITEM_CODE',values_from='OME',values_fill=0, values_fn=sum) %>%
    write_rds(file='../data/modified-data/stratified-daily-ome-by-i.rds', compress='gz', version=3)
  gc()
}

merge.monthly.opioid.ome.no.grouping <- function(){
  # Creates a dataset with the total OME dispensed per month for each item code
  # Not stratified
  ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  age.breaks <- get.age.breaks()
  lapply(opioid.files, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100), !is.na(PATIENT_SEX)) %>%
      left_join(.,ome.conv,by='ITEM_CODE') %>% 
      group_by(ITEM_CODE, year(SUPP_DATE),month(SUPP_DATE)) %>% 
      dplyr::summarize(OME = sum(QTY*OME_CONV))
    }) %>% reduce(full_join, by = c("ITEM_CODE", "year(SUPP_DATE)", "month(SUPP_DATE)", "OME")) %>%
    mutate_if(is.numeric,coalesce,0) %>% 
    pivot_wider(names_from='ITEM_CODE',values_from='OME',values_fill=0, values_fn=sum) %>%
    write_rds(file='../data/modified-data/monthly-ome-by-item-code.rds', compress='gz', version=3)
  gc()
}
  
merge.daily.opioid.ome.no.grouping <- function(){
  # Creates a dataset with the total OME dispensed per day for each item code
  # Not stratified
  files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  age.breaks <- get.age.breaks()
  lapply(opioid.files, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
      mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH) %>%
      filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
             !is.na(PATIENT_SEX)) %>%
      left_join(.,ome.conv,by='ITEM_CODE') %>% 
      group_by(ITEM_CODE,SUPP_DATE) %>% 
      summarize(OME = sum(QTY*OME_CONV))
    }) %>% reduce(full_join,by=c("ITEM_CODE", "SUPP_DATE","OME")) %>%
    mutate_if(is.numeric,coalesce,0) %>% 
    pivot_wider(names_from='ITEM_CODE',values_from='OME', values_fill=0, values_fn=sum) %>%
    write_rds(file='../data/modified-data/daily-ome-by-item-code.rds', compress='gz', version=3)
  gc()
}

merge.daily.opioid.repeats.allowed.no.grouping <- function(){
  # FAULTY! NEEDS FIX ASAP
  stop('Bad function')
  # Monthly
  # files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  # opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  # ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  # age.breaks <- get.age.breaks()
  # lapply(opioid.files, function(x){
  #   readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
  #     mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH,
  #            repeats.allowed=`REPEATS > 0`) %>%
  #     filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
  #            !is.na(PATIENT_SEX)) %>%
  #     left_join(.,ome.conv,by='ITEM_CODE') %>% 
  #     group_by(ITEM_CODE,SUPP_DATE,repeats.allowed) %>% 
  #     dplyr::summarize(prop.repeats.allowed = mean(repeats.allowed))
  #   }) %>% reduce(full_join,by=c("ITEM_CODE", "SUPP_DATE","prop.repeats.allowed")) %>%
  #   mutate_if(is.numeric,coalesce,0) %>% 
  #   pivot_wider(names_from='ITEM_CODE',values_from='prop.repeats.allowed', values_fill=0, values_fn=sum) %>%
  #   write_rds(file='../data/modified-data/daily-repeats-allowed-by-item-code.rds', compress='gz', version=3)
  # gc()
}

merge.daily.opioid.prop.repeats.no.grouping <- function(){
  # FAULTY! NEEDS FIX ASAP
  stop('Bad function')
  # files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  # opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  # ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  # age.breaks <- get.age.breaks()
  # lapply(opioid.files, function(x){
  #   readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
  #     mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH,
  #            is.repeat=`PREV_SUPP_NUM > 0`) %>%
  #     filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
  #            !is.na(PATIENT_SEX)) %>%
  #     left_join(.,ome.conv,by='ITEM_CODE') %>% 
  #     group_by(ITEM_CODE,SUPP_DATE) %>% 
  #     dplyr::summarize(prop.repeats = mean(is.repeat))
  #   }) %>% reduce(full_join,by=c("ITEM_CODE", "SUPP_DATE","prop.repeats")) %>%
  #   mutate_if(is.numeric,coalesce,0) %>% 
  #   pivot_wider(names_from='ITEM_CODE',values_from='prop.repeats', values_fill=0, values_fn=sum) %>%
  #   write_rds(file='../data/modified-data/daily-prop-repeats-by-item-code.rds', compress='gz', version=3)
  # gc()
}

merge.daily.opioid.repeats.no.grouping <- function(){
  # Do not use
  stop("Bad function")
  # files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  # opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  # ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  # age.breaks <- get.age.breaks()
  # lapply(opioid.files, function(x){
  #   readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
  #     mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH,
  #            is.repeat=`PREV_SUPP_NUM > 0`) %>%
  #     filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
  #            !is.na(PATIENT_SEX)) %>%
  #     left_join(.,ome.conv,by='ITEM_CODE') %>% 
  #     group_by(ITEM_CODE,SUPP_DATE) %>% 
  #     dplyr::summarize(repeats = sum(is.repeat))
  #   }) %>% reduce(full_join,by=c("ITEM_CODE", "SUPP_DATE","repeats")) %>%
  #   mutate_if(is.numeric,coalesce,0) %>% 
  #   pivot_wider(names_from='ITEM_CODE',values_from='repeats', values_fill=0, values_fn=sum) %>%
  #   write_rds(file='../data/modified-data/daily-repeats-by-item-code.rds', compress='gz', version=3)
  # gc()
}

merge.daily.opioid.counts.no.grouping <- function(){
  # do not use bad datasets
  stop("Bad function")
  # files <- list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')
  # opioid.files <- files[grep('^(N02A|N07BC02|R05DA04)',files)]
  # ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  # age.breaks <- get.age.breaks()
  # lapply(opioid.files, function(x){
  #   readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x)) %>% 
  #     mutate(PATIENT_AGE=year(SUPP_DATE)-YEAR_BIRTH,
  #            is.repeat=`PREV_SUPP_NUM > 0`) %>%
  #     filter(!is.na(PATIENT_AGE), (PATIENT_AGE>=18)&(PATIENT_AGE<100),
  #            !is.na(PATIENT_SEX)) %>%
  #     left_join(.,ome.conv,by='ITEM_CODE') %>% 
  #     group_by(ITEM_CODE,SUPP_DATE) %>% 
  #     tally() # This is nonsense .... this is just te number of unique groups?
  #   }) %>% reduce(full_join,by=c("ITEM_CODE", "SUPP_DATE","n")) %>%
  #   mutate_if(is.numeric,coalesce,0) %>% 
  #   pivot_wider(names_from='ITEM_CODE',values_from='n', values_fill=0, values_fn=sum) %>%
  #   write_rds(file='../data/modified-data/daily-counts-by-item-code.rds', compress='gz', version=3)
  # gc()
}
# Attempt to work out dosage information for each item code
convert.to.grams <- function(w, unit){
  ifelse(unit == 'g', w,
  ifelse(unit == 'gram', w,
  ifelse(unit == 'mg', w*1e-3,
  ifelse(unit == 'milligram', w*1e-3,
  ifelse(unit == 'ug', w*1e-6,
  ifelse(unit == 'microgram', w*1e-6, NA))))))
}

drug.dosage <- function(){
  drug.map <- read.csv('../data/supplemental-data/PBSdrugmap.csv')
  dosage <- str_extract(drug.map$FORM_STRENGTH, '[0-9.,]+ *(g|mg|ug|gram|microgram|milligram)')
  #drug.map$amt <- as.numeric(str_extract(dosage,'[0-9.,]+'))
  #drug.map$unit <- str_extract(dosage,'(g|mg|ug|gram|microgram|milligram)')
  drug.map %<>% mutate(amt=as.numeric(str_extract(dosage,'[0-9.,]+')),
                       unit=str_extract(dosage,'(g|mg|ug|gram|microgram|milligram)'))
  drug.map %<>% mutate(grams_per=convert.to.grams(amt,unit))
  drug.map
}


stratified.daily.weight <- function(){
  drug.map <- drug.dosage()
  nona.drug.map <- filter(drug.map,!is.na(grams_per)) %>% dplyr::select(ITEM_CODE, ATC_CODE, grams_per)
  atc.to.use <- intersect(unique(nona.drug.map$ATC_CODE),
                          gsub('.rds','',list.files('../data/modified-data/by-atc/stratified-entries_by-item-code')))
  age.breaks <- get.age.breaks()
  lapply(atc.to.use, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x,'.rds')) %>% 
      left_join(.,nona.drug.map) %>% mutate(grams=QTY*grams_per) %>%
      group_by(SUPP_DATE,PHARMACY_STATE,PATIENT_SEX,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
      filter.unusable.data() %>%
      summarize(!!x := sum(grams))
    }) %>% reduce(full_join,by=c('SUPP_DATE', 'PHARMACY_STATE', 'PATIENT_SEX', 'AGE_RANGE')) %>% 
    mutate_if(is.numeric,coalesce,0) %>% 
    reformat.age.range() %>%
    filter.unusable.data() %>%
    write_rds(file='../data/modified-data/stratified-daily-weight-all-atc.rds', compress='gz', version=3)
  gc()
}

stratified.daily.weight.nationwide <- function(){
  lapply(atc.to.use, function(x){
    readRDS(paste0('../data/modified-data/by-atc/stratified-entries_by-item-code/',x,'.rds')) %>% 
      left_join(.,nona.drug.map) %>% mutate(grams=QTY*grams_per) %>%
      group_by(SUPP_DATE,PATIENT_SEX,
               AGE_RANGE=cut(PATIENT_AGE,breaks=age.breaks,include.lowest=T,right=F)
               ) %>% 
     reformat.age.range() %>%
     filter.unusable.data() %>%
     summarize(!!x := sum(grams))
    }) %>% reduce(full_join,by=c('SUPP_DATE', 'PATIENT_SEX', 'AGE_RANGE')) %>% 
    mutate_if(is.numeric,coalesce,0) %>% 
    filter(!is.na(AGE_RANGE)) %>%
    ungroup() %>%
    write_rds(file='../data/modified-data/stratified-daily-weight-all-atc-filtered-national.rds')
}

ir.opioid.entries.only <- function(){
  opioid.table <- read.csv('../data/supplemental-data/ome-conversion-table.csv')
  ir.items <- opioid.table %>% filter(grepl('^\\(IR\\)',DRUG_CLASS,perl=T)) %>% .$ITEM_CODE
  pat <- read_sas('../data/original-data/patient_ids.sas7bdat') %>% 
    dplyr::select(PAT_ID,PATIENT_SEX,YEAR_BIRTH)
  list.files('../data/modified-data/by-atc/raw-entries') %>% 
    subset(.,grepl('^(N02A|N07BC02|R05DA04)',.)) %>% #subset(.,grepl('_(2018|2019|2020|2021).rds$',.)) %>%
    gsub('^','../data/modified-data/by-atc/raw-entries/',.) %>%
    lapply(.,function(x){
      readRDS(x) %>% dplyr::select(PAT_ID, ITEM_CODE, SUPP_DATE, QTY, REPEATS, PREV_SUPP_NUM) %>%
        filter(ITEM_CODE %in% ir.items) %>%
        left_join(pat,by="PAT_ID") %>%
        filter((year(SUPP_DATE)-YEAR_BIRTH)>=18,(year(SUPP_DATE)-YEAR_BIRTH)<100)
      }) %>%
    reduce(rbind) %>%
    write_rds(file='../data/modified-data/raw-ir-opioid-entries.rds', compress='gz', version=3)
}

cr.opioid.entries.only <- function(){
  opioid.table <- read.csv('../data/supplemental-data/ome-conversion-table.csv')
  ir.items <- opioid.table %>% filter(grepl('^\\(CR\\)',DRUG_CLASS,perl=T)) %>% .$ITEM_CODE
  pat <- read_sas('../data/original-data/patient_ids.sas7bdat') %>% 
    dplyr::select(PAT_ID,PATIENT_SEX,YEAR_BIRTH)
  list.files('../data/modified-data/by-atc/raw-entries') %>% 
    subset(.,grepl('^(N02A|N07BC02|R05DA04)',.)) %>% #subset(.,grepl('_(2018|2019|2020|2021).rds$',.)) %>%
    gsub('^','../data/modified-data/by-atc/raw-entries/',.) %>%
    lapply(.,function(x){
      readRDS(x) %>% dplyr::select(PAT_ID, ITEM_CODE, SUPP_DATE, QTY, REPEATS, PREV_SUPP_NUM) %>%
        filter(ITEM_CODE %in% ir.items) %>%
        left_join(pat,by="PAT_ID") %>%
        filter(year(SUPP_DATE)-YEAR_BIRTH>=18,year(SUPP_DATE)-YEAR_BIRTH<100)
      }) %>%
    reduce(rbind) %>%
    write_rds(file='../data/modified-data/raw-cr-opioid-entries.rds', compress='gz', version=3)
}

opioid.entries.only <- function(){
  full_join(readRDS('../data/modified-data/raw-cr-opioid-entries.rds'),readRDS('../data/modified-data/raw-ir-opioid-entries.rds') ) %>%
    write_rds(file='../data/modified-data/raw-opioid-entries.rds', compress='gz', version=3)
}

make.daily.counts.by.item.code <- function(){
  # Fixed and correct datasets
    rbind(
      readRDS(file='../data/modified-data/raw-cr-opioid-entries.rds'),
      readRDS(file='../data/modified-data/raw-ir-opioid-entries.rds')
    ) %>% 
      group_by(ITEM_CODE, SUPP_DATE) %>% tally() %>%
      pivot_wider(values_from=n,names_from=ITEM_CODE,values_fill=0) %>%
      write_rds(file='../data/modified-data/daily-counts-by-item-code.rds', compress='gz', version=3)
}

make.daily.repeats.by.item.code <- function(){
  # Fixed and correct datasets
    rbind(
      readRDS(file='../data/modified-data/raw-cr-opioid-entries.rds'),
      readRDS(file='../data/modified-data/raw-ir-opioid-entries.rds')
    ) %>%  filter(PREV_SUPP_NUM > 1) %>% 
      group_by(ITEM_CODE, SUPP_DATE) %>% tally() %>%
      pivot_wider(values_from=n,names_from=ITEM_CODE,values_fill=0) %>%
      write_rds(file='../data/modified-data/daily-repeats-by-item-code.rds', compress='gz', version=3)
}

opioid.counts <- function(){
  readRDS('../data/modified-data/raw-opioid-entries.rds') 
    group_by(ITEM_CODE,year(SUPP_DATE),month(SUPP_DATE),REPEATS > 0, PREV_SUPP_NUM > 0) %>% 
    tally() %>% 
    ungroup() %>% 
    mutate(date=as.Date(paste0(`year(SUPP_DATE)`,'-',`month(SUPP_DATE)`,'-01')),
           is.repeat=`PREV_SUPP_NUM > 0`,
           repeats.allowed=`REPEATS > 0`) %>%
    dplyr::select(ITEM_CODE,n,date,is.repeat,repeats.allowed) %>%
    pivot_wider(values_from=n,names_from=ITEM_CODE,values_fill=0, values_fn=sum) %>%
    arrange(date) %>%
    write_rds(file='../data/modified-data/monthly-opioid-dispensings-all-item-code.rds',compress='gz',version=3)
}

opioid.counts.daily <- function(){
  readRDS('../data/modified-data/raw-opioid-entries.rds') 
    group_by(ITEM_CODE,SUPP_DATE,REPEATS > 0, PREV_SUPP_NUM > 0) %>% 
    tally() %>% 
    ungroup() %>% 
    mutate(date=SUPP_DATE,
           is.repeat=`PREV_SUPP_NUM > 0`,
           repeats.allowed=`REPEATS > 0`) %>%
    dplyr::select(ITEM_CODE,n,date,is.repeat,repeats.allowed) %>%
    pivot_wider(values_from=n,names_from=ITEM_CODE,values_fill=0, values_fn=sum) %>%
    arrange(date) %>%
    write_rds(file='../data/modified-data/daily-opioid-dispensings-all-item-code.rds',compress='gz',version=3)
}

make.initiations <- function(lookback=90,out.file='../data/modified-data/opioid-initiations.rds'){
  readRDS('../data/modified-data/raw-opioid-entries.rds') %>% 
    group_by(PAT_ID, ITEM_CODE) %>% #mutate(CONCESSIONAL=ifelse(CONCESSIONAL_STATUS %in% c('A','B','C'),'G','C')) %>% # C is concession
    arrange(SUPP_DATE, by.group=T) %>% 
    mutate(gap=c(NA,as.numeric(diff(SUPP_DATE))),
           init=ifelse(row_number()==1,T,
                       ifelse(gap>=lookback,T,F))) %>% 
    ungroup() %>%
    filter(SUPP_DATE >= as.Date('2016-06-01'), init==T) %>%
    dplyr::select(PAT_ID, ITEM_CODE,SUPP_DATE, PATIENT_SEX, YEAR_BIRTH) %>% #PHARMACY_STATE, CONCESSIONAL) %>%
    write_rds(file=out.file, compress='gz', version=3)
}

make.ir.initiations <- function(lookback=90,out.file='../data/modified-data/opioid-ir-initiations.rds'){
  readRDS('../data/modified-data/raw-ir-opioid-entries.rds') %>% #group_by(PAT_ID, ITEM_CODE) %>% 
    group_by(PAT_ID) %>% 
    arrange(SUPP_DATE, by.group=T) %>% 
    mutate(gap=c(NA,as.numeric(diff(SUPP_DATE))),
           init=ifelse(row_number()==1,T,
                       ifelse(gap>=lookback,T,F))) %>% 
    ungroup() %>%
    filter(SUPP_DATE >= as.Date('2016-06-01'), init==T) %>%
    dplyr::select(PAT_ID, ITEM_CODE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH) %>%
    write_rds(file=out.file, compress='gz', version=3)
}

make.ir.continuations <- function(lookback=90,out.file='../data/modified-data/opioid-ir-continuations.rds'){
  readRDS('../data/modified-data/raw-ir-opioid-entries.rds') %>% #group_by(PAT_ID, ITEM_CODE) %>% 
    group_by(PAT_ID) %>% 
    arrange(SUPP_DATE, by.group=T) %>% 
    mutate(gap=c(NA,as.numeric(diff(SUPP_DATE))),
           init=ifelse(row_number()==1,T,
                       ifelse(gap>=lookback,T,F))) %>% 
    ungroup() %>%
    filter(SUPP_DATE >= as.Date('2016-06-01'), init==F) %>%
    dplyr::select(PAT_ID, ITEM_CODE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH) %>%
    write_rds(file=out.file, compress='gz', version=3)
}

make.cr.continuations <- function(lookback=90,out.file='../data/modified-data/opioid-cr-initiations.rds'){
  readRDS('../data/modified-data/raw-cr-opioid-entries.rds') %>% #group_by(PAT_ID, ITEM_CODE) %>% 
    group_by(PAT_ID) %>% 
    arrange(SUPP_DATE, by.group=T) %>% 
    mutate(gap=c(NA,as.numeric(diff(SUPP_DATE))),
           init=ifelse(row_number()==1,T,
                       ifelse(gap>=lookback,T,F))) %>% 
    ungroup() %>%
    filter(SUPP_DATE >= as.Date('2016-06-01'), init==F) %>%
    dplyr::select(PAT_ID, ITEM_CODE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH) %>%
    write_rds(file=out.file, compress='gz', version=3)
}

make.cr.initiations <- function(lookback=90,out.file='../data/modified-data/opioid-cr-initiations.rds'){
  readRDS('../data/modified-data/raw-cr-opioid-entries.rds') %>% #group_by(PAT_ID, ITEM_CODE) %>% 
    group_by(PAT_ID) %>% 
    arrange(SUPP_DATE, by.group=T) %>% 
    mutate(gap=c(NA,as.numeric(diff(SUPP_DATE))),
           init=ifelse(row_number()==1,T,
                       ifelse(gap>=lookback,T,F))) %>% 
    ungroup() %>%
    filter(SUPP_DATE >= as.Date('2016-06-01'), init==T) %>%
    dplyr::select(PAT_ID, ITEM_CODE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH) %>%
    write_rds(file=out.file, compress='gz', version=3)
}

aggregate.initiations <- function(filename='../data/modified-data/opioid-initiations.rds'){
  readRDS(filename) %>%
    group_by(ITEM_CODE,year(SUPP_DATE),month(SUPP_DATE)) %>% 
    tally() %>% 
    ungroup() %>% 
    mutate(date=as.Date(paste0(`year(SUPP_DATE)`,'-',`month(SUPP_DATE)`,'-01'))) %>% 
    dplyr::select(ITEM_CODE,date,n) %>% 
    pivot_wider(values_from=n,names_from=ITEM_CODE,values_fill=0, values_fn=sum) %>%
    arrange(date)
}

aggregate.concessional.initiations <- function(filename='../data/modified-data/opioid-initiations-by-atc.rds'){
  readRDS(filename) %>%
    group_by(ITEM_CODE,year(SUPP_DATE),month(SUPP_DATE),CONCESSIONAL) %>% 
    tally() %>% 
    ungroup() %>% 
    mutate(date=as.Date(paste0(`year(SUPP_DATE)`,'-',`month(SUPP_DATE)`,'-01'))) %>% 
    dplyr::select(ITEM_CODE,CONCESSIONAL,date,n) %>% 
    pivot_wider(values_from=n,names_from=c(ITEM_CODE,CONCESSIONAL),values_fill=0, values_fn=sum) %>%
    arrange(date)
}

make.initiations.by.atc <- function(lookback=90,
                                    out.file='../data/modified-data/opioid-initiations-by-atc.rds'){
  drug.map <- read.csv('../data/supplemental-data/PBSdrugmap.csv') %>% dplyr::select(ITEM_CODE,ATC_CODE)
  readRDS('../data/modified-data/raw-opioid-entries.rds') %>% 
    left_join(drug.map,by='ITEM_CODE') %>% #mutate(CONCESSIONAL=ifelse(CONCESSIONAL_STATUS %in% c('A','B','C'),'G','C')) %>% # C is concession
    group_by(PAT_ID, ATC_CODE) %>% 
    arrange(SUPP_DATE, by.group=T) %>% 
    mutate(gap=c(NA,as.numeric(diff(SUPP_DATE))),
           init=ifelse(row_number()==1,T,
                       ifelse(gap>=lookback,T,F))) %>% 
    ungroup() %>%
    filter(SUPP_DATE >= as.Date('2016-06-01'), init==T) %>%
    dplyr::select(PAT_ID, ITEM_CODE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH) %>% # PHARMACY_STATE, CONCESSIONAL) %>%
    write_rds(file=out.file, compress='gz', version=3)
}

make.discontinuations.by.atc <- function(lookback=90,
                                    out.file='../data/modified-data/opioid-discontinuations-by-atc.rds'){
  drug.map <- read.csv('../data/supplemental-data/PBSdrugmap.csv') %>% dplyr::select(ITEM_CODE,ATC_CODE)
  readRDS('../data/modified-data/raw-opioid-entries.rds') %>% 
    left_join(drug.map,by='ITEM_CODE') %>%
    mutate(CONCESSIONAL=ifelse(CONCESSIONAL_STATUS %in% c('A','B','C'),'G','C')) %>% # C is concession
    group_by(PAT_ID, ATC_CODE) %>% 
    arrange(desc(SUPP_DATE), by.group=T) %>% 
    mutate(gap=c(NA,as.numeric(diff(SUPP_DATE))),
           discon=ifelse(row_number()==1,T,
                       ifelse(gap<=lookback,T,F))) %>% 
    ungroup() %>%
    filter(SUPP_DATE < as.Date('2021-03-01'), discon==T) %>%
    dplyr::select(PAT_ID, ITEM_CODE, PHARMACY_STATE, SUPP_DATE, PATIENT_SEX, YEAR_BIRTH, CONCESSIONAL) %>%
    arrange(SUPP_DATE) %>%
    write_rds(file=out.file, compress='gz', version=3)
}

aggregate.initiations.by.atc <- function(){
  aggregate.initiations(filename='../data/modified-data/opioid-initiations-by-atc.rds')
}

item.switching.event <- function(target.item='08611F',lookback=90,event=as.Date('2020-06-01')){
  # Get PAT_ID's of patients dispensed target.item in the date range.
  pat.ids <- readRDS('../data/modified-data/raw-opioid-entries.rds') %>% 
    filter(ITEM_CODE==target.item,(SUPP_DATE+lookback)>=event) %>%
    distinct(PAT_ID)
  readRDS('../data/modified-data/opioid-initiations.rds') %>%
    filter(PAT_ID %in% pat.ids$PAT_ID) %>%
    group_by(ITEM_CODE,year(SUPP_DATE),month(SUPP_DATE)) %>% 
    tally() %>% 
    ungroup() %>% 
    mutate(date=as.Date(paste0(`year(SUPP_DATE)`,'-',`month(SUPP_DATE)`,'-01'))) %>% 
    dplyr::select(ITEM_CODE,date,n) %>% 
    pivot_wider(values_from=n,names_from=ITEM_CODE,values_fill=0, values_fn=sum) %>%
    arrange(date)
}

monthly.ome.of.switching.group <- function(target.item='08611F',lookback=90,event=as.Date('2020-06-01')){
  ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  # Get PAT_ID's of patients dispensed target.item in the date range.
  pat.ids <- readRDS('../data/modified-data/raw-opioid-entries.rds') %>% 
    filter(ITEM_CODE==target.item,(SUPP_DATE+lookback)>=event) %>%
    distinct(PAT_ID)
  entries <- readRDS('../data/modified-data/raw-opioid-entries.rds') %>% 
    filter(PAT_ID %in% pat.ids$PAT_ID) %>% 
    left_join(ome.conv,by='ITEM_CODE') %>%
    dplyr::select(SUPP_DATE, QTY, OME_CONV) %>%
    group_by(year(SUPP_DATE), month(SUPP_DATE)) %>%
    summarize(ome=sum(QTY*OME_CONV)) %>%
    ungroup() %>% 
    mutate(date=as.Date(paste0(`year(SUPP_DATE)`,'-',`month(SUPP_DATE)`,'-01'))) %>% 
    dplyr::select(date,ome)
  entries
}

monthly.ome.concession.split <- function(){
  ome.conv <- read.csv('../data/supplemental-data/ome-conversion-table.csv') %>% dplyr::select(ITEM_CODE,OME_CONV)
  readRDS('../data/modified-data/raw-opioid-entries.rds') %>%
    left_join(ome.conv,by='ITEM_CODE') %>%
    mutate(CONCESSIONAL=ifelse(CONCESSIONAL_STATUS %in% c('A','B','C'),'G','C')) %>% # C is concession
    group_by(ITEM_CODE, CONCESSIONAL, year(SUPP_DATE), month(SUPP_DATE)) %>%
    summarize(ome=sum(QTY*OME_CONV)) %>%
    ungroup() %>%
    mutate(date=as.Date(paste0(`year(SUPP_DATE)`,'-',`month(SUPP_DATE)`,'-01'))) %>% 
    arrange(date) %>%
    dplyr::select(date,ITEM_CODE, CONCESSIONAL, ome) %>%
    pivot_wider(names_from=c('ITEM_CODE','CONCESSIONAL'), values_from='ome', values_fill=0, values_fn=sum) %>%
    write_rds(file='../data/modified-data/monthly-ome-concession-split.rds', compress='gz', version=3)
}

