require(magrittr)
require(haven)
require(tibble)
require(dplyr)
require(progress)
require(readr)

#' Taken from https://www.r-bloggers.com/2010/01/progress-bars-in-r-part-ii-a-wrapper-for-apply-functions/
lapply_pb <- function(X, FUN, ...)
{
     env <- environment()
 pb_Total <- length(X)
  counter <- 0
  pb <- txtProgressBar(min = 0, max = pb_Total, style = 3)   
   # wrapper around FUN
   wrapper <- function(...){
          curVal <- get("counter", envir = env)
     assign("counter", curVal +1 ,envir=env)
        setTxtProgressBar(get("pb", envir=env), curVal +1)
        FUN(...)
         }
   res <- lapply(X, wrapper, ...)
    close(pb)
    res
}

#' Return a subset of the 10% PBS data
#' 
#' Provided a list of ATC codes, years, and the path to the yXXXXsuply.sas7bdat
#' files, this function returns a tibble of the raw dispensing entries.
#' ATC codes can be provided as regex strings and are conatenated into the following
#' regex: '(^atc1|^atc2|...|^atcN)'
#' 
#' @param atc.list A list of atc strings. Regex strings are accepted.
#' @param year.list A list of years to be used.
#' @param data.folder The path to '\\rfs.med.unsw.edu.au\PearsonData\Pearson\PBS 10% Sample\Source Data'
get.PBS10perc.subset <- function(atc.list,
                                 year.list=c(2016:2022),
                                 data.folder='/mnt/r/PBS 10% Sample/Source Data',
                                 progress.bar=T){
  
    lap <- ifelse(progress.bar, lapply_pb, lapply)

    regex <- paste0('(', paste0('^', atc.list, collapse='|'), ')')
    lap(year.list, FUN= function(year){
        print('')
        haven::read_sas(paste0(data.folder,'/y',year,'supply.sas7bdat')) %>%
            dplyr::filter(grepl(regex, ATC_CODE))
                                 }) %>% bind_rows
}

l02.meds <- c('L02BG04','L02BG03','L02BG06','L02BA01','L02BA02', 
              'L02AE03','L02AE02','L02BB03','L02AE51','L02BX02')

l01.meds <- c('L01AA01','L01AA02','L01AA03','L01AA06','L01AA09',
              'L01AB01','L01AD05','L01AX03','L01BA01','L01BA03',
              'L01BA04','L01BA05','L01BB02','L01BB03','L01BB04',
              'L01BB05','L01BC01','L01BC02','L01BC05','L01BC06',
              'L01BC07','L01BC59','L01CA01','L01CA02','L01CA04',
              'L01CB01','L01CD01','L01CD02','L01CE01','L01CE02',
              'L01DB01','L01DB03','L01DB06','L01DB07','L01EA01',
              'L01EA02','L01EA03','L01EA05','L01EB01','L01EB02',
              'L01EB03','L01EB04','L01EC01','L01EC02','L01EC03',
              'L01ED01','L01ED02','L01ED03','L01ED04','L01ED05',
              'L01EE01','L01EE02','L01EE03','L01EF01','L01EF02',
              'L01EF03','L01EG02','L01EH01','L01EJ01','L01EK01',
              'L01EL01','L01EL02','L01EM01','L01EX01','L01EX02',
              'L01EX03','L01EX07','L01EX08','L01EX09','L01EX10',
              'L01XA01','L01XA02','L01XA03','L01XC02','L01XC03',
              'L01XC06','L01XC07','L01XC08','L01XC10','L01XC11',
              'L01XC12','L01XC13','L01XC14','L01XC15','L01XC17',
              'L01XC18','L01XC19','L01XC24','L01XC26','L01XC28',
              'L01XC31','L01XC32','L01XE11','L01XG01','L01XG02',
              'L01XH01','L01XJ01','L01XJ02','L01XK01','L01XX05',
              'L01XX27','L01XX41','L01XX46','L01XX52')

sup.meds <- c('A04AA','A04AD12','A03FA01','L03AA02','L03AA13','L03AA14')

#' Replicate the data initialization process for the Covid-Cancer project
#' 
#' @param steps.to.run a numerical list of steps to run
#' * 1 - Create a dataset of raw entries for the ATC's of interest
initialize.dataset <- function(steps.to.run=c(1)){

    if(1 %in% steps.to.run){
        if(!dir.exists('../data/supply-files')){
            stop("Please run this script from the repository root")
        }

        if(!dir.exists('../data/derivatives')){
            dir.create('../data/derivatives', showWarnings=F)
        }

        get.PBS10perc.subset(atc.list= c(l01.meds, l02.meds, sup.meds),
                             year.list= c(2012:2022),
                             data.folder='../data/supply-files') %>%
                               readr::write_rds('../data/derivatives/raw-entries_l01-l02-supps_2012-2022.rds',
                               compress='gz',
                               version=3)
    }

}

drugmap.path <- '../data/supplemental/pbs-item-drug-map.csv'
drugmap.url <- 'https://www.pbs.gov.au/statistics/dos-and-dop/files/pbs-item-drug-map.csv'
download.drugmap <- function(path=drugmap.path, url=drugmap.url){
    if(!dir.exists('../data/supply-files')){
        stop("Please run this function from the repository root")
    }

    if(!dir.exists('../data/supplemental')){
        dir.create('../data/supplemental', showWarnings=F)
    }
    
    download.file(url, path)
}

load.drugmap <- function(path=drugmap.path, url=drugmap.url, update.drugmap=F){
    # if(!file.exists(path) | update.drugmap){
    #     download.drugmap(path=path, url=url)
    # }
    read.csv(path)
}
