#!/usr/bin/Rscript
setupCluster <- function(){
  cl <- makePSOCKcluster(detectCores(), outfile = "")
  registerDoParallel(cl)
  
  cc <- clusterEvalQ(cl, {
    require(RPostgreSQL)
    require(RCurl)
    require(XML)
    drv2 <- dbDriver("PostgreSQL")
    con2 <- dbConnect(drv2, dbname = "postgres", user="postgres", host="localhost", password="usgs")
    source("download.R")
  })
}

closeCluster <- function(){
  cc <- clusterEvalQ(cl, {
    print(System.time())
    dbDisconnect(con2)
    dbUnloadDriver(drv2)
  })
  cc <- stopCluster(cl)
}


getDay <- function(sites, date, params, offset){
  cat("===========================================\n")
  cat(paste("Building Table ::", date, "\n"))
  
  cc <- dbGetQuery(con, paste("CREATE TABLE IF NOT EXISTS \"", date, "\"(ts timestamp with time zone NOT NULL, seriesId text NOT NULL, familyId text, value numeric, paramcd text, validated integer, imported timestamp with time zone, updated timestamp with time zone);", sep = ""))
  cat(paste("Downloading data from",
            nrow(sites), "sites...\n"))

  map <- unlist(lapply(sites, as.character))
  map <- split(map, ceiling(seq_along(map)/100))
  
  pb <- txtProgressBar(min = 1, max = length(map)+1, style = 3)
  
  print(paste("Map size: ", length(map)));

  cc <- foreach(i = 1) %dopar% { 
    setTxtProgressBar(pb, i)
    send <- paste(map[[i]], collapse=',')

	download(site = send,  
           params = params,
           date = date,
           offset = offset)
  }
 
  cc <- dbGetQuery(con, paste(
	"ALTER TABLE \"",  date, "\" SET (autovacuum_enabled = false);", sep = ""))

  cc <- foreach(i = 2:length(map)) %dopar% { 
    setTxtProgressBar(pb, i)
    
    send <- paste(map[[i]], collapse=',')
    result = tryCatch({
      download(site = send,
                params = params,
                date = date,
                offset = offset,
                delay = runif(1, 0.1, 0.6))
    }, warning = function(w) {
    }, error = function(e) {
      error <- paste("\nSite:",
                     sites[i,1],
                     "at index",
                     i,
                     "failed:",
                     e)
      
      cat(error)
    }, finally = {
    })
  }
  
  setTxtProgressBar(pb, nrow(sites)+1)
}

daysAgo<- function(x){
  return(format(Sys.Date() - x, "%Y-%m-%d"))
}

require(RPostgreSQL)
require(RCurl)
require(XML)
require(doParallel)
require(yaml)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")
sites <- dbGetQuery(con, "SELECT site_no from activesites;")

full <- lapply(1:35, daysAgo)

config <- yaml.load_file("config.yaml")

setupCluster()

yesterday = daysAgo(31)

getDay(sites = sites, 
       date = yesterday, 
       params = config$collections$params,
       offset = config$midnight.offset.standard)

source("netcdf4.2.R")

build.ncdf(yesterday)
