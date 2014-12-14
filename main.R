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
    dbDisconnect(con2)
    dbUnloadDriver(drv2)
  })
  cc <- stopCluster(cl)
}

getDay <- function(sites, date, params, offset){
  cat("===========================================\n")
  cat(paste("Building Table ::", date))
  cat(paste("Downloading data from",
            nrow(sites), "sites...\n"))
  
  pb <- txtProgressBar(min = 1, max = nrow(sites)+1, style = 3)
  
  cc <- foreach(i = 1) %dopar% { 
    download(site = sites[i,1],  
           params = params,
           date = date,
           offset = offset)
  }
  
  cc <- foreach(i = 2:nrow(sites)) %dopar% { 
    setTxtProgressBar(pb, i)
    result = tryCatch({
      download(site = sites[i,1],
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

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")
sites <- dbGetQuery(con, "SELECT site_no from activesites;")

full <- lapply(1:35, daysAgo)

config <- yaml.load_file("config.yaml")

require(RPostgreSQL)
require(RCurl)
require(XML)
require(doParallel)
require(yaml)
setupCluster()


getDay(sites = sites, 
       date = daysAgo(1), 
       params = config$collections$params,
       offset = config$midnight.offset.standard)






