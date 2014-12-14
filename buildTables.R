#!/usr/bin/Rscript
cat("===========================================\n")
cat("		TABLE BUILDER			\n")
cat("===========================================\n")
cat("Loading libraries...\n")
require(RPostgreSQL)
require(dataRetrieval)
require(doParallel)
require(plyr)

#################################
#	Connecting To Database
#################################
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")
cat("===========================================\n")
cat("Connecting to database...\n")

#################################
#	Connect To Cluster
#################################
cat("===========================================\n")
cat("Setting up cluster...\n")
cl <- makePSOCKcluster(detectCores(), outfile = "")
registerDoParallel(cl)
cc <- clusterEvalQ(cl, {
	require(dataRetrieval)
	require(RPostgreSQL)
	require(plyr)
	drv2 <- dbDriver("PostgreSQL")
	con2 <- dbConnect(drv2, dbname = "postgres", user = "postgres", host = "localhost", password = "usgs")
	source("download.site.filesFn.R")
	})

###########################################
#	Create Table of Active Sites
###########################################
cat("===========================================\n")
cat("Downloading list of active sites...\n")
pb <- txtProgressBar(min = 1, max = 49, style = 3)

cc <- foreach(i = 1) %dopar% { download.site.file(state.abb[i]) }

cc <- foreach(i = 2:50) %dopar% {
		setTxtProgressBar(pb, i)
		download.site.file(state.abb[i]) 
	}

setTxtProgressBar(pb, 49)


###########################################
#	Create Table of Site Inventories
###########################################

sites <- dbGetQuery(con, "SELECT site_no from activesites;")
cat(paste("\nDownloading site inventories for",
	nrow(sites), "sites...\n"))


map <- unlist(lapply(sites, as.character))
map <- split(map, ceiling(seq_along(map)/50))

pb <- txtProgressBar(min = 1, max = length(map), style = 3)

cc <- foreach(i = 1) %dopar% { 
		if(length(map[[i]]) > 1){
			send <- paste(map[[i]], collapse=',')
			download.site.inventory(send)
		} else {
			download.site.inventory(map[[i]])
		}
 }

cc <- foreach(i = 2:length(map)) %dopar% { 
	setTxtProgressBar(pb, i)
	result = tryCatch({
		if(length(map[[i]]) > 1){
			send <- paste(map[[i]], collapse=',')
			download.site.inventory(send)
		} else {
			download.site.inventory(map[[i]])
		} 
	}, warning = function(w) {
	}, error = function(e) {
	    cat(paste("Site:",
		sites[i,1],
		"at index",
		i,
		"failed:",
		e)
	    )
	}, finally = {
	})
}

setTxtProgressBar(pb, length(map))

###########################################
#	Create Parameter Code Table
###########################################
cat("\nBuilding parameter code table...\n")
res <- importRDB1("support/pmcodes")
cc <- dbWriteTable(con, "pmcodes", res)

##########################################
#	Site Metadata
##########################################
cat("===========================================\n")
cat("Building site metadata table...\n")

cc <- dbGetQuery(con, "SELECT DISTINCT
  a.familyid,
  a.site_no,
  a.dd_nu,
  a.station_nm,
  a.site_tp_cd,
  a.dec_lat_va,
  a.dec_long_va,
  a.dec_coord_datum_cd,
  a.alt_va,
  a.alt_datum_cd,
  a.huc_cd,
  b.tz_cd,
  b.agency_cd,
  b.district_cd,
  b.county_cd,
  b.country_cd
INTO meta_station
FROM 
  public.assets a
LEFT OUTER JOIN 
  public.activesites b
ON
  (a.site_no = b.site_no);")
  
##########################################
#	Site Metadata
##########################################
cat("===========================================\n")
cat("Building variable metadata table...\n")

cc <- dbGetQuery(con, "SELECT
  a.familyid,
  a.parm_cd,
  a.loc_web_ds
INTO meta_param
FROM 
  public.assets a
LEFT OUTER JOIN 
  public.activesites b
ON
  (a.site_no = b.site_no);")

cat("===========================================\n")
cat("Cleaning up connections...\n")
cc <- dbDisconnect(con)
cc <- dbUnloadDriver(drv)
cc <- clusterEvalQ(cl, {
        dbDisconnect(con2)
        dbUnloadDriver(drv2)
})
cc <- stopCluster(cl)

cat("===========================================\n")
cat("	Table building complete. 

	Ready to `make bootstrap`.		\n")
cat("===========================================\n")

