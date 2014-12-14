download.site.file <- function(state){
  #print(paste("Collecting active sites from ", state, "...", sep = ""))
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=",
               state,
               "&period=P5W&siteOutput=expanded&hasDataTypeCd=iv,id",
               sep = "")

  active <- importRDB1(url)
  dbWriteTable(con2, "activesites", active, append = TRUE, row.names = FALSE, overwrite = FALSE)

  #pathToFile <- tempfile()
  #download.file(url, pathToFile)
  #active <- importRDB(pathToFile)
}

download.site.inventory <- function(site){
  url <- paste("http://waterservices.usgs.gov/nwis/site/?format=rdb,1.0&sites=",
               site,
               "&seriesCatalogOutput=true&outputDataTypeCd=iv",
               sep = "")

  active <- importRDB1(url)
  active <- transform(active, seriesid = paste(
	agency_cd, ":", site_no, ":", parm_cd, ":00011:", 
  	formatC(dd_nu, width = 5, format = "d", flag = "0"), sep = ""),
  familyid = paste(agency_cd, ":", site_no, ":00011:", formatC(dd_nu, width = 5, format = "d", flag = "0"), sep = ""))
  
  if(nrow(active) > 0){
  	dbWriteTable(con2, "assets", active, append = TRUE, row.names = FALSE, overwrite = FALSE)
  }
}	
