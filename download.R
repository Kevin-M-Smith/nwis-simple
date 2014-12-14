download <- function(site, params, date, offset, delay = 0.1){ 
  
  Sys.sleep(delay);
  
  g = basicTextGatherer() 
  
  url = "http://waterservices.usgs.gov/nwis/iv/?format=waterml,1.1"
  url = paste(url, "&parameterCd=", params, sep = "")
  url = paste(url, "&sites=", site, sep = "")  
  url = paste(url, "&startDT=", date, "T00:00:00", offset, sep = "")
  url = paste(url, "&endDT=", date, "T23:59:59", offset, sep = "")
    
  xml = curlPerform(url = url, writefunction = g$update, httpheader = c(AcceptEncoding="gzip,deflate")) 
  
  doc <- xmlTreeParse(g$value(), getDTD = FALSE, useInternalNodes = TRUE) 
  doc <- xmlRoot(doc) 
  
  vars <- xpathApply(doc, "//ns1:timeSeries") 
  now <- format(Sys.time(), "%FT%T%z") 
  
  valid <- function(x){
    if(x == "P") 0 else 1
  }
  
  if(length(vars) > 0){
    for (i in 1:length(vars)){ 
      parent <- xmlDoc(vars[[i]]) 
      parent <- xmlRoot(parent) 
      parentName <- unlist(xpathApply(parent, "//ns1:timeSeries/@name")) 
      sensors <- xpathApply(parent, "//ns1:values") 
      parameter <- xpathApply(parent, "//ns1:variableCode", xmlValue)
      familyName <- paste(unlist(strsplit(parentName, ":", fixed = TRUE))[-3], collapse = ":")
      for (j in 1:length(sensors)){ 
        child <- xmlDoc(sensors[[j]]) 
        child <- xmlRoot(child) 
        if(!is.null(unlist(xpathApply(child, "//@dateTime")))){
          childName <- unlist(xpathApply(child, "//ns1:method/@methodID")) 
          childName <- formatC(strtoi(childName), width = 5, format = "d", flag = "0")  
          
          res <- data.frame( 
            unlist(xpathApply(child, "//@dateTime")), 
            paste(parentName, ":", childName, sep = ""), 
            paste(familyName, ":", childName, sep = ""),
            unlist(xpathApply(child, "//ns1:value", xmlValue)),
            parameter, 
            unlist(lapply(xpathApply(child, "//@qualifiers"), valid)), 
            now, 
            now 
          ) 

          
          colnames(res) <- c("ts", "seriesid", "familyid", "value", "paramcd", "validated", "imported", "updated") 
          cc <- dbWriteTable(con2, date, res, append = TRUE, row.names = FALSE, overwrite = FALSE) 
        }
      } 
    }
  }   
} 
