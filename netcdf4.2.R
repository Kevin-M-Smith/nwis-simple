build.ncdf <- function(startDate, endDate){
  
  debugMode = FALSE;
  
  tc <- function(X){
    time1 <- as.POSIXct(X)
    # time1 <- fast_strptime(X, format="%Y-%m-%dT%H:%M:%OS%z")
    time0 <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
    sec <- as.numeric(difftime(time1, time0, units="secs"))
    gettextf("%.0f", sec)
  }
  
  data <- dbGetQuery(con2, paste("
SELECT 
data.ts,
data.familyid,
data.paramcd,
data.value,
data.validated
FROM 
public.data
WHERE 
data.ts >= '", startDate,"' AND 
data.ts < '", endDate, "';", sep = ""))
  
  metadata <- dbGetQuery(con2, paste("SELECT * FROM  meta_station"))
  descript <- dbGetQuery(con2, paste("SELECT * FROM  meta_param"))
  
  # nLayers
  layers <- sort(unique(data$familyid))
  nLayers <- length(layers)
  
  # nTimes
  data$ts <- tc(data$ts)
  times <- sort(unique(data$ts))
  nTimes <- length(times)
  
  # nParams
  params <- unique(data$paramcd)
  nParams <- length(params)
  
  # build padded table
   data.pad <- expand.grid(ts = times, familyid = layers, paramcd = params, stringsAsFactors = FALSE)
  #data.pad <- CJ(ts = times, familyid = layers, paramcd = params)
  
  # fill padded with data 
  data.pad <- join(data.pad, data, by = c("ts", "familyid", "paramcd"))
  
  # metadata padding (really a reorder in this revision)
   meta.pad <- join(expand.grid(familyid = layers, stringsAsFactors = FALSE), 
                   metadata,
                 by = c("familyid"))
  
  
  
  
  # free memory
  if(debugMode == FALSE) remove(data, metadata)
  
  # familyid            hidden  :: layer
  layer_dim <- ncdim_def("layer_dim", units = "", vals = 1:nLayers, create_dimvar = FALSE)
  
  # ts                  long    :: time
  ts_dim <- ncdim_def("ts_dim", units = "", vals = 1:nTimes, create_dimvar = FALSE)
  
  # time var
  timeVar <- ncvar_def("time", units = "", dim = list(ts_dim), prec = 'integer')
  
  # v#####_value          double  :: layer, time
  buildParameterVars <- function(paramcode){
    ncvar_def(paste("v", paramcode, "_value", sep = ""),
              units = "", dim = list(layer_dim, ts_dim), prec = "double")
  }
  parameterVars <- lapply(params, buildParameterVars)
  
  # v#####_valiated       boolean :: layer, time
  buildValidatedVars <- function(paramcode){
    ncvar_def(paste("v", paramcode, "_validated", sep = ""), 
              units = "", dim = list(layer_dim, ts_dim), prec = 'double')
  }
  validatedVars <- lapply(params, buildValidatedVars)
  
  dChar <- max(unlist(lapply(descript, nchar)))
  dDim <- ncdim_def("descriptChar", units = "", vals = 1:dChar, create_dimvar = FALSE)
  
  buildDescriptorVars <- function(paramcode){
    ncvar_def(paste("v", paramcode, "_description", sep = ""), 
              units = "", dim = list(dDim, layer_dim), prec = 'char')
  }
  
  descriptorVars <- lapply(params, buildDescriptorVars)
  
  
  # site_no             char    :: layer, nchar
  # station_nm          char    :: layer, nchar
  # site_tp_cd          char    :: layer, nchar
  # dec_coord_datum     char    :: layer, nchar
  # alt_datum_cd        char    :: layer, nchar
  # huc_cd              char    :: layer, nchar
  # parm_cd             char    :: layer, nchar
  # tz_cd               char    :: layer, nchar
  # agency_cd           char    :: layer, nchar
  # district_cd         char    :: layer, nchar
  # county_cd           char    :: layer, nchar
  # country_cd          char    :: layer, nchar
  # dec_lat_va          double  :: layer
  # dec_long_va         double  :: layer
  # alt_va              double  :: layer
  
  #meta.pad["familyid"] <- NULL
  
  varTypes <- lapply(meta.pad[1,], typeof)
  stringVars <- names(meta.pad)[which(varTypes == "character")]
  
  # only string vars have extra dim
  buildMetadataDims <- function(name){
    maxChar <- max(unlist(lapply(meta.pad[name], nchar)))
    ncdim_def(paste(name, "Char", sep = ""), units = "", vals = 1:maxChar, create_dimvar = FALSE )
  }
  metadataDims <- lapply(stringVars, buildMetadataDims)
  
  buildMetadataVars <- function(name){
    if(varTypes[name] == 'character'){
      ncvar_def(name, units = "", dim = list(metadataDims[[which(stringVars == name)]], layer_dim), prec = 'char')    
    } else {
      if(varTypes[name] == 'integer'){
        ncvar_def(name, units = "", dim = list(layer_dim), prec = 'integer')          
      } else {
        if(varTypes[name] == 'double'){
          ncvar_def(name, units = "", dim = list(layer_dim), prec = 'double')
        } else {
          print("ERROR WHILE BUILDING METADATAVARS")
          print(paste("(", varTypes[name], ",", name))
        }
      }
    }
  }
  metadataVars <- lapply(names(meta.pad), buildMetadataVars)
  
  
  # use verbose = TRUE to get more info on nc creation
  out <- nc_create(paste("out/", startDate, ".nc", sep = ""), vars = c(list(timeVar), parameterVars, validatedVars, descriptorVars, metadataVars))
  
  ncvar_put(out, "time", times)
  
  data.pad.value <- data.pad[,-5] 
  data.pad.valid <- data.pad[,-4]
  
  putData <- function(param_cd){
    value = t(dcast(data.pad.value, ts ~ familyid, subset = .(paramcd == param_cd)))[-1,]
    valid = t(dcast(data.pad.valid, ts ~ familyid, value.var = "validated", subset = .(paramcd == param_cd)))[-1,]
    descr <- subset(descript, parm_cd = param_cd)
    ncvar_put(out, paste("v", param_cd, "_value", sep = ""), value)
    ncvar_put(out, paste("v", param_cd, "_validated", sep = ""), valid)
    ncvar_put(out, paste("v", param_cd, "_description", sep = ""), unlist(descr))
  }
  
  cc <- lapply(params, putData)
  
  # free memory
  remove(data.pad.value, data.pad.valid, data.pad)
  
  putStationMetadata <- function(name){
    ncvar_put(out, name, vals = unlist(meta.pad[name]))
  }
  
  cc <- lapply(names(meta.pad), putStationMetadata)
  
  nc_close(out)
  
}

