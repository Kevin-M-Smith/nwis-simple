#!/usr/bin/Rscript
cat("===========================================\n")
cat("		BOOTSTRAPPER			\n")
cat("===========================================\n")
cat("Loading libraries...\n")
require(RPostgreSQL)
require(RCurl)
require(XML)
require(doParallel)

cat("===========================================\n")
cat("Connecting to database...\n")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")
sites <- dbGetQuery(con, "SELECT site_no from activesites;")


cat("===========================================\n")
cat("Setting up cluster...\n")
cl <- makePSOCKcluster(detectCores(), outfile = "")
registerDoParallel(cl)

cc <- clusterEvalQ(cl, {
        require(RPostgreSQL)
        require(RCurl)
        require(XML)
        drv2 <- dbDriver("PostgreSQL")
        con2 <- dbConnect(drv2, dbname = "postgres", user="postgres", host="localhost", password="usgs")
        source("bootstrapFn.R")
})

cat("===========================================\n")
cat(paste("Bootstrapping",
	nrow(sites), "sites...\n"))

cc <- dbGetQuery(con, "DROP TABLE BootstrapErrors;")
cc <- dbGetQuery(con, "CREATE TABLE BootstrapErrors (site_no text, error text, file text);")

pb <- txtProgressBar(min = 1, max = nrow(sites), style = 3)

cc <- foreach(i = 1) %dopar% { 
	bootstrap(sites[i,1])
 }

cc <- foreach(i = 2:nrow(sites)) %dopar% { 
	setTxtProgressBar(pb, i)
	result = tryCatch({
		bootstrap(sites[i,1], delay = runif(1, 0.1, 0.6))
	}, warning = function(w) {
	}, error = function(e) {
		error <- paste("\nSite:",
		sites[i,1],
		"at index",
		i,
		"failed:",
		e)

		cat(error)
	   	cc <- dbGetQuery(con2, 
			paste("INSERT INTO BootstrapErrors VALUES ('", 
				sites[i,1], "', '", 
				error, "', '", 
				g.value(), "');", sep = "")
			)
	}, finally = {
	})
}

setTxtProgressBar(pb, nrow(sites))

cat(“\nCleaning up connections...\n")
cc <- clusterEvalQ(cl, {
        dbDisconnect(con2)
        dbUnloadDriver(drv2)
})
cc <- stopCluster(cl)
cc <- dbDisconnect(con)
cc <- dbUnloadDriver(drv)


cat("===========================================\n")
cat("	Bootstrapping complete. 		 

	Ready to `make buildall` 		\n”)
cat("===========================================\n")
