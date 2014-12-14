#!/usr/bin/Rscript

cat("===========================================\n")
cat("		TABLE DESTROYER			\n")
cat("===========================================\n")
cat("Loading libraries...\n")

require(RPostgreSQL)

cat("===========================================\n")
cat("Dropping tables...\n")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "postgres", user="postgres", host="localhost", password="usgs")

cc <- dbGetQuery(con, "DROP TABLE IF EXISTS data;")
cc <- dbGetQuery(con, "DROP TABLE IF EXISTS activesites;")
cc <- dbGetQuery(con, "DROP TABLE IF EXISTS assets;")
cc <- dbGetQuery(con, "DROP TABLE IF EXISTS pmcodes;")
cc <- dbGetQuery(con, "DROP TABLE IF EXISTS meta_param;")
cc <- dbGetQuery(con, "DROP TABLE IF EXISTS meta_station;")

cat("===========================================\n")
cat("Cleaning up...\n")
cc <- dbDisconnect(con)
cc <- dbUnloadDriver(drv)

cat("===========================================\n")
cat("	Tables dropped. 

	Ready to `make setup`.			\n")
cat("===========================================\n")
