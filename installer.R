#!/usr/bin/Rscript

pkgs = c("RPostgreSQL",
	"XML",
	"RCurl",
	"dataRetrieval",
	"doParallel",
	"lubridate",
	"ncdf4",
	"data.table",
	"yaml")

install.packages(pkgs, repos = "http://cran.us.r-project.org", type = "source")
