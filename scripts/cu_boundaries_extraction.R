################################################################################
# Title: CU Boundary Extraction Script
# Author: Stephen Finnis
# Date Created: November 4, 2025

# Description:
# This script extracts Conservation Unit (CU) boundary metadata for Pacific
# salmon species using SQL logic originally written for a database query.
# The SQL code has been adapted to run in R using "DuckDB".

# The source data comes from the CU_PROFILE_VW database view from NuSEDS, which
# has been exported as an Excel file ('cu_profile_vw.xlsx').

# IMPORTANT:
# The original SQL code contains multiple SELECT statements.
# DuckDB (i.e., the dbGetQuery function) can only execute ONE SELECT query at a time.
# Therefore, each query is run separately and stored in its own named data frame
# for inspection and export.

# OUTPUT:
# - Each query result is saved as a named data frame (e.g., sockeye_lake_boundary)
# - Each data frame is written to a separate CSV file (e.g., sockeye_lake_boundary.csv)
# - Files are saved in the working directory with lowercase, underscore-separated names

# Usage:
# 1. Ensure the Excel file 'cu_profile_vw.xlsx' is in your working directory.
# 2. Run the script to generate and inspect data frames.
# 3. CSVs will be written after inspection.

################################################################################

# Install and Load R packages
install.packages("dbi")
install.packages("duckdb")
install.packages("readxl")

library(dbi)
library(duckdb)
library(readxl)

# Clear environment
rm(list = ls(all.names = TRUE))

# Create a DuckDB connection
con = dbConnect(duckdb())

# Read in Excel file
CU_PROFILE_DF = read_excel("data/cu_profile_vw.xlsx")

# Register data frame as table
duckdb_register(con, "CU_PROFILE_VW", CU_PROFILE_DF)

# Define queries and output file names
queries = list(
  "Chinook Salmon CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Chinook'
    ORDER BY FULL_CU_IN ASC
  ",
  "SBC Chinook Salmon CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Chinook'
    AND FULL_CU_IN IN (
      'CK-01','CK-02','CK-03','CK-04','CK-05','CK-06','CK-07','CK-08','CK-09','CK-10',
      'CK-11','CK-12','CK-13','CK-14','CK-15','CK-16','CK-17','CK-18','CK-19','CK-20',
      'CK-21','CK-22','CK-25','CK-27','CK-28','CK-29','CK-31','CK-32','CK-33','CK-34',
      'CK-35','CK-82','CK-83','CK-9005','CK-9008'
    )
    ORDER BY FULL_CU_IN ASC
  ",
  "Chum Salmon CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Chum'
    ORDER BY FULL_CU_IN ASC
  ",
  "Coho Salmon CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Coho'
    ORDER BY FULL_CU_IN ASC
  ",
  "Pink Salmon Even Year CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Pink' AND LH_TYPE='Even Year'
    ORDER BY FULL_CU_IN ASC
  ",
  "Pink Salmon Odd Year CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Pink' AND LH_TYPE='Odd Year'
    ORDER BY FULL_CU_IN ASC
  ",
  "Sockeye Salmon Lake Type CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Sockeye' AND LH_TYPE='Lake Type'
    ORDER BY FULL_CU_IN ASC
  ",
  "Sockeye Salmon River Type CU Boundary" = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL, SPECIES, CU_TYPE
    FROM CU_PROFILE_VW
    WHERE SPECIES='Sockeye' AND LH_TYPE='River Type'
    ORDER BY FULL_CU_IN ASC
  "
)

# Run each query and store as a named data frame
results = list()
for (name in names(queries)) {
  results[[name]] = dbGetQuery(con, queries[[name]])
}

# Now you can inspect each result in RStudio
# View(results$`Chinook Salmon CU Boundary`)

# Write to a CSV
for (name in names(results)) {
  filename = file.path("output", paste0(name, ".csv"))
  write.csv(results[[name]], file = filename, row.names = FALSE)
}

# Disconnect from DuckDB
dbDisconnect(con, shutdown = TRUE)