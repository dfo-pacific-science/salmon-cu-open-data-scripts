
################################################################################
# Title: WSP Status Extraction Script
# Author: Stephen Finnis
# Date Created: November 4, 2025

# Description:
# This script extracts Wild Salmon Policy (WSP) status information
# for Pacific salmon Conservation Units (CUs) using SQL logic originally
# written for a database query. The SQL code has been adapted to run in R
# using "DuckDB".

# The source data comes from the CU_PROFILE_VW database view from NuSEDS, which
# has been exported as an Excel file ('cu_profile_vw.xlsx')

# Each SQL query targets a specific species or life history type and
# retrieves WSP Rapid and Integrated Status assessments.

# IMPORTANT:
# The original SQL code contains multiple SELECT statements.
# DuckDB (i.e, the dbGetQuery function) can only execute ONE SELECT query at a time.
# Therefore, each query is run separately and stored in its own named
# data frame for inspection and export.

# OUTPUT:
# - Each query result is saved as a named data frame (e.g., coho_status)
# - Each data frame is written to a separate CSV file (e.g., coho_status.csv)
# - Files are saved in the working directory with lowercase, underscore-separated names

# Usage:
# 1. Ensure the Excel file 'cu_profile_vw.xlsx' is in your working directory.
# 2. Run the script to generate and inspect data frames.
# 3. CSVs will be written after inspection.
################################################################################


# Install and load required packages
install.packages("DBI")
install.packages("duckdb")
install.packages("readxl")

library(dbi)
library(duckdb)
library(readxl)

# Clear the environment
rm(list = ls(all.names = TRUE))

# Create a duckdb connection
con = dbConnect(duckdb())

# Read the required Excel file into a data frame
cu_profile_df = read_excel("data/cu_profile_vw.xlsx")

# Register the data frame as a duckdb table
duckdb_register(con, "CU_PROFILE_VW", cu_profile_df)

# Define SQL queries and output names
queries = list(
  chinook_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Chinook'
    ORDER BY FULL_CU_IN ASC
  ",
  sbc_chinook_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Chinook'
      AND FULL_CU_IN IN (
        'CK-01','CK-02','CK-03','CK-04','CK-05','CK-06','CK-07','CK-08','CK-09','CK-10',
        'CK-11','CK-12','CK-13','CK-14','CK-15','CK-16','CK-17','CK-18','CK-19','CK-20',
        'CK-21','CK-22','CK-25','CK-27','CK-28','CK-29','CK-31','CK-32','CK-33','CK-34',
        'CK-35','CK-82','CK-83','CK-9005','CK-9008'
      )
    ORDER BY FULL_CU_IN ASC
  ",
  chum_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Chum'
    ORDER BY FULL_CU_IN ASC
  ",
  coho_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 2) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Coho'
    ORDER BY FULL_CU_IN ASC
  ",
  pink_even_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Pink' AND LH_TYPE = 'Even Year'
    ORDER BY FULL_CU_IN ASC
  ",
  pink_odd_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Pink' AND LH_TYPE = 'Odd Year'
    ORDER BY FULL_CU_IN ASC
  ",
  sockeye_lake_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Sockeye' AND LH_TYPE = 'Lake Type'
    ORDER BY FULL_CU_IN ASC
  ",
  sockeye_river_status = "
    SELECT CU_NAME, FULL_CU_IN, SUBSTR(FULL_CU_IN, 1, 3) AS SP_QUAL,
           LH_TYPE AS LIFE_HISTORY_TYPE, SPECIES, CU_TYPE,
           WSP_RAPID_STATUS, WSP_RAPID_CONFIDENCE,
           CASE WHEN WSP_RAPID_STATUS IS NULL THEN NULL ELSE WSP_RAPID_STATUS_YEAR END AS WSP_RAPID_STATUS_YEAR,
           INTEGRATED_STATUS, INTEGRATED_STATUS_YEAR
    FROM CU_PROFILE_VW
    WHERE SPECIES = 'Sockeye' AND LH_TYPE = 'River Type'
    ORDER BY FULL_CU_IN ASC
  "
)

# Run each query and store results in named data frames
status_results = list()
for (name in names(queries)) {
  status_results[[name]] = dbGetQuery(con, queries[[name]])
}

# Inspect results in RStudio
# Example: View(status_results$coho_status)

# Write each result to a CSV file
for (name in names(status_results)) {
  filename = file.path("output", paste0(name, ".csv"))
  write.csv(status_results[[name]], file = filename, row.names = FALSE)
}

# Disconnect from DuckDB
dbDisconnect(con, shutdown = TRUE)

