################################################################################
# Title: CU Sites Extraction Script
# Author: Stephen Finnis
# Date Created: November 4, 2025

# Description:
# This script extracts site-level information for Pacific salmon Conservation
# Units (CUs) using SQL logic originally written for a database query.
# The SQL code has been adapted to run in R using "DuckDB". The original SQL
# code is saved as a text file and found in the SQL folder.

# The source data comes from two database views from NuSEDS:
# - CONSERV_UNIT_SYSTEM_SITES_MV: contains CU site metadata
# - GEO_FEATURES: contains geographic names
# These have been exported as Excel files ('conserv_unit_system_sites_mv.xlsx'
# and 'Geo_Features.xlsx').

# Each SQL query targets a specific species or life history type and retrieves
# site-level details including coordinates, watershed codes, and CU identifiers.

# Output:
# - Each query result is saved as a named data frame (e.g., coho_sites)
# - Each data frame is written to a separate CSV file (e.g., coho_sites.csv)
# - Files are saved in the output folder with lowercase, underscore-separated names

# Important:
# The original SQL code contains multiple SELECT statements.
# DuckDB (i.e., the dbGetQuery function) can only execute ONE SELECT query at a time.
# Therefore, each query is run separately and stored in its own named data frame
# for inspection and export.

# Usage:
# 1. Ensure the Excel files 'conserv_unit_system_sites_mv.xlsx' and
#    'Geo_Features.xlsx' are in your working directory.
# 2. Run the script to generate and inspect data frames.
# 3. CSVs will be written to the output folder
################################################################################


# Install and load required packages
install.packages("dbi")
install.packages("duckdb")
install.packages("readxl")

library(dbi)
library(duckdb)
library(readxl)

# Clear the environment
rm(list = ls(all.names = TRUE))

# Create a DuckDB connection
con = dbConnect(duckdb())

# Read Excel files into data frames
conserv_units_system_sites_df = read_excel("data/conserv_unit_system_sites_mv.xlsx")
geo_features_df = read_excel("data/Geo_Features.xlsx")

# Register data frames as DuckDB tables
duckdb_register(con, "CONSERV_UNIT_SYSTEM_SITES_MV", conserv_units_system_sites_df)
duckdb_register(con, "GEO_FEATURES", geo_features_df)

# Define SQL queries and output names
queries = list(
  chinook_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'CK'
    ORDER BY FULL_CU_IN ASC
  ",
  sbc_chinook_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'CK'
      AND FULL_CU_IN IN (
        'CK-01','CK-02','CK-03','CK-04','CK-05','CK-06','CK-07','CK-08','CK-09','CK-10',
        'CK-11','CK-12','CK-13','CK-14','CK-15','CK-16','CK-17','CK-18','CK-19','CK-20',
        'CK-21','CK-22','CK-25','CK-27','CK-28','CK-29','CK-31','CK-32','CK-33','CK-34',
        'CK-35','CK-82','CK-83','CK-9005','CK-9008'
      )
    ORDER BY FULL_CU_IN ASC
  ",
  chum_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'CM'
    ORDER BY FULL_CU_IN ASC
  ",
  coho_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'CO'
    ORDER BY FULL_CU_IN ASC
  ",
  pink_even_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'PKE'
    ORDER BY FULL_CU_IN ASC
  ",
  pink_odd_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'PKO'
    ORDER BY FULL_CU_IN ASC
  ",
  sockeye_lake_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'SEL'
    ORDER BY FULL_CU_IN ASC
  ",
  sockeye_river_sites = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'SER'
    ORDER BY FULL_CU_IN ASC
  "
)

# Run each query and store results in named data frames
site_results = list()
for (name in names(queries)) {
  site_results[[name]] = dbGetQuery(con, queries[[name]])
}


# Inspect results in RStudio
# View(site_results$coho_sites)

# Write each to CSV
for (name in names(site_results)) {
  filename = file.path("output", paste0(name, ".csv"))
  write.csv(site_results[[name]], file = filename, row.names = FALSE)
}

# Disconnect from DuckDB
dbDisconnect(con, shutdown = TRUE)
