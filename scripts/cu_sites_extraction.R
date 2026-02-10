################################################################################
# Title: CU Sites Extraction Script
# Author: Stephen Finnis
# Date Created: November 4, 2025
# Date Modified: Feb 2026 by Erika to update names

# Description:
# This script extracts site-level information for Pacific salmon Conservation
# Units (CUs) using SQL logic originally written for a database query.
# The SQL code has been adapted to run in R using "DuckDB". The original SQL
# code is saved as a text file and found in the SQL folder.

# The source data comes from three database views from NuSEDS:
# - CONSERV_UNIT_SYSTEM_SITES_MV: contains CU site metadata
# - GEO_FEATURES: contains geographic names
# - cu_profile_vw: contains CU CU type in French
# These have been exported as Excel files ('conserv_unit_system_sites_mv.xlsx,'
#  'Geo_Features.xlsx', cu_profile_vw.xlsx).

# Each SQL query targets a specific species or life history type and retrieves
# site-level details including coordinates, watershed codes, and CU identifiers.

# Output:
# - Each query result is saved as a named data frame (e.g., CK_CU_SITES_En)
# - Each data frame is written to a separate CSV file (e.g., CK_CU_SITES_En.csv)
# - Files are saved in the output folder with Open data format names

# Important:
# The original SQL code contains multiple SELECT statements.
# DuckDB (i.e., the dbGetQuery function) can only execute ONE SELECT query at a time.
# Therefore, each query is run separately and stored in its own named data frame
# for inspection and export.

# Usage:
# 1. Ensure the Excel files 'conserv_unit_system_sites_mv.xlsx',
#    'Geo_Features.xlsx' and CU_PROFILE_VW.xlxs are in your working directory.
# 2. Run the script to generate and inspect data frames.
# 3. CSVs will be written to the output folder
################################################################################


# Install and load required packages
#install.packages("DBI")
#install.packages("duckdb")
#install.packages("readxl")

library(DBI)
library(duckdb)
library(readxl)

# Clear the environment
rm(list = ls(all.names = TRUE))

# Create a DuckDB connection
con = dbConnect(duckdb())

# force three geo_feature columns to text to reduce warnings
xlsx_path <- "data/geo_features.xlsx"       # <-- update path
sheet     <- NULL                      # or a sheet name/index if needed

# Read just the header to get column names
hdr <- read_excel(xlsx_path, sheet = sheet, n_max = 0)
n   <- ncol(hdr)

# Start with 'guess' for all columns
col_types <- rep("guess", n)
names(col_types) <- names(hdr)

# Columns to force as text (by name)
force_text_names <- c("SOURCE_TYP", "CREATED_BY", "SOURCE", "PURPOSE",
                      "ABBREV", "GCL_NME", "DFO_AREA_FR")


# Validate names exist in the file (warn if any are missing)
missing <- setdiff(force_text_names, names(hdr))
if (length(missing)) {
  warning(
    "These columns were not found in the Excel header and will not be forced to text: ",
    paste(missing, collapse = ", ")
  )
}


# Force matching columns to 'text'
col_types[names(col_types) %in% force_text_names] <- "text"

# Read Excel files into data frames
# geo_features with three columns explicitedly as text to reduce warnings
conserv_units_system_sites_df = read_excel("data/conserv_unit_system_sites_mv.xlsx")
geo_features_df = read_excel(xlsx_path, sheet = sheet, col_types = unname(col_types))
cu_profile_df = read_excel("data/cu_profile_vw.xlsx")

# Register data frames as DuckDB tables
duckdb_register(con, "CONSERV_UNIT_SYSTEM_SITES_MV", conserv_units_system_sites_df)
duckdb_register(con, "GEO_FEATURES", geo_features_df)
duckdb_register(con, "CU_PROFILE_VW", cu_profile_df)

# Define SQL queries and output names
queries = list(
  CK_CU_SITES_En = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'CK'
    ORDER BY FULL_CU_IN ASC
  ",
  CK_SBC_CU_SITES_En = "
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
  CM_CU_SITES_En = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'CM'
    ORDER BY FULL_CU_IN ASC
  ",
  CO_CU_SITES_En = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'CO'
    ORDER BY FULL_CU_IN ASC
  ",
  PKE_CU_SITES_En = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'PKE'
    ORDER BY FULL_CU_IN ASC
  ",
  PKO_CU_SITES_En = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'PKO'
    ORDER BY FULL_CU_IN ASC
  ",
  SEL_CU_SITES_En = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'SEL'
    ORDER BY FULL_CU_IN ASC
  ",
  SER_CU_SITES_En = "
    SELECT CUSS.SYSTEM_SITE AS CENSUS_SITE, GFE.GAZETTED_NME AS GAZ_NAME,
           CUSS.GFE_ID, CUSS.Y_LAT, CUSS.X_LONGT AS X_LONG, CUSS.GFE_TYPE,
           CUSS.FWA_WATERSHED_CDE, CUSS.WATERSHED_CDE AS WS_CDE_50K,
           CUSS.CU_NAME, CUSS.FULL_CU_IN, CUSS.SPECIES_QUALIFIED AS SP_QUAL,
           CUSS.CU_TYPE, CUSS.POP_ID, CUSS.FAZ_ACRO, CUSS.MAZ_ACRO, CUSS.JAZ_ACRO
    FROM CONSERV_UNIT_SYSTEM_SITES_MV CUSS, GEO_FEATURES GFE
    WHERE CUSS.GFE_ID = GFE.ID AND CUSS.SPECIES_QUALIFIED = 'SER'
    ORDER BY FULL_CU_IN ASC
  ",

  CK_CU_SITES_Fr = "
  SELECT
      CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
      GFE.GAZETTED_NME AS NOM_GAZETTÉ,
      CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
      CUSS.Y_LAT AS LATITUDE_Y,
      CUSS.X_LONGT AS LONGITUDE_X,
      CASE
              WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
              WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
              WHEN GFE_TYPE = 'Slough' THEN 'marigot'
              WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
              WHEN GFE_TYPE = 'Lake' THEN 'lac'
              WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
              WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
              WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
              ELSE GFE_TYPE
          END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
      CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
      CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
      CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
      CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
      CUSS.SPECIES_QUALIFIED AS SP_QUAL,
      CUPV.CU_TYPE_FR AS TYPE_UC,
      CUSS.POP_ID AS ID_DE_LA_POPULATION,
      CUSS.FAZ_ACRO AS ACRO_ZAEU,
      CUSS.MAZ_ACRO AS ACRO_ZAM,
      CUSS.JAZ_ACRO AS ACRO_ZAC
    FROM
      CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
      GEO_FEATURES GFE,
      CU_PROFILE_VW CUPV
      WHERE
      CUSS.GFE_ID=GFE.ID
      AND
      CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
      AND
      CUSS.SPECIES_QUALIFIED='CK'
      ORDER BY CUSS.FULL_CU_IN ASC;",

  CK_SBC_CU_SITES_Fr = "
  SELECT
  CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
  GFE.GAZETTED_NME AS NOM_GAZETTÉ,
  CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
  CUSS.Y_LAT AS LATITUDE_Y,
  CUSS.X_LONGT AS LONGITUDE_X,
  CASE
  WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
  WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
  WHEN GFE_TYPE = 'Slough' THEN 'marigot'
  WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
  WHEN GFE_TYPE = 'Lake' THEN 'lac'
  WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
  WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
  WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
  ELSE GFE_TYPE
  END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
  CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
  CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
  CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
  CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
  CUSS.SPECIES_QUALIFIED AS SP_QUAL,
  CUPV.CU_TYPE_FR AS TYPE_UC,
  CUSS.POP_ID AS ID_DE_LA_POPULATION,
  CUSS.FAZ_ACRO AS ACRO_ZAEU,
  CUSS.MAZ_ACRO AS ACRO_ZAM,
  CUSS.JAZ_ACRO AS ACRO_ZAC
FROM
  CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
  GEO_FEATURES GFE,
  CU_PROFILE_VW CUPV
WHERE
  CUSS.GFE_ID=GFE.ID
  AND
  CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
  AND
  CUSS.SPECIES_QUALIFIED='CK'
  AND
  CUSS.FULL_CU_IN IN ('CK-01',
                      'CK-02',
                      'CK-03',
                      'CK-04',
                      'CK-05',
                      'CK-06',
                      'CK-07',
                      'CK-08',
                      'CK-09',
                      'CK-10',
                      'CK-11',
                      'CK-12',
                      'CK-13',
                      'CK-14',
                      'CK-15',
                      'CK-16',
                      'CK-17',
                      'CK-18',
                      'CK-19',
                      'CK-20',
                      'CK-21',
                      'CK-22',
                      'CK-25',
                      'CK-27',
                      'CK-28',
                      'CK-29',
                      'CK-31',
                      'CK-32',
                      'CK-33',
                      'CK-34',
                      'CK-35',
                      'CK-82',
                      'CK-83',
                      'CK-9005',
                      'CK-9008')
  ORDER BY CUSS.FULL_CU_IN ASC;",

    CM_CU_SITES_Fr = "
  SELECT
      CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
      GFE.GAZETTED_NME AS NOM_GAZETTÉ,
      CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
      CUSS.Y_LAT AS LATITUDE_Y,
      CUSS.X_LONGT AS LONGITUDE_X,
      CASE
              WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
              WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
              WHEN GFE_TYPE = 'Slough' THEN 'marigot'
              WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
              WHEN GFE_TYPE = 'Lake' THEN 'lac'
              WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
              WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
              WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
              ELSE GFE_TYPE
          END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
      CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
      CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
      CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
      CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
      CUSS.SPECIES_QUALIFIED AS SP_QUAL,
      CUPV.CU_TYPE_FR AS TYPE_UC,
      CUSS.POP_ID AS ID_DE_LA_POPULATION,
      CUSS.FAZ_ACRO AS ACRO_ZAEU,
      CUSS.MAZ_ACRO AS ACRO_ZAM,
      CUSS.JAZ_ACRO AS ACRO_ZAC
    FROM
      CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
      GEO_FEATURES GFE,
      CU_PROFILE_VW CUPV
    WHERE
      CUSS.GFE_ID=GFE.ID
      AND
      CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
      AND
      CUSS.SPECIES_QUALIFIED='CM'
      ORDER BY CUSS.FULL_CU_IN ASC;",

  CO_CU_SITES_Fr = "
  SELECT
      CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
      GFE.GAZETTED_NME AS NOM_GAZETTÉ,
      CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
      CUSS.Y_LAT AS LATITUDE_Y,
      CUSS.X_LONGT AS LONGITUDE_X,
      CASE
              WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
              WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
              WHEN GFE_TYPE = 'Slough' THEN 'marigot'
              WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
              WHEN GFE_TYPE = 'Lake' THEN 'lac'
              WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
              WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
              WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
              ELSE GFE_TYPE
          END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
      CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
      CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
      CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
      CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
      CUSS.SPECIES_QUALIFIED AS SP_QUAL,
      CUPV.CU_TYPE_FR AS TYPE_UC,
      CUSS.POP_ID AS ID_DE_LA_POPULATION,
      CUSS.FAZ_ACRO AS ACRO_ZAEU,
      CUSS.MAZ_ACRO AS ACRO_ZAM,
      CUSS.JAZ_ACRO AS ACRO_ZAC
    FROM
      CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
      GEO_FEATURES GFE,
      CU_PROFILE_VW CUPV
    WHERE
      CUSS.GFE_ID=GFE.ID
      AND
      CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
      AND
      CUSS.SPECIES_QUALIFIED='CO'
      ORDER BY CUSS.FULL_CU_IN ASC;",

  # PKE_CU_SITES_Fr is not working
  PKE_CU_SITES_Fr = "
  SELECT
      CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
      GFE.GAZETTED_NME AS NOM_GAZETTÉ,
      CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
      CUSS.Y_LAT AS LATITUDE_Y,
      CUSS.X_LONGT AS LONGITUDE_X,
      CASE
              WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
              WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
              WHEN GFE_TYPE = 'Slough' THEN 'marigot'
              WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
              WHEN GFE_TYPE = 'Lake' THEN 'lac'
              WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
              WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
              WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
              ELSE GFE_TYPE
          END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
      CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
      CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
      CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
      CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
      CUSS.SPECIES_QUALIFIED AS SP_QUAL,
      CUPV.CU_TYPE_FR AS TYPE_UC,
      CUSS.POP_ID AS ID_DE_LA_POPULATION,
      CUSS.FAZ_ACRO AS ACRO_ZAEU,
      CUSS.MAZ_ACRO AS ACRO_ZAM,
      CUSS.JAZ_ACRO AS ACRO_ZAC
    FROM
      CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
      GEO_FEATURES GFE,
      CU_PROFILE_VW CUPV
    WHERE
      CUSS.GFE_ID=GFE.ID
      AND
      CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
      AND
      CUSS.SPECIES_QUALIFIED='PKE'
      ORDER BY CUSS.FULL_CU_IN ASC;",

  # PKO_CU_SITES_Fr not working
  PKO_CU_SITES_Fr = "
  SELECT
    CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
    GFE.GAZETTED_NME AS NOM_GAZETTÉ,
    CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
    CUSS.Y_LAT AS LATITUDE_Y,
    CUSS.X_LONGT AS LONGITUDE_X,
    CASE
            WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
            WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
            WHEN GFE_TYPE = 'Slough' THEN 'marigot'
            WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
            WHEN GFE_TYPE = 'Lake' THEN 'lac'
            WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
            WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
            WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
            ELSE GFE_TYPE
        END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
    CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
    CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
    CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
    CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
    CUSS.SPECIES_QUALIFIED AS SP_QUAL,
    CUPV.CU_TYPE_FR AS TYPE_UC,
    CUSS.POP_ID AS ID_DE_LA_POPULATION,
    CUSS.FAZ_ACRO AS ACRO_ZAEU,
    CUSS.MAZ_ACRO AS ACRO_ZAM,
    CUSS.JAZ_ACRO AS ACRO_ZAC
  FROM
    CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
    GEO_FEATURES GFE,
    CU_PROFILE_VW CUPV
  WHERE
    CUSS.GFE_ID=GFE.ID
    AND
    CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
    AND
    CUSS.SPECIES_QUALIFIED='PKO'
    ORDER BY CUSS.FULL_CU_IN ASC;",

  # SEL_CU_SITES_Fr not working
  SEL_CU_SITES_Fr = "
  SELECT
      CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
      GFE.GAZETTED_NME AS NOM_GAZETTÉ,
      CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
      CUSS.Y_LAT AS LATITUDE_Y,
      CUSS.X_LONGT AS LONGITUDE_X,
      CASE
              WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
              WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
              WHEN GFE_TYPE = 'Slough' THEN 'marigot'
              WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
              WHEN GFE_TYPE = 'Lake' THEN 'lac'
              WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
              WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
              WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
              ELSE GFE_TYPE
          END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
      CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
      CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
      CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
      CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
      CUSS.SPECIES_QUALIFIED AS SP_QUAL,
      CUPV.CU_TYPE_FR AS TYPE_UC,
      CUSS.POP_ID AS ID_DE_LA_POPULATION,
      CUSS.FAZ_ACRO AS ACRO_ZAEU,
      CUSS.MAZ_ACRO AS ACRO_ZAM,
      CUSS.JAZ_ACRO AS ACRO_ZAC
      FROM
      CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
      GEO_FEATURES GFE,
      CU_PROFILE_VW CUPV
      WHERE
      CUSS.GFE_ID=GFE.ID
      AND
      CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
      AND
      CUSS.SPECIES_QUALIFIED='SEL'
      ORDER BY CUSS.FULL_CU_IN ASC;",

  # SER_CU_SITES_Fr not working
  SER_CU_SITES_Fr = "
  SELECT
      CUSS.SYSTEM_SITE_FR AS SITE_DE_DÉNOMBREMENT,
      GFE.GAZETTED_NME AS NOM_GAZETTÉ,
      CUSS.GFE_ID AS ID_SITE_DE_DÉNOMBREMENT,
      CUSS.Y_LAT AS LATITUDE_Y,
      CUSS.X_LONGT AS LONGITUDE_X,
      CASE
              WHEN GFE_TYPE = 'Stream Segment' THEN 'segment de cours d’eau'
              WHEN GFE_TYPE = 'Artificial Channel' THEN 'chenal artificiel'
              WHEN GFE_TYPE = 'Slough' THEN 'marigot'
              WHEN GFE_TYPE = 'Stream Aggregate' THEN 'agrégat de cours d’eau'
              WHEN GFE_TYPE = 'Lake' THEN 'lac'
              WHEN GFE_TYPE = 'Slough Segment' THEN 'segment de marigot'
              WHEN GFE_TYPE = 'Stream' THEN 'cours d''eau'
              WHEN GFE_TYPE = 'Lake Portion' THEN ' partie du lac'
              ELSE GFE_TYPE
          END AS TYPE_DE_CARACTÉRISTIQUE_GÉOLOGIQUE,
      CUSS.FWA_WATERSHED_CDE AS CODE_DE_BASSIN_VERSANT_FWA,
      CUSS.WATERSHED_CDE AS CODE_DU_BASSIN,
      CUPV.CU_NAME_FR AS 'NOM_DE_L’UC',
      CUSS.FULL_CU_IN AS INDEX_COMPLET_DES_UNITÉS_DE_CONSERVATION,
      CUSS.SPECIES_QUALIFIED AS SP_QUAL,
      CUPV.CU_TYPE_FR AS TYPE_UC,
      CUSS.POP_ID AS ID_DE_LA_POPULATION,
      CUSS.FAZ_ACRO AS ACRO_ZAEU,
      CUSS.MAZ_ACRO AS ACRO_ZAM,
      CUSS.JAZ_ACRO AS ACRO_ZAC
    FROM
      CONSERV_UNIT_SYSTEM_SITES_MV CUSS,
      GEO_FEATURES GFE,
      CU_PROFILE_VW CUPV
    WHERE
      CUSS.GFE_ID=GFE.ID
      AND
      CUSS.FULL_CU_IN=CUPV.FULL_CU_IN
      AND
      CUSS.SPECIES_QUALIFIED='SER'
      ORDER BY CUSS.FULL_CU_IN ASC;
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
