--sql to generate input files for scripts
--Run on NuSEDS PROD V2
--export three excel files and save into date folder
--Erika Anderson 2026-02-05


--Conserv Unit System Sites MV
--save as conserv_unit_system_sites_mv.xlsx

SELECT *
  FROM conserv_unit_system_sites_mv;


--Cu Profile View
--save as cu_profile_vw.xlsx

SELECT *
  FROM CU_PROFILE_VW;

--Geofeatures
--save as geo_features.xlsx

SELECT *
  FROM GEO_FEATURES;