# Extracting California/CIMIS OpenET Monthly Data for CADWR target regions (basins and counties)

## Data Extraction Tools
The extraction tools (`cadwr_gw_extract_ag_lands.py` and `cadwr_gw_extract_ag_lands.py`) should be run first to generate the CSV data files for each OpenET model monthly image.  By default, all data from 2003-10-10 to 2025-12-31 will be included in the extraction. These files are written into the respective `csv_ag_lands` and `csv_all_lands` main folders and then separated by model.  A separate CSV file is generated for each model and monthly image date. 

* For the "all_lands" extraction tool, data from all models except SIMS is used and no masking is applied.
* For the "ag_lands" extraction, data from all models was used but the 2024 California Statewide Crop Mapping (https://data.cnra.ca.gov/dataset/statewide-crop-mapping) mask was applied to only include agricultural pixels. For the crop map, all features except those labeled as "Urban" were included.

## Combining Data
After the individual csv files have been generated, the `cadwr_combine_csv.py` tool can be run to combine the CSV files by model and to generate a single CSV containing all models and dates.  These files are saved in the `csv_ag_lands` and `csv_all_lands` folders. 

## Saving extracted data to shapefiles
The `stitch_to_gw_shapefile.py` script combines the tabular output with the basin geometery and saves each model's data to a separate shapefile. These files are written into the respective `shapefile_gw_basin_ag_lands`, `shapefile_gw_basin_all_lands`, `shapefile_counties_ag_lands`, & `shapefile_counties_all_lands` main folders and then separated by model and statistic.

To run the `stitch_to_gw_shapefile.py` python script, use the 4 parameters:
- shp-file  : feature boundaries
- et-csv    : et timeseries
- merge-key : the column that the shapefile and et data use (must be identical)
- out   : output directory

Example to run:
```python
python stitch_to_gw_shapefile.py --shp-file ./ca_counties/CA_Counties.shp --et-csv ./csv_county_ag_lands/county_ag_lands_all_models.csv --merge-key NAME --out ./shapefile_counties_ag_lands
```
## Metadata 
* The shapefiles are formatted such that outputs are saved as 'MODELNAME_STAT.shp'
  * 'MODELNAME' refers to individual OpenET models or the ensemble ET value.
  * 'STAT' represents the individual statistics for each groundwater basin or county. The statistics are:
    * mean: average ET depth 
    * median: median ET depth
    * pct25: 25th percentile of ET depth
    * pct75: 75th percentile of ET depth
    * per_cov: percent of masked area with valid values (higher values have more coverage)
    * stdev: standard deviation of ET depth values within each aggregation unit
    * vol: volume of ET in units of acre-ft for each unit
* The attribute tables of each shapefile include:
  * Each table contains information on the county or groundwater basin name ('NAME'), area ('area_sq_me', 'area_acres'), masked area ('max_mask_a') and monthly data for each statistic.
  * Monthly data in each statistic can be interpretted from the column name. For example, 'ENSVOL0503' indicates the OpenET ensemble ('ENS') volume ('VOL') for 2005 ('05') in March ('03').
