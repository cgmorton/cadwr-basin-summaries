# Tools for extracting California/CIMIS OpenET Monthly Data for CADWR Groundwater Basins

The extraction tools (`cadwr_gw_extract_ag_lands.py` and `cadwr_gw_extract_ag_lands.py`) should be run first to generate the CSV data files for each OpenET model monthly image.  These files are written into the respective `csv_ag_lands` and `csv_all_lands` main folders and then separated by model.  A separate CSV file is generated for each model and monthly image date.  

By default, all data from 2003-10-10 to 2025-12-31 will be included in the extraction. 

For the "All Lands" extraction tool, data from all models except SIMS is used and no masking is applied.

For the "ag lands" extraction, data from all models was used but the California Statewide Crop Mapping (https://data.cnra.ca.gov/dataset/statewide-crop-mapping) mask was applied to only include agricultural pixels.  For the crop map, all features except those labeled as Urban were included.

After the individual csv files have been generated, the `cadwr_combine_csv.py` tool can be run to combine the CSV files by model and to generate a single CSV containing all models and dates.  These files are saved in the `csv_ag_lands` and `csv_all_lands` folders.
