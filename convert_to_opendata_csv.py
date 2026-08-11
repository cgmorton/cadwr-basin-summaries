import argparse
from heapq import merge
from unittest import result
import geopandas as gpd
import glob
import numpy as np
import pandas as pd

METERS_TO_ACRES = 4046.86
ACREFT_TO_METERS3 = 1233.48
MM_TO_IN = 25.4

parser = argparse.ArgumentParser()
parser.add_argument("--feature")#'county','gw_basin','hydrologic_region'
parser.add_argument("--shp-file")
parser.add_argument("--et-csv")
parser.add_argument("--merge-key")
parser.add_argument("--out")

id_columns = {'county':['NAME','county_id'],
             'gw_basin':['Basin_Subb','basin_id'],
             'hydrologic_region':['HR_NAME','hr_id']}
verbose=False
                    
if __name__ == "__main__":
    args = parser.parse_args()
    merge_on = args.merge_key
    features = args.feature
    out_path = args.out
    if features.lower() in ['hydrologic_region', 'hr', 'hydrologic_regions']:
        reference_shp = gpd.read_file('i03_Hydrologic_Regions/i03_Hydrologic_Regions.shp')
        reference_shp = reference_shp[reference_shp['OBJECTID'] != 12]
        reference_shp = reference_shp[reference_shp['OBJECTID'] != 20]
    elif features.lower() in ['']:
        reference_shp = gpd.read_file
    else:
        reference_shp = gpd.read_file(args.shp_file)
    if verbose ==True:
        print(reference_shp.head())
    # Features are meter-based CRS - so area actually works here
    reference_shp["area_sq_meters"] = reference_shp.area
    # Conversion to acres from sq meters
    reference_shp["area_acres"] = np.round(reference_shp["area_sq_meters"] / METERS_TO_ACRES,2)
    ref_shp_slim = reference_shp[[merge_on, "area_sq_meters", "area_acres", "geometry"]].copy()

    # All model ET data
    all_data_df = pd.read_csv(args.et_csv, parse_dates=["DATE"])
    
    if verbose ==True:
        print(all_data_df.columns)
    
    all_data_df["date_combo"] = all_data_df["DATE"].dt.strftime("%y%m")
    all_data_df = all_data_df.merge(ref_shp_slim.drop_duplicates(subset=merge_on),on=merge_on,how="left")
    
    # compute maximum pixels by feature
    max_pixels = all_data_df[["PIXEL_COUNT", merge_on]].groupby(merge_on).max()

    # convert to series to divide into dataframe along rows
    max_pixels_series = max_pixels["PIXEL_COUNT"]
    
    # Each pixel is 900m^2
    # Multiplying pixel area by max pixel count gives max total area
    max_pixels["max_mask_area_acres"] = np.round(max_pixels["PIXEL_COUNT"] * 900 / METERS_TO_ACRES,2)
    
    # print(max_pixels)
    max_area_series = max_pixels["max_mask_area_acres"]
    
    all_data_df = all_data_df.merge(
        max_pixels.reset_index(),
        on=merge_on,
        how="left"
    )
    
    # convert m2 to acres
    max_area_series_acres = max_area_series / METERS_TO_ACRES

    # Multiply the mean ET value by the maximum total area to compute the volume of ET for each groundwater basin or county boundary.
    all_data_df["ET_VOL"] = all_data_df.apply(lambda g: (g["ET_MEAN"] / 1000) * (max_area_series[g[merge_on]] / ACREFT_TO_METERS3), axis=1)
    all_data_df["ET_acre_ft"] = all_data_df["ET_VOL"].round(2).copy()
    
    # Unit conversion to inches.
    all_data_df["ET_mean_in"] = np.round(all_data_df["ET_MEAN"] / MM_TO_IN, 2) 
    
    # Assign max area acres to each polygon
    # ref_shp_slim["max_mask_area_acres"] = reference_shp.apply(lambda g: max_area_series_acres[g[merge_on]], axis=1)
    
    all_data_df["DATE"] = pd.to_datetime(all_data_df["DATE"])
    all_data_df["year_month"] = all_data_df["DATE"].dt.strftime("%Y-%m")
    all_data_df["year"] = all_data_df["DATE"].dt.strftime("%Y")
    all_data_df["month"] = all_data_df["DATE"].dt.strftime("%m")
    all_data_df['timestep']='month'

    out_df = all_data_df[[merge_on,'MODEL','area_acres','max_mask_area_acres','ET_mean_in','ET_acre_ft','timestep','year','month','year_month']].copy()
    out_df["year"] = out_df["year"].astype(int)
    out_df["month"] = out_df["month"].astype(int)
    
    # Add water year to monthly rows
    out_df["water_year"] = np.where(
        out_df["month"] >= 10,
        out_df["year"] + 1,
        out_df["year"]
    )
    
    group_cols = [
        merge_on,
        "MODEL",
        "area_acres",
        "max_mask_area_acres",
        "water_year"
    ]

    def sum_only_complete_water_year(group):
        required_months = set(range(1, 13))
        months_present = set(group["month"].dropna().astype(int))
    
        has_all_months = months_present == required_months
        has_no_missing = group[["ET_mean_in", "ET_acre_ft"]].notna().all().all()
    
        return pd.Series({
            "ET_mean_in": group["ET_mean_in"].sum() if has_all_months and has_no_missing else np.nan,
            "ET_acre_ft": group["ET_acre_ft"].sum() if has_all_months and has_no_missing else np.nan,
            "n_months": group["month"].nunique(),
            "n_missing_ET_mean_in": group["ET_mean_in"].isna().sum(),
            "n_missing_ET_acre_ft": group["ET_acre_ft"].isna().sum()
        })

    # Create water-year total rows
    water_year_rows = (
        out_df
        .groupby(group_cols, dropna=False)
        .apply(sum_only_complete_water_year)
        .reset_index()
    )
    # Assign row-level fields so columns match monthly table
    water_year_rows["timestep"] = "water_year"
    water_year_rows["year"] = water_year_rows["water_year"]
    
    # Choose ONE of these month options:
    water_year_rows["month"] = np.nan
    water_year_rows["year_month"] = water_year_rows["water_year"].astype(str)+'-00'
    
    # Add any missing columns so concat preserves the full schema
    for col in out_df.columns:
        if col not in water_year_rows.columns:
            water_year_rows[col] = np.nan
    
    # Reorder water-year rows to match out_df columns
    water_year_rows = water_year_rows[out_df.columns]
    
    # Append water-year rows to the original monthly table
    out_all_data_df = pd.concat( [out_df, water_year_rows],
        ignore_index=True)
    if verbose ==True:
        print(out_all_data_df[[merge_on,'MODEL','area_acres','max_mask_area_acres','ET_mean_in','ET_acre_ft','timestep','year','month','year_month']].head(-4))#,'timestep'
        print('hr_id, area_acres, max_mask_area_acres, mean_inches, mean_acre_ft, timestep, year, month, year_month')

    out = (
        out_all_data_df.pivot(
            index=[
                merge_on, "area_acres", "max_mask_area_acres",
                "year_month", "year", "month", "timestep"
            ],
            columns="MODEL",
            values=["ET_mean_in", "ET_acre_ft"]
        )
    )
    
    out.columns = [f"{model}_{metric}" for metric, model in out.columns]
    
    out = out.reset_index()

    print('file saved to:\n',f"{args.out}{features}_ag_mask_open_data.csv")
    out.to_csv(f"{args.out}{features}_ag_mask_open_data.csv")
