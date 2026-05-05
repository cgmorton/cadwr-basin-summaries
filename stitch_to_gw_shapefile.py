import argparse
from heapq import merge
from unittest import result
import geopandas as gpd
import glob
import numpy as np
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--shp-file")
parser.add_argument("--et-csv")
parser.add_argument("--merge-key")
parser.add_argument("--out")

if __name__ == "__main__":
    args = parser.parse_args()
    merge_on = args.merge_key

    reference_shp = gpd.read_file(args.shp_file)
    # Features are meter-based CRS - so area actually works here
    reference_shp["area_sq_meters"] = reference_shp.area
    # Conversion to acres from sq meters
    reference_shp["area_acres"] = reference_shp["area_sq_meters"] / 4046.86
    ref_shp_slim = reference_shp[[merge_on, "area_acres", "geometry"]]

    # All model ET data
    all_data_df = pd.read_csv(args.et_csv, parse_dates=["DATE"])
    # yymm
    all_data_df["date_combo"] = all_data_df["DATE"].dt.strftime("%y%m")

    # compute maximum pixels by feature
    max_pixels = all_data_df[["PIXEL_COUNT", merge_on]].groupby(merge_on).max()
    # convert to series to divide into dataframe along rows
    max_pixels_series = max_pixels["PIXEL_COUNT"]
    # Each pixel is 900m^2
    # Multiplying pixel area by max pixel count gives max total area
    max_pixels["MAX_PIXEL_AREA"] = max_pixels["PIXEL_COUNT"] * 900
    max_area_series = max_pixels["MAX_PIXEL_AREA"]

    # Multiply the mean ET value by the maximum total area to compute the volume of ET for each groundwater basin or county boundary.
    all_data_df["ET_VOL"] = all_data_df.apply(lambda g: g["ET_MEAN"] * max_area_series[g["NAME"]], axis=1)
    all_data_df["ET_VOL"] = all_data_df["ET_VOL"].round(2)

    stat_names = ["MEAN", "MEDIAN", "STDDEV", "PCT75", "PCT25", "PER_COV", "VOL"]
    abreviations = {
        "MEAN": "avg",
        "MEDIAN": "med",
        "STDDEV": "std",
        "PCT75": "p75",
        "PCT25": "p25",
        "PER_COV": "cov",
        "VOL": "vol"
    }

    i = 0
    for stat in stat_names:
        stat_abbr = abreviations[stat]

        # ensembleMEAN2203  = ensemble mean march 2023
        all_data_df[stat_abbr] = (
            all_data_df["MODEL"].str.slice(0, 3)
            + f"{stat_abbr}"
            + all_data_df["date_combo"].astype(str)
        )

        if stat == "PER_COV":    
            # Pivot so basin becomes rows and concatenated values become columns
            result_i_all = all_data_df.pivot_table(
                index=merge_on, columns=stat_abbr, values="PIXEL_COUNT", aggfunc="first"
            )
            result_i = result_i_all.div(max_pixels_series, axis=0)
        else:
            # Pivot so basin becomes rows and concatenated values become columns
            result_i = all_data_df.pivot_table(
                index=merge_on, columns=stat_abbr, values=f"ET_{stat}", aggfunc="first"
            )

        if i == 0:
            merged = pd.merge(
                ref_shp_slim,
                result_i,
                left_on=merge_on,
                right_index=True,
                how="left",
            )
        else:
            merged = pd.merge(
                merged, result_i, left_on=merge_on, right_index=True, how="left"
            )
        i += 1

    modshort = all_data_df["MODEL"].str.slice(0, 3).unique()
    model_name_long = dict(
        zip(
            all_data_df["MODEL"].str.slice(0, 3).unique(), all_data_df["MODEL"].unique()
        )
    )

    for modname in modshort:
        for vname in stat_names:
            stat_abbr = abreviations[vname]
            ss_cols = (
                [merge_on]
                + list(merged.filter(like=modname + stat_abbr, axis=1).columns)
                + ["area_acres", "geometry"]
            )

            full_mod_name = model_name_long[modname]
            merged_out_df = merged[ss_cols]
            merged_out_df.to_file(
                f"{args.out}/{full_mod_name.lower()}_{vname.lower()}.shp"
            )
