import argparse
from datetime import datetime
import logging
import multiprocessing
import os
import pprint

import ee
import pandas as pd
import openet.core

# logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
# logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

MODELS = ['DISALEXI', 'EEMETRIC', 'GEESEBAL', 'PTJPL', 'SIMS', 'SSEBOP']
PROJECT_ID = 'openet'
START_DATE = '2003-10-01'
END_DATE = '2026-01-01'


def main(
        models=MODELS,
        start_date=START_DATE,
        end_date=END_DATE,
        project_id=PROJECT_ID,
        overwrite_flag=False,
        reverse_flag=False,
):
    """Extract California/CIMIS OpenET monthly aggregations for agricultural lands

    Parameters
    ----------
    models : list, optional
        List of models to process.  All models will be processed if not set.
    start_date : str, optional
        Start date (in ISO format YYYY-MM-DD).
    end_date : str, optional
        End date (in ISO format YYYY-MM-DD).
    project_id : str, optional
        Google cloud project ID to use for GEE authentication.
    overwrite_flag : bool, optional
        If True, remove all existing CSV files.
    reverse_flag : bool, optional
        If True, dates will be processed in reverse order (the default is False).

    """
    export_name = 'ag_lands'

    # # CM - Defining inside extraction function for now
    # # Exclude urban pixels/polygons in the California statewide crop mapping data
    # ag_mask = ee.Image('projects/openet/assets/crop_type/california/2024')
    # ag_mask = ag_mask.updateMask(ag_mask.neq(82))

    feature_coll_id = 'projects/ee-cgmorton/assets/ca_gw_basins'

    # Feature property used to uniquely identify each feature
    feature_id_property = 'Basin_Subb'
    # feature_id_property = 'Filter_NAM'

    # These feature properties will be written to the CSV files
    feature_properties = ['Basin_Numb', 'Basin_Name', 'Basin_Su_1']

    et_band = 'et'
    model_coll_ids = {
        'DISALEXI': f'projects/openet/assets/disalexi/california/cimis/monthly/v2_1',
        'EEMETRIC': f'projects/openet/assets/eemetric/california/cimis/monthly/v2_1',
        'GEESEBAL': f'projects/openet/assets/geesebal/california/cimis/monthly/v2_1',
        'PTJPL': f'projects/openet/assets/ptjpl/california/cimis/monthly/v2_1',
        'SIMS': f'projects/openet/assets/sims/california/cimis/monthly/v2_1',
        'SSEBOP': f'projects/openet/assets/ssebop/california/cimis/monthly/v2_1',
        # CGM - Supporting the ensemble will require changing the default et band
        # 'ENSEMBLE': f'projects/openet/assets/ensemble/california/cimis/monthly/v2_1',
    }

    export_ws = os.path.join(os.getcwd(), f'csv_{export_name}')
    # if not os.path.isdir(export_ws):
    #     os.makedirs(export_ws)

    ee_initializer(project_id=project_id, opt_url='https://earthengine-highvolume.googleapis.com')

    # # CIMIS Albers Equal Area Projection
    # # Using the EPSG:3310 code wasn't working, so pulling wkt from a CIMIS image
    # # Reduced the extent slightly from the default used for CIMIS
    # export_crs = ee.Image('projects/openet/assets/meteorology/cimis/ancillary/mask').projection().wkt()
    # export_extent = [-376010, -606000, 542010, 452010]
    # cellsize = 30
    # export_geo = [cellsize, 0, export_extent[0], 0, -cellsize, export_extent[3]]
    # # CGM - Testing out other California export extents
    # # export_extent = [-374000, -604300, 540340, 450320]
    # # export_extent = [-373990-2000-20, -604000-2000, 540000+2000+10, 450000+2000+20]
    # # export_crs = 'EPSG:3310'
    # # export_extent = [-410000, -660010, 610000, 460010]
    # # # UTM Zone 11
    # # export_crs = 'EPSG:32611'
    # # export_extent = [-134685, 3597465, 765315, 4677465]
    # # # UTM Zone 10
    # # export_crs = 'EPSG:32610'
    # # export_extent = [374415, 3613515, 1319415, 4654515]
    # # # WGS84
    # # export_crs = 'EPSG:4326'
    # # export_extent = [-124.5, 32.4, -114.0, 42.1]
    # # cellsize = 0.000269494585235856472

    # Read the feature properties
    feature_info = {
        ftr['properties'][feature_id_property]: ftr['properties']
        for ftr in ee.FeatureCollection(feature_coll_id).getInfo()['features']
    }

    # Process by model and date
    for model_name in models:
        print(f'\n{model_name}')

        model_coll_id = model_coll_ids[model_name]
        logging.debug(f'  {model_coll_id}')

        image_id_list = (
            ee.ImageCollection(model_coll_id)
            .filterDate(start_date, end_date)
            .aggregate_array('system:index')
            .getInfo()
        )
        date_list = list(set(
            datetime.strptime(image_id.split('_')[-2], '%Y%m%d')
            for image_id in image_id_list
        ))

        model_export_ws = os.path.join(export_ws, model_name)
        if not os.path.isdir(model_export_ws):
            os.makedirs(model_export_ws)

        for image_date in sorted(date_list):
            print(image_date.strftime("%Y-%m-%d"))

            model_date_csv = os.path.join(
                model_export_ws,
                f'{export_name}_{model_name.lower()}_{image_date.strftime("%Y%m%d")}.csv'
            )

            if os.path.exists(model_date_csv) and not overwrite_flag:
                logging.debug('  csv already exist and overwrite is False')
                continue

            input_list = [
                [image_date, model_coll_id, ftr_id, feature_coll_id, feature_id_property, et_band]
                for ftr_id, ftr_properties in feature_info.items()
            ]

            # DEADBEEF - Test the function call for a single image and feature
            # print(feature_extract(
            #     image_date, model_coll_id, list(feature_info.keys())[0],
            #     feature_coll_id, feature_id_property, et_band='et',
            # ))
            # break

            logging.debug('  requesting data')
            with multiprocessing.Pool(
                    processes=20,
                    initializer=ee_initializer,
                    initargs=(project_id, 'https://earthengine-highvolume.googleapis.com')
            ) as p:
                output = p.starmap(feature_extract, input_list)

            logging.debug('  building dataframe')
            output_df = pd.DataFrame(output)
            output_df.insert(loc=0, column='Model', value=model_name)

            # Copy any source collection properties
            # Writing them this way so that they are written after the model and date
            #   but before the ET and pixel count
            for p in feature_properties[::-1]:
                output_df.insert(loc=3, column=p, value=None)
                for i, row in output_df.iterrows():
                    output_df.loc[i, p] = feature_info[row[feature_id_property]][p]

            logging.debug('  writing csv')
            output_df.to_csv(model_date_csv, index=False)

    print('\nDone')


def ee_initializer(project_id='openet', opt_url='https://earthengine-highvolume.googleapis.com'):
    ee.Initialize(project=project_id, opt_url=opt_url)


def feature_extract(
        image_date,
        model_coll_id,
        ftr_id,
        feature_coll_id,
        feature_id_property,
        et_band='et',
):
    """"""


    # CGM - Defining here to reduce the number of parameters passed to the function
    # CIMIS Albers Equal Area Projection
    # Using the EPSG:3310 code wasn't working, so pulling wkt from a CIMIS image
    # Reduced the extent slightly from the default used for CIMIS
    export_crs = ee.Image('projects/openet/assets/meteorology/cimis/ancillary/mask').projection().wkt()
    export_extent = [-376010, -606000, 542010, 452010]
    cellsize = 30
    export_geo = [cellsize, 0, export_extent[0], 0, -cellsize, export_extent[3]]

    # CGM - Defining here to reduce the number of parameters passed to the function
    # Exclude urban pixels/polygons in the California statewide crop mapping data
    ag_mask = ee.Image('projects/openet/assets/crop_type/california/2024')
    ag_mask = ag_mask.updateMask(ag_mask.neq(82))

    feature = (
        ee.FeatureCollection(feature_coll_id)
        .filterMetadata(feature_id_property, 'equals', ftr_id)
        .first()
    )

    # try:
    output_info = (
        ee.ImageCollection(model_coll_id)
        .filterDate(image_date, ee.Date(image_date).advance(1, 'month'))
        .select([et_band], ['et'])
        .mosaic()
        .updateMask(ag_mask)
        .reduceRegion(
            geometry=feature.geometry(),
            reducer=ee.Reducer.mean().unweighted().combine(ee.Reducer.count(), sharedInputs=True),
            crs=export_crs,
            crsTransform=export_geo,
            bestEffort=False,
            # maxPixels=,
            # tileScale=,
        )
        .getInfo()
    )
    # except Exception as e:
    #     print('  unhandled exception, skipping feature')
    #     continue

    if output_info['et_mean']:
        output_info['et_mean'] = round(output_info['et_mean'], 6)

    return {
        'Date': image_date.strftime('%Y-%m-%d'),
        feature_id_property: ftr_id,
        'ET': output_info['et_mean'],
        'Pixel_Count': output_info['et_count'],
    }


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Extract California/CIMIS OpenET monthly aggregations for agricultural lands',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--models', nargs='+', metavar='', default=MODELS, choices=MODELS,
        help='Space separated list of OpenET models to process')
    parser.add_argument(
        '--start', type=openet.core.utils.arg_valid_date, metavar='DATE', default=START_DATE,
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=openet.core.utils.arg_valid_date, metavar='DATE', default=END_DATE,
        help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--project', default='openet',
        help='Google cloud project ID to use for GEE authentication')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()

    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(
        models=args.models,
        start_date=args.start,
        end_date=args.end,
        project_id=args.project,
        overwrite_flag = args.overwrite,
        reverse_flag=args.reverse,
    )
