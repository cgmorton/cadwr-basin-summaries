import argparse
from datetime import datetime
import logging
import os
import pprint

import pandas as pd

MODELS = ['DISALEXI', 'EEMETRIC', 'GEESEBAL', 'PTJPL', 'SIMS', 'SSEBOP']


def main(overwrite_flag=False):

    for export_name in ['ag_lands', 'all_lands']:
        export_ws = os.path.join(os.getcwd(), f'csv_{export_name}')
        print(f'\n{export_name}')

        export_df_list = []
        for model in MODELS:
            model_ws = os.path.join(export_ws, model)
            if not os.path.isdir(model_ws):
                print(f'\n{model} - folder does not exist, skipping')
                continue
            else:
                print(f'\n{model}')

            csv_list = sorted([
                os.path.join(model_ws, item)
                for item in os.listdir(model_ws)
                if item.endswith('.csv')
            ])
            print(f'Files: {len(csv_list)}')

            # # Check if any of the CSV files are missing features
            # for csv_path in csv_list:
            #     if len(pd.read_csv(csv_path)) != 514:
            #         print(csv_path)

            model_df = pd.concat(map(pd.read_csv, csv_list), ignore_index=True)
            model_df.sort_values(['Basin_Subb', 'Date'], inplace=True)

            model_df['ET_MM'] = model_df['ET']
            model_df['ET_INCH'] = round(model_df['ET'] / 25.4, 6)
            del model_df['ET']
            print(f'Rows: {len(model_df.index)}')

            export_df_list.append(model_df)
            # print(model_df)

            model_df.to_csv(os.path.join(export_ws, f'{export_name}_{model.lower()}.csv'), index=False)

        export_df = pd.concat(export_df_list, ignore_index=True)
        export_df.sort_values(['Basin_Subb', 'Model', 'Date'], inplace=True)
        export_df.to_csv(os.path.join(export_ws, f'{export_name}_all_models.csv'), index=False)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Extract California/CIMIS OpenET monthly aggregations for all lands',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument(
    #     '--models', nargs='+', metavar='', default=MODELS, choices=MODELS,
    #     help='Space separated list of OpenET models to process')
    # parser.add_argument(
    #     '--start', type=openet.core.utils.arg_valid_date, metavar='DATE', default=None,
    #     help='Start date (format YYYY-MM-DD)')
    # parser.add_argument(
    #     '--end', type=openet.core.utils.arg_valid_date, metavar='DATE', default=None,
    #     help='End date (format YYYY-MM-DD)')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--project', default='openet',
        help='Google cloud project ID to use for GEE authentication')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    args = arg_parse()

    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(
        # models=args.models,
        # start_date=args.start,
        # end_date=args.end,
        overwrite_flag=args.overwrite,
    )