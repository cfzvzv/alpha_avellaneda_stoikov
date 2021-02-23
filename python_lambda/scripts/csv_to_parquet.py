import sys

import pandas as pd
import os
import glob
from pathlib import Path


# %%
def get_properties(csv_path: str):
    filename = csv_path.split(os.sep)[-1].split('.')[0]
    filename_split = filename.split('_')
    ## TODO something better
    if len(filename_split) == 4:
        instrument_pk = filename_split[0] + "_" + filename_split[1]
        typeData = filename_split[2]
        date_str = filename_split[3]
    return instrument_pk, typeData, date_str


# %%
def main(csv_raw_path: str, parquet_base_path):
    # ethusdt_binance_trade_20201129.csv
    # csv_raw_path = r'D:\javif\Coding\cryptotradingdesk\data'
    # parquet_base_path = 'D:\javif\Coding\cryptotradingdesk\data'
    csv_files = glob.glob(csv_raw_path + os.sep + "**.csv")
    files_processed = []
    renamed_columns_depth = {}
    for side in ['ask', 'bid']:
        for level in range(5):
            renamed_columns_depth['%s_quantity%d' % (side, level)] = '%sQuantity%d' % (
                side,
                level,
            )
            renamed_columns_depth['%s%d' % (side, level)] = '%sPrice%d' % (side, level)

    print('going to process %d files' % len(csv_files))
    for csv_file in csv_files:
        instrument_pk, typeData, date_str = get_properties(csv_file)
        output_path = (
            parquet_base_path
            + os.sep
            + 'type=%s' % typeData
            + os.sep
            + 'instrument=%s' % instrument_pk
            + os.sep
            + 'date=%s' % date_str
            + os.sep
        )

        clean_csv_file = csv_file + '.temp.csv'
        Path(output_path).mkdir(parents=True, exist_ok=True)
        print('processing %s into parquet' % csv_file)
        try:
            # clean the file
            with open(csv_file) as infile, open(clean_csv_file, 'w') as outfile:
                for line in infile:
                    if line.strip() == '':
                        continue
                    if line.strip() == 'null':
                        continue
                    outfile.write(line)  # non-empty line. Write it to output

            input_df = pd.read_csv(clean_csv_file)
            input_df_formatted = pd.DataFrame(input_df)

            if typeData == 'depth':
                input_df_formatted.rename(columns=renamed_columns_depth, inplace=True)
            # remove the date column
            input_df_formatted.sort_values(by='timestamp', inplace=True)
            input_df_formatted.drop(columns=input_df_formatted.columns[0], inplace=True)
            # input_df_formatted = input_df_formatted.astype({"timestamp": int})
            input_df_formatted.set_index('timestamp', inplace=True)
            input_df_formatted.to_parquet(
                output_path + 'data.parquet', compression='GZIP', engine='fastparquet'
            )
            print('processed %s into parquet' % csv_file)
            files_processed.append(csv_file)
            os.remove(clean_csv_file)
        except Exception as e:
            print("Error processing %s  %s" % (csv_file, str(e)))
            os.remove(clean_csv_file)

    for csv_file in files_processed:
        os.remove(csv_file)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception(
            "must launch with 2 input arguments , csv_raw_path and parquet_base_path"
        )
    csv_raw_path = sys.argv[1]
    parquet_base_path = sys.argv[2]
    main(csv_raw_path=csv_raw_path, parquet_base_path=parquet_base_path)
