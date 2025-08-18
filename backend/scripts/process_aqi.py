import pandas as pd
import os
from datetime import datetime, timedelta

# 这个脚本是用来处理数据的
DATA_DIR = './beijing_20230101-20231231/'

OUTPUT_FILE = 'beijing_aqi_full_2023.csv'

STATION_MAPPING = {
    "东城东四": "东四",
    "东城天坛": "天坛",
    "西城官园": "官园",
    "西城万寿西宫": "万寿西宫",
    "朝阳奥体中心": "奥体中心",
    "朝阳农展馆": "农展馆",
    "海淀万柳": "海淀万柳",
    "丰台小屯": "丰台小屯",
    "丰台云岗": "丰台云岗",
    "石景山古城": "八角北路9号",
    "昌平镇": "昌平镇",
    "定陵(对照点)": "定陵",
    "延庆夏都": "延庆夏都",
    "延庆石河营": "延庆石河营",
    "怀柔镇": "怀柔镇",
    "怀柔新城": "怀柔新城",
    "密云镇": "密云镇",
    "密云新城": "密云新城",
    "顺义新城": "顺义新城",
    "通州东关": "通州东关",
    "大兴旧宫": "大兴旧宫",
    "门头沟三家店": "门头沟三家店",
    "房山燕山": "房山燕山",
}

SELECTED_CSV_STATION_COLUMNS = list(STATION_MAPPING.keys())

MASTER_STATION_ID_MAPPING_FROM_IMAGE = {
    "东四": 1, "天坛": 2, "万寿西宫": 3, "农展馆": 4, "官园": 5,
    "海淀万柳": 6, "顺义新城": 7, "怀柔镇": 8, "昌平镇": 9,
    "延庆夏都": 10, "密云新城": 11, "奥体中心": 12,
    "门头沟三家店": 13, "丰台云岗": 14, "通州东关": 15,
    "房山燕山": 16, "大兴旧宫": 17, "延庆石河营": 18,
    "怀柔新城": 19, "丰台小屯": 20, "密云镇": 21,
    "八角北路9号": 22, "洳河巡河路": 23, "定陵": 24
}

STATION_ID_FOR_OUTPUT = {
    standard_name: MASTER_STATION_ID_MAPPING_FROM_IMAGE.get(standard_name)
    for standard_name in STATION_MAPPING.values()
    if standard_name in MASTER_STATION_ID_MAPPING_FROM_IMAGE
}

COMMON_ID_COLUMNS = ['date', 'hour', 'type']

POLLUTANT_MAP = {
    'PM2.5': 'pm25', 'PM10': 'pm10', 'AQI': 'aqi',
    'SO2': 'so2', 'NO2': 'no2', 'O3': 'o3', 'CO': 'co'
}

MODEL_EXTRA_FIELDS = {
    'quality': '', 'description': '', 'measure': '', 'timestr': '',
    'raw_data': '{}'
}

FINAL_COLUMNS_ORDER = [
    'timestamp', 'aqi', 'quality', 'description', 'measure', 'timestr',
    'co', 'no2', 'o3', 'pm10', 'pm25', 'so2', 'raw_data', 'created_at', 'station_id',
]


# 处理单个日期的函数

def process_daily_data(current_date):
    date_str = current_date.strftime('%Y%m%d')
    file_all_path = os.path.join(DATA_DIR, f'beijing_all_{date_str}.csv')
    file_extra_path = os.path.join(DATA_DIR, f'beijing_extra_{date_str}.csv')

    print(f"\n--- Checking files for date: {date_str} ---")

    df_all_processed = pd.DataFrame()
    df_extra_processed = pd.DataFrame()
    merged_df = pd.DataFrame()

    any_file_found_and_read = False

    if os.path.exists(file_all_path):
        df_all = pd.read_csv(file_all_path)
        print(f"  _all file '{os.path.basename(file_all_path)}' found. Original rows: {len(df_all)}")
        any_file_found_and_read = True

        df_all = df_all[~df_all['type'].str.endswith('_24h')]
        print(f"  _all after '_24h' filter: {len(df_all)} rows.")

        df_all_list = []
        for pollutant_type in ['PM2.5', 'PM10', 'AQI']:
            if pollutant_type in df_all['type'].unique():
                df_pollutant = df_all[df_all['type'] == pollutant_type].copy()
                df_melted = df_pollutant.melt(
                    id_vars=['date', 'hour'],
                    value_vars=[col for col in df_pollutant.columns if col in SELECTED_CSV_STATION_COLUMNS],
                    var_name='station_csv_name',
                    value_name=pollutant_type
                )
                df_all_list.append(df_melted)

        if df_all_list:
            df_all_processed = pd.concat(df_all_list, ignore_index=True)
            df_all_processed = df_all_processed.groupby(['date', 'hour', 'station_csv_name']).agg({
                'PM2.5': 'first', 'PM10': 'first', 'AQI': 'first'
            }).reset_index()
            print(f"  _all processed into {len(df_all_processed)} records for PM/AQI.")
        else:
            print(f"  _all file existed, but no relevant PM/AQI data after filtering/melting.")
    else:
        print(f"  Warning: _all file '{os.path.basename(file_all_path)}' not found.")

    if os.path.exists(file_extra_path):
        df_extra = pd.read_csv(file_extra_path)
        print(f"  _extra file '{os.path.basename(file_extra_path)}' found. Original rows: {len(df_extra)}")
        any_file_found_and_read = True

        df_extra = df_extra[~df_extra['type'].str.endswith('_24h')]
        print(f"  _extra after '_24h' filter: {len(df_extra)} rows.")

        df_extra_list = []
        for pollutant_type in ['SO2', 'NO2', 'O3', 'CO']:
            if pollutant_type in df_extra['type'].unique():
                df_pollutant = df_extra[df_extra['type'] == pollutant_type].copy()
                df_melted = df_pollutant.melt(
                    id_vars=['date', 'hour'],
                    value_vars=[col for col in df_pollutant.columns if col in SELECTED_CSV_STATION_COLUMNS],
                    var_name='station_csv_name',
                    value_name=pollutant_type
                )
                df_extra_list.append(df_melted)

        if df_extra_list:
            df_extra_processed = pd.concat(df_extra_list, ignore_index=True)
            df_extra_processed = df_extra_processed.groupby(['date', 'hour', 'station_csv_name']).agg({
                'SO2': 'first', 'NO2': 'first', 'O3': 'first', 'CO': 'first'
            }).reset_index()
            print(f"  _extra processed into {len(df_extra_processed)} records for SO2/NO2/O3/CO.")
        else:
            print(f"  _extra file existed, but no relevant SO2/NO2/O3/CO data after filtering/melting.")


    else:
        print(f"  Warning: _extra file '{os.path.basename(file_extra_path)}' not found.")

    if not df_all_processed.empty and not df_extra_processed.empty:
        merged_df = pd.merge(df_all_processed, df_extra_processed,
                             on=['date', 'hour', 'station_csv_name'],
                             how='outer')
    elif not df_all_processed.empty:
        merged_df = df_all_processed
    elif not df_extra_processed.empty:
        merged_df = df_extra_processed
    else:
        return pd.DataFrame()

    merged_df['date'] = merged_df['date'].astype(str)
    merged_df['hour'] = merged_df['hour'].astype(str).str.zfill(2)
    merged_df['timestamp'] = pd.to_datetime(merged_df['date'] + merged_df['hour'], format='%Y%m%d%H')

    merged_df['station_standard_name'] = merged_df['station_csv_name'].map(STATION_MAPPING)
    merged_df['station_id'] = merged_df['station_standard_name'].map(STATION_ID_FOR_OUTPUT)

    merged_df['station_id'] = merged_df['station_id'].fillna(-1).astype(int)

    merged_df['created_at'] = datetime.now()

    merged_df = merged_df.drop(columns=['date', 'hour', 'station_csv_name', 'station_standard_name'])

    merged_df = merged_df.rename(columns=POLLUTANT_MAP)

    for field, default_value in MODEL_EXTRA_FIELDS.items():
        if field not in merged_df.columns:
            merged_df[field] = default_value

    for col_csv_val in POLLUTANT_MAP.values():
        if col_csv_val in merged_df.columns:
            merged_df[col_csv_val] = pd.to_numeric(merged_df[col_csv_val], errors='coerce')
            merged_df[col_csv_val] = merged_df[col_csv_val].fillna('').astype(str).replace(r'\.0$', '', regex=True)
            merged_df[col_csv_val] = merged_df[col_csv_val].replace('nan', '')

    final_cols_present = [col for col in FINAL_COLUMNS_ORDER if col in merged_df.columns]
    merged_df = merged_df[final_cols_present]

    print(f"--- Finished processing {date_str}: {len(merged_df)} total records from this date ---")
    return merged_df



if __name__ == "__main__":
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)

    all_yearly_records = []
    current_date = start_date
    while current_date <= end_date:
        daily_records = process_daily_data(current_date)
        if not daily_records.empty:
            all_yearly_records.append(daily_records)
        current_date += timedelta(days=1)

    final_yearly_df = pd.concat(all_yearly_records, ignore_index=True)

    final_yearly_df = final_yearly_df[[col for col in FINAL_COLUMNS_ORDER if col in final_yearly_df.columns]]

    pollutant_cols = list(POLLUTANT_MAP.values())
    for col in pollutant_cols:
        if col in final_yearly_df.columns:
            final_yearly_df[col] = pd.to_numeric(final_yearly_df[col], errors='coerce')
            final_yearly_df[col] = final_yearly_df[col].fillna('')
            final_yearly_df[col] = final_yearly_df[col].astype(str)
            final_yearly_df[col] = final_yearly_df[col].str.replace(r'\.0$', '', regex=True)
            final_yearly_df[col] = final_yearly_df[col].str.replace(r'^nan$', '', regex=True)

    final_yearly_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Data successfully exported to {OUTPUT_FILE}")
