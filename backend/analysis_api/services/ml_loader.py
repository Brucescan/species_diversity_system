# analysis_api/services/ml_loader.py
import os
import joblib
import pandas as pd
import geopandas as gpd
import xarray as xr
import numpy as np
import warnings
from scipy.spatial import KDTree
from django.conf import settings

warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)

MODELS = {}
GLOBAL_DF_HISTORY_PROCESSED = None


def load_ml_models():
    """加载所有 .joblib 模型文件到全局 MODELS 字典中"""
    print("开始加载机器学习模型...")
    model_path = os.path.join(settings.BASE_DIR, 'analysis_api', 'machine_learning')

    model_files = {
        'presence_classifier': 'lgbm_model_presence_classifier.joblib',
        'richness_regressor': 'lgbm_model_richness_regressor.joblib',
        'abundance_regressor': 'lgbm_model_abundance_regressor.joblib',
        'shannon_regressor': 'lgbm_model_shannon_regressor.joblib',
    }

    for name, filename in model_files.items():
        path = os.path.join(model_path, filename)
        try:
            MODELS[name] = joblib.load(path)
            print(f"成功加载模型: {filename}")
        except Exception as e:
            print(f"加载模型 {filename} 时出错: {e}")

    print(f"模型加载完成。共加载 {len(MODELS)} 个模型。")


def load_and_process_historical_data():
    """
    加载并处理 2020-2025 年的所有历史数据，构建特征，
    并将最终结果存储在全局变量 GLOBAL_DF_HISTORY_PROCESSED 中。
    """
    global GLOBAL_DF_HISTORY_PROCESSED

    print("开始加载和处理历史数据...")

    # 数据加载
    base_path = r"./历史数据"
    all_data_frames = []

    for year in range(2020, 2026):
        year_path = os.path.join(base_path, str(year), f"processed_data_{year}.gdb")
        if not os.path.exists(year_path):
            print(f"GDB 未找到: {year_path}, 跳过年份 {year}.")
            continue
        for month in range(1, 13):
            month_str = str(month).zfill(2)
            feature_class_name = f"timespace_{year}_{month_str}"
            try:
                gdf = gpd.read_file(year_path, layer=feature_class_name)
                gdf['timestamp'] = pd.to_datetime(gdf['timestamp'], unit='ms')
                all_data_frames.append(gdf)
                print(f"已加载 {feature_class_name}")
            except Exception as e:
                print(f"加载图层 '{feature_class_name}' 出错: {e}")
                continue

    if not all_data_frames:
        print("未加载任何数据。历史数据处理中止。")
        return

    full_gdf = pd.concat(all_data_frames, ignore_index=True)

    if 'richness' in full_gdf.columns:
        full_gdf['has_richness'] = (full_gdf['richness'] > 0).astype(int)
        print("'has_richness' 列已成功创建。")

    if full_gdf.duplicated(subset=['Grid_ID', 'timestamp']).any():
        print("警告: 发现重复的 (Grid_ID, timestamp) 组合。正在删除...")
        full_gdf.drop_duplicates(subset=['Grid_ID', 'timestamp'], keep='first', inplace=True)

    print("所有GDB数据已合并。开始特征工程...")

    #特征工程
    df_for_xarray = full_gdf.copy().drop(columns=[full_gdf.geometry.name])
    df_for_xarray = df_for_xarray.set_index(['Grid_ID', 'timestamp'])
    ds = df_for_xarray.to_xarray()

    lags = [1, 3, 6, 12]
    lag_vars = ['richness', 'abundance', 'shannon', 'avg_pm25', 'temp_c', 'evi', 'Tree_Pct', 'Water_Pct']
    for var in lag_vars:
        if var in ds:
            for lag in lags: ds[f'{var}_lag{lag}'] = ds[var].shift(timestamp=lag)

    rolling_windows = [3, 6]
    for window in rolling_windows:
        for var in ['avg_pm25', 'temp_c']:
            if var in ds:
                ds[f'{var}_mean_{window}mo'] = ds[var].rolling(timestamp=window, center=False).mean()
                ds[f'{var}_std_{window}mo'] = ds[var].rolling(timestamp=window, center=False).std()
        if 'precip_mm' in ds:
            ds[f'precip_mm_sum_{window}mo'] = ds['precip_mm'].rolling(timestamp=window, center=False).sum()

    month_of_year = ds['timestamp'].dt.month
    ds['month_sin'] = np.sin(2 * np.pi * (month_of_year - 1) / 12)
    ds['month_cos'] = np.cos(2 * np.pi * (month_of_year - 1) / 12)

    try:
        unique_grids_gdf = full_gdf[['Grid_ID', 'geometry']].drop_duplicates('Grid_ID').reset_index(drop=True)
        unique_grids_gdf = unique_grids_gdf[unique_grids_gdf.geometry.notna() & ~unique_grids_gdf.geometry.is_empty]
        if len(unique_grids_gdf) > 0:
            unique_grids_gdf['centroid'] = unique_grids_gdf.geometry.centroid
            coords = np.array([(p.x, p.y) for p in unique_grids_gdf['centroid']])
            grid_ids = unique_grids_gdf['Grid_ID'].values
            tree = KDTree(coords)
            distances, indices = tree.query(coords, k=8 + 1)
            all_neighbor_pairs = []
            for i, source_grid_id in enumerate(grid_ids):
                valid_indices = indices[i][indices[i] != -1]
                for neighbor_idx_in_coords in valid_indices:
                    neighbor_grid_id = grid_ids[neighbor_idx_in_coords]
                    if source_grid_id != neighbor_grid_id:
                        all_neighbor_pairs.append(
                            {'source_Grid_ID': source_grid_id, 'neighbor_Grid_ID': neighbor_grid_id})

            neighbor_map_df = pd.DataFrame(all_neighbor_pairs)
            spatial_vars = ['avg_pm25', 'Tree_Pct', 'Water_Pct', 'evi']
            df_for_spatial = ds[spatial_vars].to_dataframe().reset_index()
            all_grid_timestamps = df_for_spatial[['Grid_ID', 'timestamp']].drop_duplicates().rename(
                columns={'Grid_ID': 'source_Grid_ID'})
            df_with_all_neighbors_and_timestamps = pd.merge(all_grid_timestamps, neighbor_map_df, on='source_Grid_ID',
                                                            how='left')
            df_for_spatial_renamed = df_for_spatial.copy().rename(
                columns={var: f'{var}_neighbor_data' for var in spatial_vars})
            df_for_spatial_renamed = df_for_spatial_renamed.rename(columns={'Grid_ID': 'neighbor_Grid_ID_for_merge'})
            merged_neighbor_data_with_timestamps = pd.merge(df_with_all_neighbors_and_timestamps,
                                                            df_for_spatial_renamed,
                                                            left_on=['neighbor_Grid_ID', 'timestamp'],
                                                            right_on=['neighbor_Grid_ID_for_merge', 'timestamp'],
                                                            how='left')

            cols_to_aggregate = [f'{var}_neighbor_data' for var in spatial_vars]
            agg_dict = {col: 'mean' for col in cols_to_aggregate}
            neighbor_features = merged_neighbor_data_with_timestamps.groupby(['source_Grid_ID', 'timestamp']).agg(
                agg_dict)
            neighbor_features = neighbor_features.rename(
                columns=lambda c: f"neighbor_{c.replace('_neighbor_data', '')}_mean").reset_index()
            neighbor_features = neighbor_features.rename(columns={'source_Grid_ID': 'Grid_ID'})

            neighbor_features_ds = neighbor_features.set_index(['Grid_ID', 'timestamp']).to_xarray()
            ds = ds.merge(neighbor_features_ds)
            print("空间邻域特征已成功创建并合并。")
        else:
            print("没有有效的网格几何数据，跳过空间特征创建。")
    except Exception as e:
        print(f"创建空间特征时出错: {e}")

    interaction_pairs = [('Tree_Pct', 'precip_mm'), ('BuiltArea_', 'avg_pm25'), ('FloodedVeg', 'richness_lag1')]
    for var1, var2 in interaction_pairs:
        if var1 in ds and var2 in ds:
            ds[f'inter_{var1}_x_{var2}'] = ds[var1] * ds[var2]

    print("特征工程完成。")

    #存储最终结果
    final_df = ds.to_dataframe().reset_index()

    print("正在将地理信息合并回最终数据集...")
    geometry_mapping = full_gdf[['Grid_ID', 'geometry']].drop_duplicates('Grid_ID').set_index('Grid_ID')
    final_df_with_geom = final_df.set_index('Grid_ID').join(geometry_mapping).reset_index()

    GLOBAL_DF_HISTORY_PROCESSED = final_df_with_geom
    print(f"历史数据处理完成。最终DataFrame维度: {GLOBAL_DF_HISTORY_PROCESSED.shape}")
    print("所有资源已准备就绪！")


def load_all_resources():
    print("Django 应用启动，开始加载ML资源...")
    load_ml_models()
    load_and_process_historical_data()
    print("所有ML资源加载完毕。服务器已准备好接收预测请求。")