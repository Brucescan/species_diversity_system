# analysis_api/services/ml_loader.py

import os
import joblib
import pandas as pd
import geopandas as gpd
import xarray as xr
import numpy as np
import warnings
from scipy.spatial import KDTree
from shapely.geometry import Point
from django.conf import settings

# 设置日志记录器，方便在 Django 启动时查看进度
warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)

# ==============================================================================
# 1. 全局变量：用于在内存中存储加载好的资源
# ==============================================================================
# 存储加载的模型，键是模型的简称，值是加载后的模型对象
MODELS = {}
# 存储处理好的历史数据，这将是一个巨大的 DataFrame 或 Xarray Dataset
GLOBAL_DF_HISTORY_PROCESSED = None


# ==============================================================================
# 2. 核心加载与处理函数
# ==============================================================================

def load_ml_models():
    """加载所有 .joblib 模型文件到全局 MODELS 字典中"""
    print("开始加载机器学习模型...")
    # 假设模型文件与 manage.py 在同一目录下的 'trained_models_final' 文件夹
    # 或者提供绝对路径
    model_path = os.path.join(settings.BASE_DIR, 'analysis_api', 'machine_learning')

    # 根据您的图二和训练脚本，模型文件名是固定的
    model_files = {
        'presence_classifier': 'lgbm_model_presence_classifier.joblib',
        'richness_regressor': 'lgbm_model_richness_regressor.joblib',
        'abundance_regressor': 'lgbm_model_abundance_regressor.joblib',
        'shannon_regressor': 'lgbm_model_shannon_regressor.joblib',
        # 注意：您的图二中有一个 lgbm_model_classifier.joblib，但您的训练脚本中
        # 似乎只保存了 presence_classifier。请根据实际情况调整。
        # 这里我们假设您只需要训练脚本中保存的4个模型。
    }

    for name, filename in model_files.items():
        path = os.path.join(model_path, filename)
        try:
            MODELS[name] = joblib.load(path)
            print(f"成功加载模型: {filename}")
        except FileNotFoundError:
            print(f"模型文件未找到: {path}。请检查路径。")
        except Exception as e:
            print(f"加载模型 {filename} 时出错: {e}")

    print(f"模型加载完成。共加载 {len(MODELS)} 个模型。")


def load_and_process_historical_data():
    """
    加载并处理 2020-2025 年的所有历史数据，构建特征，
    并将最终结果存储在全局变量 GLOBAL_DF_HISTORY_PROCESSED 中。
    """
    global GLOBAL_DF_HISTORY_PROCESSED

    print("开始加载和处理历史数据 (2020-2025)... 这可能需要一些时间。")

    # --- 1. 数据加载 ---
    # 请确保这里的路径对于运行 Django 服务器的环境是正确的
    base_path = r"D:\训练数据\最终数据"  # 强烈建议使用相对路径或环境变量
    all_data_frames = []

    # 循环范围已修改为包含 2025
    for year in range(2020, 2026):
        year_path = os.path.join(base_path, str(year), f"processed_data_{year}.gdb")
        if not os.path.exists(year_path):
            print(f"GDB 未找到: {year_path}, 跳过年份 {year}.")
            continue
        for month in range(1, 13):
            month_str = str(month).zfill(2)
            feature_class_name = f"timespace_{year}_{month_str}"

            # 跳过已知的问题数据
            if feature_class_name == 'timespace_2020_06':
                continue

            try:
                gdf = gpd.read_file(year_path, layer=feature_class_name)

                # --- 针对 2025 年的特殊处理逻辑 ---
                if year == 2025:
                    print(f"检测到 {year} 年数据，修正 {feature_class_name} 的时间信息...")
                    # 创建正确的目标时间戳 (该月最后一天)
                    target_timestamp = pd.Timestamp(f'{year}-{month_str}-01') + pd.offsets.MonthEnd(0)
                    gdf['timestamp'] = target_timestamp

                    # 更新 month_id 字段 (如果存在)
                    if 'month_id' in gdf.columns:
                        gdf['month_id'] = int(f'{year}{month_str}')
                    print(f"  > timestamp 已更新为 {target_timestamp.strftime('%Y-%m-%d')}")
                else:
                    # 对于非 2025 年的数据，使用原始的转换逻辑
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

    # 在进一步处理前，检查并移除重复的 (Grid_ID, timestamp) 组合
    if full_gdf.duplicated(subset=['Grid_ID', 'timestamp']).any():
        print("警告: 发现重复的 (Grid_ID, timestamp) 组合。正在删除...")
        full_gdf.drop_duplicates(subset=['Grid_ID', 'timestamp'], keep='first', inplace=True)
        print(f"去重后行数: {len(full_gdf)}")

    print("所有GDB数据已合并。开始特征工程...")

    # --- 2. 特征工程 (基本与您的脚本相同) ---
    df_for_xarray = full_gdf.copy().drop(columns=[full_gdf.geometry.name])
    df_for_xarray = df_for_xarray.set_index(['Grid_ID', 'timestamp'])
    ds = df_for_xarray.to_xarray()

    # 时间滞后特征
    lags = [1, 3, 6, 12]
    lag_vars = ['richness', 'abundance', 'shannon', 'avg_pm25', 'temp_c', 'evi', 'Tree_Pct', 'Water_Pct']
    for var in lag_vars:
        if var in ds:
            for lag in lags: ds[f'{var}_lag{lag}'] = ds[var].shift(timestamp=lag)

    # 时间聚合特征
    rolling_windows = [3, 6]
    for window in rolling_windows:
        for var in ['avg_pm25', 'temp_c']:
            if var in ds:
                ds[f'{var}_mean_{window}mo'] = ds[var].rolling(timestamp=window, center=False).mean()
                ds[f'{var}_std_{window}mo'] = ds[var].rolling(timestamp=window, center=False).std()
        if 'precip_mm' in ds:
            ds[f'precip_mm_sum_{window}mo'] = ds['precip_mm'].rolling(timestamp=window, center=False).sum()

    # 时间周期性特征
    month_of_year = ds['timestamp'].dt.month
    ds['month_sin'] = np.sin(2 * np.pi * (month_of_year - 1) / 12)
    ds['month_cos'] = np.cos(2 * np.pi * (month_of_year - 1) / 12)

    # 空间相关特征 (您的代码)
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

    # 交互特征
    interaction_pairs = [('Tree_Pct', 'precip_mm'), ('BuiltArea_', 'avg_pm25'), ('FloodedVeg', 'richness_lag1')]
    for var1, var2 in interaction_pairs:
        if var1 in ds and var2 in ds:
            ds[f'inter_{var1}_x_{var2}'] = ds[var1] * ds[var2]

    print("特征工程完成。")

    # --- 3. 存储最终结果 ---
    # 将最终的 Xarray Dataset 转换回 Pandas DataFrame
    # 注意：我们不执行 dropna()，因为我们需要完整的历史记录。
    # 在预测时，我们将选择特定时间点的数据，那里的 NaN 已被滞后/滚动值填充。
    final_df = ds.to_dataframe().reset_index()

    # 根据您的要求，不需要提取静态特征，因为它们已经包含在每月数据中

    # 将处理好的数据赋值给全局变量
    GLOBAL_DF_HISTORY_PROCESSED = final_df
    print(f"历史数据处理完成。最终DataFrame维度: {GLOBAL_DF_HISTORY_PROCESSED.shape}")
    print("所有资源已准备就绪！")


def load_all_resources():
    """
    一个主函数，用于在服务器启动时调用所有加载和处理函数。
    """
    print("Django 应用启动，开始加载ML资源...")
    load_ml_models()
    load_and_process_historical_data()
    print("所有ML资源加载完毕。服务器已准备好接收预测请求。")