# analysis_api/services/prediction_service.py
import pandas as pd
import numpy as np
import xarray as xr
from .ml_loader import MODELS, GLOBAL_DF_HISTORY_PROCESSED


def _recalculate_temporal_features_batch(full_df):
    """
    在一个包含历史和未来基线的大 DataFrame 上，为所有网格批量重新计算时间特征。
    """
    # 确保时间戳是 datetime 类型并且已排序
    full_df['timestamp'] = pd.to_datetime(full_df['timestamp'])
    full_df = full_df.sort_values(by=['Grid_ID', 'timestamp'])

    # 在处理前，确保 'has_richness' 列不存在 (因为它不是一个独立的特征，而是目标的衍生物)
    if 'has_richness' in full_df.columns:
        full_df = full_df.drop(columns=['has_richness'])

    # 使用 xarray 进行高效的 groupby + shift/rolling 操作
    cols_to_process = [col for col in full_df.columns if col != 'geometry']
    ds = full_df[cols_to_process].set_index(['Grid_ID', 'timestamp']).to_xarray()

    # --- 滞后特征 ---
    lags = [1, 3, 6, 12]
    lag_vars = ['richness', 'abundance', 'shannon', 'avg_pm25', 'temp_c', 'evi', 'Tree_Pct', 'Water_Pct']
    for var in lag_vars:
        if var in ds:
            for lag in lags:
                ds[f'{var}_lag{lag}'] = ds[var].shift(timestamp=lag)

    # --- 滚动特征 ---
    rolling_windows = [3, 6]
    for window in rolling_windows:
        for var in ['avg_pm25', 'temp_c']:
            if var in ds:
                ds[f'{var}_mean_{window}mo'] = ds[var].rolling(timestamp=window, center=False).mean()
                ds[f'{var}_std_{window}mo'] = ds[var].rolling(timestamp=window, center=False).std()
        if 'precip_mm' in ds:
            ds[f'precip_mm_sum_{window}mo'] = ds['precip_mm'].rolling(timestamp=window, center=False).sum()

    return ds.to_dataframe().reset_index()


def _calculate_composite_index(richness, abundance, shannon):
    """计算生态环境综合指标 (占位符)。"""
    return shannon


def perform_prediction(target_dates):
    """
    执行完整的预测循环 (高性能向量化版本)。
    此版本已修复 'has_richness' not in index 错误。
    """
    if GLOBAL_DF_HISTORY_PROCESSED is None:
        raise Exception("历史数据尚未加载，服务无法预测。")

    first_target_date = min(target_dates)
    history_df = GLOBAL_DF_HISTORY_PROCESSED[
        GLOBAL_DF_HISTORY_PROCESSED['timestamp'] < first_target_date
        ].copy()
    print(f"用于预测的真实历史数据范围： {history_df['timestamp'].min()} to {history_df['timestamp'].max()}")

    # --- 预计算和准备 (现在基于干净的 history_df) ---
    print("开始预测前的预计算...")
    static_cols = ['Avg_Height', 'Avg_Slope', 'Avg_Aspect', 'Avg_Relief',
                   'Water_Pct', 'Tree_Pct', 'Crop_Pct', 'BuiltArea_']
    dynamic_cols = ['avg_pm25', 'temp_c', 'precip_mm', 'evi']

    latest_static_features = history_df.sort_values('timestamp').drop_duplicates('Grid_ID', keep='last')[
        ['Grid_ID'] + static_cols]

    monthly_avg_dynamic = history_df.groupby([history_df['Grid_ID'], history_df['timestamp'].dt.month])[
        dynamic_cols].mean().reset_index()
    monthly_avg_dynamic.rename(columns={'timestamp': 'month'}, inplace=True)
    print("预计算完成。")

    # 提前获取模型特征列名
    try:
        cls_feature_cols = MODELS['presence_classifier'].feature_name_
        richness_feature_cols = MODELS['richness_regressor'].feature_name_
        abundance_feature_cols = MODELS['abundance_regressor'].feature_name_
        shannon_feature_cols = MODELS['shannon_regressor'].feature_name_
    except Exception as e:
        raise Exception(f"加载模型或获取特征名时出错: {e}")

    # 用于存储最终结果
    all_results = {grid_id: [] for grid_id in history_df['Grid_ID'].unique()}

    # 预测循环依赖于前一个月的预测结果，所以需要动态更新历史
    current_history = history_df.copy()

    for target_date in target_dates:
        print(f"--- 正在预测月份: {target_date.strftime('%Y-%m')} ---")

        # --- 步骤 1: 批量创建所有网格的基线特征行 ---
        target_month = target_date.month

        future_dynamic_features = monthly_avg_dynamic[monthly_avg_dynamic['month'] == target_month]
        baseline_features = pd.merge(latest_static_features, future_dynamic_features, on='Grid_ID', how='left')
        baseline_features[dynamic_cols] = baseline_features[dynamic_cols].fillna(0)

        baseline_features['timestamp'] = target_date
        baseline_features['month_sin'] = np.sin(2 * np.pi * (target_month - 1) / 12)
        baseline_features['month_cos'] = np.cos(2 * np.pi * (target_month - 1) / 12)
        print(f"    为 {len(baseline_features)} 个网格批量创建了基线行。")

        # --- 步骤 2: 批量重新计算时空特征 ---
        history_context = current_history.groupby('Grid_ID').tail(12)
        df_for_recalc = pd.concat([history_context, baseline_features], ignore_index=True)

        print("    开始批量重新计算时空特征...")
        recalculated_df = _recalculate_temporal_features_batch(df_for_recalc)
        print("    批量计算完成。")

        final_feature_rows = recalculated_df[recalculated_df['timestamp'] == target_date].copy()
        final_feature_rows.dropna(how='all', inplace=True)

        if final_feature_rows.empty:
            print(f"    在 {target_date.strftime('%Y-%m')} 没有可预测的数据行。")
            continue

        # --- 步骤 3: 批量预测 ---
        print("    开始批量预测...")
        X_cls = final_feature_rows[cls_feature_cols]
        presence_preds = MODELS['presence_classifier'].predict(X_cls)
        presence_probs = MODELS['presence_classifier'].predict_proba(X_cls)[:, 1]

        final_feature_rows['presence_prob'] = presence_probs
        # --- 关键修改：为预测结果创建 has_richness 列 ---
        final_feature_rows['has_richness'] = presence_preds

        pred_richness = MODELS['richness_regressor'].predict(final_feature_rows[richness_feature_cols])
        pred_abundance = MODELS['abundance_regressor'].predict(final_feature_rows[abundance_feature_cols])
        pred_shannon = MODELS['shannon_regressor'].predict(final_feature_rows[shannon_feature_cols])
        print("    批量预测完成。")

        # --- 步骤 4: 整理结果并更新历史 ---
        final_feature_rows['richness'] = np.maximum(0, pred_richness)
        final_feature_rows['abundance'] = np.maximum(0, pred_abundance)
        final_feature_rows['shannon'] = np.maximum(0, pred_shannon)

        # 如果预测 richness 为 0，确保 has_richness 也为 0 (逻辑校正)
        final_feature_rows.loc[final_feature_rows['richness'] == 0, 'has_richness'] = 0

        for _, row in final_feature_rows.iterrows():
            grid_id = int(row['Grid_ID'])
            richness = float(row['richness'])
            abundance = float(row['abundance'])
            shannon = float(row['shannon'])

            result = {
                'date': target_date.strftime('%Y-%m-%d'),
                'richness': richness,
                'abundance': abundance,
                'shannon': shannon,
                'composite_index': _calculate_composite_index(richness, abundance, shannon)
            }
            if grid_id in all_results:
                all_results[grid_id].append(result)

        # 更新 current_history，现在列结构完全一致
        cols_to_keep_for_history = [col for col in current_history.columns if col in final_feature_rows.columns]
        current_history = pd.concat([current_history, final_feature_rows[cols_to_keep_for_history]], ignore_index=True)
        print(f"    结果已整理，历史记录已更新。")

    # --- 步骤 5: 返回最终结果 ---
    final_output = [
        {"grid_id": grid_id, "predictions": preds}
        for grid_id, preds in all_results.items() if preds
    ]
    return final_output