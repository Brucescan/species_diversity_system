# analysis_api/services/prediction_service.py
import pandas as pd
import numpy as np
import xarray as xr
from .ml_loader import MODELS, GLOBAL_DF_HISTORY_PROCESSED


def _recalculate_temporal_features_batch(full_df):
    """
    在一个包含历史和未来基线的大 DataFrame 上，为所有网格批量计算时间特征。
    """
    full_df['timestamp'] = pd.to_datetime(full_df['timestamp'])
    full_df = full_df.sort_values(by=['Grid_ID', 'timestamp'])

    if 'has_richness' in full_df.columns:
        full_df = full_df.drop(columns=['has_richness'])

    cols_to_process = [col for col in full_df.columns if col != 'geometry']
    ds = full_df[cols_to_process].set_index(['Grid_ID', 'timestamp']).to_xarray()

    lags = [1, 3, 6, 12]
    lag_vars = ['richness', 'abundance', 'shannon', 'avg_pm25', 'temp_c', 'evi', 'Tree_Pct', 'Water_Pct']
    for var in lag_vars:
        if var in ds:
            for lag in lags:
                ds[f'{var}_lag{lag}'] = ds[var].shift(timestamp=lag)

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
    """计算生态环境综合指标 """
    return 0.8*shannon+0.2*richness


def perform_prediction(target_dates):
    """
    执行完整的预测循环，并返回包含上下文特征的丰富结果。
    """
    if GLOBAL_DF_HISTORY_PROCESSED is None:
        raise Exception("历史数据尚未加载，服务无法预测。")

    first_target_date = min(target_dates)
    history_df = GLOBAL_DF_HISTORY_PROCESSED[
        GLOBAL_DF_HISTORY_PROCESSED['timestamp'] < first_target_date
        ].copy()
    print(f"用于预测的真实历史数据范围： {history_df['timestamp'].min()} to {history_df['timestamp'].max()}")

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

    try:
        cls_feature_cols = MODELS['presence_classifier'].feature_name_
        richness_feature_cols = MODELS['richness_regressor'].feature_name_
        abundance_feature_cols = MODELS['abundance_regressor'].feature_name_
        shannon_feature_cols = MODELS['shannon_regressor'].feature_name_
    except Exception as e:
        raise Exception(f"加载模型或获取特征名时出错: {e}")

    all_results = {grid_id: [] for grid_id in history_df['Grid_ID'].unique()}
    current_history = history_df.copy()

    for target_date in target_dates:
        print(f"--- 正在预测月份: {target_date.strftime('%Y-%m')} ---")

        target_month = target_date.month
        future_dynamic_features = monthly_avg_dynamic[monthly_avg_dynamic['month'] == target_month]
        baseline_features = pd.merge(latest_static_features, future_dynamic_features, on='Grid_ID', how='left')
        baseline_features[dynamic_cols] = baseline_features[dynamic_cols].fillna(0)
        baseline_features['timestamp'] = target_date
        baseline_features['month_sin'] = np.sin(2 * np.pi * (target_month - 1) / 12)
        baseline_features['month_cos'] = np.cos(2 * np.pi * (target_month - 1) / 12)
        history_context = current_history.groupby('Grid_ID').tail(12)
        df_for_recalc = pd.concat([history_context, baseline_features], ignore_index=True)
        recalculated_df = _recalculate_temporal_features_batch(df_for_recalc)
        final_feature_rows = recalculated_df[recalculated_df['timestamp'] == target_date].copy()
        final_feature_rows.dropna(how='all', inplace=True)
        if final_feature_rows.empty: continue

        X_cls = final_feature_rows[cls_feature_cols]
        presence_preds = MODELS['presence_classifier'].predict(X_cls)
        presence_probs = MODELS['presence_classifier'].predict_proba(X_cls)[:, 1]
        final_feature_rows['presence_prob'] = presence_probs
        final_feature_rows['has_richness'] = presence_preds
        pred_richness = MODELS['richness_regressor'].predict(final_feature_rows[richness_feature_cols])
        pred_abundance = MODELS['abundance_regressor'].predict(final_feature_rows[abundance_feature_cols])
        pred_shannon = MODELS['shannon_regressor'].predict(final_feature_rows[shannon_feature_cols])

        # 整理结果并更新历史
        final_feature_rows['richness'] = np.maximum(0, pred_richness)
        final_feature_rows['abundance'] = np.maximum(0, pred_abundance)
        final_feature_rows['shannon'] = np.maximum(0, pred_shannon)
        final_feature_rows.loc[final_feature_rows['richness'] == 0, 'has_richness'] = 0

        for _, row in final_feature_rows.iterrows():
            grid_id = int(row['Grid_ID'])

            # 提取预测值
            richness = float(row['richness'])
            abundance = float(row['abundance'])
            shannon = float(row['shannon'])
            composite_index = _calculate_composite_index(richness, abundance, shannon)

            # 提取并组织上下文特征
            context_features = {
                "presence_probability": round(float(row.get('presence_prob', 0.0)), 4),
                "environment": {
                    "avg_pm25": round(float(row.get('avg_pm25', 0.0)), 2),
                    "avg_temp_c": round(float(row.get('temp_c', 0.0)), 2),
                    "evi": round(float(row.get('evi', 0.0)), 4)
                },
                "land_use_pct": {
                    "water": round(float(row.get('Water_Pct', 0.0)), 4),
                    "tree": round(float(row.get('Tree_Pct', 0.0)), 4),
                    "built_area": round(float(row.get('BuiltArea_', 0.0)), 4),
                    "crop": round(float(row.get('Crop_Pct', 0.0)), 4)
                }
            }

            # 组装新的、结构化的结果对象
            result = {
                'date': target_date.strftime('%Y-%m-%d'),
                'predictions': {
                    'richness': round(richness, 4),
                    'abundance': round(abundance, 4),
                    'shannon': round(shannon, 4),
                    'composite_index': round(composite_index, 4)
                },
                'context_features': context_features
            }

            if grid_id in all_results:
                all_results[grid_id].append(result)

        # 更新 current_history
        cols_to_keep_for_history = [col for col in current_history.columns if col in final_feature_rows.columns]
        current_history = pd.concat([current_history, final_feature_rows[cols_to_keep_for_history]], ignore_index=True)
        print(f"    结果已整理，历史记录已更新。")

    # 返回最终结果
    final_output = [
        {"grid_id": grid_id, "predictions": preds}
        for grid_id, preds in all_results.items() if preds
    ]
    return final_output


def perform_scenario_prediction(grid_ids, target_dates, modifications):
    """
    根据用户定义的修改执行情景模拟预测。
    """
    if GLOBAL_DF_HISTORY_PROCESSED is None:
        raise Exception("历史数据尚未加载，服务无法预测。")
    if not grid_ids or not modifications:
        raise ValueError("grid_ids 和 modifications 不能为空。")

    first_target_date = min(target_dates)
    history_df = GLOBAL_DF_HISTORY_PROCESSED[
        GLOBAL_DF_HISTORY_PROCESSED['timestamp'] < first_target_date
        ].copy()

    # 预计算和准备
    print("开始情景模拟的预计算...")
    static_cols = ['Avg_Height', 'Avg_Slope', 'Avg_Aspect', 'Avg_Relief',
                   'Water_Pct', 'Tree_Pct', 'Crop_Pct', 'BuiltArea_']
    dynamic_cols = ['avg_pm25', 'temp_c', 'precip_mm', 'evi']

    latest_static_features = history_df.sort_values('timestamp').drop_duplicates('Grid_ID', keep='last')[
        ['Grid_ID'] + static_cols]

    monthly_avg_dynamic = history_df.groupby([history_df['Grid_ID'], history_df['timestamp'].dt.month])[
        dynamic_cols].mean().reset_index()
    monthly_avg_dynamic.rename(columns={'timestamp': 'month'}, inplace=True)

    try:
        cls_feature_cols = MODELS['presence_classifier'].feature_name_
        richness_feature_cols = MODELS['richness_regressor'].feature_name_
        abundance_feature_cols = MODELS['abundance_regressor'].feature_name_
        shannon_feature_cols = MODELS['shannon_regressor'].feature_name_
    except Exception as e:
        raise Exception(f"加载模型或获取特征名时出错: {e}")

    all_results = {grid_id: [] for grid_id in grid_ids}  # 只初始化需要的 grid_id
    current_history = history_df.copy()

    for target_date in target_dates:
        print(f"--- 正在模拟月份: {target_date.strftime('%Y-%m')} ---")

        #批量创建所有网格的基线特征行
        target_month = target_date.month
        future_dynamic_features = monthly_avg_dynamic[monthly_avg_dynamic['month'] == target_month]
        baseline_features = pd.merge(latest_static_features, future_dynamic_features, on='Grid_ID', how='left')
        baseline_features[dynamic_cols] = baseline_features[dynamic_cols].fillna(0)
        baseline_features['timestamp'] = target_date
        baseline_features['month_sin'] = np.sin(2 * np.pi * (target_month - 1) / 12)
        baseline_features['month_cos'] = np.cos(2 * np.pi * (target_month - 1) / 12)

        print(f"    应用情景修改到 {len(grid_ids)} 个网格...")
        for feature, new_value in modifications.items():
            if feature in baseline_features.columns:
                # 使用 .loc 精确地为指定的 grid_ids 更新特征值
                baseline_features.loc[baseline_features['Grid_ID'].isin(grid_ids), feature] = new_value
                print(f"      > '{feature}' 已更新为 {new_value}")
            else:
                print(f"      > 警告: 特征 '{feature}' 不在基线数据中，无法修改。")

        # 批量重新计算时空特征
        history_context = current_history.groupby('Grid_ID').tail(12)
        df_for_recalc = pd.concat([history_context, baseline_features], ignore_index=True)
        recalculated_df = _recalculate_temporal_features_batch(df_for_recalc)
        final_feature_rows = recalculated_df[recalculated_df['timestamp'] == target_date].copy()
        final_feature_rows.dropna(how='all', inplace=True)
        if final_feature_rows.empty: continue

        # 批量预测
        X_cls = final_feature_rows[cls_feature_cols]
        presence_preds = MODELS['presence_classifier'].predict(X_cls)
        presence_probs = MODELS['presence_classifier'].predict_proba(X_cls)[:, 1]
        final_feature_rows['presence_prob'] = presence_probs
        final_feature_rows['has_richness'] = presence_preds
        pred_richness = MODELS['richness_regressor'].predict(final_feature_rows[richness_feature_cols])
        pred_abundance = MODELS['abundance_regressor'].predict(final_feature_rows[abundance_feature_cols])
        pred_shannon = MODELS['shannon_regressor'].predict(final_feature_rows[shannon_feature_cols])

        # 整理结果并更新历史
        final_feature_rows['richness'] = np.maximum(0, pred_richness)
        final_feature_rows['abundance'] = np.maximum(0, pred_abundance)
        final_feature_rows['shannon'] = np.maximum(0, pred_shannon)
        final_feature_rows.loc[final_feature_rows['richness'] == 0, 'has_richness'] = 0

        # 只处理受影响的网格
        for _, row in final_feature_rows[final_feature_rows['Grid_ID'].isin(grid_ids)].iterrows():
            grid_id = int(row['Grid_ID'])
            richness = float(row['richness'])
            abundance = float(row['abundance'])
            shannon = float(row['shannon'])
            composite_index = _calculate_composite_index(richness, abundance, shannon)

            context_features = {
                "presence_probability": round(float(row.get('presence_prob', 0.0)), 4),
                "environment": {"avg_pm25": round(float(row.get('avg_pm25', 0.0)), 2),
                                "avg_temp_c": round(float(row.get('temp_c', 0.0)), 2),
                                "evi": round(float(row.get('evi', 0.0)), 4)},
                "land_use_pct": {"water": round(float(row.get('Water_Pct', 0.0)), 4),
                                 "tree": round(float(row.get('Tree_Pct', 0.0)), 4),
                                 "built_area": round(float(row.get('BuiltArea_', 0.0)), 4),
                                 "crop": round(float(row.get('Crop_Pct', 0.0)), 4)}
            }

            result = {
                'date': target_date.strftime('%Y-%m-%d'),
                'predictions': {'richness': round(richness, 4), 'abundance': round(abundance, 4),
                                'shannon': round(shannon, 4), 'composite_index': round(composite_index, 4)},
                'context_features': context_features
            }

            if grid_id in all_results:
                all_results[grid_id].append(result)

        cols_to_keep_for_history = [col for col in current_history.columns if col in final_feature_rows.columns]
        current_history = pd.concat([current_history, final_feature_rows[cols_to_keep_for_history]], ignore_index=True)
        print(f"    情景模拟结果已整理，历史记录已更新。")

    # 返回最终结果
    final_output = [
        {"grid_id": grid_id, "predictions": preds}
        for grid_id, preds in all_results.items() if preds
    ]
    return final_output

