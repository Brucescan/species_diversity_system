# analysis_api/services/prediction_service.py

import pandas as pd
import numpy as np
from .ml_loader import MODELS, GLOBAL_DF_HISTORY_PROCESSED


def _create_baseline_row(grid_id, target_date, history_df):
    """为单个 Grid_ID 和未来月份创建基线特征行"""
    # 1. 提取静态特征：从该网格最新的历史记录中获取
    latest_grid_record = history_df[history_df['Grid_ID'] == grid_id].sort_values('timestamp').iloc[-1]

    # 假设这些是静态特征
    static_cols = [ 'Avg_Height', 'Avg_Slope', 'Avg_Aspect', 'Avg_Relief',
                   'Water_Pct', 'Tree_Pct', 'Crop_Pct', 'BuiltArea_']
    baseline = latest_grid_record[static_cols].to_dict()
    baseline['Grid_ID'] = grid_id
    baseline['timestamp'] = target_date

    # 2. 计算月度动态特征的平均值 ("最新一年同月平均值")
    target_month = target_date.month
    # 优先使用前一年的数据
    last_year_date_start = pd.Timestamp(year=target_date.year - 1, month=target_month, day=1)
    last_year_date_end = last_year_date_start + pd.offsets.MonthEnd(0)

    # 筛选出与目标月份相同的历史数据
    same_month_history = history_df[
        (history_df['Grid_ID'] == grid_id) &
        (history_df['timestamp'].dt.month == target_month)
        ]

    # 筛选出前一年的同月数据
    last_year_same_month_data = same_month_history[
        (same_month_history['timestamp'] >= last_year_date_start) &
        (same_month_history['timestamp'] <= last_year_date_end)
        ]

    dynamic_features_source = same_month_history
    if not last_year_same_month_data.empty:
        # 如果前一年有数据，优先使用
        dynamic_features_source = last_year_same_month_data

    dynamic_cols = ['avg_pm25', 'temp_c', 'precip_mm', 'evi']
    for col in dynamic_cols:
        if not dynamic_features_source.empty:
            baseline[col] = dynamic_features_source[col].mean()
        else:
            # 如果历史上完全没有这个月的数据，用0或全局平均值填充
            baseline[col] = 0

    return pd.DataFrame([baseline])


def _recalculate_temporal_features(temp_df):
    """在一个临时的 DataFrame 上重新计算时间特征（滞后、滚动）"""
    ds = temp_df.set_index(['Grid_ID', 'timestamp']).to_xarray()

    # 时间滞后特征
    lags = [1, 3, 6, 12]
    lag_vars = ['richness', 'abundance', 'shannon', 'avg_pm25', 'temp_c', 'evi']  # Tree/Water Pct 是静态的
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

    final_row_df = ds.to_dataframe().reset_index().iloc[[-1]]
    return final_row_df


def _calculate_composite_index(richness, abundance, shannon):
    """
    计算生态环境综合指标。
    !!!注意!!!: 这是一个占位符。您需要根据您的标准化和加权逻辑来实现它。
    """
    # 示例：简单的加权平均（权重需要您来定义）
    # 假设所有值都已被标准化到 0-1 之间
    # weights = {'richness': 0.4, 'abundance': 0.3, 'shannon': 0.3}
    # composite_index = (richness * weights['richness'] +
    #                    abundance * weights['abundance'] +
    #                    shannon * weights['shannon'])
    # 为了演示，我们暂时返回丰富度的值
    return shannon


def perform_prediction(target_dates):
    """
    执行完整的预测循环。
    """
    history_df = GLOBAL_DF_HISTORY_PROCESSED
    if history_df is None:
        raise Exception("历史数据尚未加载，服务无法预测。")

    all_grid_ids = history_df['Grid_ID'].unique()
    all_results = {}  # 使用字典 {grid_id: [predictions]} 结构

    # 准备一个所有模型输入特征的模板列，以保证一致性
    # 从一个加载好的回归模型中获取特征列表
    model_feature_cols = MODELS['richness_regressor'].feature_name_
    leaky_features_for_classifier = [col for col in model_feature_cols if
                                     'richness' in col or 'abundance' in col or 'shannon' in col]
    leaky_features_for_classifier.append('BuiltArea_')

    for grid_id in all_grid_ids:
        all_results[grid_id] = []

    for target_date in target_dates:
        print(f"--- 正在预测月份: {target_date.strftime('%Y-%m')} ---")

        # 准备一个包含所有 Grid 当月预测输入的 DataFrame
        current_month_input_df = pd.DataFrame(columns=model_feature_cols)

        for grid_id in all_grid_ids:
            # a. 获取历史上下文
            history_context = history_df[
                (history_df['Grid_ID'] == grid_id) &
                (history_df['timestamp'] < target_date)
                ].tail(12)

            if history_context.empty:
                continue  # 如果一个网格完全没有历史，则跳过

            # b. 构建基线
            baseline_row = _create_baseline_row(grid_id, target_date, history_df)

            # c. 临时拼接并重新计算特征
            temp_df = pd.concat([history_context, baseline_row], ignore_index=True)
            final_feature_row = _recalculate_temporal_features(temp_df)

            # d. 生成 presence_prob
            X_cls = final_feature_row.drop(columns=leaky_features_for_classifier, errors='ignore')[
                MODELS['presence_classifier'].feature_name_]
            presence_prob = MODELS['presence_classifier'].predict_proba(X_cls)[:, 1][0]

            # e. 存储并组合最终特征向量
            final_feature_row['presence_prob'] = presence_prob

            # 确保列的顺序和数量与模型训练时完全一致
            current_month_input_df = pd.concat([current_month_input_df, final_feature_row[model_feature_cols]],
                                               ignore_index=True)

        # f. 批量预测（对当月所有网格）
        if not current_month_input_df.empty:
            pred_richness = MODELS['richness_regressor'].predict(current_month_input_df)
            pred_abundance = MODELS['abundance_regressor'].predict(current_month_input_df)
            pred_shannon = MODELS['shannon_regressor'].predict(current_month_input_df)

            # 将预测结果合并回 DataFrame 以便查询
            current_month_input_df['pred_richness'] = np.maximum(0, pred_richness)
            current_month_input_df['pred_abundance'] = np.maximum(0, pred_abundance)
            current_month_input_df['pred_shannon'] = np.maximum(0, pred_shannon)

            # 3. 计算综合指标并存入结果
            for idx, row in current_month_input_df.iterrows():
                grid_id = int(row['Grid_ID'])
                composite_index = _calculate_composite_index(
                    row['pred_richness'], row['pred_abundance'], row['pred_shannon']
                )
                all_results[grid_id].append({
                    "timestamp": target_date.strftime('%Y-%m-%d'),
                    "predicted_composite_index": composite_index
                })

    # 4. 格式化最终输出
    final_output = [
        {"grid_id": grid_id, "predictions": preds}
        for grid_id, preds in all_results.items() if preds
    ]

    return final_output