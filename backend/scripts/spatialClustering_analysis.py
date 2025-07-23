# -*- coding: utf-8 -*-
import arcpy
import os
import traceback
import time
import json  # 导入json库，用于将Python对象转换为JSON字符串


def main():
    """
    主执行函数，用于空间聚类分析 (Multivariate Clustering)。
    该脚本实现了基于属性和可选空间位置的K-Means聚类，并自动解读聚类结果，生成描述性名称。
    [新版本: 实现了将评估表数据作为JSON字符串返回的功能]
    """
    arcpy.AddMessage("开始执行空间聚类分析脚本...")

    # --- 环境设置 ---
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = arcpy.env.scratchGDB
    arcpy.AddMessage(f"当前工作空间已设置为: {arcpy.env.workspace}")
    BEIJING_BOUNDARY_URL = "https://product.geoscene.cn/server/rest/services/Hosted/beijing_shp/FeatureServer/0"

    # --- 声明所有临时变量 ---
    local_input_grid = None
    input_with_xy = None
    temp_clipped_output = None
    overall_stats_table = None
    cluster_stats_table = None
    # [新] 为临时评估表声明变量
    temp_evaluation_table = None

    try:
        # --- 1. 获取并处理输入参数 ---
        shared_input_grid_path = arcpy.GetParameterAsText(0)
        analysis_fields_str = arcpy.GetParameterAsText(1)
        num_clusters_str = arcpy.GetParameterAsText(2)
        if not num_clusters_str or not num_clusters_str.isdigit():
            raise Exception(f"错误: 聚类数量 (K) 必须是一个有效的正整数，但收到了 '{num_clusters_str}'。")
        num_clusters = int(num_clusters_str)
        include_spatial_str = arcpy.GetParameterAsText(3)
        include_spatial = (include_spatial_str.lower() == 'true')

        user_output_features_param = arcpy.GetParameterAsText(4)
        output_cluster_features = os.path.join(arcpy.env.scratchGDB, os.path.basename(user_output_features_param))

        # [修改] 获取新的布尔型输入参数，决定是否生成图表数据
        generate_chart_data = arcpy.GetParameterAsText(5)
        generate_chart_data = (generate_chart_data.lower() == 'true')

        # 参数6 'chart_data_string' 是输出，我们将在后面设置它的值

        # --- 2. 准备输入数据 (此部分保持不变) ---
        arcpy.AddMessage(f"接收到上游服务的输入路径: {shared_input_grid_path}")
        if not arcpy.Exists(shared_input_grid_path):
            raise Exception(f"错误: 无法找到输入要素 '{shared_input_grid_path}'。")
        local_input_grid = os.path.join(arcpy.env.workspace, "local_cluster_input_copy")
        arcpy.AddMessage(f"正在将输入数据复制到本地进行分析: {local_input_grid}")
        arcpy.management.CopyFeatures(shared_input_grid_path, local_input_grid)
        arcpy.AddMessage("数据复制成功。")

        # --- 3. 验证和处理参数 (此部分保持不变) ---
        if not analysis_fields_str:
            raise Exception("错误: 必须至少指定一个分析字段。")
        analysis_fields_list = [field.strip() for field in analysis_fields_str.split(';') if field.strip()]
        original_analysis_fields = list(analysis_fields_list)
        arcpy.AddMessage(f"用于聚类的属性字段: {', '.join(analysis_fields_list)}")
        arcpy.AddMessage(f"最终聚类结果将保存至: {output_cluster_features}")

        # --- 4. 核心分析逻辑 ---
        # [修改] 只有当用户请求时，才为评估表准备一个内存中的临时路径
        if generate_chart_data:
            temp_evaluation_table = "in_memory/temp_eval_table_for_chart"
            arcpy.AddMessage("用户请求生成评估图表数据，将创建临时评估表。")

        # (添加XY坐标的逻辑保持不变)
        current_input_for_tool = local_input_grid
        if include_spatial:
            arcpy.AddMessage("\n正在为要素添加质心坐标作为聚类变量...")
            input_with_xy = os.path.join(arcpy.env.workspace, "temp_input_with_xy")
            arcpy.management.CopyFeatures(local_input_grid, input_with_xy)
            arcpy.management.AddField(input_with_xy, "CENTROID_X", "DOUBLE")
            arcpy.management.AddField(input_with_xy, "CENTROID_Y", "DOUBLE")
            arcpy.management.CalculateField(input_with_xy, "CENTROID_X", "!SHAPE.centroid.X!", "PYTHON3")
            arcpy.management.CalculateField(input_with_xy, "CENTROID_Y", "!SHAPE.centroid.Y!", "PYTHON3")
            analysis_fields_list.extend(["CENTROID_X", "CENTROID_Y"])
            current_input_for_tool = input_with_xy

        arcpy.AddMessage("\n正在执行多元聚类分析 (Multivariate Clustering)...")
        arcpy.stats.MultivariateClustering(
            in_features=current_input_for_tool,
            output_features=output_cluster_features,
            analysis_fields=analysis_fields_list,
            number_of_clusters=num_clusters,
            initialization_method="OPTIMIZED_SEED_LOCATIONS",
            # [修改] 传入临时表路径。如果用户没选，这里的值是None，工具会自动跳过
            output_table=temp_evaluation_table
        )
        arcpy.AddMessage("聚类分析成功！")

        # (裁剪逻辑保持不变)
        arcpy.AddMessage("\n开始使用硬编码的北京市边界进行自动裁剪...")
        temp_clipped_output = os.path.join(arcpy.env.scratchGDB, "temp_clipped_clusters")
        arcpy.analysis.Clip(output_cluster_features, BEIJING_BOUNDARY_URL, temp_clipped_output)
        arcpy.management.Delete(output_cluster_features)
        arcpy.management.CopyFeatures(temp_clipped_output, output_cluster_features)
        arcpy.AddMessage("裁剪成功。")

        # --- 5. 自动解读聚类结果并添加名称 (此部分保持不变) ---
        arcpy.AddMessage("\n--- 开始自动解读聚类特征 ---")
        overall_stats_table = os.path.join("in_memory", "overall_stats")
        stats_fields_for_tool = [[field, "MEAN"] for field in original_analysis_fields]
        arcpy.Statistics_analysis(output_cluster_features, overall_stats_table, stats_fields_for_tool)
        global_averages = {}
        with arcpy.da.SearchCursor(overall_stats_table, ["MEAN_" + f for f in original_analysis_fields]) as cursor:
            for row in cursor:
                for i, field in enumerate(original_analysis_fields):
                    global_averages[field] = row[i]

        cluster_stats_table = os.path.join("in_memory", "cluster_stats")
        arcpy.Statistics_analysis(output_cluster_features, cluster_stats_table, stats_fields_for_tool, "CLUSTER_ID")

        cluster_names = {}
        HIGH_THRESHOLD_MULTIPLIER = 1.15
        LOW_THRESHOLD_MULTIPLIER = 0.85
        cursor_fields = ["CLUSTER_ID"] + ["MEAN_" + f for f in original_analysis_fields]
        with arcpy.da.SearchCursor(cluster_stats_table, cursor_fields) as cursor:
            for row in cursor:
                cluster_id = int(row[0])
                descriptions = []
                for i, field in enumerate(original_analysis_fields):
                    cluster_avg = row[i + 1];
                    global_avg = global_averages.get(field);
                    level = "中"
                    if global_avg is not None and global_avg != 0:
                        if cluster_avg > global_avg * HIGH_THRESHOLD_MULTIPLIER:
                            level = "高"
                        elif cluster_avg < global_avg * LOW_THRESHOLD_MULTIPLIER:
                            level = "低"
                    simple_field_name = field.replace('avg_', '').replace('mean_', '')
                    descriptions.append(f"{simple_field_name}:{level}")
                cluster_names[cluster_id] = ", ".join(descriptions)

        field_name = "ClusterName"
        arcpy.management.AddField(output_cluster_features, field_name, "TEXT", field_length=250)
        with arcpy.da.UpdateCursor(output_cluster_features, ["CLUSTER_ID", field_name]) as cursor:
            for row in cursor:
                if row[0] in cluster_names: row[1] = cluster_names[row[0]]; cursor.updateRow(row)
        arcpy.AddMessage("类别名称字段已成功更新。")

        # --- [新代码块] 5b. 处理并返回图表数据字符串 ---
        if generate_chart_data and temp_evaluation_table and arcpy.Exists(temp_evaluation_table):
            arcpy.AddMessage("\n正在处理评估表并生成图表数据...")
            chart_data_list = []
            fields_to_read = ["NUM_GROUPS","PSEUDO_F"]
            with arcpy.da.SearchCursor(temp_evaluation_table, fields_to_read) as cursor:
                for row in cursor:
                    row_data = {
                        "k": row[0],
                        "pseudo_f": round(row[1], 2) if row[1] is not None else None
                    }
                    chart_data_list.append(row_data)

            chart_json_string = json.dumps(chart_data_list)
            arcpy.SetParameterAsText(6, chart_json_string)  # 将JSON字符串设置为参数6的值
            arcpy.AddMessage("图表数据JSON字符串已成功生成。")

        # --- 6. 设置输出参数 ---
        arcpy.SetParameterAsText(4, output_cluster_features)
        # 参数5是输入, 参数6的值已在上面设置

        arcpy.AddMessage("\n脚本成功完成。")

    except arcpy.ExecuteError:
        # (错误处理逻辑不变)
        arcpy.AddError("ArcPy 执行错误:")
        arcpy.AddError(arcpy.GetMessages(2))
        raise
    except Exception as e:
        # (错误处理逻辑不变)
        arcpy.AddError("脚本执行失败:")
        arcpy.AddError(f"错误类型: {type(e).__name__}")
        arcpy.AddError(f"错误信息: {e}")
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        arcpy.AddMessage("正在清理临时文件...")

        def silent_delete(item):
            if item and arcpy.Exists(item):
                try:
                    arcpy.management.Delete(item)
                except:
                    pass

        # (清理逻辑不变，但增加了对新临时表的清理)
        silent_delete(local_input_grid)
        silent_delete(input_with_xy)
        silent_delete(temp_clipped_output)
        silent_delete(overall_stats_table)
        silent_delete(cluster_stats_table)
        silent_delete(temp_evaluation_table)  # 清理内存中的临时评估表

        arcpy.AddMessage("清理完成。")


if __name__ == '__main__':
    main()