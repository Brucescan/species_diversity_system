# ==========================================================================================
# 脚本名称:   全局莫兰指数计算脚本 (ArcGIS Pro GP服务兼容版)
# 版本:       v1.2 - 增加报告和多结果输出
# 修复重点:
#   1. 启用HTML报告生成。
#   2. 新增Z分数、P值和报告文件路径作为脚本的输出参数。
# ==========================================================================================
import os
import arcpy
import traceback


def main():
    """
    主执行函数，用于计算全局莫兰指数。
    """
    spatial_statistics_licensed = False # 脚本中已不再使用此变量，但保留以防未来需要

    try:
        arcpy.env.overwriteOutput = True
        arcpy.env.workspace = arcpy.env.scratchGDB  # 明确设置工作空间
        arcpy.AddMessage("脚本开始执行，正在设置环境...")

        # --- 1. 获取输入参数 ---
        input_data_path = arcpy.GetParameterAsText(0)  # 这是网络路径
        analysis_field = arcpy.GetParameterAsText(1)
        spatial_relationship_concept = arcpy.GetParameterAsText(2)
        distance_threshold = arcpy.GetParameter(3)
        num_neighbors = arcpy.GetParameter(4)

        arcpy.AddMessage(f"接收到的输入网络路径: {input_data_path}")
        arcpy.AddMessage(f"分析字段: {analysis_field}")
        arcpy.AddMessage(f"空间关系概念: {spatial_relationship_concept}")

        # --- 2. [新增] 复制网络数据到本地临时工作空间 ---
        arcpy.AddMessage("正在将网络数据复制到本地临时工作空间进行分析...")
        if not arcpy.Exists(input_data_path):
            raise Exception(f"输入数据在路径 '{input_data_path}' 上不存在。")

        local_input_copy = os.path.join(arcpy.env.workspace, "temp_morans_i_input")
        arcpy.management.CopyFeatures(input_data_path, local_input_copy)
        arcpy.AddMessage(f"数据成功复制到本地: {local_input_copy}")

        # --- 3. 参数映射 (将UI参数映射到工具参数) ---
        # 注意：邻接关系在工具中的具体参数值
        if spatial_relationship_concept == "CONTIGUITY":
            conceptualization_param = "CONTIGUITY_EDGES_CORNERS"
            arcpy.AddMessage("  空间关系参数: 邻接 (边和角点 - Queen)")
        elif spatial_relationship_concept == "FIXED_DISTANCE":
            conceptualization_param = "FIXED_DISTANCE_BAND"
            arcpy.AddMessage(f"  空间关系参数: 固定距离带")
        elif spatial_relationship_concept == "K_NEAREST_NEIGHBORS":
            conceptualization_param = "K_NEAREST_NEIGHBORS"
            arcpy.AddMessage(f"  空间关系参数: K最近邻")
        else:
            conceptualization_param = spatial_relationship_concept

        distance_threshold_param = distance_threshold.value if distance_threshold and distance_threshold.value is not None else ""
        neighbors_param = num_neighbors if num_neighbors is not None else ""

        # --- 4. 执行全局莫兰指数工具 ---
        arcpy.AddMessage("正在执行全局莫兰指数计算 (使用官方推荐的 SpatialAutocorrelation 函数)...")

        morans_i_result = arcpy.stats.SpatialAutocorrelation(
            Input_Feature_Class=input_data_path,
            Input_Field=analysis_field,
            Generate_Report="GENERATE_REPORT",  # <-- 修改点 1: 启用报告生成
            Conceptualization_of_Spatial_Relationships=conceptualization_param,
            Distance_Method="EUCLIDEAN_DISTANCE",
            Standardization="ROW",
            Distance_Band_or_Threshold_Distance=distance_threshold_param,
            number_of_neighbors=neighbors_param
        )

        # --- 5. 提取并报告结果 ---
        global_moran_i_value = float(morans_i_result.getOutput(0))
        z_score = float(morans_i_result.getOutput(1))
        p_value = float(morans_i_result.getOutput(2))
        report_file_path = morans_i_result.getOutput(3) # <-- 修改点 2: 获取报告文件路径

        arcpy.AddMessage("==================================")
        arcpy.AddMessage("          全局莫兰指数结果          ")
        arcpy.AddMessage("----------------------------------")
        arcpy.AddMessage(f"莫兰指数 (Moran's I): {global_moran_i_value:.4f}")
        arcpy.AddMessage(f"Z 分数 (Z-Score): {z_score:.4f}")
        arcpy.AddMessage(f"P 值 (P-Value): {p_value:.4f}")
        arcpy.AddMessage(f"HTML 报告已生成: {report_file_path}") # 报告报告路径
        arcpy.AddMessage("==================================")

        # ... 解释部分代码保持不变 ...
        if -0.1 < global_moran_i_value < 0.1 and p_value > 0.05:
            arcpy.AddMessage("结论: 莫兰指数接近0，且P值不显著，表示数据可能呈随机分布。")
        elif global_moran_i_value > 0 and p_value <= 0.05:
            arcpy.AddMessage("结论: 莫兰指数接近+1，P值显著，表示数据存在强烈的正空间自相关（高值聚类，低值也聚类）。")
        elif global_moran_i_value < 0 and p_value <= 0.05:
            arcpy.AddMessage(
                "结论: 莫兰指数接近-1，P值显著，表示数据存在强烈的负空间自相关（高值被低值包围，或低值被高值包围）。")
        else:
            arcpy.AddMessage("结论: 莫兰指数结果或P值未达到明确的显著性或解释阈值。")

        # --- 6. 设置输出参数 ---
        # <-- 修改点 3: 设置所有需要返回的输出参数 -->
        arcpy.SetParameter(5, global_moran_i_value)  # 莫兰指数 (已存在)
        arcpy.SetParameter(6, z_score)               # Z分数 (新增)
        arcpy.SetParameter(7, p_value)               # P值 (新增)
        arcpy.SetParameter(8, report_file_path)      # 报告文件路径 (新增)

        arcpy.AddMessage("全局莫兰指数计算成功完成。")

    except arcpy.ExecuteError:
        arcpy.AddError("\nArcGIS 工具执行错误:")
        arcpy.AddError(arcpy.GetMessages(2))
        raise
    except Exception:
        arcpy.AddError("\n脚本执行过程中发生未知错误:")
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        # 许可检查的代码已删除，根据您的要求，这部分也不再需要
        pass


if __name__ == '__main__':
    main()