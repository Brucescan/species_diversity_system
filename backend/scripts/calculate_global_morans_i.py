import os
import arcpy
import traceback

def main():
    spatial_statistics_licensed = False

    try:
        arcpy.env.overwriteOutput = True
        arcpy.env.workspace = arcpy.env.scratchGDB
        arcpy.AddMessage("脚本开始执行，正在设置环境...")

        # 获取输入参数
        input_data_path = arcpy.GetParameterAsText(0)
        analysis_field = arcpy.GetParameterAsText(1)
        spatial_relationship_concept = arcpy.GetParameterAsText(2)
        distance_threshold = arcpy.GetParameter(3)
        num_neighbors = arcpy.GetParameter(4)

        arcpy.AddMessage(f"接收到的输入网络路径: {input_data_path}")
        arcpy.AddMessage(f"分析字段: {analysis_field}")
        arcpy.AddMessage(f"空间关系概念: {spatial_relationship_concept}")

        #复制网络数据到本地临时工作空间
        arcpy.AddMessage("正在将网络数据复制到本地临时工作空间进行分析...")
        if not arcpy.Exists(input_data_path):
            raise Exception(f"输入数据在路径 '{input_data_path}' 上不存在。")

        local_input_copy = os.path.join(arcpy.env.workspace, "temp_morans_i_input")
        arcpy.management.CopyFeatures(input_data_path, local_input_copy)
        arcpy.AddMessage(f"数据成功复制到本地: {local_input_copy}")

        #参数映射
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

        # 执行全局莫兰

        morans_i_result = arcpy.stats.SpatialAutocorrelation(
            Input_Feature_Class=local_input_copy,
            Input_Field=analysis_field,
            Generate_Report="GENERATE_REPORT",  # <-- 修改点 1: 启用报告生成
            Conceptualization_of_Spatial_Relationships=conceptualization_param,
            Distance_Method="EUCLIDEAN_DISTANCE",
            Standardization="ROW",
            Distance_Band_or_Threshold_Distance=distance_threshold_param,
            number_of_neighbors=neighbors_param
        )

        global_moran_i_value = float(morans_i_result.getOutput(0))
        z_score = float(morans_i_result.getOutput(1))
        p_value = float(morans_i_result.getOutput(2))
        report_file_path = morans_i_result.getOutput(3) # <-- 修改点 2: 获取报告文件路径


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

        arcpy.SetParameter(5, global_moran_i_value)
        arcpy.SetParameter(6, z_score)
        arcpy.SetParameter(7, p_value)
        arcpy.SetParameter(8, report_file_path)

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
        pass


if __name__ == '__main__':
    main()