import arcpy
import os
import traceback
import time

# --- 在脚本开头硬编码北京市边界URL，便于管理 ---
BEIJING_BOUNDARY_URL = "https://product.geoscene.cn/server/rest/services/Hosted/beijing_shp/FeatureServer/0"


def main():
    """
    GWR地理处理工具最终兼容版：
    - 使用最基础的Python语法，避免服务器解析错误。
    - 内置数据过滤、转点、可选插值和裁剪。
    """
    arcpy.AddMessage("开始执行 GWR 脚本 (最终兼容版)...")
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = arcpy.env.scratchGDB
    arcpy.AddMessage("当前工作空间已设置为: {0}".format(arcpy.env.workspace))

    # --- 声明所有临时变量 ---
    input_layer_for_select = "gwr_input_layer_for_select"
    filtered_features = None
    gwr_output_points = None
    unclipped_raster = None

    try:
        # --- 1. 一次性获取所有参数 ---
        in_features_path = arcpy.GetParameterAsText(0)
        dependent_variable = arcpy.GetParameterAsText(1)
        model_type = arcpy.GetParameterAsText(2)
        explanatory_variables_str = arcpy.GetParameterAsText(3)
        output_features = arcpy.GetParameterAsText(4)
        neighborhood_type = arcpy.GetParameterAsText(5)
        local_weighting_scheme = arcpy.GetParameterAsText(6)
        neighborhood_selection_method = arcpy.GetParameterAsText(7)
        user_defined_value_str = arcpy.GetParameterAsText(8)
        manual_min_value_str = arcpy.GetParameterAsText(9)
        manual_max_value_str = arcpy.GetParameterAsText(10)
        manual_increments_num_str = arcpy.GetParameterAsText(11)
        interpolation_field = arcpy.GetParameterAsText(12)

        # --- 2. 记录参数 ---
        arcpy.AddMessage("输入要素路径: {0}".format(in_features_path))

        # [兼容性修改] 使用标准的 if-else 块代替三元表达式
        if explanatory_variables_str:
            explanatory_variables = explanatory_variables_str.split(';')
        else:
            explanatory_variables = []
        arcpy.AddMessage("解释变量: {0}".format(', '.join(explanatory_variables)))

        # --- 3. [终极修改] 使用 SelectLayerByAttribute 进行过滤 ---
        if not arcpy.Exists(in_features_path):
            raise ValueError("无法找到输入要素 '{0}'。".format(in_features_path))

        arcpy.management.MakeFeatureLayer(in_features_path, input_layer_for_select)
        arcpy.AddMessage("已创建内存图层用于选择。")

        # [兼容性修改] 使用 .format() 构造查询语句
        delimited_field = arcpy.AddFieldDelimiters(input_layer_for_select, dependent_variable)
        where_clause = "{0} > 0".format(delimited_field)
        arcpy.AddMessage("过滤条件: {0}".format(where_clause))

        arcpy.management.SelectLayerByAttribute(
            in_layer_or_view=input_layer_for_select,
            selection_type="NEW_SELECTION",
            where_clause=where_clause
        )

        count_result = arcpy.management.GetCount(input_layer_for_select)
        feature_count = int(count_result.getOutput(0))
        if feature_count == 0:
            raise ValueError("过滤后没有找到任何因变量大于0的要素，无法进行GWR分析。")
        arcpy.AddMessage("数据过滤成功，共有 {0} 个有效要素被选中。".format(feature_count))

        filtered_features = os.path.join(arcpy.env.workspace, "local_gwr_filtered")
        arcpy.management.CopyFeatures(input_layer_for_select, filtered_features)
        arcpy.AddMessage("已将选中要素复制到: {0}".format(filtered_features))

        # --- 4. 准备GWR参数 ---
        gwr_kwargs = {}
        if neighborhood_selection_method == 'USER_DEFINED':
            if not user_defined_value_str:
                raise ValueError("USER_DEFINED需要邻域值。")
            if neighborhood_type == 'DISTANCE_BAND':
                gwr_kwargs['distance_band'] = user_defined_value_str
            else:
                gwr_kwargs['number_of_neighbors'] = int(user_defined_value_str)
        elif neighborhood_selection_method == 'MANUAL_INTERVALS':
            if not all([manual_min_value_str, manual_max_value_str, manual_increments_num_str]):
                raise ValueError("MANUAL_INTERVALS需要完整参数。")
            gwr_kwargs['number_of_increments'] = int(manual_increments_num_str)
            if neighborhood_type == 'DISTANCE_BAND':
                gwr_kwargs['minimum_search_distance'] = manual_min_value_str
                gwr_kwargs['maximum_search_distance'] = manual_max_value_str
            else:
                gwr_kwargs['minimum_number_of_neighbors'] = int(manual_min_value_str)
                gwr_kwargs['maximum_number_of_neighbors'] = int(manual_max_value_str)

        # --- 5. 执行GWR分析 ---
        arcpy.AddMessage("正在执行GWR分析...")
        arcpy.stats.GWR(in_features=filtered_features, dependent_variable=dependent_variable, model_type=model_type,
                        explanatory_variables=explanatory_variables, output_features=output_features,
                        neighborhood_type=neighborhood_type, local_weighting_scheme=local_weighting_scheme,
                        neighborhood_selection_method=neighborhood_selection_method, scale="SCALE_DATA", **gwr_kwargs)
        arcpy.AddMessage("GWR分析成功，结果已保存到: {0}".format(output_features))

        # --- 6. 执行可选的插值和裁剪 ---
        if interpolation_field:
            arcpy.AddMessage("开始对字段 '{0}' 进行插值...".format(interpolation_field))

            arcpy.AddMessage("正在将GWR输出结果（多边形）转换为点以进行插值...")
            gwr_output_points = os.path.join(arcpy.env.workspace, "gwr_output_points")
            arcpy.management.FeatureToPoint(in_features=output_features, out_feature_class=gwr_output_points,
                                            point_location="CENTROID")
            arcpy.AddMessage("要素转点成功。")

            with arcpy.EnvManager(
                    outputCoordinateSystem=arcpy.Describe(in_features_path).spatialReference,
                    extent=arcpy.Describe(in_features_path).extent
            ):
                unclipped_raster = arcpy.sa.Idw(in_point_features=gwr_output_points, z_field=interpolation_field,
                                                power=2)
                arcpy.AddMessage("IDW插值计算完成。")

            arcpy.AddMessage("正在使用北京边界进行裁剪: {0}".format(BEIJING_BOUNDARY_URL))
            clipped_raster = arcpy.sa.ExtractByMask(unclipped_raster, BEIJING_BOUNDARY_URL)
            arcpy.AddMessage("裁剪成功。")

            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_raster_name = "gwr_interp_{0}".format(timestamp)
            output_raster_path = os.path.join(arcpy.env.scratchGDB, output_raster_name)

            clipped_raster.save(output_raster_path)
            arcpy.AddMessage("最终裁剪后的栅格已保存到: {0}".format(output_raster_path))

            arcpy.SetParameter(13, output_raster_path)
        else:
            arcpy.AddMessage("未指定插值字段，跳过插值和裁剪步骤。")

        # --- 7. 设置GWR的输出参数 ---
        arcpy.SetParameter(4, output_features)
        arcpy.AddMessage("脚本成功完成。")

    except arcpy.ExecuteError as e:
        arcpy.AddError("ArcPy 执行错误:")
        arcpy.AddError(arcpy.GetMessages(2))
        raise
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        arcpy.AddError("脚本执行失败:")
        arcpy.AddError("错误类型: {0}".format(error_type))
        arcpy.AddError("错误信息: {0}".format(error_msg))
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        if arcpy.Exists(input_layer_for_select):
            arcpy.management.Delete(input_layer_for_select)
        for temp_feature in [filtered_features, gwr_output_points]:
            if temp_feature and arcpy.Exists(temp_feature):
                try:
                    arcpy.AddMessage("正在清理临时文件: {0}".format(temp_feature))
                    arcpy.Delete_management(temp_feature)
                except Exception as e:
                    arcpy.AddWarning("无法清理临时文件 '{0}': {1}".format(temp_feature, str(e)))


if __name__ == '__main__':
    main()