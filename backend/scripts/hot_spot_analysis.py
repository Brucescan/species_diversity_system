import arcpy
import os
import traceback
import time


def main():
    try:
        if arcpy.CheckExtension("Spatial") == "Available":
            arcpy.CheckOutExtension("Spatial")
            arcpy.AddMessage("已成功检出 Spatial Analyst 扩展许可。")
        else:
            raise arcpy.ExecuteError("错误: 无法获取 Spatial Analyst 扩展许可。")
    except arcpy.ExecuteError as e:
        arcpy.AddError(str(e))
        raise

    BEIJING_BOUNDARY_URL = "https://product.geoscene.cn/server/rest/services/Hosted/beijing_shp/FeatureServer/0"
    INTERPOLATION_CELL_SIZE = 250

    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = arcpy.env.scratchGDB
    arcpy.AddMessage(f"当前工作空间已设置为: {arcpy.env.workspace}")

    local_input_grid = None
    temp_hotspot_polygons = None
    temp_centroid_points = None
    temp_interpolated_raster = None

    try:
        shared_input_grid_path = arcpy.GetParameterAsText(0)
        analysis_field = arcpy.GetParameterAsText(1)
        conceptualization = arcpy.GetParameterAsText(2)
        distance_threshold = arcpy.GetParameterAsText(3)
        num_neighbors = arcpy.GetParameterAsText(4)
        output_final_raster = arcpy.GetParameterAsText(5)

        #准备输入数据
        if not arcpy.Exists(shared_input_grid_path):
            raise Exception(f"严重错误: 无法在共享路径上找到输入分析网格 '{shared_input_grid_path}'。")

        # 将数据复制到本地临时工作空间以提高性能和稳定性
        local_input_grid = os.path.join(arcpy.env.workspace, "local_analysis_grid_copy")
        arcpy.management.CopyFeatures(shared_input_grid_path, local_input_grid)
        arcpy.AddMessage(f"数据已复制到本地进行分析: {local_input_grid}")

        #检查并处理输出路径
        if not output_final_raster:
            arcpy.AddWarning("警告: 未指定输出栅格路径。将在临时工作空间中自动生成输出。")
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_name = f"InterpolatedHotSpot_{analysis_field}_{timestamp}"
            output_final_raster = os.path.join(arcpy.env.scratchGDB, output_name)
            arcpy.AddMessage(f"自动生成的输出栅格路径为: {output_final_raster}")

        arcpy.AddMessage(f"最终输出栅格将保存到: {output_final_raster}")


        temp_hotspot_polygons = os.path.join(arcpy.env.workspace, "temp_hotspot_result_polygons")

        distance_param = ""
        num_neighbors_param = ""
        if "FIXED_DISTANCE_BAND" in conceptualization:
            distance_param = distance_threshold
        elif "K_NEAREST_NEIGHBORS" in conceptualization:
            num_neighbors_param = int(num_neighbors) if num_neighbors else ""

        arcpy.stats.HotSpots(
            Input_Feature_Class=local_input_grid,
            Input_Field=analysis_field,
            Output_Feature_Class=temp_hotspot_polygons,
            Conceptualization_of_Spatial_Relationships=conceptualization,
            Distance_Method="EUCLIDEAN_DISTANCE",
            Distance_Band_or_Threshold_Distance=distance_param,
            number_of_neighbors=num_neighbors_param
        )
        arcpy.AddMessage(f"热点分析成功，中间多边形结果已生成。")

        # 将多边形网格转换为质心点
        temp_centroid_points = os.path.join(arcpy.env.workspace, "temp_hotspot_centroids")
        arcpy.management.FeatureToPoint(
            in_features=temp_hotspot_polygons,
            out_feature_class=temp_centroid_points,
            point_location="CENTROID"
        )
        arcpy.AddMessage(f"质心点创建成功。")

        # 执行空间插值 (IDW) 生成连续表面
        interpolation_field_name = "GiZScore"
        arcpy.AddMessage(f"插值将使用固定的像元大小: {INTERPOLATION_CELL_SIZE} 米。")

        idw_raster_obj = arcpy.sa.Idw(
            in_point_features=temp_centroid_points,
            z_field=interpolation_field_name,
            cell_size=INTERPOLATION_CELL_SIZE
        )

        temp_interpolated_raster = os.path.join(arcpy.env.workspace, "temp_unclipped_raster")
        idw_raster_obj.save(temp_interpolated_raster)
        arcpy.AddMessage(f"插值成功，生成了未裁剪的连续表面栅格。")

        # 使用北京市边界裁剪栅格
        arcpy.AddMessage(f"使用裁剪边界: {BEIJING_BOUNDARY_URL}")

        # 使用 ExtractByMask 工具裁剪栅格
        clipped_raster_obj = arcpy.sa.ExtractByMask(
            in_raster=temp_interpolated_raster,
            in_mask_data=BEIJING_BOUNDARY_URL
        )

        clipped_raster_obj.save(output_final_raster)
        arcpy.AddMessage(f"栅格裁剪成功，最终结果已保存到: {output_final_raster}")

        arcpy.SetParameter(5, output_final_raster)
        arcpy.AddMessage("\n脚本成功完成。")

    except arcpy.ExecuteError:
        arcpy.AddError("ArcPy 执行错误:")
        arcpy.AddError(arcpy.GetMessages(2))
        raise
    except Exception as e:
        # 捕获所有其他Python错误
        arcpy.AddError("脚本执行失败。")
        arcpy.AddError(f"错误类型: {type(e).__name__}")
        arcpy.AddError(f"错误信息: {e}")
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        arcpy.CheckInExtension("Spatial")
        arcpy.AddMessage("已归还 Spatial Analyst 扩展许可。")

        arcpy.AddMessage("正在清理临时文件...")

        def cleanup(path):
            if path and arcpy.Exists(path):
                try:
                    arcpy.Delete_management(path)
                except:
                    pass

        cleanup(local_input_grid)
        cleanup(temp_hotspot_polygons)
        cleanup(temp_centroid_points)
        cleanup(temp_interpolated_raster)
        arcpy.AddMessage("临时文件清理完毕。")


if __name__ == '__main__':
    main()
