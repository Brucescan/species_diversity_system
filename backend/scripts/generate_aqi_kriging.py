import arcpy
import datetime
import os
from arcpy.sa import *
import time

def get_aqi_data_for_time_tool(sde_connection_input, target_datetime_str, out_feature_class_name):  # Renamed parameter
    """
    从PostgreSQL数据库查询指定时间点之前最新的AQI站点数据，并输出到要素类。
    sde_connection_input: SDE 连接信息 (可以是 .sde 文件路径或 {"itemId": "..."} JSON 字符串)。
    target_datetime_str: 目标时间的字符串，格式如 'YYYY-MM-DD HH:MI:SS'
    out_feature_class_name: 输出要素类的完整路径。
    """
    arcpy.AddMessage(f"Using SDE connection input: {sde_connection_input}")

    try:
        target_dt = datetime.datetime.strptime(target_datetime_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        arcpy.AddError("Invalid datetime format. Please use 'YYYY-MM-DD HH:MI:SS'.")
        raise

    sql_query = f"""
    SELECT
        s.id::integer as station_pk,
        s.name as station_name,
        s.location::geometry as shape,
        la.aqi
    FROM
        data_pipeline_aqistation s
    INNER JOIN
        (SELECT
            r.station_id,
            r.aqi,
            r.timestamp,
            ROW_NUMBER() OVER (PARTITION BY r.station_id ORDER BY r.timestamp DESC) as rn
        FROM
            data_pipeline_aqirecord r
        WHERE
            r.aqi IS NOT NULL AND r.timestamp <= '{target_dt.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp
        ) AS la ON s.id = la.station_id
    WHERE
        la.rn = 1 AND s.location IS NOT NULL
    """

    query_layer_name = "AQIStations_Time_QueryLayer_Tool"
    try:
        # MakeQueryLayer可以直接处理 {"itemId": "..."} 或 SDE 文件路径
        arcpy.management.MakeQueryLayer(
            input_database=sde_connection_input,  # 使用修改后的参数名
            out_layer_name=query_layer_name,
            query=sql_query,
            oid_fields="station_pk",
            shape_type="POINT",
            srid="4326",  # SRID for PostgreSQL/PostGIS geometry
            spatial_reference=arcpy.SpatialReference(4326)  # WGS84
        )
        arcpy.AddMessage(f"Attempted to create query layer: {query_layer_name}")

        if not arcpy.Exists(query_layer_name):
            arcpy.AddError(f"错误：查询图层 '{query_layer_name}' 未能成功创建。请检查数据库连接和SQL查询。")
            # 添加更多调试信息
            arcpy.AddError(f"Input database parameter was: {sde_connection_input}")
            arcpy.AddError(f"SQL Query was: {sql_query}")
            raise Exception(f"查询图层 '{query_layer_name}' 创建失败。")

        count_result = arcpy.management.GetCount(query_layer_name)
        count = int(count_result[0])
        arcpy.AddMessage(f"查询图层 '{query_layer_name}' 中包含 {count} 个要素。")

        if count == 0:
            arcpy.AddWarning("警告：查询图层中没有要素。请检查 SQL 查询和目标时间。")
            # 可以考虑在这里不复制要素，而是直接返回或抛出特定错误，如果这是期望的行为

        arcpy.management.CopyFeatures(query_layer_name, out_feature_class_name)
        arcpy.AddMessage(f"Features copied to {out_feature_class_name}")
    except arcpy.ExecuteError as ee:
        arcpy.AddError(f"ArcPy ExecuteError during query layer creation or copy: {arcpy.GetMessages(2)}")
        arcpy.AddError(f"ArcPy ExecuteError during query layer creation or copy: {arcpy.GetMessages(1)}")
        arcpy.AddError(f"ArcPy ExecuteError during query layer creation or copy: {arcpy.GetMessages(0)}")
        # 添加更多调试信息
        arcpy.AddError(f"Input database parameter was: {sde_connection_input}")
        raise
    except Exception as e:
        arcpy.AddError(f"General error during query layer creation or copy: {str(e)}")
        import traceback
        arcpy.AddError(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        if arcpy.Exists(query_layer_name):
            arcpy.management.Delete(query_layer_name)
            arcpy.AddMessage(f"Deleted temporary query layer: {query_layer_name}")

    return out_feature_class_name


def run_spatial_analyst_kriging_tool(input_features, z_field, out_raster_name):
    """执行Spatial Analyst普通克里金插值，并根据要素服务裁剪"""

    beijing_mask_service_url = "https://product.geoscene.cn/server/rest/services/Hosted/beijing_shp/FeatureServer/0"
    arcpy.AddMessage(f"Using hardcoded mask service URL: {beijing_mask_service_url}")

    # --------------------  定义分析环境的投影坐标系 --------------------
    analysis_sr = arcpy.SpatialReference(4545)  # CGCS2000_3_Degree_GK_Zone_38
    arcpy.env.outputCoordinateSystem = analysis_sr
    arcpy.AddMessage(f"Analysis environment output SR set to: {analysis_sr.name}, WKID: {analysis_sr.factoryCode}")
    arcpy.env.extent = None  # 清除之前可能的全局范围设置

    cell_size = 500  # 克里金的像元大小，在投影坐标系下通常以米为单位更合理，0.005度太小了。

    k_model_semivariogram_type = "SPHERICAL"
    k_model_lag_size = cell_size * 2  # Lag size可以调整，通常是cell_size的倍数
    semi_variogram_props_obj = KrigingModelOrdinary(
        k_model_semivariogram_type,
        k_model_lag_size,
    )
    search_radius_obj = RadiusVariable(8)  # 8个点进行插值

    # 定义临时投影后的要素类路径
    temp_points_fc_projected = os.path.join(arcpy.env.scratchGDB, "aqi_points_projected")
    temp_mask_fc_projected = os.path.join(arcpy.env.scratchGDB, "beijing_mask_projected")

    # --- 下载和投影掩膜数据 ---
    temp_mask_feature_class = os.path.join(arcpy.env.scratchGDB, "beijing_mask_fc_orig")  # 原始下载的临时掩膜
    arcpy.AddMessage(f"正在从要素服务下载掩膜数据到: {temp_mask_feature_class}")
    try:
        arcpy.management.MakeFeatureLayer(beijing_mask_service_url, "beijing_mask_layer")
        arcpy.management.CopyFeatures("beijing_mask_layer", temp_mask_feature_class)
        arcpy.AddMessage("掩膜数据下载成功。")

        mask_count_result = arcpy.management.GetCount(temp_mask_feature_class)
        mask_count = int(mask_count_result[0])
        arcpy.AddMessage(f"掩膜要素类 '{temp_mask_feature_class}' 中包含 {mask_count} 个要素。")

        if mask_count == 0:
            arcpy.AddError("错误：下载的掩膜要素类中没有要素。请检查要素服务的数据。")
            raise Exception("掩膜要素类为空，无法进行裁剪。")

        # 检查原始掩膜和输入点的坐标系
        desc_mask_orig = arcpy.Describe(temp_mask_feature_class)
        arcpy.AddMessage(
            f"原始掩膜要素类空间参考: {desc_mask_orig.spatialReference.name} (WKID: {desc_mask_orig.spatialReference.factoryCode})")

        desc_points_orig = arcpy.Describe(input_features)
        arcpy.AddMessage(
            f"原始输入点空间参考: {desc_points_orig.spatialReference.name} (WKID: {desc_points_orig.spatialReference.factoryCode})")

        # 投影掩膜数据到分析坐标系
        arcpy.AddMessage(f"正在投影掩膜数据到 '{analysis_sr.name}' ({analysis_sr.factoryCode})...")
        arcpy.management.Project(temp_mask_feature_class, temp_mask_fc_projected, analysis_sr)
        arcpy.AddMessage("掩膜数据投影完成。")
        mask_extent_proj = arcpy.Describe(temp_mask_fc_projected).extent

        # 投影输入点数据到分析坐标系
        arcpy.AddMessage(f"正在投影输入点数据到 '{analysis_sr.name}' ({analysis_sr.factoryCode})...")
        arcpy.management.Project(input_features, temp_points_fc_projected, analysis_sr)
        arcpy.AddMessage("输入点数据投影完成。")
        points_extent_proj = arcpy.Describe(temp_points_fc_projected).extent

        # -------------------- 手动计算联合范围 --------------------
        unified_xmin = min(points_extent_proj.XMin, mask_extent_proj.XMin)
        unified_ymin = min(points_extent_proj.YMin, mask_extent_proj.YMin)
        unified_xmax = max(points_extent_proj.XMax, mask_extent_proj.XMax)
        unified_ymax = max(points_extent_proj.YMax, mask_extent_proj.YMax)

        arcpy.env.extent = arcpy.Extent(unified_xmin, unified_ymin, unified_xmax, unified_ymax)
        arcpy.AddMessage(
            f"Environment extent set to: XMin={arcpy.env.extent.XMin}, YMin={arcpy.env.extent.YMin}, XMax={arcpy.env.extent.XMax}, YMax={arcpy.env.extent.YMax} (in {analysis_sr.name} units)")
        # --------------------------------------------------------------------

    except Exception as e:
        arcpy.AddError(f"数据准备（下载/投影）失败: {e}")
        import traceback
        arcpy.AddError(f"Traceback: {traceback.format_exc()}")
        # 如果数据准备阶段就失败了，无法进行克里金，直接抛出错误
        raise

    arcpy.AddMessage(f"开始执行克里金插值，输入要素: {temp_points_fc_projected}, Z字段: {z_field}")
    # Kriging 工具将使用 arcpy.env.extent 和 arcpy.env.outputCoordinateSystem
    out_krig_raster_obj = Kriging(
        in_point_features=temp_points_fc_projected,  # 使用投影后的点数据
        z_field=z_field,
        kriging_model=semi_variogram_props_obj,
        cell_size=cell_size,
        search_radius=search_radius_obj,
        out_variance_prediction_raster=""  # 不需要输出方差预测栅格
    )

    # --- 使用投影后的掩膜数据进行裁剪 ---
    arcpy.AddMessage(f"开始使用投影后的掩膜进行裁剪: {temp_mask_fc_projected}")
    try:
        clipped_raster_obj = ExtractByMask(out_krig_raster_obj, temp_mask_fc_projected)  # 使用投影后的掩膜
        arcpy.AddMessage("栅格裁剪完成。")

        # -------------------- 投影最终栅格到WGS84 (4326) --------------------
        # 如果最终结果需要以WGS84（经纬度）形式输出
        final_output_sr = arcpy.SpatialReference(4326)  # WGS84
        arcpy.AddMessage(f"将最终裁剪栅格投影到 {final_output_sr.name} ({final_output_sr.factoryCode})...")
        # ProjectRaster 函数：
        # Input raster, Output raster, Output coordinate system, Resampling type, Cell size
        # 注意：cell_size在这里需要根据目标SR进行调整，或者让系统自动计算
        # 如果不指定cell_size，系统会根据输入和输出SR以及环境设置自动选择。
        # 这里为了确保最终栅格在4326下也有合适的密度，可以重新计算一个近似的度数大小
        # 例如，500米在赤道附近约为0.0045度，但会随纬度变化。为简化，我们仍然使用0.005作为近似值。
        # 或者直接不提供cell_size，让ProjectRaster自动处理。
        # 建议让ProjectRaster自动处理，因为它会更好地考虑转换。
        arcpy.management.ProjectRaster(clipped_raster_obj, out_raster_name, final_output_sr, "BILINEAR")
        arcpy.AddMessage(
            f"Spatial Analyst Kriging (Ordinary, Spherical) 和裁剪完成，并投影到WGS84。输出: {out_raster_name}")
        # ------------------------------------------------------------------------------------

    except arcpy.ExecuteError:
        arcpy.AddError(f"ArcPy 执行错误 (裁剪或最终投影时):")
        arcpy.AddError(arcpy.GetMessages(2))
        arcpy.AddWarning("由于裁剪或投影失败，将尝试保存未裁剪/未投影的原始栅格。")
        unclipped_output_name = out_raster_name.replace(".tif", "_unclipped_unprojected.tif")
        if arcpy.Exists(unclipped_output_name) and arcpy.env.overwriteOutput == False:
            unclipped_output_name = arcpy.CreateUniqueName(unclipped_output_name, arcpy.env.scratchFolder)
        out_krig_raster_obj.save(unclipped_output_name)
        arcpy.AddWarning(f"原始克里金栅格已保存到: {unclipped_output_name}")
        raise
    except Exception as e:
        arcpy.AddError(f"裁剪或最终投影过程中发生未知错误: {e}")
        arcpy.AddWarning("由于裁剪或投影失败，将尝试保存未裁剪/未投影的原始栅格。")
        unclipped_output_name = out_raster_name.replace(".tif", "_unclipped_unprojected.tif")
        if arcpy.Exists(unclipped_output_name) and arcpy.env.overwriteOutput == False:
            unclipped_output_name = arcpy.CreateUniqueName(unclipped_output_name, arcpy.env.scratchFolder)
        out_krig_raster_obj.save(unclipped_output_name)
        arcpy.AddWarning(f"原始克里金栅格已保存到: {unclipped_output_name}")
        raise
    finally:
        # 清理所有临时要素类
        if arcpy.Exists(temp_mask_feature_class):
            arcpy.management.Delete(temp_mask_feature_class)
            arcpy.AddMessage(f"Deleted temporary mask feature class (original): {temp_mask_feature_class}")
        if arcpy.Exists(temp_points_fc_projected):
            arcpy.management.Delete(temp_points_fc_projected)
            arcpy.AddMessage(f"Deleted temporary projected points: {temp_points_fc_projected}")
        if arcpy.Exists(temp_mask_fc_projected):
            arcpy.management.Delete(temp_mask_fc_projected)
            arcpy.AddMessage(f"Deleted temporary projected mask: {temp_mask_fc_projected}")
        if arcpy.Exists("beijing_mask_layer"):  # 确保删除MakeFeatureLayer创建的图层
            arcpy.management.Delete("beijing_mask_layer")
            arcpy.AddMessage("Deleted temporary feature layer: beijing_mask_layer")

    return out_raster_name


# 主逻辑，将被ArcGIS Pro工具调用
if __name__ == '__main__':
    # 获取工具参数
    sde_connection_input = arcpy.GetParameterAsText(0)
    arcpy.AddWarning(f"GP Script: Received parameter value for SDE connection: {sde_connection_input}")
    target_datetime_str = arcpy.GetParameterAsText(1)
    output_kriging_raster_param = arcpy.GetParameterAsText(2)

    arcpy.env.overwriteOutput = True

    # source_known_good_raster = r"C:\ArcGIS_Server_Debug_Rasters\kriging_raw_debug.tif"  # 替换为一个服务器上已存在的小型完好TIFF
    #
    # try:
    #     arcpy.CopyRaster_management(source_known_good_raster, output_kriging_raster_param)
    #     arcpy.AddMessage(f"Copied known good raster to {output_kriging_raster_param}")
    # except Exception as e:
    #     arcpy.AddError(f"Failed to copy raster: {e}")

    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
        arcpy.AddMessage("Spatial Analyst extension checked out.")
    else:
        arcpy.AddError("Spatial Analyst extension is not available.")
        raise arcpy.ExecuteError("Spatial Analyst extension not available.")

    temp_points_fc = os.path.join(arcpy.env.scratchGDB, "aqi_points_for_kriging")
    if arcpy.Exists(temp_points_fc):
        arcpy.management.Delete(temp_points_fc)

    arcpy.AddMessage(f"--- Geoprocessing Tool Run Start ---")
    arcpy.AddMessage(f"Target Time: {target_datetime_str}")
    arcpy.AddMessage(f"Input SDE: {sde_connection_input}")
    arcpy.AddMessage(f"Intermediate points will be created at: {temp_points_fc}")
    arcpy.AddMessage(f"Output Clipped Raster: {output_kriging_raster_param}")

    try:
        get_aqi_data_for_time_tool(sde_connection_input, target_datetime_str, temp_points_fc)

        if not arcpy.Exists(temp_points_fc):
            arcpy.AddError(f"Intermediate feature class {temp_points_fc} was not created.")
            raise arcpy.ExecuteError("Failed to create input points for Kriging.")

        point_count_result = arcpy.management.GetCount(temp_points_fc)
        point_count = int(point_count_result[0])

        if point_count == 0:
            arcpy.AddWarning(
                "No AQI data points found for the specified time or failed to create feature class. Kriging will not be performed.")
            arcpy.AddError("No input points for Kriging. Cannot generate raster.")
            raise arcpy.ExecuteError("No input points for Kriging, cannot proceed.")

        # 调用修改后的函数，不再传递掩膜URL
        run_spatial_analyst_kriging_tool(temp_points_fc, "aqi", output_kriging_raster_param)
        arcpy.AddMessage(f"SUCCESS: Clipped Kriging raster created at {output_kriging_raster_param}")

    except arcpy.ExecuteError as ae:
        arcpy.AddError(f"ArcPy ExecuteError: {arcpy.GetMessages(2)}")
        raise
    except Exception as e:
        arcpy.AddError(f"An error occurred: {str(e)}")
        import traceback

        arcpy.AddError(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        if arcpy.CheckExtension("Spatial") == "CheckedOut":
            arcpy.CheckInExtension("Spatial")
            arcpy.AddMessage("Spatial Analyst extension checked in.")
        arcpy.AddMessage(f"--- Geoprocessing Tool Run End ---")
