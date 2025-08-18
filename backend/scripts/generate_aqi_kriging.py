import arcpy
import datetime
import os
from arcpy.sa import *


def get_aqi_data_for_time_tool(sde_connection_input, target_datetime_str, out_feature_class_name):

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

    query_layer_name = f"AQI_QueryLayer_{int(datetime.datetime.now().timestamp())}"  # 使用时间戳确保唯一性
    try:
        arcpy.management.MakeQueryLayer(
            input_database=sde_connection_input,
            out_layer_name=query_layer_name,
            query=sql_query,
            oid_fields="station_pk",
            shape_type="POINT",
            srid="4326",
            spatial_reference=arcpy.SpatialReference(4326)
        )

        count = int(arcpy.management.GetCount(query_layer_name)[0])
        if count == 0:
            raise arcpy.ExecuteError("No data points found for the specified time. Cannot perform interpolation.")

        arcpy.management.CopyFeatures(query_layer_name, out_feature_class_name)

    except arcpy.ExecuteError as ee:
        raise
    except Exception as e:
        import traceback
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        if arcpy.Exists(query_layer_name):
            arcpy.management.Delete(query_layer_name)

    return out_feature_class_name



def run_spatial_analyst_idw_tool(input_features, z_field, out_raster_name):
    cell_size_meters = 500
    power_value = 2
    num_points_variable = 12

    search_radius_config = RadiusVariable(num_points_variable)

    beijing_mask_service_url = "https://product.geoscene.cn/server/rest/services/Hosted/beijing_shp/FeatureServer/0"
    analysis_sr = arcpy.SpatialReference(4545)
    final_output_sr = arcpy.SpatialReference(4326)

    gdb = arcpy.env.scratchGDB
    temp_points_projected = os.path.join(gdb, "aqi_points_projected")
    temp_mask_orig = os.path.join(gdb, "beijing_mask_orig")
    temp_mask_projected = os.path.join(gdb, "beijing_mask_projected")

    temp_layer_mask = "beijing_mask_layer_temp"

    try:
        arcpy.env.outputCoordinateSystem = analysis_sr

        arcpy.management.MakeFeatureLayer(beijing_mask_service_url, temp_layer_mask)
        arcpy.management.CopyFeatures(temp_layer_mask, temp_mask_orig)
        arcpy.management.Project(temp_mask_orig, temp_mask_projected, analysis_sr)

        arcpy.management.Project(input_features, temp_points_projected, analysis_sr)
        arcpy.env.extent = arcpy.Describe(temp_mask_projected).extent

        out_idw_raster = Idw(
            in_point_features=temp_points_projected,
            z_field=z_field,
            cell_size=cell_size_meters,
            power=power_value,
            search_radius=search_radius_config
        )

        #掩膜裁剪
        clipped_raster = ExtractByMask(out_idw_raster, temp_mask_projected)

        # 最终投影和保存
        arcpy.management.ProjectRaster(
            in_raster=clipped_raster,
            out_raster=out_raster_name,
            out_coor_system=final_output_sr,
            resampling_type="BILINEAR"
        )

    except arcpy.ExecuteError as ee:
        arcpy.AddError(f"An ArcPy error occurred during the IDW process: {arcpy.GetMessages(2)}")
        raise
    except Exception as e:
        arcpy.AddError(f"A general error occurred during the IDW process: {e}")
        import traceback
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        # 清除数据
        for item in [temp_points_projected, temp_mask_orig, temp_mask_projected, temp_layer_mask]:
            if arcpy.Exists(item):
                try:
                    arcpy.management.Delete(item)
                    arcpy.AddMessage(f"  - Deleted: {item}")
                except:
                    arcpy.AddWarning(f"  - Failed to delete: {item}")
        arcpy.env.extent = None  # 重置环境范围

    return out_raster_name



if __name__ == '__main__':
    sde_connection_input = arcpy.GetParameterAsText(0)
    target_datetime_str = arcpy.GetParameterAsText(1)
    output_raster_param = arcpy.GetParameterAsText(2)

    arcpy.env.overwriteOutput = True
    temp_points_fc = os.path.join(arcpy.env.scratchGDB, "aqi_points_for_interpolation")


    try:
        get_aqi_data_for_time_tool(sde_connection_input, target_datetime_str, temp_points_fc)
        run_spatial_analyst_idw_tool(temp_points_fc, "aqi", output_raster_param)
    except Exception as e:
        import traceback
        arcpy.AddError(f"Traceback:\n{traceback.format_exc()}")
        raise
    finally:
        if arcpy.Exists(temp_points_fc):
            arcpy.management.Delete(temp_points_fc)
        if arcpy.CheckExtension("Spatial") == "CheckedOut":
            arcpy.CheckInExtension("Spatial")
            arcpy.AddMessage("Spatial Analyst extension has been checked in.")