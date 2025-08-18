import arcpy
import arcpy.sa
import os
import traceback


def calculate_zonal_statistics_for_fishnet(input_fishnet_fc, raster_layers_info, output_fc_name):
    try:
        arcpy.env.overwriteOutput = True

        actual_fishnet_path = input_fishnet_fc.dataSource if hasattr(input_fishnet_fc,
                                                                     'dataSource') else input_fishnet_fc

        output_workspace = os.path.dirname(actual_fishnet_path)
        arcpy.env.workspace = output_workspace
        arcpy.AddMessage(f"输出工作空间设置为: {output_workspace}")

        if arcpy.CheckExtension("Spatial") != "Available":
            arcpy.AddError("Spatial Analyst 扩展不可用。")
            return
        arcpy.CheckOutExtension("Spatial")

        output_fishnet_fc = os.path.join(output_workspace, output_fc_name)

        arcpy.AddMessage(f"步骤 1/{len(raster_layers_info) + 1}: 复制渔网特征类到 {output_fishnet_fc}...")
        arcpy.CopyFeatures_management(actual_fishnet_path, output_fishnet_fc)
        arcpy.AddMessage("渔网特征类复制完成。")

        fishnet_oid_field = arcpy.Describe(output_fishnet_fc).OIDFieldName

        step_count = 2
        for raster_map_name, output_field_prefix in raster_layers_info.items():
            arcpy.AddMessage(f"\n--- 正在处理栅格图层: {raster_map_name} ---")

            aprx = arcpy.mp.ArcGISProject("CURRENT")
            m = aprx.activeMap
            found_raster_layer = m.listLayers(raster_map_name)
            if not found_raster_layer:
                arcpy.AddWarning(f"警告: 未在地图中找到名为 '{raster_map_name}' 的栅格图层，跳过此图层。")
                continue
            found_raster_layer = found_raster_layer[0]  # 取第一个匹配的图层

            if not found_raster_layer.isRasterLayer:
                arcpy.AddWarning(f"警告: 图层 '{raster_map_name}' 不是栅格图层，跳过。")
                continue

            actual_raster_path = found_raster_layer.dataSource if hasattr(found_raster_layer,
                                                                          'dataSource') else raster_map_name

            arcpy.AddMessage("正在检查输入数据与当前栅格的坐标系...")
            sr_vector = arcpy.Describe(output_fishnet_fc).spatialReference
            sr_raster = arcpy.Describe(actual_raster_path).spatialReference
            if not sr_vector or not sr_raster or sr_vector.name != sr_raster.name:
                arcpy.AddError(
                    f"致命错误：渔网与栅格 '{raster_map_name}' 坐标系不匹配或缺失! "
                    f"Vector: {sr_vector.name if sr_vector else 'None'}, Raster: {sr_raster.name if sr_raster else 'None'}. "
                    "请统一坐标系。")
                return
            arcpy.AddMessage(f"坐标系检查通过，均为: {sr_vector.name}")

            temp_zonal_table = os.path.join(arcpy.env.scratchGDB, f"temp_{output_field_prefix}_stats")
            output_field_name = f"{output_field_prefix}"  # 直接使用前缀作为字段名，例如 Avg_Height

            arcpy.AddMessage(
                f"步骤 {step_count}/{len(raster_layers_info) + 1}: 对 {raster_map_name} 执行分区平均统计...")
            arcpy.sa.ZonalStatisticsAsTable(
                in_zone_data=output_fishnet_fc,
                zone_field=fishnet_oid_field,
                in_value_raster=actual_raster_path,
                out_table=temp_zonal_table,
                ignore_nodata="DATA",
                statistics_type="MEAN"
            )
            arcpy.AddMessage(f"分区统计完成，结果保存在: {temp_zonal_table}")

            arcpy.AddMessage(f"步骤 {step_count + 1}/{len(raster_layers_info) + 1}: 将统计结果添加回渔网...")

            if output_field_name in [f.name for f in arcpy.ListFields(output_fishnet_fc)]:
                arcpy.AddWarning(f"字段 '{output_field_name}' 已存在，正在删除旧字段...")
                arcpy.DeleteField_management(output_fishnet_fc, output_field_name)

            arcpy.AddField_management(output_fishnet_fc, output_field_name, "DOUBLE")

            zonal_stats_data = {}
            with arcpy.da.SearchCursor(temp_zonal_table, [fishnet_oid_field, "MEAN"]) as cursor:
                for row in cursor:
                    zonal_stats_data[row[0]] = row[1]

            # 更新渔网特征类
            with arcpy.da.UpdateCursor(output_fishnet_fc, [fishnet_oid_field, output_field_name]) as cursor:
                for row in cursor:
                    oid = row[0]
                    # 获取平均值，如果渔网单元格在统计表中没有对应数据
                    mean_value = zonal_stats_data.get(oid, None)
                    row[1] = mean_value
                    cursor.updateRow(row)
            arcpy.AddMessage(f"字段 '{output_field_name}' 已成功添加到渔网。")

            # 清理临时表
            if arcpy.Exists(temp_zonal_table):
                arcpy.Delete_management(temp_zonal_table)
                arcpy.AddMessage(f"临时表 {temp_zonal_table} 已删除。")

            step_count += 1  # 为下一个栅格更新步骤计数

        arcpy.AddMessage("\n所有栅格图层的统计已完成！")
        arcpy.AddMessage(f"最终输出渔网特征类: {output_fishnet_fc}")

    except arcpy.ExecuteError:
        arcpy.AddError(f"\nArcPy 执行错误:\n{arcpy.GetMessages(2)}")
    except Exception as e:
        arcpy.AddError(f"\n发生意外错误: {e}\n{traceback.format_exc()}")
    finally:
        arcpy.CheckInExtension("Spatial")  # 确保释放扩展许可


if __name__ == '__main__':
    try:

        fishnet_layer_name_in_map = "Analysis_grid_20240101_to_20241231"

        raster_layers_to_process = {
            "beijing_Clip": "Avg_Height",
            "slope_90m": "Avg_Slope",
            "aspect_90m": "Avg_Aspect",
            "relief_90m": "Avg_Relief"
        }

        output_feature_class_name = "Fishnet_Zonal_Statistics_Result"

        aprx = arcpy.mp.ArcGISProject("CURRENT")
        m = aprx.activeMap
        arcpy.AddMessage(f"已连接到当前地图: {m.name}")

        found_fishnet_layer = m.listLayers(fishnet_layer_name_in_map)
        if not found_fishnet_layer:
            arcpy.AddError(f"错误: 未在地图中找到名为 '{fishnet_layer_name_in_map}' 的图层。请确保它已添加到地图。")
            exit()
        found_fishnet_layer = found_fishnet_layer[0]  # 取第一个匹配的图层

        if not found_fishnet_layer.isFeatureLayer:
            arcpy.AddError(f"错误: 图层 '{fishnet_layer_name_in_map}' 不是特征图层（应为面）。")
            exit()

        calculate_zonal_statistics_for_fishnet(
            found_fishnet_layer,
            raster_layers_to_process,
            output_feature_class_name
        )

    except Exception as e:
        arcpy.AddError(f"主程序中发生错误: {e}")