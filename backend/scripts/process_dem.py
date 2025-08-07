import arcpy
import arcpy.sa
import os
import traceback  # 引入traceback用于更详细的错误输出


def calculate_zonal_statistics_for_fishnet(input_fishnet_fc, raster_layers_info, output_fc_name):
    """
    计算渔网中每个单元格的平均高度、坡度、坡向和地形起伏度。
    """
    try:
        # --- 环境设置 ---
        arcpy.env.overwriteOutput = True

        # 确保输入路径是字符串，即使传入的是图层对象
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

        # --- 步骤 1/X: 复制渔网特征类作为输出 ---
        arcpy.AddMessage(f"步骤 1/{len(raster_layers_info) + 1}: 复制渔网特征类到 {output_fishnet_fc}...")
        arcpy.CopyFeatures_management(actual_fishnet_path, output_fishnet_fc)
        arcpy.AddMessage("渔网特征类复制完成。")

        # 获取渔网的唯一ID字段名，通常是OBJECTID
        fishnet_oid_field = arcpy.Describe(output_fishnet_fc).OIDFieldName

        # --- 遍历每个栅格图层进行统计 ---
        step_count = 2
        for raster_map_name, output_field_prefix in raster_layers_info.items():
            arcpy.AddMessage(f"\n--- 正在处理栅格图层: {raster_map_name} ---")

            # 找到栅格图层对象
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

            # --- 坐标系检查 (每次处理新栅格时都检查) ---
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

            # 定义临时表和最终字段名
            temp_zonal_table = os.path.join(arcpy.env.scratchGDB, f"temp_{output_field_prefix}_stats")
            output_field_name = f"{output_field_prefix}"  # 直接使用前缀作为字段名，例如 Avg_Height

            # --- 步骤 X/X: 执行分区统计 ---
            arcpy.AddMessage(
                f"步骤 {step_count}/{len(raster_layers_info) + 1}: 对 {raster_map_name} 执行分区平均统计...")
            # 使用 DATA 选项，确保只有有数据的像元参与计算，并计算平均值 (MEAN)
            arcpy.sa.ZonalStatisticsAsTable(
                in_zone_data=output_fishnet_fc,
                zone_field=fishnet_oid_field,
                in_value_raster=actual_raster_path,
                out_table=temp_zonal_table,
                ignore_nodata="DATA",
                statistics_type="MEAN"
            )
            arcpy.AddMessage(f"分区统计完成，结果保存在: {temp_zonal_table}")

            # --- 步骤 X/X: 添加字段并更新渔网特征类 ---
            arcpy.AddMessage(f"步骤 {step_count + 1}/{len(raster_layers_info) + 1}: 将统计结果添加回渔网...")

            # 检查字段是否已存在，如果存在则删除，以避免冲突
            if output_field_name in [f.name for f in arcpy.ListFields(output_fishnet_fc)]:
                arcpy.AddWarning(f"字段 '{output_field_name}' 已存在，正在删除旧字段...")
                arcpy.DeleteField_management(output_fishnet_fc, output_field_name)

            # 添加新字段
            arcpy.AddField_management(output_fishnet_fc, output_field_name, "DOUBLE")

            # 读取临时统计表中的数据
            zonal_stats_data = {}
            with arcpy.da.SearchCursor(temp_zonal_table, [fishnet_oid_field, "MEAN"]) as cursor:
                for row in cursor:
                    zonal_stats_data[row[0]] = row[1]  # {OBJECTID: MEAN_Value}

            # 更新渔网特征类
            with arcpy.da.UpdateCursor(output_fishnet_fc, [fishnet_oid_field, output_field_name]) as cursor:
                for row in cursor:
                    oid = row[0]
                    # 获取平均值，如果渔网单元格在统计表中没有对应数据（例如，完全在栅格NoData区域外），则默认为None
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
        # !!! 在运行前，请确保所有相关图层已添加到当前的ArcGIS Pro地图中 !!!

        # 1. 渔网数据 (在地图中的图层名称)
        fishnet_layer_name_in_map = "Analysis_grid_20240101_to_20241231"
        # 如果你的渔网是 SDE 数据源，确保它已在地图中，并且脚本能够访问其底层路径
        # 更好的做法是导出 SDE 数据到本地地理数据库 (.gdb) 并使用本地路径，以避免权限和性能问题。

        # 2. 栅格数据列表 (在地图中的图层名称及其对应的输出字段前缀)
        raster_layers_to_process = {
            "beijing_Clip": "Avg_Height",  # 对应高度
            "slope_90m": "Avg_Slope",  # 对应坡度
            "aspect_90m": "Avg_Aspect",  # 对应坡向
            "relief_90m": "Avg_Relief"  # 对应地形起伏度
        }

        # 3. 输出渔网特征类的名称
        output_feature_class_name = "Fishnet_Zonal_Statistics_Result"

        # 获取当前ArcGIS Pro项目和地图
        aprx = arcpy.mp.ArcGISProject("CURRENT")
        m = aprx.activeMap
        arcpy.AddMessage(f"已连接到当前地图: {m.name}")

        # 查找渔网图层
        found_fishnet_layer = m.listLayers(fishnet_layer_name_in_map)
        if not found_fishnet_layer:
            arcpy.AddError(f"错误: 未在地图中找到名为 '{fishnet_layer_name_in_map}' 的图层。请确保它已添加到地图。")
            exit()
        found_fishnet_layer = found_fishnet_layer[0]  # 取第一个匹配的图层

        if not found_fishnet_layer.isFeatureLayer:
            arcpy.AddError(f"错误: 图层 '{fishnet_layer_name_in_map}' 不是特征图层（应为面）。")
            exit()

        # 调用主函数
        calculate_zonal_statistics_for_fishnet(
            found_fishnet_layer,
            raster_layers_to_process,
            output_feature_class_name
        )

    except Exception as e:
        arcpy.AddError(f"主程序中发生错误: {e}")