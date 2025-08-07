import arcpy
import arcpy.sa
import os
import traceback  # 引入traceback用于更详细的错误输出


def calculate_land_cover_percentage_in_fishnet(input_fishnet_fc, input_land_cover_raster, output_fc_name):
    try:
        # --- 环境设置 ---
        arcpy.env.overwriteOutput = True

        actual_fishnet_path = input_fishnet_fc.dataSource if hasattr(input_fishnet_fc,
                                                                     'dataSource') else input_fishnet_fc
        actual_land_cover_path = input_land_cover_raster.dataSource if hasattr(input_land_cover_raster,
                                                                               'dataSource') else input_land_cover_raster

        # --- 坐标系检查 ---
        arcpy.AddMessage("正在检查输入数据的坐标系...")
        sr_vector = arcpy.Describe(actual_fishnet_path).spatialReference
        sr_raster = arcpy.Describe(actual_land_cover_path).spatialReference
        if not sr_vector or not sr_raster or sr_vector.name != sr_raster.name:
            arcpy.AddError(
                f"致命错误：坐标系不匹配或缺失! Vector: {sr_vector.name}, Raster: {sr_raster.name}. 请统一坐标系。")
            return
        arcpy.AddMessage(f"坐标系检查通过，均为: {sr_vector.name}")

        output_workspace = os.path.dirname(actual_fishnet_path)
        arcpy.env.workspace = output_workspace
        arcpy.AddMessage(f"输出工作空间设置为: {output_workspace}")

        if arcpy.CheckExtension("Spatial") != "Available":
            arcpy.AddError("Spatial Analyst 扩展不可用。")
            return
        arcpy.CheckOutExtension("Spatial")

        output_fishnet_fc = os.path.join(output_workspace, output_fc_name)
        temp_tabulate_table = os.path.join(arcpy.env.scratchGDB, "temp_landcover_tabulation")

        land_cover_types = {
            1: "Water", 2: "Tree", 4: "FloodedVegetation", 5: "Crop", 7: "BuiltArea",
            8: "BareGround", 9: "Snow", 10: "Cloud", 11: "Pasture"
        }

        arcpy.AddMessage("步骤 1/6: 复制渔网特征类...")
        arcpy.CopyFeatures_management(actual_fishnet_path, output_fishnet_fc)

        # --- 已修正的步骤 2/6 ---
        arcpy.AddMessage("步骤 2/6: 计算每个渔网单元的总面积...")
        fields_before = {f.name for f in arcpy.ListFields(output_fishnet_fc)}
        arcpy.AddGeometryAttributes_management(output_fishnet_fc, "AREA_GEODESIC", "", "SQUARE_METERS")
        fields_after = {f.name for f in arcpy.ListFields(output_fishnet_fc)}
        new_fields = fields_after - fields_before

        if len(new_fields) == 1:
            area_field_name = new_fields.pop()
            arcpy.AddMessage(f"成功找到并使用面积字段: {area_field_name}")
        else:
            arcpy.AddWarning("无法通过集合比对精确找到面积字段，将尝试按名称搜索...")
            try:
                area_field_name = next(f.name for f in arcpy.ListFields(output_fishnet_fc)
                                       if "AREA" in f.name.upper() and f.type == "Double")
                arcpy.AddMessage(f"通过名称搜索找到面积字段: {area_field_name}")
            except StopIteration:
                arcpy.AddError("致命错误: 运行 AddGeometryAttributes 后未能找到任何面积字段。")
                return

        # --- 后续步骤 ---
        arcpy.AddMessage("步骤 3/6: 统计渔网单元内的土地利用面积...")
        # 让工具自动使用输入栅格的像元大小
        arcpy.sa.TabulateArea(output_fishnet_fc, "OBJECTID", actual_land_cover_path, "Value", temp_tabulate_table)
        arcpy.AddMessage(f"临时统计表已创建: {temp_tabulate_table}")

        arcpy.AddMessage("步骤 4/6: 处理统计结果...")
        fishnet_lc_areas = {}
        # TabulateArea 输出的字段名是 VALUE_1, VALUE_2 等
        with arcpy.da.SearchCursor(temp_tabulate_table, "*") as cursor:
            field_names = cursor.fields
            zone_id_field_index = field_names.index("OBJECTID")
            for row in cursor:
                zone_oid = row[zone_id_field_index]
                fishnet_lc_areas.setdefault(zone_oid, {})
                for i, field_name in enumerate(field_names):
                    if field_name.startswith("VALUE_"):
                        try:
                            lc_id = int(field_name.split('_')[1])
                            area_sq_m = row[i] if row[i] is not None else 0
                            fishnet_lc_areas[zone_oid][lc_id] = area_sq_m
                        except (ValueError, IndexError):
                            continue  # 忽略无法解析的字段

        arcpy.AddMessage("步骤 5/6: 添加土地利用百分比字段...")
        fields_to_update = ["OID@", area_field_name]
        for lc_name in land_cover_types.values():
            percent_field_name = f"{lc_name}_Pct"
            arcpy.AddField_management(output_fishnet_fc, percent_field_name, "DOUBLE", 10, 5)
            fields_to_update.append(percent_field_name)

        arcpy.AddMessage("步骤 6/6: 计算并更新土地利用百分比...")
        with arcpy.da.UpdateCursor(output_fishnet_fc, fields_to_update) as cursor:
            for row in cursor:
                current_oid, total_zone_area = row[0], row[1]
                if total_zone_area and total_zone_area > 0:
                    cell_lc_data = fishnet_lc_areas.get(current_oid, {})
                    for i, field_name in enumerate(fields_to_update[2:], 2):
                        lc_name = field_name.replace("_Pct", "")
                        lc_id = next((k for k, v in land_cover_types.items() if v == lc_name), None)
                        if lc_id is not None:
                            lc_area_in_zone = cell_lc_data.get(lc_id, 0)
                            row[i] = (lc_area_in_zone / total_zone_area) * 100
                        else:
                            row[i] = 0.0
                else:
                    for i in range(2, len(fields_to_update)):
                        row[i] = 0.0
                cursor.updateRow(row)

        arcpy.AddMessage(f"脚本运行完成！输出特征类已生成: {output_fishnet_fc}")

    except arcpy.ExecuteError:
        arcpy.AddError(f"\nArcPy 执行错误:\n{arcpy.GetMessages(2)}")
    except Exception as e:
        arcpy.AddError(f"\n发生意外错误: {e}\n{traceback.format_exc()}")
    finally:
        if 'temp_tabulate_table' in locals() and arcpy.Exists(temp_tabulate_table):
            try:
                arcpy.Delete_management(temp_tabulate_table)
                arcpy.AddMessage("临时表已删除。")
            except:
                pass
        arcpy.CheckInExtension("Spatial")


if __name__ == '__main__':
    try:
        # !!! 在运行前，请确保已将SDE数据导出到本地，并使用本地副本的名称 !!!
        # 渔网数据 (本地矢量副本)
        fishnet_layer_name_in_map = "Analysis_grid_20240101_to_20241231"  # <-- 使用你导出的本地副本的图层名称

        # 土地利用数据 (栅格)
        land_cover_raster_name_in_map = "ls_merge_Clip1"

        output_feature_class_name = "Fishnet_LandCover_Percentages_Final"

        aprx = arcpy.mp.ArcGISProject("CURRENT")
        m = aprx.activeMap
        arcpy.AddMessage(f"已连接到当前地图: {m.name}")

        layers = {lyr.name: lyr for lyr in m.listLayers()}
        found_fishnet_layer = layers.get(fishnet_layer_name_in_map)
        found_land_cover_raster = layers.get(land_cover_raster_name_in_map)

        if not found_fishnet_layer or not found_fishnet_layer.isFeatureLayer:
            arcpy.AddError(
                f"错误: 未在地图中找到名为 '{fishnet_layer_name_in_map}' 的矢量图层。请确保它已导出并添加到地图。")
        elif not found_land_cover_raster or not found_land_cover_raster.isRasterLayer:
            arcpy.AddError(f"错误: 未在地图中找到名为 '{land_cover_raster_name_in_map}' 的栅格图层。")
        else:
            calculate_land_cover_percentage_in_fishnet(found_fishnet_layer, found_land_cover_raster,
                                                       output_feature_class_name)

    except Exception as e:
        arcpy.AddError(f"主程序中发生错误: {e}")
