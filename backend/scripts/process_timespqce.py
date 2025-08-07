import arcpy
import os
import calendar
from arcpy import sa
import traceback


def print_fields(fc_path, step_name):
    try:
        if arcpy.Exists(fc_path):
            fields = [f.name for f in arcpy.ListFields(fc_path)]
            arcpy.AddMessage(
                f">>> 调试信息 ({step_name}): '{os.path.basename(fc_path)}' 的字段列表: {', '.join(fields)}")
            return fields
        else:
            arcpy.AddWarning(f">>> 调试信息 ({step_name}): 要素类 '{fc_path}' 不存在。")
            return []
    except Exception as e:
        arcpy.AddError(f">>> 调试信息 ({step_name}): 打印字段时出错: {e}")
        return []


def merge_and_enrich_monthly_data(year_to_process):
    """
    最终修正版v12: 修复因字段名大小写不匹配导致的 'EVI' not in list 错误。
    统一在代码中使用小写的 'evi' 作为字段名。
    """
    try:
        if arcpy.CheckExtension("Spatial") == "Available":
            arcpy.CheckOutExtension("Spatial")
            arcpy.AddMessage("已成功检出 Spatial Analyst 扩展许可。")
        else:
            raise arcpy.ExecuteError("Spatial Analyst 扩展许可不可用。")
    except arcpy.ExecuteError as e:
        arcpy.AddError(f"无法检出Spatial Analyst扩展: {e}")
        return

    arcpy.env.overwriteOutput = True
    base_grid_gdb = r"D:\geoscene_connection\sde_connections\output.gdb"
    lulc_local_path = r"D:\训练数据\土地利用数据\LULC.shp"
    dem_local_path = r"D:\训练数据\DEM数据\dem_data.shp"
    climate_data_base_dir = r"D:\训练数据\气候数据_逐月"
    evi_data_base_dir = r"D:\训练数据\植被指数_逐月"
    output_base_dir = r"D:\训练数据\最终数据"

    TARGET_SHORT_NAMES = {
        "temperature_C": "temp_c",
        "precipitation_mm": "precip_mm",
        "wind_speed_ms": "wind_ms",
        "vapor_pressure_kPa": "vp_kpa",
        "EVI": "evi"  # --- 修正点 ---
    }

    arcpy.AddMessage(f"开始处理年份: {year_to_process}")
    output_year_dir = os.path.join(output_base_dir, str(year_to_process))
    os.makedirs(output_year_dir, exist_ok=True)
    output_gdb_name = f"processed_data_{year_to_process}.gdb"
    output_gdb_path = os.path.join(output_year_dir, output_gdb_name)
    if not arcpy.Exists(output_gdb_path):
        arcpy.CreateFileGDB_management(output_year_dir, output_gdb_name)

    for path in [lulc_local_path, dem_local_path]:
        if not arcpy.Exists(path):
            arcpy.AddError(f"错误: 静态数据文件未找到: {path}")
            return

    for month_num in range(8, 13):
        month_str = str(month_num).zfill(2)
        num_days = calendar.monthrange(year_to_process, month_num)[1]
        day_count_str = str(num_days).zfill(2)
        arcpy.AddMessage(f"\n--- 正在处理 {year_to_process}年{month_str}月 ---")

        base_fc_name = f"Analysis_grid_{year_to_process}{month_str}01_to_{year_to_process}{month_str}{day_count_str}"
        base_fc_path = os.path.join(base_grid_gdb, base_fc_name)
        climate_raster_path = os.path.join(climate_data_base_dir, str(year_to_process),
                                           f"TerraClimate_Beijing_2km_UTM50N_{year_to_process}_{month_str}.tif")
        evi_raster_path = os.path.join(evi_data_base_dir, str(year_to_process),
                                       f"EVI_Beijing_2km_UTM50N_{year_to_process}_{month_str}.tif")
        final_output_fc_name = f"timespace_{year_to_process}_{month_str}"
        final_output_fc_path = os.path.join(output_gdb_path, final_output_fc_name)

        paths_to_check = {"基础要素": base_fc_path, "气候栅格": climate_raster_path, "EVI栅格": evi_raster_path}
        if any(not arcpy.Exists(p) for p in paths_to_check.values()):
            arcpy.AddWarning(f"当月所需的一个或多个文件未找到，跳过 {year_to_process}-{month_str}。")
            continue

        try:
            # 1 & 2. 准备数据并添加栅格属性
            arcpy.AddMessage("1 & 2. 准备数据并添加栅格属性...")
            base_polygons_in_mem = "in_memory/base_polygons"
            arcpy.CopyFeatures_management(base_fc_path, base_polygons_in_mem)
            temp_centroids = "in_memory/temp_centroids"
            arcpy.FeatureToPoint_management(base_polygons_in_mem, temp_centroids, "CENTROID")
            all_rasters_to_extract = [[climate_raster_path, "climate"], [evi_raster_path, "evi"]]
            fields_before_extract = {f.name for f in arcpy.ListFields(temp_centroids)}
            sa.ExtractMultiValuesToPoints(temp_centroids, all_rasters_to_extract, "BILINEAR")
            fields_after_extract = {f.name for f in arcpy.ListFields(temp_centroids)}
            newly_added_fields = sorted(list(fields_after_extract - fields_before_extract))
            original_climate_bands = arcpy.Raster(climate_raster_path).bandNames
            original_evi_band = arcpy.Raster(evi_raster_path).bandNames[0]
            expected_original_bands = original_climate_bands + [original_evi_band]
            if len(newly_added_fields) != len(expected_original_bands): raise Exception("字段提取数量不匹配")
            fields_from_rasters = []
            for i, actual_name in enumerate(newly_added_fields):
                target_short_name = TARGET_SHORT_NAMES.get(expected_original_bands[i])
                if not target_short_name: continue
                if actual_name != target_short_name: arcpy.AlterField_management(temp_centroids, actual_name,
                                                                                 target_short_name, target_short_name)
                fields_from_rasters.append(target_short_name)
            arcpy.JoinField_management(base_polygons_in_mem, "OBJECTID", temp_centroids, "ORIG_FID",
                                       fields_from_rasters)

            # 3. 链式添加矢量属性并清理字段
            arcpy.AddMessage("3. 链式添加矢量属性并清理字段...")
            field_mappings_lulc = arcpy.FieldMappings()
            field_mappings_lulc.addTable(base_polygons_in_mem)
            field_mappings_lulc.addTable(lulc_local_path)
            lulc_joined_fc = "in_memory/lulc_joined_fc"
            arcpy.SpatialJoin_analysis(base_polygons_in_mem, lulc_local_path, lulc_joined_fc, "JOIN_ONE_TO_ONE",
                                       "KEEP_ALL", field_mappings_lulc, "INTERSECT")
            field_mappings_dem = arcpy.FieldMappings()
            field_mappings_dem.addTable(lulc_joined_fc)
            field_mappings_dem.addTable(dem_local_path)
            final_enriched_fc = "in_memory/final_enriched_fc"
            arcpy.SpatialJoin_analysis(lulc_joined_fc, dem_local_path, final_enriched_fc, "JOIN_ONE_TO_ONE", "KEEP_ALL",
                                       field_mappings_dem, "INTERSECT")
            fields_to_delete = [f.name for f in arcpy.ListFields(final_enriched_fc) if
                                f.name.startswith('Join_Count') or f.name.startswith('TARGET_FID') or f.name.endswith(
                                    '_1')]
            if fields_to_delete: arcpy.DeleteField_management(final_enriched_fc, fields_to_delete)

            # 4. 检查并填充第一行的NULL值
            arcpy.AddMessage(f"4. 检查并填充第一行的NULL值...")
            final_fields_obj = arcpy.ListFields(final_enriched_fc)
            oid_field_name = arcpy.Describe(final_enriched_fc).OIDFieldName
            fields_to_exclude = {oid_field_name, "Shape", "Shape_Length", "Shape_Area", "Grid_ID"}
            fields_to_update = [f.name for f in final_fields_obj if
                                f.name not in fields_to_exclude and f.type not in ['Geometry']]
            arcpy.AddMessage(f"   将要检查和填充的字段: {', '.join(fields_to_update)}")
            row1_data, row2_data = None, None
            with arcpy.da.SearchCursor(final_enriched_fc, [oid_field_name] + fields_to_update,
                                       f"{oid_field_name} IN (1, 2)",
                                       sql_clause=(None, f"ORDER BY {oid_field_name}")) as cursor:
                for row in cursor:
                    if row[0] == 1:
                        row1_data = row
                    else:
                        row2_data = row
            arcpy.AddMessage(f"   调试信息: 获取到的第一行数据为: {row1_data}")
            arcpy.AddMessage(f"   调试信息: 获取到的第二行数据为: {row2_data}")
            evi_field_name = "evi"  # --- 修正点 ---
            if evi_field_name not in fields_to_update: raise Exception(f"关键字段 {evi_field_name} 丢失")
            if row1_data and row2_data and row1_data[fields_to_update.index(evi_field_name) + 1] is None:
                arcpy.AddMessage(f"   检测到第一行 '{evi_field_name}' 值为NULL，将使用第二行数据填充所有相关字段...")
                with arcpy.da.UpdateCursor(final_enriched_fc, fields_to_update, f"{oid_field_name} = 1") as u_cursor:
                    for u_row in u_cursor:
                        for i in range(len(fields_to_update)): u_row[i] = row2_data[i + 1]
                        u_cursor.updateRow(u_row)
                        arcpy.AddMessage("   第一行数据填充完成。")
                        break
            else:
                arcpy.AddMessage("   第一行数据有效或第二行数据不可用，无需填充。")

            # 5. 保存最终结果
            arcpy.AddMessage(f"5. 保存最终要素类...")
            arcpy.CopyFeatures_management(final_enriched_fc, final_output_fc_path)
            arcpy.AddMessage(f"已成功处理并保存: {final_output_fc_name}")

        except arcpy.ExecuteError as e:
            arcpy.AddError(f"ArcPy执行错误 (处理 {year_to_process}-{month_str}): {e}\n{arcpy.GetMessages(2)}")
        except Exception as e:
            arcpy.AddError(f"一般错误 (处理 {year_to_process}-{month_str}): {e}")
            arcpy.AddError(traceback.format_exc())
        finally:
            arcpy.AddMessage("正在清理in_memory临时数据集...")
            if arcpy.Exists("in_memory"):
                arcpy.Delete_management("in_memory")

    arcpy.AddMessage(f"\n--- {year_to_process}年所有数据处理完成。 ---")


if __name__ == "__main__":
    target_year = 2025
    try:
        merge_and_enrich_monthly_data(target_year)
    except Exception as main_e:
        arcpy.AddError(f"脚本执行过程中发生致命错误: {main_e}")
    finally:
        if arcpy.CheckExtension("Spatial") == "CheckedOut":
            arcpy.CheckInExtension("Spatial")
            arcpy.AddMessage("已释放 Spatial Analyst 扩展许可。")