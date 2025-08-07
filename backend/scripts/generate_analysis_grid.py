# -*- coding: utf-8 -*-
import arcpy
import math
import os
import time
import traceback
from collections import defaultdict


# ======================================================================================
# --- create_analysis_grid 函数 (已将f-string替换为 .format()) ---
# ======================================================================================
def create_analysis_grid(output_grid_fc, template_fc, grid_cell_size):
    """根据模板要素的范围创建渔网。"""
    arcpy.AddMessage("  正在创建渔网...")
    desc = arcpy.Describe(template_fc)
    extent = desc.extent

    arcpy.CreateFishnet_management(
        out_feature_class=output_grid_fc,
        origin_coord="{} {}".format(extent.XMin, extent.YMin),
        y_axis_coord="{} {}".format(extent.XMin, extent.YMax),
        corner_coord="{} {}".format(extent.XMax, extent.YMax),
        cell_width=grid_cell_size,
        cell_height=grid_cell_size,
        labels="NO_LABELS",
        template=template_fc,
        geometry_type="POLYGON"
    )

    count = int(arcpy.GetCount_management(output_grid_fc)[0])
    if count > 0:
        arcpy.AddMessage("  渔网创建成功，包含 {} 个网格。".format(count))
    else:
        raise Exception("无法创建有效的渔网，请检查输入边界。脚本终止。")

    arcpy.AddMessage("  添加并计算 'Grid_ID' 字段...")
    arcpy.AddField_management(output_grid_fc, "Grid_ID", "LONG")

    # --- 这段代码未使用f-string，保持不变 ---
    oid_field_name = arcpy.Describe(output_grid_fc).OIDFieldName
    expression = "!" + oid_field_name + "!"
    arcpy.CalculateField_management(output_grid_fc, "Grid_ID", expression, "PYTHON3")
    arcpy.AddMessage("  'Grid_ID' 字段计算完成。")


# ======================================================================================
# --- process_aqi_data 函数 (已将f-string替换为 .format()) ---
# ======================================================================================
def process_aqi_data(grid_fc, start_date, end_date, db_connection_sde, pollutants_list, aqi_record_table,
                     aqi_station_table, grid_cell_size, target_projected_crs):
    """
    从数据库提取空气质量数据，进行插值分析，并将结果连接到网格。
    此版本修复了插值范围及snapRaster参数类型错误的问题。
    """
    arcpy.AddMessage("  1. 从数据库提取并处理空气质量数据...")
    # 视图名称
    view_name = "public.v_station_daily_averages"

    # 构建所有污染物的AVG()表达式，注意字段名和视图中一致
    avg_expressions = ", ".join(["AVG({}) AS avg_{}".format(p, p) for p in pollutants_list])

    # 最终的SQL查询
    query_sql = """
        SELECT
            station_id,
            station_name,
            location,
            {avg_expr}
        FROM
            {view}
        WHERE
            record_date >= '{s_date}'::date AND
            record_date <= '{e_date}'::date
        GROUP BY
            station_id, station_name, location
        """.format(
        avg_expr=avg_expressions,
        view=view_name,
        s_date=start_date,
        e_date=end_date
    )

    # 使用无路径的临时图层名
    temp_query_layer = "temp_aqi_query_layer_instance"
    arcpy.management.MakeQueryLayer(
        input_database=db_connection_sde,
        out_layer_name=temp_query_layer,
        query=query_sql,
        oid_fields="station_id",
        shape_type="POINT",
        srid="4326",
        spatial_reference=arcpy.SpatialReference(4326)
    )

    workspace = arcpy.env.workspace
    station_points_gcs = os.path.join(workspace, "temp_aqi_stations_gcs")

    if arcpy.Exists(station_points_gcs): arcpy.Delete_management(station_points_gcs)
    arcpy.management.CopyFeatures(temp_query_layer, station_points_gcs)
    arcpy.Delete_management(temp_query_layer)

    count = int(arcpy.GetCount_management(station_points_gcs)[0])
    if count == 0:
        arcpy.AddWarning(
            "警告：在时间段 {} 到 {} 内没有找到任何有效的AQI记录。AQI字段将为空。".format(start_date, end_date))
        for pollutant in pollutants_list:
            field_name = "avg_{}".format(pollutant)
            if not arcpy.ListFields(grid_fc, field_name):
                arcpy.AddField_management(grid_fc, field_name, "DOUBLE")
        return

    arcpy.AddMessage("  成功从数据库物化 {} 个站点的平均AQI数据。".format(count))

    station_points_projected = os.path.join(workspace, "temp_aqi_stations_projected")
    arcpy.Project_management(station_points_gcs, station_points_projected, target_projected_crs)

    arcpy.AddMessage("  正在为环境设置创建模板栅格...")
    template_raster = os.path.join(workspace, "template_snap_raster")
    if arcpy.Exists(template_raster):
        arcpy.Delete_management(template_raster)

    arcpy.conversion.PolygonToRaster(
        in_features=grid_fc,
        value_field="Grid_ID",
        out_rasterdataset=template_raster,
        cellsize=grid_cell_size
    )
    arcpy.AddMessage("  模板栅格创建成功。")

    for pollutant in pollutants_list:
        field_name = "avg_{}".format(pollutant)
        valid_points_query = "{} IS NOT NULL".format(field_name)

        valid_count = 0
        with arcpy.da.SearchCursor(station_points_projected, [field_name], where_clause=valid_points_query) as cursor:
            for row in cursor:
                if row[0] is not None:
                    valid_count += 1

        if valid_count < 3:
            arcpy.addWarning("    警告: 污染物 {} 的有效数据点不足3个 ({})，跳过插值。".format(pollutant, valid_count))
            if not arcpy.ListFields(grid_fc, field_name):
                arcpy.AddField_management(grid_fc, field_name, "DOUBLE", field_is_nullable=True)
            continue

        arcpy.AddMessage("\n  处理污染物: {}...".format(pollutant.upper()))

        raster_path = os.path.join(workspace, "temp_raster_{}".format(pollutant))
        stats_table = os.path.join(workspace, "temp_stats_table_{}".format(pollutant))

        if arcpy.Exists(raster_path): arcpy.Delete_management(raster_path)
        if arcpy.Exists(stats_table): arcpy.Delete_management(stats_table)

        arcpy.AddMessage("    正在将处理范围设置为整个分析网格...")
        with arcpy.EnvManager(
                extent=grid_fc,
                snapRaster=template_raster,
                cellSize=grid_cell_size
        ):
            raster_out = arcpy.sa.Idw(station_points_projected, field_name, float(grid_cell_size) / 10)
            raster_out.save(raster_path)

        arcpy.sa.ZonalStatisticsAsTable(grid_fc, "Grid_ID", raster_path, stats_table, "DATA", "MEAN")
        arcpy.JoinField_management(grid_fc, "Grid_ID", stats_table, "Grid_ID", ["MEAN"])

        if not arcpy.ListFields(grid_fc, field_name):
            arcpy.AddField_management(grid_fc, field_name, "DOUBLE")

        expression = "float(!MEAN!) if !MEAN! is not None else 0"
        arcpy.CalculateField_management(grid_fc, field_name, expression, "PYTHON3")
        arcpy.DeleteField_management(grid_fc, ["MEAN"])

        arcpy.Delete_management(raster_path)
        arcpy.Delete_management(stats_table)
        arcpy.AddMessage("    {} 处理完成。".format(pollutant.upper()))

    arcpy.AddMessage("  正在清理临时文件...")
    if arcpy.Exists(template_raster):
        arcpy.Delete_management(template_raster)
    arcpy.Delete_management(station_points_gcs)
    arcpy.Delete_management(station_points_projected)
    arcpy.AddMessage("\n  AQI处理完成，临时文件已清理。")


# ======================================================================================
# --- calculate_bird_diversity_optimized 函数 (已将f-string替换为 .format()) ---
# ======================================================================================
def calculate_bird_diversity_optimized(grid_fc, start_date, end_date, db_connection_sde, bird_observation_table,
                                       bird_species_table):
    """
    为每个网格计算鸟类多样性指数 (最终修复版，采用手动分步物化)。
    """
    arcpy.AddMessage("  使用“手动分步物化”策略计算鸟类多样性...")
    workspace = arcpy.env.workspace
    target_crs_gcs = arcpy.SpatialReference(4326)

    obs_query_sql = """
    SELECT id, location
    FROM {bird_obs_tbl}
    WHERE
        start_time BETWEEN '{s_date} 00:00:00' AND '{e_date} 23:59:59' AND location IS NOT NULL
    """.format(
        bird_obs_tbl=bird_observation_table,
        s_date=start_date,
        e_date=end_date
    )

    temp_query_layer_name = "bird_obs_temp_query_layer"
    materialized_gcs_points = os.path.join(workspace, "temp_materialized_bird_obs_gcs")
    if arcpy.Exists(materialized_gcs_points):
        arcpy.Delete_management(materialized_gcs_points)

    query_layer = None
    try:
        query_layer = arcpy.management.MakeQueryLayer(
            input_database=db_connection_sde,
            out_layer_name=temp_query_layer_name,
            query=obs_query_sql,
            oid_fields="id",
            shape_type="POINT",
            srid="4326",
            spatial_reference=target_crs_gcs
        )

        count = int(arcpy.GetCount_management(query_layer)[0])
        if count == 0:
            arcpy.addWarning("警告：在应用严格几何验证后，未找到任何有效的鸟类观测记录。")
            for field, ftype in [('richness', 'LONG'), ('abundance', 'LONG'), ('shannon', 'DOUBLE')]:
                if not arcpy.ListFields(grid_fc, field): arcpy.AddField_management(grid_fc, field, ftype)
                arcpy.CalculateField_management(grid_fc, field, "0", "PYTHON3")
            return

        arcpy.AddMessage("  临时查询图层创建成功，包含 {} 个有效观测点。".format(count))
        arcpy.AddMessage("  现在开始手动分步物化...")

        arcpy.management.CreateFeatureclass(
            out_path=workspace,
            out_name="temp_materialized_bird_obs_gcs",
            geometry_type="POINT",
            spatial_reference=target_crs_gcs
        )
        arcpy.management.AddField(materialized_gcs_points, "observation_id", "LONG")
        arcpy.AddMessage("  已成功创建目标要素类结构。")

        source_fields = ["id", "SHAPE@"]
        target_fields = ["observation_id", "SHAPE@"]

        insert_cursor = arcpy.da.InsertCursor(materialized_gcs_points, target_fields)
        search_cursor = arcpy.da.SearchCursor(query_layer, source_fields)

        processed_count = 0
        with insert_cursor, search_cursor:
            for row in search_cursor:
                insert_cursor.insertRow(row)
                processed_count += 1

        arcpy.AddMessage("  手动物化完成。成功处理并插入了 {} 条记录。".format(processed_count))
        arcpy.AddMessage("  数据已成功物化到: {}".format(materialized_gcs_points))

    except Exception as e:
        arcpy.AddError("在“手动分步物化”过程中发生致命错误！")
        arcpy.AddError("错误类型: {}".format(type(e).__name__))
        arcpy.AddError("错误信息: {}".format(e))
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        if query_layer and arcpy.Exists(query_layer):
            arcpy.Delete_management(query_layer)
            arcpy.AddMessage("  临时查询图层已删除，数据库连接已释放。")

    arcpy.AddMessage("  2. 开始基于物化的数据进行空间分析...")
    grid_desc = arcpy.Describe(grid_fc)
    target_crs = grid_desc.spatialReference

    projected_obs_points = os.path.join(workspace, "temp_projected_obs_points")
    spatially_joined_points = os.path.join(workspace, "temp_spatial_join_points")

    arcpy.Project_management(materialized_gcs_points, projected_obs_points, target_crs)

    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(projected_obs_points)
    field_mappings.addTable(grid_fc)
    grid_id_map = field_mappings.getFieldMap(field_mappings.findFieldMapIndex("Grid_ID"))
    obs_id_map = field_mappings.getFieldMap(field_mappings.findFieldMapIndex("observation_id"))
    new_field_mappings = arcpy.FieldMappings()
    new_field_mappings.addFieldMap(grid_id_map)
    new_field_mappings.addFieldMap(obs_id_map)

    arcpy.analysis.SpatialJoin(projected_obs_points, grid_fc, spatially_joined_points, "JOIN_ONE_TO_ONE", "KEEP_ALL",
                               field_mapping=new_field_mappings, match_option="WITHIN")

    grid_to_obs_ids = defaultdict(list)
    all_obs_ids_in_grids = set()
    with arcpy.da.SearchCursor(spatially_joined_points, ["Grid_ID", "observation_id"]) as cursor:
        for grid_id, obs_id in cursor:
            if grid_id is not None and obs_id is not None:
                grid_to_obs_ids[grid_id].append(obs_id)
                all_obs_ids_in_grids.add(obs_id)

    total_obs_found = len(all_obs_ids_in_grids)
    arcpy.AddMessage(
        "  空间连接完成。共找到 {} 个唯一观测点，分布在 {} 个网格中。".format(total_obs_found, len(grid_to_obs_ids)))

    if total_obs_found == 0:
        arcpy.addWarning("警告：空间连接后在网格内未找到任何观测点。")
        arcpy.Delete_management(materialized_gcs_points)
        arcpy.Delete_management(projected_obs_points)
        arcpy.Delete_management(spatially_joined_points)
        return

    species_table_path = os.path.join(db_connection_sde, bird_species_table)
    oid_field = arcpy.AddFieldDelimiters(species_table_path, "observation_id")

    if total_obs_found > 999:
        arcpy.AddMessage("  观测点数量较多，分批次查询物种数据...")
        ids_list = list(all_obs_ids_in_grids)
        obs_to_species_data = defaultdict(list)
        for i in range(0, len(ids_list), 900):
            chunk = ids_list[i:i + 900]
            where_clause = "{} IN {}".format(oid_field, tuple(chunk))
            with arcpy.da.SearchCursor(species_table_path, ["observation_id", "taxon_name", "count"],
                                       where_clause=where_clause) as s_cursor:
                for obs_id, species_name, count_val in s_cursor:
                    if obs_id and species_name and count_val is not None:
                        obs_to_species_data[obs_id].append((species_name, count_val))
    else:
        where_clause = "{} IN {}".format(oid_field, tuple(all_obs_ids_in_grids))
        obs_to_species_data = defaultdict(list)
        with arcpy.da.SearchCursor(species_table_path, ["observation_id", "taxon_name", "count"],
                                   where_clause=where_clause) as s_cursor:
            for obs_id, species_name, count_val in s_cursor:
                if obs_id and species_name and count_val is not None:
                    obs_to_species_data[obs_id].append((species_name, count_val))

    arcpy.AddMessage("  物种数据批量查询完成，数据已读入内存。")

    arcpy.AddMessage("  正在内存中计算多样性指数...")
    results_dict = {}
    for grid_id, obs_ids_list in grid_to_obs_ids.items():
        species_counts_in_grid = defaultdict(int)
        for obs_id in obs_ids_list:
            if obs_id in obs_to_species_data:
                for species_name, count_val in obs_to_species_data[obs_id]:
                    species_counts_in_grid[species_name] += count_val

        if not species_counts_in_grid:
            continue

        richness = len(species_counts_in_grid)
        abundance = sum(species_counts_in_grid.values())
        shannon = 0.0
        if abundance > 0:
            pi_values = [c / abundance for c in species_counts_in_grid.values()]
            shannon = -sum(pi * math.log(pi) for pi in pi_values if pi > 0)

        results_dict[grid_id] = (richness, abundance, shannon)

    arcpy.AddMessage("  成功计算了 {} 个网格的多样性指数。".format(len(results_dict)))

    arcpy.AddMessage("  正在将计算结果合并回网格...")
    for field, ftype in [('richness', 'LONG'), ('abundance', 'LONG'), ('shannon', 'DOUBLE')]:
        if not arcpy.ListFields(grid_fc, field):
            arcpy.AddField_management(grid_fc, field, ftype)

    fields_to_update = ['Grid_ID', 'richness', 'abundance', 'shannon']
    updated_grids = 0
    with arcpy.da.UpdateCursor(grid_fc, fields_to_update) as cursor:
        for row in cursor:
            grid_id = row[0]
            if grid_id in results_dict:
                row[1], row[2], row[3] = results_dict[grid_id]
                updated_grids += 1
            else:
                row[1], row[2], row[3] = 0, 0, 0.0
            cursor.updateRow(row)

    arcpy.AddMessage("    成功更新了 {} 个网格的计算结果。".format(updated_grids))

    arcpy.Delete_management(materialized_gcs_points)
    arcpy.Delete_management(projected_obs_points)
    arcpy.Delete_management(spatially_joined_points)
    arcpy.AddMessage("  鸟类多样性数据合并完成。")


# ======================================================================================
# --- main 函数 (已将f-string替换为 .format()) ---
# ======================================================================================
def main():
    """主执行函数，包含完整的错误处理和资源管理。"""

    spatial_analyst_licensed = False
    temp_gdb_path = None

    try:
        start_date_obj = arcpy.GetParameter(0)
        end_date_obj = arcpy.GetParameter(1)
        db_connection_sde_file = arcpy.GetParameterAsText(2)

        start_date_sql = start_date_obj.strftime("%Y-%m-%d")
        end_date_sql = end_date_obj.strftime('%Y-%m-%d')
        arcpy.AddMessage("开始生成分析数据，时间范围: {} to {}".format(start_date_sql, end_date_sql))
        arcpy.AddMessage("使用数据库连接文件: {}".format(db_connection_sde_file))

        start_date_for_name = start_date_obj.strftime("%Y%m%d")
        end_date_for_name = end_date_obj.strftime("%Y%m%d")

        month_id_value = start_date_obj.strftime("%Y-%m")
        timestamp_value = int(start_date_obj.timestamp() * 1000)
        arcpy.AddMessage("将使用 '{}' 作为 month_id，'{}' 作为 timestamp。".format(month_id_value, timestamp_value))

        arcpy.env.overwriteOutput = True
        temp_folder = arcpy.env.scratchFolder
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        temp_gdb_name = "analysis_{}.gdb".format(timestamp)
        temp_gdb_path = os.path.join(temp_folder, temp_gdb_name)

        if arcpy.Exists(temp_gdb_path):
            arcpy.Delete_management(temp_gdb_path)
        arcpy.CreateFileGDB_management(temp_folder, temp_gdb_name)
        arcpy.AddMessage("成功创建独立的临时工作空间: {}".format(temp_gdb_path))

        arcpy.env.workspace = temp_gdb_path
        arcpy.AddMessage("当前工作空间已设置为: {}".format(arcpy.env.workspace))

        if arcpy.CheckExtension("Spatial") == "Available":
            arcpy.CheckOutExtension("Spatial")
            spatial_analyst_licensed = True
            arcpy.AddMessage("Spatial Analyst 许可已检出。")
        else:
            raise arcpy.ExecuteError("Spatial Analyst 许可不可用。")

        target_projected_crs = arcpy.SpatialReference(32650)
        grid_cell_size = "2000"
        pollutants_list = ['pm25', 'no2', 'o3', 'so2', 'co','aqi']
        db_schema = "public"
        aqi_station_table = '{}.data_pipeline_aqistation'.format(db_schema)
        aqi_record_table = '{}.data_pipeline_aqirecord'.format(db_schema)
        bird_observation_table = '{}.data_pipeline_birdobservation'.format(db_schema)
        bird_species_table = '{}.data_pipeline_birdspeciesrecord'.format(db_schema)
        input_boundary_url = r"https://product.geoscene.cn/server/rest/services/Hosted/beijing_shp/FeatureServer/0"
        arcpy.env.outputCoordinateSystem = target_projected_crs

        arcpy.AddMessage("\n--- 步骤一: 创建分析网格 ---")
        workspace = arcpy.env.workspace
        local_boundary_copy = os.path.join(workspace, "temp_boundary_copy")
        projected_boundary = os.path.join(workspace, "temp_projected_boundary")
        temp_analysis_grid_fc = os.path.join(workspace, "AnalysisGrid")

        temp_boundary_layer = "temp_boundary_layer"
        arcpy.management.MakeFeatureLayer(input_boundary_url, temp_boundary_layer)
        arcpy.management.CopyFeatures(temp_boundary_layer, local_boundary_copy)
        arcpy.Project_management(local_boundary_copy, projected_boundary, target_projected_crs)
        create_analysis_grid(temp_analysis_grid_fc, projected_boundary, grid_cell_size)
        arcpy.Delete_management(temp_boundary_layer)
        arcpy.Delete_management(local_boundary_copy)
        arcpy.Delete_management(projected_boundary)

        arcpy.AddMessage("\n--- 步骤二: 处理空气质量数据 ---")
        process_aqi_data(temp_analysis_grid_fc, start_date_sql, end_date_sql, db_connection_sde_file, pollutants_list,
                         aqi_record_table, aqi_station_table, grid_cell_size, target_projected_crs)

        arcpy.AddMessage("\n--- 步骤三: 计算鸟类多样性指数 (GP服务兼容版) ---")
        calculate_bird_diversity_optimized(temp_analysis_grid_fc, start_date_sql, end_date_sql, db_connection_sde_file,
                                           bird_observation_table, bird_species_table)

        arcpy.AddMessage("\n--- 步骤四：正在将最终结果写入共享文件夹的GDB中... ---")

        shared_gdb_path = r"\\PRODUCT\sde_connections\output.gdb"
        if not arcpy.Exists(shared_gdb_path):
            error_message = "严重错误：找不到目标共享数据库 '{}'。".format(shared_gdb_path)
            arcpy.AddError(error_message)
            raise arcpy.ExecuteError(error_message)
        arcpy.AddMessage("目标共享数据库验证成功: {}".format(shared_gdb_path))

        final_output_fc_name = "Analysis_grid_{}_to_{}".format(start_date_for_name, end_date_for_name)
        final_output_fc_path = os.path.join(shared_gdb_path, final_output_fc_name)

        if arcpy.Exists(final_output_fc_path):
            arcpy.AddWarning("警告：要素类 {} 已存在，将被覆盖。".format(final_output_fc_path))
            arcpy.Delete_management(final_output_fc_path)

        arcpy.AddMessage("准备将临时结果复制到: {}".format(final_output_fc_path))
        arcpy.management.CopyFeatures(temp_analysis_grid_fc, final_output_fc_path)
        arcpy.AddMessage("结果已成功写入共享GDB!")

        arcpy.AddMessage("正在添加并计算 'month_id' 和 'timestamp' 字段...")
        arcpy.AddField_management(final_output_fc_path, "month_id", "TEXT", field_length=10, field_alias="month_id")
        arcpy.AddField_management(final_output_fc_path, "timestamp", "DOUBLE", field_alias="timestamp")

        # 使用.format()构建CalculateField的表达式
        arcpy.CalculateField_management(final_output_fc_path, "month_id", "'{}'".format(month_id_value), "PYTHON3")
        arcpy.CalculateField_management(final_output_fc_path, "timestamp", str(timestamp_value), "PYTHON3")
        arcpy.AddMessage("新字段添加和计算完成。")

        arcpy.SetParameter(3, final_output_fc_path)
        arcpy.AddMessage("脚本成功完成，已返回路径: {}".format(final_output_fc_path))

    except Exception:
        arcpy.AddError("脚本执行失败。")
        arcpy.AddError(traceback.format_exc())
        raise
    finally:
        if spatial_analyst_licensed:
            arcpy.CheckInExtension("Spatial")
            arcpy.AddMessage("Spatial Analyst 许可已交还。")

        if temp_gdb_path and arcpy.Exists(temp_gdb_path):
            try:
                arcpy.Delete_management(temp_gdb_path)
                arcpy.AddMessage("本地临时工作空间 {} 已清理。".format(temp_gdb_path))
            except:
                arcpy.AddWarning("无法删除本地临时工作空间 {}。可能仍被占用。".format(temp_gdb_path))


if __name__ == '__main__':
    main()
