# ==========================================================================================
# 脚本名称:   动态分析网格生成脚本 (ArcGIS Server 兼容版本)
# 功能描述:   此脚本设计为在 ArcGIS Pro 中编写并发布为地理处理服务。
#             它使用 ArcGIS 标准数据访问方法（通过注册的数据存储）来连接数据库，
#             执行空间分析，并生成包含空气质量和鸟类多样性信息的分析网格。
# ==========================================================================================

import arcpy
import numpy as np
import os
import time
import traceback
from collections import defaultdict


# ======================================================================================
# --- 辅助函数 --- (create_analysis_grid 保持不变)
# ======================================================================================

def create_analysis_grid(output_grid_fc, template_fc, grid_cell_size):
    """根据模板要素的范围创建渔网。"""
    arcpy.AddMessage("  正在创建渔网...")
    desc = arcpy.Describe(template_fc)
    extent = desc.extent

    arcpy.CreateFishnet_management(
        out_feature_class=output_grid_fc,
        origin_coord=f"{extent.XMin} {extent.YMin}",
        y_axis_coord=f"{extent.XMin} {extent.YMax}",
        corner_coord=f"{extent.XMax} {extent.YMax}",
        cell_width=grid_cell_size,
        cell_height=grid_cell_size,
        labels="NO_LABELS",
        template=template_fc,
        geometry_type="POLYGON"
    )

    count = int(arcpy.GetCount_management(output_grid_fc)[0])
    if count > 0:
        arcpy.AddMessage(f"  渔网创建成功，包含 {count} 个网格。")
    else:
        raise Exception("无法创建有效的渔网，请检查输入边界。脚本终止。")

    arcpy.AddMessage("  添加并计算 'Grid_ID' 字段...")
    arcpy.AddField_management(output_grid_fc, "Grid_ID", "LONG")
    oid_field_name = arcpy.Describe(output_grid_fc).OIDFieldName
    arcpy.CalculateField_management(output_grid_fc, "Grid_ID", f"!{oid_field_name}!", "PYTHON3")
    arcpy.AddMessage("  'Grid_ID' 字段计算完成。")


# --- 重构后的 process_aqi_data 函数 ---
def process_aqi_data(grid_fc, start_date, end_date, db_connection_sde, pollutants_list, aqi_record_table,
                     aqi_station_table, grid_cell_size, target_projected_crs, geographic_crs):
    """
    从数据库提取空气质量数据，进行插值分析，并将结果连接到网格。
    此版本通过立即物化查询图层来增强稳定性。
    """
    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
        arcpy.AddMessage("  Spatial Analyst 许可已检出。")
    else:
        raise arcpy.ExecuteError("Spatial Analyst 许可不可用。")

    arcpy.AddMessage("  1. 从数据库提取并处理空气质量数据...")
    pollutant_avg_expressions = [f"AVG(CAST(r.{p} AS numeric)) AS avg_{p}" for p in pollutants_list]
    query_sql = f"""
        SELECT 
            s.id as station_id, 
            s.location, 
            {', '.join(pollutant_avg_expressions)}
        FROM {aqi_station_table} s
        JOIN {aqi_record_table} r ON s.id = r.station_id
        WHERE r.timestamp BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
        GROUP BY s.id, s.location
    """

    # 创建一个临时的 in_memory 查询图层
    temp_query_layer = "in_memory/temp_aqi_query_layer"
    arcpy.management.MakeQueryLayer(db_connection_sde, temp_query_layer, query_sql, "station_id", "POINT", "4326")

    # [核心修改] 立即将查询图层复制到 scratchGDB，物化数据以提高稳定性
    station_points_gcs = os.path.join(arcpy.env.scratchGDB, "temp_aqi_stations_gcs")
    arcpy.management.CopyFeatures(temp_query_layer, station_points_gcs)
    arcpy.Delete_management(temp_query_layer)  # 立即删除 in_memory 图层

    count = int(arcpy.GetCount_management(station_points_gcs)[0])
    if count == 0:
        arcpy.addWarning(f"警告：在时间段 {start_date} 到 {end_date} 内没有找到任何有效的AQI记录。AQI字段将为空。")
        for pollutant in pollutants_list:
            if not arcpy.ListFields(grid_fc, f"avg_{pollutant}"):
                arcpy.AddField_management(grid_fc, f"avg_{pollutant}", "DOUBLE")
        arcpy.CheckInExtension("Spatial")
        return

    arcpy.AddMessage(f"  成功从数据库物化 {count} 个站点的平均AQI数据。")

    # 现在对稳定的物理数据进行投影
    station_points_projected = os.path.join(arcpy.env.scratchGDB, "temp_aqi_stations_projected")
    arcpy.Project_management(station_points_gcs, station_points_projected, target_projected_crs)

    # ... 后续插值逻辑不变 ...
    for pollutant in pollutants_list:
        field_name = f"avg_{pollutant}"
        valid_points_query = f"{field_name} IS NOT NULL"
        with arcpy.da.SearchCursor(station_points_projected, [field_name], where_clause=valid_points_query) as cursor:
            valid_count = sum(1 for row in cursor if row[0] is not None)

        if valid_count < 3:
            arcpy.addWarning(f"    警告: 污染物 {pollutant} 的有效数据点不足3个 ({valid_count})，跳过插值。")
            if not arcpy.ListFields(grid_fc, field_name):
                arcpy.AddField_management(grid_fc, field_name, "DOUBLE", field_is_nullable=True)
            continue

        arcpy.AddMessage(f"\n  处理污染物: {pollutant.upper()}...")
        raster_path = os.path.join(arcpy.env.scratchGDB, f"temp_raster_{pollutant}")
        raster_out = arcpy.sa.Idw(station_points_projected, field_name, float(grid_cell_size) / 10)
        raster_out.save(raster_path)
        stats_table = os.path.join(arcpy.env.scratchGDB, "temp_stats")
        arcpy.sa.ZonalStatisticsAsTable(grid_fc, "Grid_ID", raster_path, stats_table, "DATA", "MEAN")
        arcpy.JoinField_management(grid_fc, "Grid_ID", stats_table, "Grid_ID", ["MEAN"])
        if not arcpy.ListFields(grid_fc, field_name):
            arcpy.AddField_management(grid_fc, field_name, "DOUBLE")
        expression = "float(!MEAN!) if !MEAN! is not None else 0"
        arcpy.CalculateField_management(grid_fc, field_name, expression, "PYTHON3")
        arcpy.DeleteField_management(grid_fc, ["MEAN"])
        arcpy.Delete_management(raster_path)
        arcpy.Delete_management(stats_table)
        arcpy.AddMessage(f"    {pollutant.upper()} 处理完成。")

    arcpy.CheckInExtension("Spatial")
    arcpy.Delete_management(station_points_gcs)  # 清理GCS副本
    arcpy.Delete_management(station_points_projected)
    arcpy.AddMessage("\n  AQI处理完成，临时文件已清理。")

# --- 重构后的 calculate_bird_diversity 函数 ---
def calculate_bird_diversity(grid_fc, start_date, end_date, db_connection_sde, bird_observation_table,
                             bird_species_table):
    """
    为每个网格计算鸟类多样性指数。
    此版本通过正确处理 MakeQueryLayer 的输出对象来确保稳定性。
    """
    arcpy.AddMessage("  使用空间连接计算鸟类多样性...")

    # 1. 创建鸟类观测点的查询图层
    obs_query_sql = f"""
        SELECT id, location
        FROM {bird_observation_table}
        WHERE start_time BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
    """

    # 定义查询图层的名称，而不是完整路径。它将在内存中创建。
    # 这通常比直接写入 scratchGDB 更快、更直接。
    bird_obs_query_layer_name = "bird_obs_query_layer"

    # [核心修改] 捕获 MakeQueryLayer 的返回结果 (Result 对象)
    try:
        query_layer_result = arcpy.management.MakeQueryLayer(
            input_database=db_connection_sde,
            out_layer_name=bird_obs_query_layer_name,
            query=obs_query_sql,
            oid_fields="id",
            shape_type="POINT",
            srid="4326"
        )
    except arcpy.ExecuteError as e:
        arcpy.AddError(f"ArcGIS 错误信息: {arcpy.GetMessages(2)}")
        raise e

    # [核心修改] 直接使用返回的 Result 对象作为后续工具的输入。
    # ArcPy 会自动将其解析为有效的图层。
    count_result = arcpy.GetCount_management(query_layer_result)
    count = int(count_result[0])

    if count == 0:
        arcpy.addWarning("警告：在指定时间范围内未找到任何鸟类观测记录。多样性指数将为0。")
        for field, ftype in [('richness', 'LONG'), ('abundance', 'LONG'), ('shannon', 'DOUBLE')]:
            if not arcpy.ListFields(grid_fc, field):
                arcpy.AddField_management(grid_fc, field, ftype)
            arcpy.CalculateField_management(grid_fc, field, "0", "PYTHON3")

        # 清理内存中的图层
        arcpy.Delete_management(bird_obs_query_layer_name)
        return

    arcpy.AddMessage(f"  成功创建包含 {count} 个鸟类观测点的查询图层。")

    # 2. 对鸟类观测点查询图层和网格进行空间连接
    spatially_joined_points = os.path.join(arcpy.env.scratchGDB, "temp_spatial_join_points")
    # [核心修改] 同样，使用 Result 对象作为输入
    arcpy.analysis.SpatialJoin(query_layer_result, grid_fc, spatially_joined_points, "JOIN_ONE_TO_ONE", "KEEP_ALL",
                               match_option="WITHIN")

    # 3. 将物种记录表连接到空间连接后的点上 (这部分不变)
    species_table_path = os.path.join(db_connection_sde, bird_species_table)
    arcpy.management.JoinField(spatially_joined_points, "id", species_table_path, "observation_id",
                               ["taxon_name", "count"])

    # --- 后续计算逻辑保持不变 ---
    # 4. 使用游标和字典计算多样性指数
    arcpy.AddMessage("  正在计算多样性指数...")
    grid_data = defaultdict(lambda: {'species_counts': defaultdict(int)})
    fields_to_read = ["Grid_ID", "taxon_name", "count"]
    with arcpy.da.SearchCursor(spatially_joined_points, fields_to_read) as cursor:
        for grid_id, species_name, count in cursor:
            if grid_id is not None and species_name is not None and count is not None:
                grid_data[grid_id]['species_counts'][species_name] += count

    results = []
    for grid_id, data in grid_data.items():
        species_counts = data['species_counts']
        richness = len(species_counts)
        abundance = sum(species_counts.values())
        shannon = 0.0
        if abundance > 0:
            pi_values = [c / abundance for c in species_counts.values()]
            shannon = -sum(pi * np.log(pi) for pi in pi_values)
        results.append((grid_id, richness, abundance, shannon))

    # 5. 将计算结果合并回原始网格
    arcpy.AddMessage("  正在将计算结果合并回网格...")
    for field, ftype in [('richness', 'LONG'), ('abundance', 'LONG'), ('shannon', 'DOUBLE')]:
        if not arcpy.ListFields(grid_fc, field):
            arcpy.AddField_management(grid_fc, field, ftype)

    fields_to_update = ['Grid_ID', 'richness', 'abundance', 'shannon']
    with arcpy.da.UpdateCursor(grid_fc, fields_to_update) as cursor:
        for row in cursor:
            row[1], row[2], row[3] = 0, 0, 0.0  # Initialize first
            cursor.updateRow(row)

    results_dict = {r[0]: (r[1], r[2], r[3]) for r in results}
    with arcpy.da.UpdateCursor(grid_fc, fields_to_update) as cursor:
        for row in cursor:
            grid_id = row[0]
            if grid_id in results_dict:
                row[1], row[2], row[3] = results_dict[grid_id]
                cursor.updateRow(row)

    # 清理临时数据
    arcpy.Delete_management(bird_obs_query_layer_name)  # 清理内存中的图层
    arcpy.Delete_management(spatially_joined_points)
    arcpy.AddMessage("  鸟类多样性数据合并完成。")


# ======================================================================================
# --- 主执行逻辑 ---
# ======================================================================================
def main():
    try:
        # --- 输入参数 ---
        start_date_obj = arcpy.GetParameter(0)
        end_date_obj = arcpy.GetParameter(1)
        # !! 重要: 此处应为指向已在 ArcGIS Server 中注册的数据存储的 .sde 连接文件路径
        db_connection_sde_file = arcpy.GetParameterAsText(2)

        # --- 参数处理 ---
        start_date = start_date_obj.strftime("%Y-%m-%d")
        end_date = end_date_obj.strftime('%Y-%m-%d')
        arcpy.AddMessage(f"开始生成分析数据，时间范围: {start_date} to {end_date}")
        arcpy.AddMessage(f"使用数据库连接文件: {db_connection_sde_file}")

        # --- 环境设置 ---
        workspace = arcpy.env.scratchGDB
        arcpy.env.workspace = workspace
        arcpy.env.overwriteOutput = True

        # --- 配置变量 ---
        target_projected_crs = arcpy.SpatialReference(32650)  # WGS 84 / UTM zone 50N
        geographic_crs = arcpy.SpatialReference(4326)  # WGS 84
        grid_cell_size = "2000"  # Meters
        pollutants_list = ['pm25', 'no2', 'o3', 'so2', 'co']

        # 数据库表名 (请确保 schema.table 格式正确)
        db_schema = "public"
        aqi_station_table = f'{db_schema}.data_pipeline_aqistation'
        aqi_record_table = f'{db_schema}.data_pipeline_aqirecord'
        bird_observation_table = f'{db_schema}.data_pipeline_birdobservation'
        bird_species_table = f'{db_schema}.data_pipeline_birdspeciesrecord'

        # 输入边界
        input_boundary_url = r"https://product.geoscene.cn/server/rest/services/Hosted/beijing_shp/FeatureServer/0"
        arcpy.env.outputCoordinateSystem = target_projected_crs

        # --- 步骤一: 创建分析网格 ---
        arcpy.AddMessage("\n--- 步骤一: 创建分析网格 ---")
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        local_boundary_copy = os.path.join(workspace, f"temp_boundary_copy_{timestamp}")
        projected_boundary = os.path.join(workspace, f"temp_projected_boundary_{timestamp}")
        analysis_grid_fc = os.path.join(workspace, f"AnalysisGrid_{timestamp}")

        arcpy.CopyFeatures_management(input_boundary_url, local_boundary_copy)
        arcpy.Project_management(local_boundary_copy, projected_boundary, target_projected_crs)
        create_analysis_grid(analysis_grid_fc, projected_boundary, grid_cell_size)
        arcpy.Delete_management(local_boundary_copy)
        arcpy.Delete_management(projected_boundary)

        # !! 移除了所有 SQLAlchemy 和密码处理逻辑
        # 连接将由 arcpy 工具通过 .sde 文件自动处理

        # --- 步骤二: 处理空气质量数据 ---
        arcpy.AddMessage("\n--- 步骤二: 处理空气质量数据 ---")
        process_aqi_data(analysis_grid_fc, start_date, end_date, db_connection_sde_file, pollutants_list,
                         aqi_record_table, aqi_station_table, grid_cell_size, target_projected_crs, geographic_crs)

        # --- 步骤三: 计算鸟类多样性指数 ---
        arcpy.AddMessage("\n--- 步骤三: 计算鸟类多样性指数 ---")
        # 注意：现在不需要传递地理坐标系，因为空间连接会处理好
        calculate_bird_diversity(analysis_grid_fc, start_date, end_date, db_connection_sde_file, bird_observation_table,
                                 bird_species_table)

        # --- 步骤四: 设置输出 ---
        arcpy.AddMessage("\n--- 所有步骤完成！正在设置输出结果... ---")
        arcpy.SetParameter(3, analysis_grid_fc)
        arcpy.AddMessage("脚本成功完成。")

    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
        arcpy.AddError(traceback.format_exc())
    except Exception as e:
        arcpy.AddError(f"脚本发生未知错误: {e}")
        arcpy.AddError(traceback.format_exc())


if __name__ == '__main__':
    main()