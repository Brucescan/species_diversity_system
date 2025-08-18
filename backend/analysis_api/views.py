# analysis_api/views.py
import json
from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import SpearmanAnalysisSerializer,PredictionInputSerializer
from .services import prediction_service
from .services.prediction_service import perform_prediction
from .services.ml_loader import GLOBAL_DF_HISTORY_PROCESSED
# 导入必要的第三方库
from osgeo import ogr
import pandas as pd
import geopandas as gpd
from scipy.stats import spearmanr


def perform_spearman_analysis_gdal(gdb_path, layer_name, field_list):
    """
    使用 GDAL/OGR 读取数据并执行 Spearman 相关性分析的核心函数。
    """
    try:
        # 打开数据源
        dataSource = ogr.Open(gdb_path, 0)
        if dataSource is None:
            return None, (f"GDAL/OGR 无法打开数据源: {gdb_path}", status.HTTP_404_NOT_FOUND)
        # 按名称获取图层
        layer = dataSource.GetLayerByName(layer_name)
        if layer is None:
            dataSource = None  # 在返回前确保关闭数据源
            return None, (f"在数据源 {gdb_path} 中找不到名为 '{layer_name}' 的图层。", status.HTTP_404_NOT_FOUND)
        # 将属性读取到一个字典列表中
        all_rows = []
        for feature in layer:
            row_data = {field: feature.GetField(field) for field in field_list}
            all_rows.append(row_data)
        #解引用数据源对象以关闭文件并释放锁
        dataSource = None
        if not all_rows:
            return None, ("图层为空或未读取到任何要素。", status.HTTP_400_BAD_REQUEST)
        # 5. 将字典列表转换为 Pandas DataFrame
        df = pd.DataFrame.from_records(all_rows)

    except Exception as e:
        return None, (f"使用GDAL/OGR读取数据时出错: {e}", status.HTTP_500_INTERNAL_SERVER_ERROR)

    # 计算逻辑
    num_fields = len(field_list)
    results_dict = {field: {} for field in field_list}

    for i in range(num_fields):
        for j in range(num_fields):
            col1 = field_list[i]
            col2 = field_list[j]

            if i == j:
                results_dict[col1][col2] = {"correlation": 1.0, "p_value": 0.0}
                continue

            # 从DataFrame中提取数据并移除空值
            data1 = df[col1].dropna()
            data2 = df[col2].dropna()

            # 对齐数据，只使用两个系列中都非空的行进行计算
            common_index = data1.index.intersection(data2.index)

            if len(common_index) < 3:
                results_dict[col1][col2] = {"correlation": None, "p_value": None}
                continue

            # 使用对齐后的数据进行计算
            corr, p_value = spearmanr(data1.loc[common_index], data2.loc[common_index])
            results_dict[col1][col2] = {"correlation": corr, "p_value": p_value}

    return results_dict, None


class SpearmanAnalysisView(APIView):
    """
    执行Spearman相关性分析的API端点。
    """
    permission_classes = [AllowAny]


    def post(self, request, *args, **kwargs):
        # 使用序列化器验证输入数据
        serializer = SpearmanAnalysisSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        # 获取验证后的数据
        validated_data = serializer.validated_data
        gdb_path = validated_data['gdb_path']
        layer_name = validated_data['layer_name']
        fields = validated_data['fields'].split(';')

        # 调用核心分析函数
        try:
            results, error = perform_spearman_analysis_gdal(gdb_path, layer_name, fields)

            if error:
                error_message, error_status = error
                return Response({"error": error_message}, status=error_status)

            # 成功后返回JSON结果
            return Response(results, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": f"服务器内部发生未知错误: {str(e)}"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class PredictFutureBaselineView(APIView):
    """
    根据给定的开始月份和月数，预测未来的生物多样性基线指标。
    """
    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
        serializer = PredictionInputSerializer(data=request.query_params)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        validated_data = serializer.validated_data
        start_month_str = validated_data['start_month_str']
        num_months = validated_data['num_months']

        try:
            start_date = pd.to_datetime(start_month_str)
            target_dates = pd.date_range(start=start_date, periods=num_months, freq='ME')

            # 调用核心预测服务
            prediction_results = perform_prediction(target_dates)

            # 返回结果
            return Response(prediction_results, status=status.HTTP_200_OK)

        except Exception as e:
            print(f"Prediction Error: {e}")
            import traceback
            traceback.print_exc()
            return Response(
                {"error": "服务器在预测过程中发生内部错误。", "details": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class GridGeometriesView(APIView):
    """
    提供所有网格单元的地理信息
    """
    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
        if GLOBAL_DF_HISTORY_PROCESSED is None:
            return Response(
                {"error": "服务器正在初始化地理数据，请稍后再试。"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE
            )

        try:
            unique_geometries_df = GLOBAL_DF_HISTORY_PROCESSED[['Grid_ID', 'geometry']].drop_duplicates('Grid_ID')

            valid_geometries_df = unique_geometries_df.dropna(subset=['geometry']).copy()

            if valid_geometries_df.empty:
                print("警告: 清理后没有找到任何有效的地理信息。")
                return Response({"type": "FeatureCollection", "features": []}, status=status.HTTP_200_OK)

            gdf = gpd.GeoDataFrame(valid_geometries_df, geometry='geometry')
            geojson_str = gdf.to_json()

            geojson_data = json.loads(geojson_str)
            return Response(geojson_data, status=status.HTTP_200_OK)

        except Exception as e:
            print("--- GridGeometriesView 发生错误 ---")
            return Response(
                {"error": "创建地理信息时发生内部错误。", "details": str(e)},  # 把错误详情也返回给 Postman
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ScenarioPredictionView(APIView):
    """
    接收用户定义的情景模拟参数，并返回重新预测的结果。
    """
    permission_classes = [AllowAny]

    def post(self, request, *args, **kwargs):
        # 获取并验证输入数据
        grid_ids = request.data.get('grid_ids')
        target_date_strs = request.data.get('target_dates')
        modifications = request.data.get('modifications')

        if not all([grid_ids, target_date_strs, modifications]):
            return Response(
                {"error": "请求体必须包含 'grid_ids', 'target_dates', 和 'modifications'。"},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not isinstance(grid_ids, list) or not isinstance(target_date_strs, list) or not isinstance(modifications,
                                                                                                      dict):
            return Response(
                {"error": "'grid_ids' 和 'target_dates' 必须是列表, 'modifications' 必须是字典。"},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            # 将日期字符串转换为 datetime 对象
            target_dates = [pd.to_datetime(date) for date in target_date_strs]
        except Exception as e:
            return Response({"error": f"日期格式无效: {e}"}, status=status.HTTP_400_BAD_REQUEST)

        # 调用服务层执行情景模拟
        print("接收到情景模拟请求...")
        try:
            results = prediction_service.perform_scenario_prediction(
                grid_ids=grid_ids,
                target_dates=target_dates,
                modifications=modifications
            )
            return Response(results, status=status.HTTP_200_OK)

        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            print(f"情景模拟时发生服务器内部错误: {e}")
            return Response({"error": f"预测过程中发生错误: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)