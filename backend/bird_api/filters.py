from django_filters import rest_framework as filters
from .models import Record


class RecordFilter(filters.FilterSet):
    # 1. 观测地点搜索 (observation_address)，不区分大小写的模糊查询
    observation_address = filters.CharFilter(
        field_name='observation_address',
        lookup_expr='icontains'
    )

    # 2. 观测时间搜索 (observation_time)，我们接受一个日期，并查找在这一天开始的记录
    #    前端可以传参如: ?observation_date=2022-10-28
    observation_date = filters.DateFilter(
        field_name='observation_start_time',
        lookup_expr='date'
    )

    # 3. 记录用户搜索 (record_user)，根据用户名进行不区分大小写的模糊查询
    #    前端可以传参如: ?username=someuser
    username = filters.CharFilter(
        field_name='user__username',
        lookup_expr='icontains'
    )

    # 4. 报告编号搜索 (record_identifier)，精确匹配
    #    前端可以传参如: ?record_identifier=unique-id-123
    record_identifier = filters.CharFilter(
        field_name='record_identifier',
        lookup_expr='exact'
    )

    class Meta:
        model = Record
        # 在这里定义了所有可用于搜索的字段
        fields = ['observation_address', 'observation_date', 'username', 'record_identifier']