from django_filters import rest_framework as filters
from .models import Record
from django.db.models import Q


class RecordFilter(filters.FilterSet):
    search = filters.CharFilter(method='universal_search', label='Search in username and address')
    observation_address = filters.CharFilter(
        field_name='observation_address',
        lookup_expr='icontains'
    )
    observation_date = filters.DateFilter(
        field_name='observation_start_time',
        lookup_expr='date'
    )

    username = filters.CharFilter(
        field_name='user__username',
        lookup_expr='icontains'
    )

    record_identifier = filters.CharFilter(
        field_name='record_identifier',
        lookup_expr='exact'
    )

    class Meta:
        model = Record
        fields = ['observation_address', 'observation_date', 'username', 'record_identifier']

    def universal_search(self, queryset, name, value):
        return queryset.filter(
            Q(user__username__icontains=value) | Q(observation_address__icontains=value)
        )