from django_filters import rest_framework as filters

from sql.models import Users


class UserFilter(filters.FilterSet):
    class Meta:
        model = Users
        fields = {
            "id": ["exact"],
            "username": ["exact"],
        }
