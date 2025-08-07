from rest_framework import permissions

class IsOwnerOrReadOnly(permissions.BasePermission):
    """
    自定义权限，只允许对象的所有者编辑或删除它。
    对于其他用户，只提供只读权限。
    """
    def has_object_permission(self, request, view, obj):
        # 读取权限对任何请求都允许 (GET, HEAD, OPTIONS)
        if request.method in permissions.SAFE_METHODS:
            return True

        # 写入（包括删除）权限只对记录的所有者开放
        return obj.user == request.user