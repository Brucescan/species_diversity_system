from django.apps import AppConfig

from .services import ml_loader

class AnalysisApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "analysis_api"

    def ready(self):
        """
        Django 应用启动时执行的钩子函数。
        """
        import os
        if os.environ.get('RUN_MAIN', None) != 'true':
            return
        print("检测到服务器主进程启动，准备加载ML资源...")
        ml_loader.load_all_resources()