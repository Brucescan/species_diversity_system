from django.apps import AppConfig

from .services import ml_loader

class AnalysisApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "analysis_api"

    def ready(self):
        """
        Django 应用启动时执行的钩子函数。
        """
        # 确保只在主进程（runserver）中执行一次，而不是在重载器进程中也执行
        import os
        if os.environ.get('RUN_MAIN', None) != 'true':
            return

        print("检测到服务器主进程启动，准备加载ML资源...")
        # 导入我们的服务模块
        # 把导入放在这里是为了避免在应用加载早期阶段出现循环依赖问题


        # 调用主加载函数
        ml_loader.load_all_resources()