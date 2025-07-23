# 使用官方Python基础镜像（基于Debian bookworm）
FROM python:3.10-slim-bookworm

# 关键步骤：一次性解决 apt, npm, 和 Python包编译 的依赖问题
RUN rm -rf /etc/apt/sources.list.d/* \
    && echo "deb https://mirrors.aliyun.com/debian/ bookworm main contrib non-free" > /etc/apt/sources.list \
    && echo "deb https://mirrors.aliyun.com/debian/ bookworm-updates main contrib non-free" >> /etc/apt/sources.list \
    && echo "deb https://mirrors.aliyun.com/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list \
    && apt-get -o Acquire::Retries=3 update \
    && apt-get install -y --no-install-recommends \
        # 之前的依赖
        binutils \
        gdal-bin \
        libgdal-dev \
        libgeos-dev \
        wget \
        ca-certificates \
        gnupg \
        # --- 新增的关键依赖 ---
        # build-essential 包含了 gcc, g++, make 等一系列编译工具
        build-essential \
        # psycopg2 编译所需要的 PostgreSQL 客户端开发库
        libpq-dev \
    # 安装 Node.js
    && wget https://deb.nodesource.com/setup_18.x -O nodesource_setup.sh \
    && bash nodesource_setup.sh \
    && apt-get install -y nodejs \
    # 配置 npm 镜像并安装 yarn
    && npm config set registry https://registry.npmmirror.com \
    && npm install -g yarn \
    # 清理工作
    && rm nodesource_setup.sh \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件到工作目录
COPY requirements.txt .

# 设置PIP为国内源并安装依赖
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt

# 复制项目文件到工作目录
COPY . .

ENV PYTHONPATH=/app
ENV DJANGO_SETTINGS_MODULE=backend.settings
# 暴露应用端口
EXPOSE 8000

# 启动命令
ENTRYPOINT ["sh", "-c"]
CMD ["python manage.py makemigrations && python manage.py migrate && python run.py"]