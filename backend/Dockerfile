FROM python:3.10-slim-bookworm

RUN rm -rf /etc/apt/sources.list.d/* \
    && echo "deb https://mirrors.aliyun.com/debian/ bookworm main contrib non-free" > /etc/apt/sources.list \
    && echo "deb https://mirrors.aliyun.com/debian/ bookworm-updates main contrib non-free" >> /etc/apt/sources.list \
    && echo "deb https://mirrors.aliyun.com/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list \
    && apt-get -o Acquire::Retries=3 update \
    && apt-get install -y --no-install-recommends \
        binutils \
        gdal-bin \
        libgdal-dev \
        libgeos-dev \
        wget \
        ca-certificates \
        gnupg \
        build-essential \
        libpq-dev \
        nodejs \
        npm \
    && npm config set registry https://registry.npmmirror.com \
    && npm install -g yarn \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件到工作目录
COPY requirements.txt .

RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt

# 复制项目文件到工作目录
COPY . .

ENV PYTHONPATH=/app
ENV DJANGO_SETTINGS_MODULE=backend.settings
# 暴露应用端口
EXPOSE 8000

CMD ["sh", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8000"]