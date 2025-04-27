# 使用官方Python基础镜像（基于Debian bookworm）
FROM python:3.10-slim-bookworm

# 设置APT为网易163镜像源（完全替换）
RUN echo "deb http://mirrors.163.com/debian/ bookworm main non-free contrib" > /etc/apt/sources.list \
    && echo "deb http://mirrors.163.com/debian/ bookworm-updates main non-free contrib" >> /etc/apt/sources.list \
    && echo "deb http://mirrors.163.com/debian-security/ bookworm-security main" >> /etc/apt/sources.list

# 安装系统依赖（包括Node.js）
RUN apt-get update && apt-get install -y \
    binutils \
    gdal-bin \
    libgdal-dev \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 安装Node.js（直接从NodeSource安装特定版本）
RUN wget https://deb.nodesource.com/setup_18.x -O nodesource_setup.sh \
    && bash nodesource_setup.sh \
    && apt-get update && apt-get install -y nodejs \
    && rm nodesource_setup.sh \
    && npm install -g yarn

# 设置工作目录
WORKDIR /app

# 复制依赖文件到工作目录
COPY requirements.txt .

# 设置PIP为网易163源并安装依赖
RUN pip install --no-cache-dir -i http://mirrors.163.com/pypi/simple/ --trusted-host mirrors.163.com -r requirements.txt

# 复制项目文件到工作目录
COPY . .

ENV PYTHONPATH=/app
ENV DJANGO_SETTINGS_MODULE=backend.settings
# 暴露应用端口
EXPOSE 8000

# 启动命令
ENTRYPOINT ["sh", "-c"]
CMD ["python manage.py migrate && python run.py"]