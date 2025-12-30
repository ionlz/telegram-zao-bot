ARG PYTHON_VERSION=3.14
FROM python:${PYTHON_VERSION}-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# tzdata: 让 ZoneInfo 在 slim 镜像里也能正确识别常见时区（如 Asia/Shanghai）
RUN apt-get update \
  && apt-get install -y --no-install-recommends tzdata ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 先复制依赖描述文件，最大化利用 Docker layer cache
COPY pyproject.toml uv.lock /app/

# 用 uv.lock 导出 requirements，再用 pip 安装（避免项目缺少 build-system 时的安装问题）
RUN pip install --no-cache-dir uv \
  && uv export --frozen --no-dev -o /tmp/requirements.txt \
  && pip install --no-cache-dir -r /tmp/requirements.txt \
  && rm -f /tmp/requirements.txt

# 再复制源码
COPY . /app

# SQLite 默认路径会落到 ./data；这里在镜像内创建目录，运行时建议通过 volume 挂载持久化
RUN mkdir -p /data

# 默认：用环境变量配置（推荐）
# - BOT_TOKEN 必填（运行时）
# - DB_PATH 默认 /data/zao_bot.sqlite3
# - TZ 默认 Asia/Shanghai
ENV DB_PATH=/data/zao_bot.sqlite3 \
    TZ=Asia/Shanghai

CMD ["python", "main.py"]

