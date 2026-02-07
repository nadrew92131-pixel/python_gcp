# 1. 使用 Python 3.14 輕量版 (對應你之前的設定)
FROM python:3.14-slim

# 2. 設定環境變數
# 防止 Python 產生 .pyc 檔案，並確保 Log 能即時印出
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. 安裝系統工具 (pandas 和 sqlalchemy 連線 mysql 有時需要編譯環境)
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 4. 安裝 Poetry 2.0+
RUN pip install --no-cache-dir "poetry>=2.0.0"

# 5. 設定容器內的工作目錄
WORKDIR /app

# 6. 複製環境定義檔案
# 這裡先複製 lock 和 toml 是為了利用 Docker 快取層，加快以後 build 的速度
COPY pyproject.toml poetry.lock* ./

# 7. 安裝套件 (不建立虛擬環境，直接裝在容器裡)
# 加上 --no-root 是為了避免因為找不到 README 或專案資料夾而報錯
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# 8. 複製專案其餘檔案 (main.py, config.py 等)
COPY . .

# 9. 執行程式
CMD ["python", "main.py"]