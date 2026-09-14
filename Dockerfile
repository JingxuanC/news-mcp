FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=50059

WORKDIR /app

# 无第三方依赖 —— 只要标准库，所以不需要 pip install 层。
# （后续若要内置行情拉取再引入 requirements.txt）
COPY . .

RUN useradd --uid 10001 --no-create-home --home-dir /app appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 50059
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:50059/health',timeout=3).status==200 else 1)"

CMD ["python3", "server.py", "--host", "0.0.0.0", "--port", "50059"]
