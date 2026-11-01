FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONUTF8=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    RAY_USAGE_STATS_ENABLED=0 CUDA_VISIBLE_DEVICES=-1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --timeout 120 -r requirements.txt
COPY requirements-control.txt /app/requirements-control.txt
RUN pip install --no-cache-dir --timeout 120 -r /app/requirements-control.txt
COPY app /app/app
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
