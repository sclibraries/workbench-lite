FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace

WORKDIR /workspace

COPY requirements.txt /tmp/workbench-lite-requirements.txt
RUN pip install --no-cache-dir -r /tmp/workbench-lite-requirements.txt

COPY workbench_lite /workspace/workbench_lite

ENTRYPOINT ["python", "-m", "workbench_lite.cli"]
CMD ["check", "--help"]
