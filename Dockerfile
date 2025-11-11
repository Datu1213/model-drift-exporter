# ---------- Builder Stage ----------
FROM python:3.11-bullseye AS builder

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Create virtual environment with uv
# 安装 pip/uv
RUN python -m pip install --upgrade pip setuptools wheel uv && \
    uv venv /opt/exporter-venv && \
    uv pip install --no-cache-dir --python /opt/exporter-venv/bin/python -vvv -r /tmp/requirements.txt

# ---------- Runtime Stage ----------
FROM python:3.11-slim-bullseye AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Environment variables
ENV PATH="/opt/exporter-venv/bin:$PATH" \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=$JAVA_HOME/bin:$PATH \
    SPARK_CONF_DIR=/opt/spark/conf

# Copy virtual environment from builder
COPY --from=builder /opt/exporter-venv /opt/exporter-venv

WORKDIR /opt

ADD src/eviently-prometheus-exporter.py .
ADD conf/spark-defaults.conf spark/conf
# Create non-root user
RUN useradd -m -u 1000 exporter && \
    chmod +x eviently-prometheus-exporter.py && \
    chown -R exporter:exporter /opt

USER exporter

CMD [ "/opt/exporter-venv/bin/python", "eviently-prometheus-exporter.py" ]
