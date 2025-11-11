# ---------- Builder Stage ----------
FROM python:3.11-bullseye AS builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Create virtual environment with uv
# 安装 pip/uv
RUN python -m pip install --upgrade pip setuptools wheel uv && \
    uv venv /opt/exporter-venv && \
    . /opt/exporter-venv/bin/activate && \
    uv pip install --no-cache-dir -vvv -r /tmp/requirements.txt

# ---------- Runtime Stage ----------
FROM python:3.11-slim-bullseye AS runtime

# Environment variables
ENV PATH="/opt/exporter-venv/bin:$PATH"

# Copy virtual environment from builder
COPY --from=builder /opt/exporter-venv /opt/exporter-venv

WORKDIR /opt

ADD src/eviently-prometheus-exporter.py .

# Create non-root user
RUN useradd -m -u 1000 exporter && \
    chmod +x eviently-prometheus-exporter.py && \
    chown -R exporter:exporter /opt

USER exporter

CMD [ "python3", "-m", "eviently-prometheus-exporter.py" ]
