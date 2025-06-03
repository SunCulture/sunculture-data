FROM apache/airflow:2.6.1

USER root

# Install common dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    apt-transport-https \
    ca-certificates \
    chromium \
    chromium-driver \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic links for compatibility with code expecting "google-chrome"
RUN ln -s /usr/bin/chromium /usr/bin/google-chrome || true

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt