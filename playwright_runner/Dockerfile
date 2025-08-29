FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libnss3-dev \
    libxss1 \
    libasound2 \
    libdbus-glib-1-2 \
    libcups2 \
    libgtk-3-0 \
    libgbm-dev \
    jq \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install

COPY . .

CMD ["/bin/bash", "run.sh"]
