FROM node:20-slim

RUN apt-get update && apt-get install -y \
    python3 python3-pip make g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Node deps
COPY package*.json ./
RUN npm install

# Python deps — install CPU-only torch first to avoid the 2GB CUDA build
RUN pip3 install torch --index-url https://download.pytorch.org/whl/cpu --break-system-packages
COPY pipeline/requirements.txt ./pipeline/
RUN pip3 install -r pipeline/requirements.txt --break-system-packages

COPY . .

RUN mkdir -p /data
ENV PYTHONUNBUFFERED=1
RUN chmod +x start.sh

EXPOSE 8080

CMD ["./start.sh"]
