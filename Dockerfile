FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir fastapi uvicorn jpype1 --break-system-packages

COPY main.py .
COPY jars/ ./jars/
COPY database/ ./database/

EXPOSE 8000

CMD ["python3", "main.py"]
