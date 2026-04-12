FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-venv python3-dev gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

RUN pip install --no-cache-dir fastapi uvicorn jpype1

COPY main.py .
COPY jars/ ./jars/
COPY database/ ./database/

EXPOSE 8000

CMD ["python3", "main.py"]