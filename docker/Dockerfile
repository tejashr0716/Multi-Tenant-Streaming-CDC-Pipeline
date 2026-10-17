FROM python:3.11-slim

WORKDIR /app

# Install system build dependencies for C++ compilation and PostgreSQL client
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    cmake \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Compile C++ shared library
RUN g++ -O3 -shared -fPIC -std=c++17 -Iinclude src/streamforge.cpp -o libstreamforge.dylib || \
    g++ -O3 -shared -fPIC -std=c++17 -Iinclude src/streamforge.cpp -o libstreamforge.so

EXPOSE 8000

ENV PYTHONPATH=/app

CMD ["uvicorn", "python.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
