# Docker PySpark Installation Guide

> **Quick Start** - Containerized development environment

## Overview

Docker provides the fastest way to get started with PySpark development. This approach eliminates dependency conflicts and provides a consistent environment across different machines.

## Prerequisites

- **Docker Engine**: Docker Desktop (macOS/Windows) or Docker CE (Linux)
- **Memory**: 4GB RAM allocated to Docker
- **Storage**: 5GB free space for Docker images

### Verify Docker Installation
```bash
# Check Docker version
docker --version
# Should output: Docker version 20.x.x or higher

# Check Docker daemon
docker info
```

## Quick Start with Jupyter PySpark Notebook

### 1. Pull and Run Jupyter PySpark Image
```bash
# Pull the official Jupyter PySpark image
docker pull jupyter/pyspark-notebook

# Run container with port mapping
docker run -p 8888:8888 -p 4040:4040 jupyter/pyspark-notebook
```

### 2. Access Jupyter Notebook
- Open browser: http://localhost:8888
- Copy token from Docker logs or use the provided URL
- Spark UI available at: http://localhost:4040

### 3. Test PySpark in Jupyter
Create a new Python 3 notebook and run:
```python
# Test PySpark functionality
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DockerTest").getOrCreate()
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()
spark.stop()
```

## Advanced Docker Setup

### Custom Docker Compose for Development
Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  pyspark-jupyter:
    image: jupyter/pyspark-notebook:latest
    ports:
      - "8888:8888"    # Jupyter notebook
      - "4040:4040"    # Spark UI
      - "4041:4041"    # Spark UI alternative
    volumes:
      - ./notebooks:/home/jovyan/work/notebooks
      - ./data:/home/jovyan/work/data
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - GRANT_SUDO=yes
    command: start-notebook.sh --NotebookApp.token='' --NotebookApp.password=''
```

### Run with Docker Compose
```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f pyspark-jupyter

# Stop services
docker-compose down
```

## Custom PySpark Docker Image

### Dockerfile for Professional Development
```dockerfile
FROM jupyter/pyspark-notebook:latest

# Install additional packages
RUN pip install --no-cache-dir \
    pandas==1.5.3 \
    numpy==1.24.3 \
    matplotlib==3.7.2 \
    seaborn==0.12.2 \
    scikit-learn==1.3.0 \
    pytest==7.4.0 \
    black==23.7.0 \
    isort==5.12.0 \
    mypy==1.5.1

# Install development tools
RUN pip install --no-cache-dir \
    jupyterlab==4.0.5 \
    jupyter-contrib-nbextensions \
    jupyter-nbextensions-configurator

# Configure Jupyter extensions
RUN jupyter contrib nbextension install --user && \
    jupyter nbextensions_configurator enable --user

# Create working directory
RUN mkdir -p /home/jovyan/work
WORKDIR /home/jovyan/work

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Expose ports
EXPOSE 8888 4040 4041

# Default command
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
```

### Build and Run Custom Image
```bash
# Build image
docker build -t pyspark-professional:latest .

# Run container
docker run -p 8888:8888 -p 4040:4040 \
  -v $(pwd)/notebooks:/home/jovyan/work/notebooks \
  -v $(pwd)/data:/home/jovyan/work/data \
  pyspark-professional:latest
```

## Production-Ready Docker Setup

### Multi-Container Spark Cluster
```yaml
version: '3.8'
services:
  spark-master:
    image: bitnami/spark:latest
    command: bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "7077:7077"    # Master port
      - "8080:8080"    # Master web UI
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark-master

  spark-worker:
    image: bitnami/spark:latest
    command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=1

  pyspark-jupyter:
    build: .
    ports:
      - "8888:8888"
      - "4040:4040"
    depends_on:
      - spark-master
    environment:
      - SPARK_MASTER=spark://spark-master:7077
    volumes:
      - ./notebooks:/home/jovyan/work/notebooks
      - ./data:/home/jovyan/work/data
```

## Data Persistence

### Mount Host Directories
```bash
# Mount project directories
docker run -p 8888:8888 \
  -v $(pwd)/notebooks:/home/jovyan/work/notebooks \
  -v $(pwd)/data:/home/jovyan/work/data \
  -v $(pwd)/scripts:/home/jovyan/work/scripts \
  jupyter/pyspark-notebook
```

### Use Named Volumes
```bash
# Create named volume
docker volume create pyspark-data

# Run with named volume
docker run -p 8888:8888 \
  -v pyspark-data:/home/jovyan/work/data \
  jupyter/pyspark-notebook
```

## Troubleshooting

### Common Issues

#### Port Already in Use
```bash
# Find process using port
lsof -i :8888

# Kill process or use different port
docker run -p 8889:8888 jupyter/pyspark-notebook
```

#### Memory Issues
```bash
# Increase Docker memory allocation
# Docker Desktop: Preferences > Resources > Memory

# Or limit Spark memory in notebook
spark = SparkSession.builder \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
```

#### Permission Issues on Linux
```bash
# Add user to docker group
sudo usermod -aG docker $USER

# Restart session or run:
newgrp docker
```

### Performance Optimization
```bash
# Use host network for better performance (Linux only)
docker run --network host jupyter/pyspark-notebook

# Limit CPU usage
docker run --cpus=2 jupyter/pyspark-notebook

# Set memory limits
docker run --memory=4g jupyter/pyspark-notebook
```

## Next Steps

With Docker PySpark running, proceed to:
- **[First Steps](../first_steps/)** - Your first PySpark programs
- **[Local Installation](local_installation.md)** - Full local setup guide

Docker provides the fastest path to PySpark development! 🚀
