# Local PySpark Installation Guide

> **Professional Setup** - Enterprise-grade local development environment

## Overview

This guide covers installing Apache Spark and PySpark locally for development and learning.

## Prerequisites

### System Requirements
- **Operating System**: macOS 10.14+, Ubuntu 18.04+, Windows 10+
- **Memory**: 8GB RAM minimum, 16GB recommended
- **Storage**: 10GB free space for Spark and datasets

### Software Dependencies
- **Note**: Java 21 is recommended over Java 8 for better performance, security updates, and modern JVM features. Spark 3.5.0 fully supports Java 21.
- **Python 3.11+**: Core language runtime
- **Java 21**: JVM for Spark execution (latest LTS with enhanced performance)

## Quick Installation

### 1. Install Java and Python
```bash
# macOS with Homebrew
brew install python@3.11 openjdk@21

# Ubuntu/Debian  
sudo apt update
sudo apt install python3.11 openjdk-21-jdk
```

### 2. Download and Install Spark
```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
sudo mv spark-3.5.6-bin-hadoop3 /opt/spark-3.5.6
sudo ln -sf /opt/spark-3.5.6 /opt/spark
```

### 3. Configure Environment
```bash
# Add to ~/.bashrc or ~/.zshrc
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
```

### 4. Install PySpark
```bash
pip install pyspark==3.5.6 jupyter pandas numpy
```

### 5. Test Installation
```bash
pyspark --version
python3 -c "import pyspark; print('PySpark ready!')"
```

## Next Steps

- **[First Steps](../first_steps/)** - Your first PySpark programs
- **[Architecture](../architecture/)** - Understanding Spark internals

Your development environment is ready! 🎉
