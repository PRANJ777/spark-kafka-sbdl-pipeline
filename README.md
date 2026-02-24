# SBDL Spark Kafka Pipeline

## Overview
Production-style Spark pipeline that transforms structured banking contract data
into nested CDC-style JSON and publishes to Kafka.

## Architecture
Source → Spark Transform → Nested Struct Modeling → Kafka Producer

## Tech Stack
- PySpark 3.5.0
- Kafka
- Python 3.11
- Virtual Environment
- Config-driven architecture

## How to Run

Start Kafka locally.

Then:

python sbdl_main.py LOCAL 2023-01-01

## Branching Strategy
- dev
- release
- master
- feature/*