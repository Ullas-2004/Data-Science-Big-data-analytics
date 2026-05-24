#!/usr/bin/env bash
set -e
python backend/realtime/kafka_producer.py
python backend/realtime/kafka_consumer.py
