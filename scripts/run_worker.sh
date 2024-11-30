#!/bin/bash

# Set the Python virtual environment path if needed
# VENV_PATH="/path/to/your/venv"

# Activate the virtual environment if needed
# source "$VENV_PATH/bin/activate"

# Set Kafka broker address and topic
KAFKA_BROKER="localhost:9092"  # Change this to your Kafka broker address
TASK_TOPIC="task_queue"          # Change this to your actual task topic name

WORKER_SCRIPT="../worker/worker.py"

# Check if the worker script exists
if [ ! -f "$WORKER_SCRIPT" ]; then
    echo "Error: $WORKER_SCRIPT script not found!"
    exit 1
fi

export PYTHONPATH=$(dirname "$(pwd)"):$PYTHONPATH

# Run the worker script
echo "Starting the worker to consume tasks from ${TASK_TOPIC}..."
python3 -m worker.worker --broker "${KAFKA_BROKER}" --topic "${TASK_TOPIC}"

# Optional: Deactivate the virtual environment if it was activated
# deactivate

echo "Worker has stopped."

