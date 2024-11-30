#!/bin/bash

# Set the Kafka broker address and topic names
KAFKA_BROKER="localhost:9092"  # Change this to your Kafka broker address
TASK_TOPIC="task_queue"         # Change this to your actual task topic name

# Check if sample_tasks.json exists
if [ ! -f "sample_tasks.json" ]; then
    echo "Error: sample_tasks.json file not found!"
    exit 1
fi

# Activate the Python virtual environment if needed
# source /path/to/your/venv/bin/activate

# Set PYTHONPATH to include the root directory of the project (parent directory of client and core)
export PYTHONPATH=$(dirname "$(pwd)"):$PYTHONPATH

# Submit tasks via python3 -m
python3 - <<EOF
import json
import time
from client.client import Client  # Ensure the client module is imported correctly

# Initialize the client
client = Client()

# Load tasks from the JSON file
with open('sample_tasks.json', 'r') as f:
    tasks = json.load(f)

submitted_task_ids = []  # List to store submitted task IDs

# Submit each task
for task in tasks:
    task_name = task.get("task")
    args = task.get("args")
    if not task_name or args is None:
        print(f"Skipping invalid task: {task}")
        continue
    task_id = client.submit_task(task_name, args)
    print(f"Submitted task {task_name} with ID: {task_id}")
    submitted_task_ids.append(task_id)  # Store the task ID

print("\nFetching task results...")

# Fetch task results
all_results = {}
while submitted_task_ids:
    for task_id in submitted_task_ids[:]:  # Iterate over a copy of the list
        result = client.query_status(task_id)  # Fetch task result
        if result is None:  # Skip tasks with no results
            print(f"Task {task_id}: Result not available yet...")
            continue

        status = result.get("status")
        processing_status = "processing"
        if status in ["success", "failure"]:
            print(f"Task {task_id}: status: {processing_status}. Result: Calculating...")
            time.sleep(2)
            print(f"Task {task_id}: {status}. Result: {result.get('result')}")
            all_results[task_id] = result
            submitted_task_ids.remove(task_id)  # Remove completed task from list
        else:
            print(f"Task {task_id}: status: {processing_status}. Result: Calculating...")
            time.sleep(2)
            print(f"Task {task_id}: {status}...")  # "queued" or "processing"
        #print("Processing task")
        #time.sleep(2)
    time.sleep(2)  # Polling interval

print("\nFinal Results:")
print(json.dumps(all_results, indent=4))
EOF

echo "All tasks have been submitted."

