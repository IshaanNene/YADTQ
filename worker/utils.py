import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_task(task):
    """
    Validate the task dictionary.

    Parameters:
    - task: A dictionary representing the task.

    Returns:
    - bool: True if the task is valid, False otherwise.
    """
    if not isinstance(task, dict):
        logger.error("Task is not a dictionary.")
        return False

    required_keys = ['task-id', 'task', 'args']
    for key in required_keys:
        if key not in task:
            logger.error(f"Missing required key: {key}")
            return False

    if not isinstance(task['args'], list):
        logger.error("Task 'args' should be a list.")
        return False

    logger.info("Task is valid.")
    return True

def serialize_to_json(data):
    """
    Serialize data to JSON format.

    Parameters:
    - data: The data to serialize.

    Returns:
    - str: The JSON string representation of the data.
    """
    try:
        json_data = json.dumps(data)
        logger.info("Data serialized to JSON.")
        return json_data
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to serialize data to JSON: {e}")
        return None

def deserialize_from_json(json_data):
    """
    Deserialize JSON data to a Python object.

    Parameters:
    - json_data: The JSON string to deserialize.

    Returns:
    - dict or None: The deserialized Python object or None if failed.
    """
    try:
        data = json.loads(json_data)
        logger.info("Data deserialized from JSON.")
        return data
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to deserialize JSON data: {e}")
        return None

def handle_error(error_message):
    """
    Handle errors by logging them.

    Parameters:
    - error_message: The error message to log.
    """
    logger.error(error_message)

# Example usage of utility functions
if __name__ == "__main__":
    # Validate a sample task
    sample_task = {
        "task-id": "example-task-id",
        "task": "add",
        "args": [10, 20]
    }
    
    if validate_task(sample_task):
        serialized = serialize_to_json(sample_task)
        print(f"Serialized task: {serialized}")

        deserialized = deserialize_from_json(serialized)
        print(f"Deserialized task: {deserialized}")
    else:
        handle_error("Task validation failed.")