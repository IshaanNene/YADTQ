import redis
import json
import logging
from core.config.development import REDIS_URL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResultBackend:
    def __init__(self):
        # Initialize Redis connection
        self.client = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

    def store_result(self, task_id, status, result=None):
        """
        Store the result of a task in Redis.

        Parameters:
        - task_id: Unique identifier for the task.
        - status: Status of the task (e.g., "queued", "processing", "success", "failed").
        - result: The result of the task (optional).
        """
        data = {
            "status": status,
            "result": result
        }
        try:
            self.client.set(task_id, json.dumps(data))  # Store data as JSON
            logger.info(f"Stored result for task {task_id}: {data}")
        except Exception as e:
            logger.error(f"Failed to store result for task {task_id}: {e}")
            print("ERRORRRRRRRR")

    def get_result(self, task_id):
        """
        Retrieve the result of a task from Redis.

        Parameters:
        - task_id: Unique identifier for the task.

        Returns:
        - A dictionary containing the task status and result.
        """
        try:
            result = self.client.get(task_id)
            if result is None:
                return {"status": "not_found", "result": None}  # Task not found
            
            data = json.loads(result)  # Deserialize JSON
            #logger.info(f"Retrieved result for task {task_id}: {data}")
            return data
        except Exception as e:
            logger.error(f"Failed to retrieve result for task {task_id}: {e}")
            return {"status": "error", "result": str(e)}

    def store_task(self, task_id, task):
        """
        Store the original task details in Redis.

        Parameters:
        - task_id: Unique identifier for the task.
        - task: The original task details (e.g., task name and arguments).
        """
        try:
            self.client.set(f"task:{task_id}", json.dumps(task))  # Use a prefix for task storage
            logger.info(f"Stored task for task ID {task_id}: {task}")
        except Exception as e:
            logger.error(f"Failed to store task for task ID {task_id}: {e}")

    def get_task(self, task_id):
        """
        Retrieve the original task details from Redis.

        Parameters:
        - task_id: Unique identifier for the task.

        Returns:
        - A dictionary containing the original task details.
        """
        try:
            task = self.client.get(f"task:{task_id}")  # Use the same prefix
            if task is None:
                logger.warning(f"Task {task_id} not found.")
                return None

            task_data = json.loads(task)
            #logger.info(f"Retrieved task for task ID {task_id}: {task_data}")
            return task_data
        except Exception as e:
            logger.error(f"Failed to retrieve task for task ID {task_id}: {e}")
            return None

        
    def close(self):
        """Close the Redis connection."""
        self.client.close()
        logger.info("Redis connection closed.")

if __name__ == "__main__":
    # Example usage of ResultBackend
    result_backend = ResultBackend()
    
    # Store a test result
    task_id = "test-task-id"
    result_backend.store_result(task_id, "success", {"sum": 3})

    # Retrieve the test result
    result = result_backend.get_result(task_id)
    print(f"Retrieved result: {result}")


    # Close the backend
    result_backend.close()


































