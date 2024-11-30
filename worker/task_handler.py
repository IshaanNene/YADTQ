import logging
from .utils import validate_task, serialize_to_json, deserialize_from_json, handle_error

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskHandler:
    def __init__(self):
        """Initialize the TaskHandler."""
        logger.info("TaskHandler initialized.")

    def handle_task(self, task):
        """
        Handle a task by validating and executing it.

        Parameters:
        - task: A dictionary representing the task.

        Returns:
        - dict: A dictionary containing the result of the task execution.
        """
        if not validate_task(task):
            handle_error("Invalid task.")
            return {"status": "error", "message": "Invalid task."}

        task_id = task['task-id']
        task_name = task['task']
        args = task['args']

        logger.info(f"Handling task: {task_id} - {task_name} with args: {args}")

        try:
            # Execute the task based on its name
            if task_name == "add":
                result = self.add(*args)
                logger.info(f"Task {task_id} completed successfully with result: {result}")
                return {"status": "success", "task-id": task_id, "result": result}
            elif task_name == "subtract":
                result = self.subtract(*args)
                logger.info(f"Task {task_id} completed successfully with result: {result}")
                return {"status": "success", "task-id": task_id, "result": result}

            elif task_name == "multiply":
                result = self.multiply(*args)
                logger.info(f"Task {task_id} completed successfully with result: {result}")
                return {"status": "success", "task-id": task_id, "result": result}
            elif task_name == "divide":
            
                result = self.divide(*args)
                #logger.info("\nTHIS IS THE RESULTTTTT,\n",result)
                if(result!="Can't divide by 0"):
                    logger.info(f"Task {task_id} completed successfully with result: {result}")
                    return {"status": "success", "task-id": task_id, "result": result}
                else:
                    logger.info(f"Task {task_id} faced an error!")
                    return {"status": "failure", "task-id": task_id, "result": result}
                

            else:
                result = "Task not defined in worker"
                #handle_error(f"Unknown task: {task_name}")
                return {"status": "failure", "result": "Unknown task"}

            
            
        except Exception as e:
            #handle_error(f"Error executing task {task_id}: {e}")
            result = "Task not defined in worker"
            return {"status": "failure", "result": "Unknown task."}
            #return {"status": "FAILUREEEEEEEEEEEEEE", "message": str(e)}

    def add(self, *args):
        """Add numbers."""
        return sum(args)

    def subtract(self, *args):
        """Subtract numbers."""
        if len(args) < 2:
            raise ValueError("Subtract requires at least two arguments.")
        return args[0] - sum(args[1:])
    def multiply(self, *args):
        """Multiply numbers."""
        result = 1
        for arg in args:
            result *= arg
        return result

    def divide(self, *args):
        """Divide numbers."""
        if len(args) < 2:
            raise ValueError("Divide requires at least two arguments.")
        result = args[0]
        for arg in args[1:]:
            if arg == 0:
                #raise ValueError("Cannot divide by zero.")
                result = "Can't divide by 0"
                return result
            else:
                result /= arg
        return result
# Example usage of TaskHandler
if __name__ == "__main__":
    handler = TaskHandler()

    # Sample task for addition
    sample_task_add = {
        "task-id": "add-task-1",
        "task": "add",
        "args": [10, 5, 2]
    }

    # Sample task for subtraction
    sample_task_subtract = {
        "task-id": "subtract-task-1",
        "task": "subtract",
        "args": [10, 5, 2]
    }
    sample_task_multiply = {
        "task-id": "multiply-task-1",
        "task": "multiply",
        "args": [10, 5, 2]
    }

    # Sample task for division
    sample_task_divide = {
        "task-id": "divide-task-1",
        "task": "divide",
        "args": [10, 5, 2]
    }

    # Handling tasks
    result_add = handler.handle_task(sample_task_add)
    print(f"Result of addition task: {result_add}")

    result_subtract = handler.handle_task(sample_task_subtract)
    print(f"Result of subtraction task: {result_subtract}")

    result_multiply = handler.handle_task(sample_task_multiply)
    print(f"Result of multiplication task: {result_multiply}")

    # Handling division task
    result_divide = handler.handle_task(sample_task_divide)
    print(f"Result of division task: {result_divide}")
