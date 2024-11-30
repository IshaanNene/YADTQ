import uuid
import json
from core.kafka_broker import KafkaBroker
from core.result_backend import ResultBackend
import time

class Client:
    def __init__(self):
        # Initialize Kafka broker and result backend
        self.kafka_broker = KafkaBroker()
        #self.result_backend = ResultBackend()
        self.result_backend = ResultBackend()


    def submit_task(self, task_name, args):
        """
        Submits a task to the Kafka queue.

        Parameters:
        - task_name: The name of the task to be executed (e.g., "add", "subtract").
        - args: A list of arguments to be passed to the task.

        Returns:
        - task_id: A unique identifier for the submitted task.
        """
        # Generate a unique ID for the task
        task_id = str(uuid.uuid4())
        
        # Create the task dictionary
        task = {
            "task-id": task_id,
            "task": task_name,
            "args": args
        }
        
        
        self.result_backend.store_task(task_id, task)
        
        # Send the task to the Kafka broker
        self.kafka_broker.send_message(task)
        
        # Store the initial status in the result backend
        self.result_backend.store_result(task_id, "queued")
        
        return task_id

    '''def query_status(self, task_id):
        """
        Queries the status of a previously submitted task.

        Parameters:
        - task_id: The unique identifier of the task to check.

        Returns:
        - A dictionary containing the task status and result (if available).
        """
        # Retrieve the result from the result backend
        result = self.result_backend.get_result(task_id)
        
        # Check if the result is available
        if result['status'] == "success":
            return {
                "status": "success",
                "result": result['result']
            }
        elif result['status'] == "failed":
            return {
                "status": "failed",
                "error": result.get('result', 'Unknown error occurred.')
            }
        else:
            return {
                "status": result['status']  # either "queued" or "processing"
            }'''
            
    def query_status(self, task_id, timeout=3):
        #print("HELLO WORLD")
        start_time = time.time()
        while True:
            result = self.result_backend.get_result(task_id)
            status = result.get("status")
            print()
            if status =="success":
                return {
                    "status": status,
                    "result": result.get("result"),
                }
            #elif status == "failed":
                #print(f"Task {task_id} failed. Retrying...")
                #self.result_backend.store_result(task_id, "failed")
                #self.resubmit_task(task_id)
            elif status == "failure":
                if result.get("result") == "Can't divide by 0":
                    print("Error! Can't divide by 0 !")
                    return {
                        "status": status,
                        "result": result.get("result"),
                    }
                    
                else:
                    return {
                        "status": status,
                        "result": result.get("result"),
                    }
            elif time.time() - start_time > timeout:
                print(f"Task {task_id} timed out. Retrying...")
                self.resubmit_task(task_id)
            else:
                time.sleep(1)  # Poll every second

    def resubmit_task(self, task_id):
        task = self.result_backend.get_task(task_id)
        #print("\nGIRL LOOK AT ME. IM TASK = ",task)
        if task:
            #logger.info(f"Resubmitting task: {task}")
            #print("IM RE RUNNING")
            print()
            print(f"Resubmitting task: {task}")
            #self.result_backend.store_result(task_id, "queued")
            self.kafka_broker.send_message(task)
            self.result_backend.store_result(task_id, "queued")
        else:
            print(f"Task {task_id} could not be retrieved for retry.")
            #logger.error(f"Cannot resubmit. Task {task_id} not found in ResultBackend.")

if __name__ == "__main__":
    client = Client()
    
    # Example usage
    task_id = client.submit_task("add", [1, 2])
    print(f"Task submitted with ID: {task_id}")
    
    # Simulate querying the status after a delay
    import time
    time.sleep(2)  # Wait for a bit before querying status
    status = client.query_status(task_id)
    print(f"Task status: {status}")
