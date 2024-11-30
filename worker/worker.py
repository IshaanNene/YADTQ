'''
import time
import logging
import json
from kafka import KafkaConsumer, KafkaProducer
from worker.task_handler import TaskHandler
from core.result_backend import ResultBackend
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from threading import Thread

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topic(broker, topic_name, num_partitions=3, replication_factor=1):
    """
    Create a Kafka topic programmatically.

    Parameters:
    - broker: The Kafka broker address (e.g., 'localhost:9092').
    - topic_name: Name of the topic to create.
    - num_partitions: Number of partitions for the topic.
    - replication_factor: Replication factor for the topic (usually 1 for local development).
    """
    admin_client = KafkaAdminClient(bootstrap_servers=[broker])

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )

    try:
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created with {num_partitions} partitions.")
    except TopicAlreadyExistsError:
        logger.warning(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create topic '{topic_name}': {e}")
    finally:
        admin_client.close()


class Worker:
    def __init__(self, kafka_broker, task_topic, result_backend, heartbeat_topic, worker_id):
        """
        Initialize the Worker.

        Parameters:
        - kafka_broker: The Kafka broker address.
        - task_topic: The topic from which to consume tasks.
        - result_backend: Backend system for storing results (e.g., Redis).
        - heartbeat_topic: Topic for sending heartbeat messages.
        - worker_id: Unique identifier for this worker.
        """
        self.consumer = KafkaConsumer(
            task_topic,
            bootstrap_servers=[kafka_broker],
            group_id="worker_group",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        self.task_handler = TaskHandler()
        self.result_backend = result_backend
        self.heartbeat_topic = heartbeat_topic
        self.worker_id = worker_id
        self.running = True
        self.current_partition = None  # To store the current partition being processed

        logger.info(f"Worker initialized and listening on topic: {task_topic}")

    def start(self):
        """Start processing tasks from the Kafka topic and sending heartbeats."""
        logger.info("Worker started.")

        # Start the heartbeat thread
        Thread(target=self.send_heartbeat, daemon=True).start()
        print("HELLO IDIOT")
        # Process tasks from the task topic
        for message in self.consumer:
            self.current_partition = message.partition  # Get the partition information
            task = message.value
            print("HELLO IDIOT")
            print(f"Consumed message from partition: {self.current_partition}")
            logger.info(f"Received task from partition {self.current_partition}: {task}")
            result = self.task_handler.handle_task(task)
            self.store_result(task['task-id'], result)

    def send_heartbeat(self):
        """Send periodic heartbeat messages to the heartbeat topic."""
        while self.running:
            heartbeat_message = {
                "worker_id": self.worker_id,
                "status": "alive",
                "timestamp": int(time.time()),
                "last_task_partition": self.current_partition,  # Include last partition info
            }

            try:
                self.producer.send(self.heartbeat_topic, heartbeat_message)
                self.producer.flush()
                logger.info(f"Heartbeat sent: {heartbeat_message}")  # Print heartbeat with partition info
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")
            time.sleep(10)  # Send heartbeat every 10 seconds

    def store_result(self, task_id, result):
        """Store the result of the task in the backend (e.g., Redis)."""
        logger.info(f"Storing result for task {task_id}: {result}")
        self.result_backend.store_result(task_id, result['status'], result['result'])

    def stop(self):
        """Stop the worker and clean up resources."""
        self.running = False
        self.consumer.close()
        self.producer.close()
        logger.info("Worker stopped.")


# Example usage of the Worker class
if __name__ == "__main__":
    kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
    task_topic = 'task_queue'        # Replace with your actual task topic name
    heartbeat_topic = 'worker_heartbeat'  # Replace with your heartbeat topic name
    worker_id = 'worker-1'           # Unique identifier for the worker

    # Create necessary topics
    create_topic(kafka_broker, task_topic, num_partitions=3, replication_factor=1)
    create_topic(kafka_broker, heartbeat_topic, num_partitions=3, replication_factor=1)

    result_backend = ResultBackend()
    worker = Worker(kafka_broker, task_topic, result_backend, heartbeat_topic, worker_id)

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()

'''



import time
import logging
import json
import redis  # Import Redis library
from kafka import KafkaConsumer, KafkaProducer
from worker.task_handler import TaskHandler
from core.result_backend import ResultBackend
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from threading import Thread

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topic(broker, topic_name, num_partitions=3, replication_factor=1):
    """
    Create a Kafka topic programmatically.

    Parameters:
    - broker: The Kafka broker address (e.g., 'localhost:9092').
    - topic_name: Name of the topic to create.
    - num_partitions: Number of partitions for the topic.
    - replication_factor: Replication factor for the topic (usually 1 for local development).
    """
    admin_client = KafkaAdminClient(bootstrap_servers=[broker])

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )

    try:
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created with {num_partitions} partitions.")
    except TopicAlreadyExistsError:
        logger.warning(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create topic '{topic_name}': {e}")
    finally:
        admin_client.close()


class Worker:
    def __init__(self, kafka_broker, task_topic, result_backend, heartbeat_topic, worker_id, redis_host='localhost', redis_port=6379):
        """
        Initialize the Worker.

        Parameters:
        - kafka_broker: The Kafka broker address.
        - task_topic: The topic from which to consume tasks.
        - result_backend: Backend system for storing results (e.g., Redis).
        - heartbeat_topic: Topic for sending heartbeat messages.
        - worker_id: Unique identifier for this worker.
        - redis_host: Redis host (default is localhost).
        - redis_port: Redis port (default is 6379).
        """
        self.consumer = KafkaConsumer(
            task_topic,
            bootstrap_servers=[kafka_broker],
            group_id="worker_group",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        self.task_handler = TaskHandler()
        self.result_backend = result_backend
        self.heartbeat_topic = heartbeat_topic
        self.worker_id = worker_id
        self.running = True
        self.current_partition = None  # To store the current partition being processed

        # Connect to Redis
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=1, decode_responses=True)

        logger.info(f"Worker initialized and listening on topic: {task_topic}")

    def start(self):
        """Start processing tasks from the Kafka topic and sending heartbeats."""
        logger.info("Worker started.")

        # Start the heartbeat thread
        Thread(target=self.send_heartbeat, daemon=True).start()

        # Process tasks from the task topic
        for message in self.consumer:
            self.current_partition = message.partition  # Get the partition information
            task = message.value
            logger.info(f"Consumed message from partition: {self.current_partition}")
            logger.info(f"Received task from partition {self.current_partition}: {task}")
            result = self.task_handler.handle_task(task)
            logger.info("GURL I AM RESULT,",result)
            self.store_result(task['task-id'], result)

    def send_heartbeat(self):
        """Send periodic heartbeat messages to the heartbeat topic and store them in Redis."""
        while self.running:
            heartbeat_message = {
                "status": "I AM ALIVE BRO",
                "timestamp": int(time.time()),
                "last_task_partition": self.current_partition,  # Include last partition info
            }

      

            try:
                # Send heartbeat to Kafka topic
                self.producer.send(self.heartbeat_topic, heartbeat_message)
                self.producer.flush()
                logger.info(f"Heartbeat sent: {heartbeat_message}")
                
                # Store heartbeat in Redis
                self.store_heartbeat_in_redis(heartbeat_message)

            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")
            time.sleep(10)  # Send heartbeat every 10 seconds

    def store_heartbeat_in_redis(self, heartbeat_message):
        """Store the heartbeat message in Redis."""
        '''
        redis_key = f"worker_heartbeat:{self.worker_id}"
        self.redis_client.set(redis_key, json.dumps(heartbeat_message))
        logger.info(f"Heartbeat stored in Redis for worker {self.worker_id}: {heartbeat_message}")
        '''
        
        
        partition = self.current_partition if self.current_partition is not None else "unknown"
        timestamp = heartbeat_message["timestamp"]
        redis_key = f"partition:{partition}:timestamp:{timestamp}"

    # Store the heartbeat message
        self.redis_client.set(redis_key, json.dumps(heartbeat_message))
        logger.info(f"Heartbeat stored in Redis with key {redis_key}: {heartbeat_message}")
    
        
    

    def store_result(self, task_id, result):
        """Store the result of the task in the backend (e.g., Redis)."""
        logger.info(f"Storing result for task {task_id}: {result}")
        self.result_backend.store_result(task_id, result['status'], result['result'])

    def stop(self):
        """Stop the worker and clean up resources."""
        self.running = False
        self.consumer.close()
        self.producer.close()
        self.redis_client.close()
        logger.info("Worker stopped.")


# Example usage of the Worker class
if __name__ == "__main__":
    kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
    task_topic = 'task_queue1'        # Replace with your actual task topic name
    heartbeat_topic = 'worker_heartbeat'  # Replace with your heartbeat topic name
    worker_id = 'worker-1'           # Unique identifier for the worker

    # Create necessary topics
    create_topic(kafka_broker, task_topic, num_partitions=3, replication_factor=1)
    create_topic(kafka_broker, heartbeat_topic, num_partitions=3, replication_factor=1)

    result_backend = ResultBackend()
    worker = Worker(kafka_broker, task_topic, result_backend, heartbeat_topic, worker_id)

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()

