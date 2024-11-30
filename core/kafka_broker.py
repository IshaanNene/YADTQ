from kafka import KafkaProducer, KafkaConsumer
import json
import logging
from core.config.development import KAFKA_BROKER_URL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaBroker:
    def __init__(self):
        # Initialize the Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON
        )
        
        # Initialize the Kafka consumer for task queue
        self.task_consumer = KafkaConsumer(
            'task_queue1',  # Topic to consume from
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize JSON
            auto_offset_reset='earliest',  # Start from the earliest message
            group_id='task_consumers',  # Consumer group
            enable_auto_commit=True  # Automatically commit offsets
        )

        # Initialize the Kafka consumer for heartbeats
        self.heartbeat_consumer = KafkaConsumer(
            'worker_heartbeat',  # Topic for worker heartbeats
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize JSON
            auto_offset_reset='latest',  # Start from the latest message
            group_id='heartbeat_monitor',  # Consumer group for monitoring
            enable_auto_commit=True
        )

    def send_message(self, message):
        """
        Sends a message to the Kafka topic.

        Parameters:
        - message: The message to send (should be a dictionary).
        """
        try:
            self.producer.send('task_queue1', message)
            self.producer.flush()  # Ensure all messages are sent
            logger.info(f"Message sending to YATDQ: {message}")
        except Exception as e:
            logger.error(f"Failed to send message to YATDQ: {e}")

    def consume_task_messages(self):
        """
        Consumes task messages from the Kafka topic.

        Yields:
        - The consumed task message.
        """
        try:
            for message in self.task_consumer:
                logger.info(f"Task message receiving from YATDQ: {message.value}")
                yield message.value
        except Exception as e:
            logger.error(f"Failed to consume task messages from YATDQ: {e}")

    def consume_heartbeat_messages(self):
        """
        Consumes heartbeat messages from the Kafka topic.

        Yields:
        - The consumed heartbeat message.
        """
        try:
            for message in self.heartbeat_consumer:
                logger.info(f"Heartbeat message received: {message.value}")
                yield message.value
        except Exception as e:
            logger.error(f"Failed to consume heartbeat messages: {e}")

    def close(self):
        """Close the producer and consumer connections."""
        self.producer.close()
        self.task_consumer.close()
        self.heartbeat_consumer.close()
        logger.info("YATDQ connections closed.")

if __name__ == "__main__":
    # Example usage of KafkaBroker
    kafka_broker = KafkaBroker()
    
    # Send a test task message
    test_task_message = {
        "task-id": "test-task-id",
        "task": "add",
        "args": [1, 2]
    }
    kafka_broker.send_message(test_task_message)

    # Consume task and heartbeat messages
    try:
        # Start a consumer loop for both task and heartbeat messages
        for task_msg in kafka_broker.consume_task_messages():
            print(f"Processed task message: {task_msg}")

        # In a separate process/thread, consume heartbeat messages (example for testing)
        for heartbeat_msg in kafka_broker.consume_heartbeat_messages():
            print(f"Processed heartbeat message: {heartbeat_msg}")

    except KeyboardInterrupt:
        kafka_broker.close()

