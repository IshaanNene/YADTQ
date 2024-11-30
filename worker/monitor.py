import time
import logging
import json
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkerMonitor:
    def __init__(self, kafka_broker, heartbeat_topic, worker_timeout=30):
        """
        Initialize the WorkerMonitor.

        Parameters:
        - kafka_broker: The Kafka broker address.
        - heartbeat_topic: The topic to consume heartbeat messages from.
        - worker_timeout: Time (in seconds) after which a worker is considered offline if no heartbeat is received.
        """
        self.consumer = KafkaConsumer(
            heartbeat_topic,
            bootstrap_servers=[kafka_broker],
            group_id="monitor_group",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
        )
        self.worker_status = {}
        self.worker_timeout = worker_timeout
        logger.info(f"Monitor initialized and listening on topic: {heartbeat_topic}")

    def update_worker_status(self, worker_id, timestamp):
        """
        Update the status of a worker based on the received heartbeat.

        Parameters:
        - worker_id: The unique identifier of the worker.
        - timestamp: The timestamp of the received heartbeat.
        """
        self.worker_status[worker_id] = timestamp
        logger.info(f"Updated status for worker {worker_id}: Last seen at {timestamp}")

    def check_worker_health(self):
        """
        Periodically check the health of workers and log offline workers.
        """
        current_time = int(time.time())
        for worker_id, last_seen in list(self.worker_status.items()):
            if current_time - last_seen > self.worker_timeout:
                logger.warning(f"Worker {worker_id} is offline (last seen {current_time - last_seen} seconds ago).")
                del self.worker_status[worker_id]  # Optionally remove the offline worker

    def start(self):
        """
        Start monitoring workers by consuming heartbeat messages.
        """
        logger.info("WorkerMonitor started.")
        while True:
            # Process incoming heartbeats
            for message in self.consumer:
                heartbeat = message.value
                worker_id = heartbeat.get("worker_id")
                timestamp = heartbeat.get("timestamp")

                if worker_id and timestamp:
                    self.update_worker_status(worker_id, timestamp)
                else:
                    logger.error(f"Invalid heartbeat received: {heartbeat}")

            # Check worker health periodically
            self.check_worker_health()
            time.sleep(5)  # Adjust this sleep interval as needed


# Example usage of the WorkerMonitor class
if __name__ == "__main__":
    kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
    heartbeat_topic = 'worker_heartbeat'  # Replace with your heartbeat topic name

    monitor = WorkerMonitor(kafka_broker, heartbeat_topic)

    try:
        monitor.start()
    except KeyboardInterrupt:
        logger.info("WorkerMonitor stopped.")

