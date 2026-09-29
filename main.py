import os
import json
import logging
from datetime import datetime, timezone, timedelta
from azure.eventhub import EventHubProducerClient, EventData, TransportType
 
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
# Best Practice: Use Environment Variables for secrets
CONNECTION_STRING = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("AZURE_EVENTHUB_NAME")
 
def send_test_messages():
    if not CONNECTION_STRING or not EVENT_HUB_NAME:
        logger.error("Missing Connection String or Event Hub Name in environment variables.")
        return
 
    # Best Practice: Use Context Manager (with statement) for auto-closing
    with EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME,
        transport_type=TransportType.AmqpOverWebsocket
    ) as producer:
       
        event_data_batch = producer.create_batch()
        current_device_id = "sensor_01"
       
        # Generate 5 records and add them to the batch
        for i in range(5):
            telemetry_data = {
                "device_id": current_device_id,
                # Use timedelta to simulate 1-second gaps efficiently
                "date_time": (datetime.now(timezone.utc) + timedelta(seconds=i)).isoformat(),
                "temperature": 22.5 + i
            }
           
            event = EventData(json.dumps(telemetry_data))
            event.partition_key = current_device_id
           
            # Add event to the batch
            event_data_batch.add(event)
            logger.info(f"Prepared: {telemetry_data}")
       
        # Send the single batch of 5 records
        producer.send_batch(event_data_batch)
        logger.info("Batch of 5 telemetry records sent successfully!")
 
if __name__ == "__main__":
    send_test_messages()