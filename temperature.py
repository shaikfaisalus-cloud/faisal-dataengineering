"""Send random temperature events to Azure Event Hub."""

import json
import logging
import os
import random
import time
from urllib.parse import urlparse

from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError

EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "temperature-eh")
CONNECTION_STRING_ENV = "EVENTHUB_CONNECTION_STRING"
BATCH_SIZE = 50
SEND_INTERVAL_SECONDS = 1

cities = {
    "New York": (40.7128, -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298),
    "Houston": (29.7604, -95.3698),
    "Phoenix": (33.4484, -112.0740),
    "Philadelphia": (39.9526, -75.1652),
    "San Antonio": (29.4241, -98.4936),
    "San Diego": (32.7157, -117.1611),
    "Dallas": (32.7767, -96.7970),
    "San Jose": (37.3382, -121.8863),
}


def get_eventhub_connection_string() -> str:
    """Read and validate the Event Hub connection string from environment."""
    connection_string = os.getenv(CONNECTION_STRING_ENV, "").strip().strip("'\"")

    if not connection_string:
        raise ValueError(
            f"{CONNECTION_STRING_ENV} is not set. Export the full Azure Event Hub "
            "connection string before running this script."
        )

    parts = dict(
        item.split("=", 1)
        for item in connection_string.split(";")
        if "=" in item
    )

    endpoint = parts.get("Endpoint", "").strip()
    parsed_endpoint = urlparse(endpoint)

    if parsed_endpoint.scheme != "sb" or not parsed_endpoint.hostname:
        raise ValueError(
            f"{CONNECTION_STRING_ENV} is invalid. It must include an Endpoint like "
            "'Endpoint=sb://<namespace>.servicebus.windows.net/;...'"
        )

    return connection_string


def build_payload() -> dict[str, object]:
    """Create one random temperature payload."""
    city = random.choice(list(cities))
    latitude, longitude = cities[city]

    return {
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "temperature": round(random.uniform(1, 15), 2),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def main() -> None:
    """Continuously publish temperature events to Azure Event Hub."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    connection_string = get_eventhub_connection_string()

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=EVENT_HUB_NAME,
    )

    try:
        with producer:
            while True:
                batch = producer.create_batch()

                for _ in range(BATCH_SIZE):
                    payload = build_payload()
                    batch.add(EventData(json.dumps(payload)))
                    logging.info("Queued event for city=%s", payload["city"])

                producer.send_batch(batch)
                logging.info("Sent batch with %s events", BATCH_SIZE)
                time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("Stopped.")
    except EventHubError as exc:
        logging.error("Azure Event Hub error while sending events: %s", exc)
        raise


if __name__ == "__main__":
    main()