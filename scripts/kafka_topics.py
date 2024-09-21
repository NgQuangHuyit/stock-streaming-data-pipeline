import argparse

from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def create_kafka_topic(kafka_servers: str, topics: list[str]):
    conf = {
        "bootstrap.servers": kafka_servers
    }
    admin_client = AdminClient(conf)
    logger.info(f"Creating topics: {topics}")
    new_topics = []
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = metadata.topics.keys()
    for topic in topics:
        if topic in existing_topics:
            logger.info(f"Topic {topic} already exists")
        else:
            new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
            new_topics.append(new_topic)
    if not new_topics:
        logger.info("No new topics to create")
        return
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"Topic {topic} created")
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")
