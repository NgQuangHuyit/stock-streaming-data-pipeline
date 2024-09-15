from confluent_kafka.admin import AdminClient, NewTopic

def create_kafka_topic(kafka_servers: str, topic: str, n_stocks: int=1, n_nodes=1):
    conf = {
        "bootstrap.servers": kafka_servers
    }
    admin_client = AdminClient(conf)
    if topic not in admin_client.list_topics().topics:
        topic_list = [NewTopic(topic, num_partitions=n_stocks, replication_factor=n_nodes)]
        fs = admin_client.create_topics(topic_list)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
    else:
        print(f"Topic {topic} is already created")

if __name__ == "__main__":
    create_kafka_topic("localhost:9092", "stock2", 1, 1)