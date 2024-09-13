create-kafka-topics:
	docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:19092 --create --topic test --partitions 1 --replication-factor 1

start-finnhub-producer:
	docker exec -it finnhub-producer python3 FinnhubProducer.py

start-streaming-app:
	docker exec -it streaming-app python3 StreamingApp.py