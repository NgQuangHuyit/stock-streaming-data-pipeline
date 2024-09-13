create-kafka-topics:
	docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:19092 --create --topic stock --partitions 1 --replication-factor 1

delete-kafka-topics:
	docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:19092 --delete --topic stock

recreate-kafka-topics: delete-kafka-topics create-kafka-topics

start-finnhub-producer:
	docker exec -it finnhub-producer python3 FinnhubProducer.py

start-streaming-app:
	docker exec -it spark-master pip install py4j
	docker exec -it spark-master python3 StreamingApp.py

up:
	docker-compose up -d

down:
	docker-compose down -v