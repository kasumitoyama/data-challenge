# Reset offset to earliest

```
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group default-cg --reset-offsets --to-earliest --topic stream-topic --execute
```

# Describe a kafka topic

```
./bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic stream-topic
```

# Fill kafka topic (run from kafka dir)

```
cat ../doodle-data-challenge/data/stream.jsonl | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic stream-topic
```

# Check output of kafka topic (run from kafka dir)

```
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic --from-beginning --max-messages 100
```
