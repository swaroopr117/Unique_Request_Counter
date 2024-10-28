High-Level Design: Use of in-memory map for deduplication, mutex for thread safety.
Concurrency: Ensures high request throughput.
Fault Tolerance: Resetting map after each logging.
Load Balancing: To achieve deduplication across instances, a distributed cache (e.g., Redis) could be used to manage IDs globally.
Extension 3: Use Kafka, RabbitMQ, or Redis Streams for distributed logging.