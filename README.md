# RabbitMQ Streams - Spark Connector

## Description

RabbitMQ Streams - Spark Connector is a project designed to enable seamless integration between RabbitMQ Streams and Apache Spark using Spark's V2 Data Source API. This connector allows Spark to read data from RabbitMQ streams using their Java client.

## Important Notes

- **Offset Handling**: Apache Spark's V2 Data Source API expects data sources to have offsets for re-reading data. Handling was added without server manual/automatic tracking offered by RabbitMQ Stream API.

- **Super Streams**: As of now, the connector doesn't support consuming messages from RabbitMQ super streams.

- **Caching Environments**: To improve resource handling, we are using a caching mechanism for RabbitMQ stream environments.

- **Micro-Batch Support**: At present, the connector only supports micro-batching in Spark.

- **Write Support**: For now, the connector only reads from RabbitMQ streams and cannot write (nor create) streams.

## Requirements

- Java 11 or higher
- Apache Spark 3.4
- RabbitMQ Server

## Installation & Usage

### Build the Project

To build the project, run the following command:

```
./gradlew build
```

### Run the Project

Include the JAR in your Spark project and use it like any other Spark DataSource. For this, run the `shadowJar` task in Gradle.

## Configuration Options

The RabbitMQ Spark Connector can be configured using the following options:

- **stream**: Specifies the name of the RabbitMQ stream to read from.

- **queueCapacity**: Sets the capacity of the internal queue for buffering messages.

- **username**: The username for authenticating with RabbitMQ.

- **password**: The password for authenticating with RabbitMQ.

- **host**: The RabbitMQ server's hostname or IP address.

- **port**: The port number on which RabbitMQ is listening.

- **vhost**: The RabbitMQ virtual host for logical separation of environments.

- **uri**: The connection URI for RabbitMQ streams, formatted as `rabbitmq-stream://%s:%s@%s:%s/%s` where placeholders represent username, password, host, port, and vhost (encoded) respectively.

- **spark.rabbitmq_stream.keep_alive_ms**: Sets the keep-alive interval in milliseconds for maintaining the connection (this should be a system property)

- **checkpointLocation**: Specifies the location for storing Spark's checkpoint data.

## Key Components

### 1. Connection Management

- LazyRabbitMQStreamEnvironmentCache & RabbitMQStreamEnvironmentCache: Manage the caching and reuse of RabbitMQ connections, ensuring efficient resource utilization.

### 2. Data Reading

- RabbitMQStreamMicroBatchPartitionReader & Factory: Handle reading of messages from RabbitMQ in micro-batches, aligning with Spark's structured streaming model.

- RabbitMQStreamMicroBatchStream: Manages data streams from RabbitMQ, handling partitioning and reader creation.

### 3. Data Conversion

- RabbitMQMessageToRowConverter: Converts RabbitMQ messages into Spark's `Row` format for processing within Spark jobs.

### 4. Offset Management

- RabbitMQStreamOffsetStore & RabbitMQStreamTimestampOffset: Manage message offsets and timestamps to ensure messages are processed in sequence and not duplicated.

### 5. Table and Scan Management

- RabbitMQTable & RabbitMQTableProvider: Represent RabbitMQ streams as Spark tables.

- RabbitMQStreamScan & RabbitMQStreamScanBuilder: Facilitate the creation and execution of scans on RabbitMQ streams to retrieve data.

### 6. Acknowledgment Logic

The RabbitMQ Spark Connector implements an acknowledgment (ack) logic to ensure reliable message processing and to prevent message loss or duplication.

- Reading and Processing: Messages are read using a queue and processed in micro-batches.

- Acknowledgment: Once a message is successfully processed, an acknowledgment is sent back to RabbitMQ, confirming the receipt and processing of the message.

- RabbitMQ Client Interaction: The acknowledgment is typically performed using the RabbitMQ Java client API, sending an `ack` for each processed message, ensuring that the broker knows the message has been handled correctly.

## Spark DataSource V2 API Usage

The RabbitMQ Spark Connector is designed to work with the Spark DataSource V2 API.

### Key Aspects of DataSource V2 Integration

- Structured Streaming: The connector aligns with Spark's structured streaming capabilities, providing micro-batch processing.

- Partitioned Data Access: Implements partitioning logic to read data efficiently, allowing Spark to parallelize processing and enhance throughput.

- Scan and Read Support: The connector supports defining scans and reading operations.

## How It Works

### Default Schema

The connector uses a default schema for messages if none is specified. The schema includes:

- message (String): The content of the RabbitMQ message.

- offset (long): The position of the message within the stream.

### Connecting to RabbitMQ

The connector establishes connections to RabbitMQ using a caching mechanism to optimize performance and resource usage.

#### Caching Logic

- Lazy Initialization: Connections are established only when required, preventing unnecessary resource consumption.

- Environment-based Caching: Connections are cached with environment-specific keys for reuse across various streams.

- Connection Reuse: Cached connections are reused for subsequent data reads, avoiding repeated setup and teardown.

#### Environment Configurations

The connector supports customizable configurations, including:

- RabbitMQ Host: Specifies the server address.

- Port: The port on which RabbitMQ is listening.

- Queue Name: Identifies the RabbitMQ queue for message reading.

- Credentials: Provides authentication details for secure access.

- Virtual Host: Uses RabbitMQ's virtual hosts for environment separation.

### Reading Messages

The connector's reading mechanism efficiently processes data from RabbitMQ.

- Single Partition Reading: Processes data from one partition at a time to preserve message order.

- Micro-Batch Processing: Aligns with Spark's structured streaming, processing data in small chunks.

- BlockingQueue: Buffers messages between the RabbitMQ consumer and Spark, handling rate variations.

### Converting Messages

Messages must be converted into a Spark-compatible format.

- RabbitMQMessageToRowConverter: Extracts fields from RabbitMQ messages and constructs Spark `Row` objects.

### Managing Offsets

Offsets track the last processed message in the RabbitMQ stream.

- Offset Storage: Maintains offsets using classes like `RabbitMQStreamOffsetStore`.

- Timestamp-based Offsets: Supports timestamp-based offsets for time-dependent message order.

- Failure Recovery: Allows resuming processing from the last known position.

### Providing Data as a Dataset

Once messages are processed, the connector provides them as a Spark dataset.

- Table Abstraction: Represents RabbitMQ streams as tables (RabbitMQTable), enabling structured data access.

- Dataset Integration: Supports Spark's DataFrame and Dataset APIs for data manipulation.

- Scalability: Handles large data volumes efficiently.

## Support

For support or issues, please open an issue on this repo.
