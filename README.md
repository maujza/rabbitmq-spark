# RabbitMQ-Spark Connector

## Description

RabbitMQ-Spark Connector is an open-source project designed to enable seamless integration between RabbitMQ and Apache Spark using Spark's V2 Data Source API. This connector allows Spark to read data from RabbitMQ queues.

## Important Notes

- **Offsets and Re-Read**: Apache Spark's V2 Data Source API expects data sources to have offsets for re-reading data. However, RabbitMQ does not natively support this feature. As a workaround, we are using dummy offset management. This means re-reading data is currently not supported.

- **Exchanges**: As of now, the connector doesn't support consuming messages from RabbitMQ exchanges. However, this feature is on our roadmap and will be added in future releases.

- **Caching Connections**: To improve resource handling, we are using a caching mechanism for RabbitMQ connections.

- **Micro-Batch Support**: At present, the connector only supports micro-batching in Spark.

## Requirements

- Java 8 or higher
- Apache Spark 3.x
- RabbitMQ Server

## Installation & Usage

### Build the Project

To build the project, run the following command:

```
./gradlew build
```

### Run the Project

Include the JAR in your Spark project and use it like any other Spark DataSource.

## Contribution

Feel free to open issues or pull requests to improve this project.

## License

This project is licensed under the terms of the Apache 2.0 license. See `LICENSE.txt` for more details.

## Support

For support or issues, please open an issue on GitHub or reach out via email.


---

Happy coding!
