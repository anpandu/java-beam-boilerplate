Java Apache Beam Boilerplate
=========

This repo contains my old templates for Apache Beam for Java, more examples coming.

## Requirements
Requirements below need to be installed.
```
JDK 1.8
Scala 2.11.9
SBT 0.13.15
SBT Assembly 0.14.4
```

## Running Source Code

### Assembly

```sh
sbt assembly
java -cp ./target/scala-2.11/hello-assembly-1.0.jar example.Hola
```

### Running Kafka to BigQuery
Via local machine
```sh
GOOGLE_APPLICATION_CREDENTIALS=<json_key_file>
java -cp target/scala-2.11/hello-assembly-1.0.jar \
    example.BeamKafka2BigQuery \
    --kafkaHost="<host>:<port>" \
    --kafkaTopic="<topic>" \
    --project="<gcp_project>" \
    --BQTable="<gcp_project>:<bq_dataset>.<bq_table>" \
    --gcpTempLocation="gs://<path>" \
    --GSTempLocation="gs://<path>"
```

Via GCP DataFlow
```sh
GOOGLE_APPLICATION_CREDENTIALS=<json_key_file>
java -cp target/scala-2.11/hello-assembly-1.0.jar \
    example.BeamKafka2BigQuery \
    --kafkaHost="<host>:<port>" \
    --kafkaTopic="<topic>" \
    --project="<gcp_project>" \
    --BQTable="<gcp_project>:<bq_dataset>.<bq_table>" \
    --gcpTempLocation="gs://<path>" \
    --GSTempLocation="gs://<path>" \
    --serviceAccount="<your_service_account>" \
    --dataflowWorkerJar=hello-assembly-1.0.jar \
    --runner=DataflowRunner
```

### Running Kafka to Kafka
Via local machine
```sh
java -cp target/scala-2.11/hello-assembly-1.0.jar \
    example.BeamKafka2Kafka \
    --kafkaHost="<host>:<port>" \
    --kafkaTopicSource="<topic1>" \
    --kafkaTopicSink="<topic2>"
```

### Running Kafka Word Count
Via local machine
```sh
java -cp target/scala-2.11/hello-assembly-1.0.jar \
    example.BeamKafkaWordCount \
    --kafkaHost="<host>:<port>" \
    --kafkaTopic="<topic>"
```

## License

MIT © [Ananta Pandu](anpandumail@gmail.com)