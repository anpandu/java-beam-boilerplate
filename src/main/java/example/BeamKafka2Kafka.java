package example;

import com.google.common.collect.ImmutableMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BeamKafka2Kafka {

    static final String DIR_OUTPUT = "target/_output/wordcount";
    static final String KAFKA_HOST = "stde-dev:9092";
    static final String KAFKA_TOPIC = "mytopic1";
    static final String KAFKA_TOPIC_2 = "mytopic1a";
    static final String TOKENIZER_PATTERN = " +";
    static final Integer MAX_NUM_RECORDS = 100;

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply(
                KafkaIO.<String, String>read()
                    .withBootstrapServers(KAFKA_HOST)
                    .withTopic(KAFKA_TOPIC)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                    // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                    // the first MAX_NUM_RECORDS records.
                    // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                    .withMaxNumRecords(MAX_NUM_RECORDS)
                    .withoutMetadata() // PCollection<KV<Long, String>>
            )
            // Write to kafka
            .apply(
                KafkaIO.<String, String>write()
                    .withBootstrapServers(KAFKA_HOST)
                    .withTopic(KAFKA_TOPIC_2)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
            );

        pipeline.run().waitUntilFinish();
    }
}