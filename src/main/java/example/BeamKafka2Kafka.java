package example;

import com.google.common.collect.ImmutableMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BeamKafka2Kafka {

    public interface TemplateOptions extends PipelineOptions {

        @Description("Kafka host.")
        @Default.String("myhost:9092")
        String getKafkaHost();
        void setKafkaHost(String value);

        @Description("Kafka topic source.")
        @Default.String("mytopic1")
        String getKafkaTopicSource();
        void setKafkaTopicSource(String value);

        @Description("Kafka topic sink.")
        @Default.String("mytopic2")
        String getKafkaTopicSink();
        void setKafkaTopicSink(String value);
    }

    public static void main(String[] args) {

        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);

        final String KAFKA_HOST = options.getKafkaHost();
        final String KAFKA_TOPIC_SOURCE = options.getKafkaTopicSource();
        final String KAFKA_TOPIC_SINK = options.getKafkaTopicSink();
        final Integer MAX_NUM_RECORDS = 5;

        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply(
                KafkaIO.<String, String>read()
                    .withBootstrapServers(KAFKA_HOST)
                    .withTopic(KAFKA_TOPIC_SOURCE)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", "earliest"))
                    // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                    // the first MAX_NUM_RECORDS records.
                    // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                    // .withMaxNumRecords(MAX_NUM_RECORDS)
                    .withoutMetadata() // PCollection<KV<Long, String>>
            )
            // Write to kafka
            .apply(
                KafkaIO.<String, String>write()
                    .withBootstrapServers(KAFKA_HOST)
                    .withTopic(KAFKA_TOPIC_SINK)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
            );

        pipeline.run();
    }
}