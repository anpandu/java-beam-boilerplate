package example;

import com.google.common.collect.ImmutableMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
// import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BeamKafka2KafkaWordCount {

    static final String DIR_OUTPUT = "target/_output/wordcount";
    static final String KAFKA_HOST = "stde-dev:9092";
    static final String KAFKA_TOPIC = "mytopic1";
    static final String KAFKA_TOPIC_2 = "mytopic1b";
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
            // Get value
            .apply(Values.<String>create())
            // Split the string by pattern and output it as multiple words
            .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    for (String word : c.element().split(TOKENIZER_PATTERN)) {
                        if (!word.isEmpty()) {
                            c.output(word);
                        }
                    }
                }
            }))
            // Count them
            .apply(Count.<String>perElement())
            // Create message: word -> counter
            .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, KV<String, String>>() {
                @Override
                public KV<String, String> apply(KV<String, Long> input) {
                    return KV.of("", input.getKey() + " -> " + input.getValue());
                }
            }))
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