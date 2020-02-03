package example;

import com.google.common.collect.ImmutableMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BeamKafkaWordCount {

    static final String DIR_OUTPUT = "target/_output/wordcount";
    static final String KAFKA_HOST = "stde-dev:9092";
    static final String KAFKA_TOPIC = "mytopic1";
    static final String TOKENIZER_PATTERN = " +";
    static final Integer MAX_NUM_RECORDS = 1000;

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);

        p.apply(
            KafkaIO.<String, String>read()
                .withBootstrapServers(KAFKA_HOST)
                .withTopic(KAFKA_TOPIC)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                // the first 5 records.
                // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                .withMaxNumRecords(MAX_NUM_RECORDS)
                .withoutMetadata() // PCollection<KV<Long, String>>
        )
        .apply(Values.<String>create())
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
        .apply(Count.<String>perElement())
        .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
            @Override
            public String apply(KV<String, Long> input) {
                return input.getKey() + ": " + input.getValue();
            }
        }))
        .apply(TextIO.write().to(DIR_OUTPUT));

        p.run().waitUntilFinish();
    }
}