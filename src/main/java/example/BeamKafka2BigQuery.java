package example;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
// import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;


public class BeamKafka2BigQuery {

    public interface TemplateOptions extends PipelineOptions {

        @Description("Kafka host.")
        @Default.String("myhost:9092")
        String getKafkaHost();
        void setKafkaHost(String value);

        @Description("Kafka topic.")
        @Default.String("mytopic1")
        String getKafkaTopic();
        void setKafkaTopic(String value);

        @Description("Google Storage temporary location.")
        @Default.String("gs://styletheory-dwh/tmp")
        String getGSTempLocation();
        void setGSTempLocation(String value);

        @Description("Big Query table id destination (project:dataset.table).")
        @Default.String("myproject:mydataset.mytable")
        String getBQTable();
        void setBQTable(String value);
    }

    static class KafkaRecordToRowConverter<A, B> extends DoFn<KafkaRecord<A, B>, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(
                new TableRow()
                    .set("key", c.element().getKV().getKey())
                    .set("value", c.element().getKV().getValue())
                    .set("topic", c.element().getTopic())
                    .set("offset", c.element().getOffset())
                    .set("partition", c.element().getPartition())
                    .set("timestamp", c.element().getTimestamp()/1000)
            );
        }

        static TableSchema getSchema() {
            return new TableSchema()
                .setFields(
                    ImmutableList.of(
                        new TableFieldSchema().setName("key").setType("STRING"),
                        new TableFieldSchema().setName("value").setType("STRING"),
                        new TableFieldSchema().setName("topic").setType("STRING"),
                        new TableFieldSchema().setName("offset").setType("INTEGER"),
                        new TableFieldSchema().setName("partition").setType("INTEGER"),
                        new TableFieldSchema().setName("timestamp").setType("TIMESTAMP")
                    )
                );
        }
    }

    public static void main(String[] args) {

        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
        options.setTempLocation(options.getGSTempLocation());

        final String KAFKA_HOST = options.getKafkaHost();
        final String KAFKA_TOPIC = options.getKafkaTopic();
        final String BQ_TABLE = options.getBQTable();
        final Integer MAX_NUM_RECORDS = 10;

        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply(
                KafkaIO.<String, String>read()
                    .withBootstrapServers(KAFKA_HOST)
                    .withTopic(KAFKA_TOPIC)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", "earliest"))
                    // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                    // the first 5 records.
                    // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                    // .withMaxNumRecords(MAX_NUM_RECORDS)
                    // .withoutMetadata() // PCollection<KV<Long, String>>
            )
            .apply(ParDo.of(new KafkaRecordToRowConverter<String, String>()))
            .apply(
                BigQueryIO
                    .writeTableRows()
                    .to(BQ_TABLE)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withSchema(KafkaRecordToRowConverter.getSchema())
            );

        pipeline.run();
    }
}