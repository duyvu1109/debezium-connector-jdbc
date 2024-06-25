package io.debezium.connector.jdbc.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import io.debezium.connector.jdbc.Module;
import io.debezium.data.Envelope;
import io.debezium.time.Timestamp;

import org.apache.kafka.connect.data.Field;

public class InsertTimestampTransform implements Transformation<SinkRecord>, Versioned {

    public static final String DEBEZIUM_TIMESTAMP = "debezium_timestamp";

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertTimestampTransform.class);

    @Override
    public void configure(Map<String, ?> configs) {
        LOGGER.info("configure method called");

    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public SinkRecord apply(SinkRecord record) {
        LOGGER.info("apply method called");

        Schema newSchema = getSchema(record);
        Struct newValue = getValue(record, newSchema);
        SinkRecord newrRecord = record.newRecord(record.topic(),
                record.kafkaPartition(), record.keySchema(), record.key(), newSchema, newValue,
                record.timestamp());
        return newrRecord;
    }

    private Schema getSchema(SinkRecord record) {
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        schemaBuilder.name(record.valueSchema().name());
        for (Field field : record.valueSchema().fields()) {
            if (field.name().equals(Envelope.FieldName.AFTER)) {
                schemaBuilder.field(field.name(), newAfterSchema(record));
            } else {
                schemaBuilder.field(field.name(), field.schema());
            }
        }

        return schemaBuilder.build();
    }

    private Schema newAfterSchema(SinkRecord record) {
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (Field field : record.valueSchema().field(Envelope.FieldName.AFTER).schema().fields()) {
            schemaBuilder.field(field.name(), field.schema());
        }

        schemaBuilder.name(record.valueSchema().field(Envelope.FieldName.AFTER).schema().name());

        schemaBuilder.field(DEBEZIUM_TIMESTAMP, Timestamp.builder().optional().build());

        return schemaBuilder.build();
    }

    private Struct getValue(SinkRecord record, Schema schema) {
        Struct originalStruct = requireStruct(record.value(), "convert event");
        final Struct struct = new Struct(schema);
        for (Field field : record.valueSchema().fields()) {
            if (field.name().equals(Envelope.FieldName.AFTER)) {
                Struct afterStruct = originalStruct.getStruct(Envelope.FieldName.AFTER);
                Struct newAfterStruct = new Struct(schema.field(Envelope.FieldName.AFTER).schema());
                for (Field afterField : afterStruct.schema().fields()) {
                    Object value = afterStruct.get(afterField);
                    if (value != null) {
                        newAfterStruct.put(afterField.name(), value);

                    }
                }
                newAfterStruct.put(DEBEZIUM_TIMESTAMP, System.currentTimeMillis());

                struct.put(field.name(), newAfterStruct);

            } else {
                Object value = originalStruct.get(field);
                if (value != null) {
                    struct.put(field.name(), value);
                }
            }
        }

        return struct;
    }

    @Override
    public ConfigDef config() {
        LOGGER.info("config method called");
        final ConfigDef config = new ConfigDef();

        return config;
    }

    @Override
    public void close() {
        LOGGER.info("close method called");
    }

}
