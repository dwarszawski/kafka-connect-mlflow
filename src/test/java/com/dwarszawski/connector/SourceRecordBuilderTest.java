package com.dwarszawski.connector;

import com.dwarszawski.connector.model.ModelExportRequest;
import com.dwarszawski.connector.model.ModelExportRequestFactory;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.mlflow.api.proto.ModelRegistry;

import static org.junit.jupiter.api.Assertions.*;

class SourceRecordBuilderTest {
    private static final String PARTITON_KEY = "partition";
    private static final String TIMESTAMP_KEY = "timestamp";

    @Test
    void buildSourceRecord() {
        ModelRegistry.ModelVersion model = ModelRegistry.ModelVersion
                .newBuilder()
                .setCurrentStage("Production")
                .setLastUpdatedTimestamp(1l)
                .build();

        ModelExportRequest request = ModelExportRequestFactory.newModelExportRequest(model);

        final SourceRecord sourceRecord = new SourceRecordBuilder(request, "test_topic", "key").
                withPartition(PARTITON_KEY, "value")
                .withTimestamp(TIMESTAMP_KEY, 1l)
                .build();

        assertNotNull(sourceRecord);
        assertNotNull(sourceRecord.value());
        assertEquals("value", sourceRecord.sourcePartition().get(PARTITON_KEY));
        assertEquals(1l, sourceRecord.sourceOffset().get(TIMESTAMP_KEY));
        Struct struct = (Struct) sourceRecord.value();

        assertEquals("Production", struct.getString(ModelExportRequest.CURRENT_STAGE));
        assertEquals(1l, struct.getInt64(ModelExportRequest.LAST_UPDATE_TIMESTAMP).longValue());
    }
}