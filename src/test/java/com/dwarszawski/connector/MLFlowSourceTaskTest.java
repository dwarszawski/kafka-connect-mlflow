package com.dwarszawski.connector;

import com.dwarszawski.connector.model.ModelExportRequest;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mlflow.api.proto.ModelRegistry;
import org.mlflow.tracking.MlflowClient;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MLFlowSourceTaskTest {

    @Test
    void pollOnlyProductionModels(@Mock MlflowClient mlflowClient) throws InterruptedException {
        MLFlowSourceTask task = new MLFlowSourceTask();

        ModelRegistry.RegisteredModel registeredModel = sampleModelVersions(model2, model3);
        when(mlflowClient.listRegisteredModels()).thenReturn(Arrays.asList(registeredModel));
        task.client = mlflowClient;

        List<SourceRecord> sourceRecords = task.poll();

        assertEquals(1, sourceRecords.size());
        assertNotNull(sourceRecords.get(0).value());

        Struct struct = (Struct) sourceRecords.get(0).value();
        assertEquals("Production", struct.getString(ModelExportRequest.CURRENT_STAGE));
        assertEquals(3l, struct.getInt64(ModelExportRequest.LAST_UPDATE_TIMESTAMP).longValue());
    }

    @Test
    void pollOnlyLatestModels(@Mock MlflowClient mlflowClient) throws InterruptedException {
        MLFlowSourceTask task = new MLFlowSourceTask();

        ModelRegistry.RegisteredModel registeredModel = sampleModelVersions(model1, model2);
        when(mlflowClient.listRegisteredModels()).thenReturn(Arrays.asList(registeredModel));
        task.client = mlflowClient;

        List<SourceRecord> sourceRecords = task.poll();

        assertEquals(1, sourceRecords.size());
        assertNotNull(sourceRecords.get(0).value());

        Struct struct = (Struct) sourceRecords.get(0).value();
        assertEquals("Production", struct.getString(ModelExportRequest.CURRENT_STAGE));
        assertEquals(3l, struct.getInt64(ModelExportRequest.LAST_UPDATE_TIMESTAMP).longValue());
    }

    @Test
    void nothingToPoll(@Mock MlflowClient mlflowClient) throws InterruptedException {
        MLFlowSourceTask task = new MLFlowSourceTask();

        ModelRegistry.RegisteredModel registeredModel = sampleModelVersions(model3);
        when(mlflowClient.listRegisteredModels()).thenReturn(Arrays.asList(registeredModel));
        task.client = mlflowClient;

        List<SourceRecord> sourceRecords = task.poll();

        assertEquals(0, sourceRecords.size());
    }

    @Test
    void latestTimestampAsOffset(@Mock MlflowClient mlflowClient) throws InterruptedException {
        MLFlowSourceTask task = new MLFlowSourceTask();

        ModelRegistry.RegisteredModel registeredModel = sampleModelVersions(model5, model4, model2);
        when(mlflowClient.listRegisteredModels()).thenReturn(Arrays.asList(registeredModel));
        task.client = mlflowClient;

        List<SourceRecord> sourceRecords = task.poll();

        assertEquals(3, sourceRecords.size());

        assertEquals(7l, task.lastTimestamp);
        assertEquals(3l, sourceRecords.get(0).sourceOffset().get("last_timestamp"));
        assertEquals(4l, sourceRecords.get(1).sourceOffset().get("last_timestamp"));
        assertEquals(7l, sourceRecords.get(2).sourceOffset().get("last_timestamp"));
    }

    private ModelRegistry.RegisteredModel sampleModelVersions(ModelRegistry.ModelVersion... models) {
        return ModelRegistry.RegisteredModel
                .newBuilder()
                .addAllLatestVersions(Arrays.asList(models))
                .setLastUpdatedTimestamp(2l)
                .build();
    }

    private ModelRegistry.ModelVersion model1 = ModelRegistry.ModelVersion
            .newBuilder()
            .setCurrentStage("Production")
            .setLastUpdatedTimestamp(-1l)
            .build();

    private ModelRegistry.ModelVersion model2 = ModelRegistry.ModelVersion
            .newBuilder()
            .setCurrentStage("Production")
            .setLastUpdatedTimestamp(3l)
            .build();

    private ModelRegistry.ModelVersion model3 = ModelRegistry.ModelVersion
            .newBuilder()
            .setCurrentStage("Staging")
            .setLastUpdatedTimestamp(3l)
            .build();
    private ModelRegistry.ModelVersion model4 = ModelRegistry.ModelVersion
            .newBuilder()
            .setCurrentStage("Production")
            .setLastUpdatedTimestamp(4l)
            .build();

    private ModelRegistry.ModelVersion model5 = ModelRegistry.ModelVersion
            .newBuilder()
            .setCurrentStage("Production")
            .setLastUpdatedTimestamp(7l)
            .build();

}