package com.dwarszawski.connector.model;

import org.junit.jupiter.api.Test;
import org.mlflow.api.proto.ModelRegistry;

import static org.junit.jupiter.api.Assertions.*;

class ModelExportRequestFactoryTest {
    @Test
    void convertModelVersionToModelExportRequest() {
        ModelRegistry.ModelVersion model = ModelRegistry.ModelVersion
                .newBuilder()
                .setCurrentStage("Production")
                .setLastUpdatedTimestamp(1l)
                .addTags(ModelRegistry.ModelVersionTag.newBuilder().setKey("aaa").setValue("bbb").build())
                .build();

        ModelExportRequest request = ModelExportRequestFactory.newModelExportRequest(model);

        assertNotNull(request);
        assertEquals(model.getName(), request.getName());
        assertEquals(model.getVersion(), request.getVersion());
        assertEquals(model.getCreationTimestamp(), request.getCreationTimestamp());
        assertEquals(model.getLastUpdatedTimestamp(), request.getLastUpdatedTimestamp());
        assertEquals(model.getUserId(), request.getUserId());
        assertEquals(model.getCurrentStage(), request.getCurrentStage());
        assertEquals(model.getDescription(), request.getDescription());
        assertEquals(model.getSource(), request.getSource());
        assertEquals(model.getRunId(), request.getRunId());
        assertEquals(model.getStatus().name(), request.getStatus());
        assertEquals(model.getStatusMessage(), request.getStatusMessage());
        assertEquals(model.getTags(0).getKey(), request.getTags().get(0).getKey());
        assertEquals(model.getTags(0).getValue(), request.getTags().get(0).getValue());
        assertEquals(model.getRunLink(), request.getRunLink());
    }
}