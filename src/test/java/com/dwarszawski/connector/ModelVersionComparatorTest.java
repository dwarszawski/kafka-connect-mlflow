package com.dwarszawski.connector;

import org.junit.jupiter.api.Test;
import org.mlflow.api.proto.ModelRegistry;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ModelVersionComparatorTest {
    @Test
    void shouldSortModelVersions() {
        list.sort(new ModelVersionComparator());
        assertEquals(1231242325l, list.get(0).getLastUpdatedTimestamp());
        assertEquals(2231242325l, list.get(1).getLastUpdatedTimestamp());
        assertEquals(3231242325l, list.get(2).getLastUpdatedTimestamp());
    }

    private List<ModelRegistry.ModelVersion> list = Arrays.asList(
            ModelRegistry.ModelVersion
                    .newBuilder()
                    .setCurrentStage("Staging")
                    .setLastUpdatedTimestamp(2231242325l)
                    .build(),
            ModelRegistry.ModelVersion
                    .newBuilder()
                    .setCurrentStage("Production")
                    .setLastUpdatedTimestamp(3231242325l)
                    .build(),
            ModelRegistry.ModelVersion
                    .newBuilder()
                    .setCurrentStage("Production")
                    .setLastUpdatedTimestamp(1231242325l)
                    .build()
    );

}