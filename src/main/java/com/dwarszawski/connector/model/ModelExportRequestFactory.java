package com.dwarszawski.connector.model;

import org.mlflow.api.proto.ModelRegistry;

import java.util.stream.Collectors;

public class ModelExportRequestFactory {
    public static ModelExportRequest newModelExportRequest(ModelRegistry.ModelVersion modelVersion) {
        return new ModelExportRequest(
                modelVersion.getName(),
                modelVersion.getVersion(),
                modelVersion.getCreationTimestamp(),
                modelVersion.getLastUpdatedTimestamp(),
                modelVersion.getUserId(),
                modelVersion.getCurrentStage(),
                modelVersion.getDescription(),
                modelVersion.getSource(),
                modelVersion.getRunId(),
                modelVersion.getStatus().name(),
                modelVersion.getStatusMessage(),
                modelVersion.getTagsList().stream()
                        .map(v -> new ModelExportTag(v.getKey(), v.getValue()))
                        .collect(Collectors.toList()),
                modelVersion.getRunLink()
        );
    }
}
