package com.dwarszawski.connector;

import org.mlflow.api.proto.ModelRegistry;

import java.util.Comparator;

public class ModelVersionComparator implements Comparator<ModelRegistry.ModelVersion> {
    @Override
    public int compare(ModelRegistry.ModelVersion m1, ModelRegistry.ModelVersion m2) {
        return Math.toIntExact(m1.getLastUpdatedTimestamp() - m2.getLastUpdatedTimestamp());
    }
}
