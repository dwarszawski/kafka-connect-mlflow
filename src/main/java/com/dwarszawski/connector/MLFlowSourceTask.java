package com.dwarszawski.connector;

import com.dwarszawski.connector.model.ModelExportRequest;
import com.dwarszawski.connector.model.ModelExportRequestFactory;
import org.apache.kafka.connect.source.SourceRecord;
import org.mlflow.api.proto.ModelRegistry;
import org.mlflow.tracking.MlflowClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MLFlowSourceTask extends org.apache.kafka.connect.source.SourceTask {

    static final Logger log = LoggerFactory.getLogger(MLFlowSourceTask.class);

    public static final String URL = "url";
    public static final String LAST_TIMESTAMP = "last_timestamp";
    public static final String PRODUCTION_STAGE = "Production";

    MlflowClient client;
    private String topic;
    private String url;
    long lastTimestamp = 0;

    @Override
    public String version() {
        return MLFlowSourceTask.class.getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        final String trackingUri = map.get(MLFlowConnectorConfig.SOURCE_TRACKING_URL);

        this.client = new MlflowClient(trackingUri);
        this.topic = map.get(MLFlowConnectorConfig.TOPIC_CONFIG);
        this.url = trackingUri;

        Map<String, Object> persistedMap = null;

        if (context != null && context.offsetStorageReader() != null) {
            persistedMap = context.offsetStorageReader().offset(Collections.singletonMap(URL, url));
        }

        if (persistedMap != null) {
            Object lastRead = persistedMap.get(LAST_TIMESTAMP);
            if (lastRead != null) {
                lastTimestamp = (Long) lastRead;
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final List<ModelRegistry.ModelVersion> registeredModels = client.listRegisteredModels()
                .stream()
                .filter(rec -> rec.getLastUpdatedTimestamp() > lastTimestamp)
                .flatMap(rec -> rec.getLatestVersionsList().stream())
                .filter(rec -> rec.getCurrentStage().equals(PRODUCTION_STAGE))
                .filter(rec -> rec.getLastUpdatedTimestamp() > lastTimestamp) // TODO verify if not redundant
                .collect(Collectors.toList());

        registeredModels.sort(new ModelVersionComparator());

        List<SourceRecord> records = new ArrayList<>();

        for (ModelRegistry.ModelVersion model : registeredModels) {

            lastTimestamp = model.getLastUpdatedTimestamp();

            ModelExportRequest modelExportRequest = ModelExportRequestFactory.newModelExportRequest(model);
            SourceRecordBuilder builder = new SourceRecordBuilder(modelExportRequest, this.topic, this.url)
                    .withPartition(URL, this.url)
                    .withTimestamp(LAST_TIMESTAMP, lastTimestamp);

            records.add(builder.build());
        }

        return records;
    }

    @Override
    public void stop() {
        //TODO: Do whatever is required to stop your task.
    }
}