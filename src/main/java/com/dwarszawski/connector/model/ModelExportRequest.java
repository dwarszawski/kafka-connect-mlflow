package com.dwarszawski.connector.model;

import org.apache.kafka.connect.data.*;

import java.util.List;
import java.util.stream.Collectors;

public class ModelExportRequest {

    public static final String NAME = "Name";
    public static final String VERSION = "Version";
    public static final String CREATION_TIMESTAMP = "CreationTimestamp";
    public static final String LAST_UPDATE_TIMESTAMP = "lastUpdatedTimestamp";
    public static final String USER_ID = "userId";
    public static final String CURRENT_STAGE = "currentStage";
    public static final String DESCRIPTION = "description";
    public static final String SOURCE = "source";
    public static final String RUN_ID = "runId";
    public static final String STATUS = "status";
    public static final String STATUS_MESSAGE = "statusMessage";
    public static final String TAGS = "tags";
    public static final String RUN_LINK = "runLink";

    private String name;
    private String version;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;
    private String userId;
    private String currentStage;
    private String description;
    private String source;
    private String runId;
    private String status;
    private String statusMessage;
    private List<ModelExportTag> tags;
    private String runLink;

    public ModelExportRequest(String name,
                              String version,
                              long creationTimestamp,
                              long lastUpdatedTimestamp,
                              String userId,
                              String currentStage,
                              String description,
                              String source,
                              String runId,
                              String status,
                              String statusMessage,
                              List<ModelExportTag> tags,
                              String runLink) {
        this.name = name;
        this.version = version;
        this.creationTimestamp = creationTimestamp;
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
        this.userId = userId;
        this.currentStage = currentStage;
        this.description = description;
        this.source = source;
        this.runId = runId;
        this.status = status;
        this.statusMessage = statusMessage;
        this.tags = tags;
        this.runLink = runLink;
    }


    public static final Schema MODEL_EXPORT_SCHEMA = SchemaBuilder.struct()
            .name(ModelExportRequest.class.getSimpleName())
            .field(NAME, Schema.STRING_SCHEMA)
            .field(VERSION, Schema.STRING_SCHEMA)
            .field(CREATION_TIMESTAMP, Schema.INT64_SCHEMA)
            .field(LAST_UPDATE_TIMESTAMP, Schema.INT64_SCHEMA)
            .field(USER_ID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CURRENT_STAGE, Schema.STRING_SCHEMA)
            .field(DESCRIPTION, Schema.STRING_SCHEMA)
            .field(SOURCE, Schema.STRING_SCHEMA)
            .field(RUN_ID, Schema.STRING_SCHEMA)
            .field(STATUS, Schema.STRING_SCHEMA)
            .field(STATUS_MESSAGE, Schema.OPTIONAL_STRING_SCHEMA)
            .field(TAGS, SchemaBuilder.array(ModelExportTag.TAG_SCHEMA).optional().schema())
            .field(RUN_LINK, Schema.STRING_SCHEMA)
            .build();

    public Struct toStruct() {
        Struct struct = new Struct(MODEL_EXPORT_SCHEMA)
                .put(NAME, getName())
                .put(VERSION, getVersion())
                .put(CREATION_TIMESTAMP, getCreationTimestamp())
                .put(LAST_UPDATE_TIMESTAMP, getLastUpdatedTimestamp())
                .put(CURRENT_STAGE, getCurrentStage())
                .put(DESCRIPTION, getDescription())
                .put(SOURCE, getSource())
                .put(RUN_ID, getRunId())
                .put(STATUS, getStatus())
                .put(RUN_LINK, getRunLink());

        if (getUserId() != null) {
            struct.put(USER_ID, getUserId());
        }
        if (getStatusMessage() != null) {
            struct.put(STATUS_MESSAGE, getStatusMessage());
        }
        if (getTags() != null) {
            struct.put(TAGS, getTags().stream().map(ModelExportTag::toStruct).collect(Collectors.toList()));
        }

        return struct;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public long getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public String getUserId() {
        return userId;
    }

    public String getCurrentStage() {
        return currentStage;
    }

    public String getDescription() {
        return description;
    }

    public String getSource() {
        return source;
    }

    public String getRunId() {
        return runId;
    }

    public String getStatus() {
        return status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public List<ModelExportTag> getTags() {
        return tags;
    }

    public String getRunLink() {
        return runLink;
    }

}
