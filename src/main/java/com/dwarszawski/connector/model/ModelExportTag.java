package com.dwarszawski.connector.model;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class ModelExportTag {
    public static final String KEY = "Key";
    public static final String VALUE = "Value";

    private String key;
    private String value;

    public ModelExportTag(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public static final Schema TAG_SCHEMA = SchemaBuilder.struct()
            .name(ModelExportRequest.class.getSimpleName())
            .field(KEY, Schema.STRING_SCHEMA)
            .field(VALUE, Schema.STRING_SCHEMA);


    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Struct toStruct() {
        return new Struct(TAG_SCHEMA)
                .put(KEY, getKey())
                .put(VALUE, getValue());
    }
}
