package com.dwarszawski.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLflowSourceConnector extends org.apache.kafka.connect.source.SourceConnector {

    private static Logger log = LoggerFactory.getLogger(MLflowSourceConnector.class);
    private MLFlowConnectorConfig config;

    @Override
    public String version() {
        return MLflowSourceConnector.class.getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        config = new MLFlowConnectorConfig(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MLFlowSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(config.originalsStrings());
        return configs;
    }

    @Override
    public void stop() {
        //TODO: Do things that are necessary to stop your connector.
    }

    @Override
    public ConfigDef config() {
        return MLFlowConnectorConfig.config();
    }
}
