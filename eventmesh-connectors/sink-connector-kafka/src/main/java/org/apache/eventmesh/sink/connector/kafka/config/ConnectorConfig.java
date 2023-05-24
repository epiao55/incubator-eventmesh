package org.apache.eventmesh.sink.connector.kafka.config;

import lombok.Data;

@Data
public class ConnectorConfig {
    private String connectorName;
    private String tasksMax;
    private String topic;
    private String bootstrapServers;
    private String groupID;
    private String keyConverter;
    private String valueConverter;
    private String offsetFlushIntervalMS;
    private String offsetStorageTopic;
    private String offsetStorageReplicationFactor;
    private String configStorageTopic;
    private String configStorageReplicationFactor;
    private String statusStorageTopic;
    private String statusStorageReplicationFactor;
    private String offsetCommitTimeoutMS;
    private String offsetCommitIntervalMS;
    private String heartbeatIntervalMS;
    private String sessionTimeoutMS;
}
