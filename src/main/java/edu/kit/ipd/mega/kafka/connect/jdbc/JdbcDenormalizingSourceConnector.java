package edu.kit.ipd.mega.kafka.connect.jdbc;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import edu.kit.ipd.mega.kafka.connect.jdbc.source.JdbcDenormalizingSourceConnectorConfig;
import edu.kit.ipd.mega.kafka.connect.jdbc.source.JdbcDenormalizingSourceTask;
import io.confluent.connect.jdbc.JdbcSourceConnector;

public class JdbcDenormalizingSourceConnector extends JdbcSourceConnector {
  @Override
  protected void extractAndAssignConfig(Map<String, String> properties) {
    configProperties = properties;
    config = new JdbcDenormalizingSourceConnectorConfig(configProperties);
  }
  
  @Override
  public Class<? extends Task> taskClass() {
    return JdbcDenormalizingSourceTask.class;
  }
  
  @Override
  public ConfigDef config() {
    return JdbcDenormalizingSourceConnectorConfig.CONFIG_DEF;
  }
}
