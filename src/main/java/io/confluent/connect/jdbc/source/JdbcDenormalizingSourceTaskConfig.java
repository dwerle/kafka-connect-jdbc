package io.confluent.connect.jdbc.source;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class JdbcDenormalizingSourceTaskConfig extends JdbcDenormalizingSourceConnectorConfig {

  public static final String TABLES_CONFIG = "tables";
  private static final String TABLES_DOC = "List of tables for this task to watch for changes.";

  static ConfigDef config = baseConfigDef()
      .define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC);

  public JdbcDenormalizingSourceTaskConfig(Map<String, String> props) {
    super(config, props);
  }
}
