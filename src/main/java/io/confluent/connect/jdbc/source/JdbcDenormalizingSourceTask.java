package io.confluent.connect.jdbc.source;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.confluent.connect.jdbc.source.TableQuerier.QueryMode;

public class JdbcDenormalizingSourceTask extends JdbcSourceTask {
  @Override
  protected void addTableQuerier(QueryMode queryMode, String mode, String schemaPattern, String incrementingColumn,
      String timestampColumn, Long timestampDelayInterval, String tableOrQuery, Map<String, Object> offset,
      String topicPrefix, boolean mapNumerics) {
    if (mode.equals(JdbcDenormalizingSourceConnectorConfig.MODE_DENORMALIZE)) {
      tableQueue.add(new IncrementingDenormalizingTableQuerier(queryMode, tableOrQuery, topicPrefix, timestampColumn,
          incrementingColumn, offset, timestampDelayInterval, schemaPattern, mapNumerics));
    } else {
      super.addTableQuerier(queryMode, mode, schemaPattern, incrementingColumn, timestampColumn, timestampDelayInterval,
          tableOrQuery, offset, topicPrefix, mapNumerics);
    }
  }
  
  @Override
  protected void extractAndAssignConfig(Map<String, String> properties) throws ConfigException {
    config = new JdbcDenormalizingSourceTaskConfig(properties);
  }
}
