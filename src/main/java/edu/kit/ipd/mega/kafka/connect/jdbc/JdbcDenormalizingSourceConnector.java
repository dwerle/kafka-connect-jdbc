package edu.kit.ipd.mega.kafka.connect.jdbc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import edu.kit.ipd.mega.kafka.connect.jdbc.source.JdbcDenormalizingSourceConnectorConfig;
import edu.kit.ipd.mega.kafka.connect.jdbc.source.JdbcDenormalizingSourceTask;
import io.confluent.connect.jdbc.JdbcSourceConnector;

public class JdbcDenormalizingSourceConnector extends JdbcSourceConnector {
  @Override
  protected void extractAndAssignConfig(Map<String, String> properties) {
    fixPropertyMap(properties);

    System.out.println("PROPERTIES: ");
    for (String key : properties.keySet()) {
      System.out.println("  " + key + " --> " + properties.get(key));
    }
    configProperties = properties;
    config = new JdbcDenormalizingSourceConnectorConfig(configProperties);
  }

  /**
   * XXX: move this to the respective Kafka connect class. The parser does not
   * work correctly, because it does not split at the correct "=" symbol if
   * there is more than one in a line of the configuration.
   * 
   * This method fixes this by moving everything past a "=" in the key to the
   * respective value.
   * 
   * @param properties
   */
  private void fixPropertyMap(Map<String, String> properties) {
    Map<String, String> newEntries = new HashMap<>();
    Set<String> keysToRemove = new HashSet<>();
    for (Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().contains("=")) {
        keysToRemove.add(entry.getKey());
        String[] splitOldKey = entry.getKey().split("=");
        String newKey = splitOldKey[0];
        String newValue = Stream.concat(Arrays.stream(splitOldKey), Stream.of(entry.getValue())).skip(1)
            .collect(Collectors.joining("="));
        newEntries.put(newKey, newValue);
      }
    }
    for (String keyToRemove : keysToRemove) {
      properties.remove(keyToRemove);
    }
    properties.putAll(newEntries);
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
