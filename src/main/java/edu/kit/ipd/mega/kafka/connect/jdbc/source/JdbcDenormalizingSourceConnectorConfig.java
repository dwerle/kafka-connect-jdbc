package edu.kit.ipd.mega.kafka.connect.jdbc.source;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;

public class JdbcDenormalizingSourceConnectorConfig extends JdbcSourceConnectorConfig {
  public JdbcDenormalizingSourceConnectorConfig(Map<String, String> props) {
    super(props);
  }

  public static final String MODE_DENORMALIZE = "denormalize";
  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public static ConfigDef baseConfigDef() {
    ConfigDef resultBaseConfigDef = JdbcSourceConnectorConfig.baseConfigDef();

    DenormalizationAwareModeDependentsRecommender denormalizationAwareModeDependentsRecommender = new DenormalizationAwareModeDependentsRecommender();

    return new ConfigDef()
        .define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC, DATABASE_GROUP, 1, Width.LONG, CONNECTION_URL_DISPLAY, Arrays.asList(TABLE_WHITELIST_CONFIG, TABLE_BLACKLIST_CONFIG))
        .define(CONNECTION_USER_CONFIG, Type.STRING, null, Importance.HIGH, CONNECTION_USER_DOC, DATABASE_GROUP, 2, Width.LONG, CONNECTION_USER_DISPLAY)
        .define(CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, null, Importance.HIGH, CONNECTION_PASSWORD_DOC, DATABASE_GROUP, 3, Width.SHORT, CONNECTION_PASSWORD_DISPLAY)
        .define(TABLE_WHITELIST_CONFIG, Type.LIST, TABLE_WHITELIST_DEFAULT, Importance.MEDIUM, TABLE_WHITELIST_DOC, DATABASE_GROUP, 4, Width.LONG, TABLE_WHITELIST_DISPLAY,
                TABLE_RECOMMENDER)
        .define(TABLE_BLACKLIST_CONFIG, Type.LIST, TABLE_BLACKLIST_DEFAULT, Importance.MEDIUM, TABLE_BLACKLIST_DOC, DATABASE_GROUP, 5, Width.LONG, TABLE_BLACKLIST_DISPLAY,
                TABLE_RECOMMENDER)
        .define(SCHEMA_PATTERN_CONFIG, Type.STRING, null, Importance.MEDIUM, SCHEMA_PATTERN_DOC, DATABASE_GROUP, 6, Width.SHORT, SCHEMA_PATTERN_DISPLAY)
        .define(TABLE_TYPE_CONFIG, Type.LIST, TABLE_TYPE_DEFAULT, Importance.LOW,
                TABLE_TYPE_DOC, CONNECTOR_GROUP, 4, Width.MEDIUM, TABLE_TYPE_DISPLAY)
        .define(NUMERIC_PRECISION_MAPPING_CONFIG, Type.BOOLEAN, NUMERIC_PRECISION_MAPPING_DEFAULT, Importance.LOW, NUMERIC_PRECISION_MAPPING_DOC, DATABASE_GROUP, 4, Width.SHORT, NUMERIC_PRECISION_MAPPING_DISPLAY)
        .define(MODE_CONFIG, Type.STRING, MODE_UNSPECIFIED, ConfigDef.ValidString.in(MODE_UNSPECIFIED, MODE_BULK, MODE_TIMESTAMP, MODE_INCREMENTING, MODE_TIMESTAMP_INCREMENTING),
                Importance.HIGH, MODE_DOC, MODE_GROUP, 1, Width.MEDIUM, MODE_DISPLAY, Arrays.asList(INCREMENTING_COLUMN_NAME_CONFIG, TIMESTAMP_COLUMN_NAME_CONFIG, VALIDATE_NON_NULL_CONFIG))
        .define(INCREMENTING_COLUMN_NAME_CONFIG, Type.STRING, INCREMENTING_COLUMN_NAME_DEFAULT, Importance.MEDIUM, INCREMENTING_COLUMN_NAME_DOC, MODE_GROUP, 2, Width.MEDIUM, INCREMENTING_COLUMN_NAME_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER)
        .define(TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, TIMESTAMP_COLUMN_NAME_DEFAULT, Importance.MEDIUM, TIMESTAMP_COLUMN_NAME_DOC, MODE_GROUP, 3, Width.MEDIUM, TIMESTAMP_COLUMN_NAME_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER)
        .define(VALIDATE_NON_NULL_CONFIG, Type.BOOLEAN, VALIDATE_NON_NULL_DEFAULT, Importance.LOW, VALIDATE_NON_NULL_DOC, MODE_GROUP, 4, Width.SHORT, VALIDATE_NON_NULL_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER)
        .define(QUERY_CONFIG, Type.STRING, QUERY_DEFAULT, Importance.MEDIUM, QUERY_DOC, MODE_GROUP, 5, Width.SHORT, QUERY_DISPLAY)
        .define(POLL_INTERVAL_MS_CONFIG, Type.INT, POLL_INTERVAL_MS_DEFAULT, Importance.HIGH, POLL_INTERVAL_MS_DOC, CONNECTOR_GROUP, 1, Width.SHORT, POLL_INTERVAL_MS_DISPLAY)
        .define(BATCH_MAX_ROWS_CONFIG, Type.INT, BATCH_MAX_ROWS_DEFAULT, Importance.LOW, BATCH_MAX_ROWS_DOC, CONNECTOR_GROUP, 2, Width.SHORT, BATCH_MAX_ROWS_DISPLAY)
        .define(TABLE_POLL_INTERVAL_MS_CONFIG, Type.LONG, TABLE_POLL_INTERVAL_MS_DEFAULT, Importance.LOW, TABLE_POLL_INTERVAL_MS_DOC, CONNECTOR_GROUP, 3, Width.SHORT, TABLE_POLL_INTERVAL_MS_DISPLAY)
        .define(TOPIC_PREFIX_CONFIG, Type.STRING, Importance.HIGH, TOPIC_PREFIX_DOC, CONNECTOR_GROUP, 4, Width.MEDIUM, TOPIC_PREFIX_DISPLAY)
        .define(TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, Type.LONG, TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT, Importance.HIGH, TIMESTAMP_DELAY_INTERVAL_MS_DOC, CONNECTOR_GROUP, 5, Width.MEDIUM, TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY);

    return resultBaseConfigDef;
  }

  // TODO: copied from JdbcSourceConnectorConfig
  private static class DenormalizationAwareModeDependentsRecommender implements Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> config) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> config) {
      // TODO: copied from JdbcSourceConnectorConfig
      String mode = (String) config.get(MODE_CONFIG);
      switch (mode) {
        case MODE_BULK:
          return false;
        case MODE_TIMESTAMP:
          return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG) || name.equals(VALIDATE_NON_NULL_CONFIG);
        case MODE_INCREMENTING:
          return name.equals(INCREMENTING_COLUMN_NAME_CONFIG) || name.equals(VALIDATE_NON_NULL_CONFIG);
        case MODE_DENORMALIZE: // this line was added
        case MODE_TIMESTAMP_INCREMENTING:
          return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG) || name.equals(INCREMENTING_COLUMN_NAME_CONFIG)
              || name.equals(VALIDATE_NON_NULL_CONFIG);
        case MODE_UNSPECIFIED:
          throw new ConfigException("Query mode must be specified");
        default:
          throw new ConfigException("Invalid mode: " + mode);
      }
    }
  }
}
