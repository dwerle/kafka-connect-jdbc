package edu.kit.ipd.mega.kafka.connect.jdbc.source;

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

    resultBaseConfigDef
        .define(INCREMENTING_COLUMN_NAME_CONFIG, Type.STRING, INCREMENTING_COLUMN_NAME_DEFAULT, Importance.MEDIUM,
            INCREMENTING_COLUMN_NAME_DOC, MODE_GROUP, 2, Width.MEDIUM, INCREMENTING_COLUMN_NAME_DISPLAY,
            denormalizationAwareModeDependentsRecommender)
        .define(TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, TIMESTAMP_COLUMN_NAME_DEFAULT, Importance.MEDIUM,
            TIMESTAMP_COLUMN_NAME_DOC, MODE_GROUP, 3, Width.MEDIUM, TIMESTAMP_COLUMN_NAME_DISPLAY,
            denormalizationAwareModeDependentsRecommender)
        .define(VALIDATE_NON_NULL_CONFIG, Type.BOOLEAN, VALIDATE_NON_NULL_DEFAULT, Importance.LOW,
            VALIDATE_NON_NULL_DOC, MODE_GROUP, 4, Width.SHORT, VALIDATE_NON_NULL_DISPLAY,
            denormalizationAwareModeDependentsRecommender);

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
