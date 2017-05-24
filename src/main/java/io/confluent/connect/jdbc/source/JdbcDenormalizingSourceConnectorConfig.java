package io.confluent.connect.jdbc.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class JdbcDenormalizingSourceConnectorConfig extends JdbcSourceConnectorConfig {
  public JdbcDenormalizingSourceConnectorConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
    String mode = getString(JdbcSourceConnectorConfig.MODE_CONFIG);
    if (mode.equals(JdbcSourceConnectorConfig.MODE_UNSPECIFIED))
      throw new ConfigException("Query mode must be specified");
  }

  public static final String MODE_DENORMALIZE = "denormalize";
  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  protected static final Recommender DENORMALIZATION_AWARE_MODE_DEPENDENTS_RECOMMENDER = new DenormalizationAwareModeDependentsRecommender();

  /**
   * TODO: move to a utility class or to {@link ConfigDef}.
   * 
   * Removes the given key from the given config def
   * 
   * @param configDef
   *          the config def to remove the key from. must not be null.
   * @param key
   *          the key to remove. if the key is not set, an exception is thrown.
   */
  private static void removeConfigKey(ConfigDef configDef, String key) {
    Map<String, ConfigKey> configKeys = configDef.configKeys();
    if (!configKeys.containsKey(key)) {
      throw new IllegalArgumentException("Key " + key + " is not present in config def.");
    } else {
      configKeys.remove(key);
    }
  }

  public static ConfigDef baseConfigDef() {
    ConfigDef base = JdbcSourceConnectorConfig.CONFIG_DEF;
    removeConfigKey(base, MODE_CONFIG);
    removeConfigKey(base, INCREMENTING_COLUMN_NAME_CONFIG);
    removeConfigKey(base, TIMESTAMP_COLUMN_NAME_CONFIG);
    removeConfigKey(base, VALIDATE_NON_NULL_CONFIG);

    return base
        .define(MODE_CONFIG, Type.STRING, MODE_DENORMALIZE, ConfigDef.ValidString.in(MODE_DENORMALIZE), Importance.HIGH,
            MODE_DOC, MODE_GROUP, 1, Width.MEDIUM, MODE_DISPLAY,
            Arrays.asList(INCREMENTING_COLUMN_NAME_CONFIG, TIMESTAMP_COLUMN_NAME_CONFIG, VALIDATE_NON_NULL_CONFIG))
        .define(INCREMENTING_COLUMN_NAME_CONFIG, Type.STRING, INCREMENTING_COLUMN_NAME_DEFAULT, Importance.MEDIUM,
            INCREMENTING_COLUMN_NAME_DOC, MODE_GROUP, 2, Width.MEDIUM, INCREMENTING_COLUMN_NAME_DISPLAY,
            DENORMALIZATION_AWARE_MODE_DEPENDENTS_RECOMMENDER)
        .define(TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, TIMESTAMP_COLUMN_NAME_DEFAULT, Importance.MEDIUM,
            TIMESTAMP_COLUMN_NAME_DOC, MODE_GROUP, 3, Width.MEDIUM, TIMESTAMP_COLUMN_NAME_DISPLAY,
            DENORMALIZATION_AWARE_MODE_DEPENDENTS_RECOMMENDER)
        .define(VALIDATE_NON_NULL_CONFIG, Type.BOOLEAN, VALIDATE_NON_NULL_DEFAULT, Importance.LOW,
            VALIDATE_NON_NULL_DOC, MODE_GROUP, 4, Width.SHORT, VALIDATE_NON_NULL_DISPLAY,
            DENORMALIZATION_AWARE_MODE_DEPENDENTS_RECOMMENDER);
  }

  /**
   * Class that does the same as {@link ModeDependentsRecommender} but handles
   * the visibility of
   * {@link JdbcDenormalizingSourceConnectorConfig#MODE_DENORMALIZE} the same as
   * {@link JdbcSourceConnectorConfig#MODE_TIMESTAMP_INCREMENTING}.
   * 
   * @author Dominik Werle
   *
   */
  private static class DenormalizationAwareModeDependentsRecommender extends ModeDependentsRecommender {
    @Override
    public boolean visible(String name, Map<String, Object> config) {
      Map<String, Object> configWithoutDenormalize = new HashMap<>(config);
      if (configWithoutDenormalize.get(MODE_CONFIG).equals(MODE_DENORMALIZE)) {
        configWithoutDenormalize.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
      }

      return super.visible(name, configWithoutDenormalize);
    }
  }
}
