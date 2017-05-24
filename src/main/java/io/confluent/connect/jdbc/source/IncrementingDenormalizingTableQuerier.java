package io.confluent.connect.jdbc.source;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.util.JdbcUtils;

public class IncrementingDenormalizingTableQuerier extends TimestampIncrementingTableQuerier {
  private static final Logger log = LoggerFactory.getLogger(IncrementingDenormalizingTableQuerier.class);

  protected Integer currentColumnIndex = null;
  protected Integer currentMaximumColumnIndex = null;
  
  protected Set<String> columnLabels;
  private Iterator<String> columnIterator;

  protected Schema keySchema;

  public IncrementingDenormalizingTableQuerier(QueryMode mode, String name, String topicPrefix, String timestampColumn,
      String incrementingColumn, Map<String, Object> offsetMap, Long timestampDelay, String schemaPattern,
      boolean mapNumerics) {
    super(mode, name, topicPrefix, timestampColumn, incrementingColumn, offsetMap, timestampDelay, schemaPattern,
        mapNumerics);
    
    if (incrementingColumn == null) {
      throw new IllegalArgumentException("Incrementing column must not be null");
    }
    if (timestampColumn == null) {
      throw new IllegalArgumentException("Timestamp column must not be null");
    }

    columnLabels = new HashSet<>();
  }
  
  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    String quoteString = JdbcUtils.getIdentifierQuoteString(db);

    StringBuilder builder = new StringBuilder();

    builder.append("SELECT * FROM ");
    builder.append(JdbcUtils.quoteString(name, quoteString));

    builder.append(" WHERE ");
    builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
    builder.append(" > ?");
    builder.append(" ORDER BY ");
    builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
    builder.append(" ASC");

    String queryString = builder.toString();
    log.debug("{} prepared SQL query: {}", this, queryString);
    stmt = db.prepareStatement(queryString);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    Long incOffset = offset.getIncrementingOffset();
    stmt.setLong(1, incOffset);
    log.debug("Executing prepared statement with incrementing value = {}", incOffset);

    return stmt.executeQuery();
  }

  @Override
  public void maybeStartQuery(Connection db) throws SQLException {
    if (resultSet == null) {
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      schema = createDefaultSchema();
      keySchema = createKeySchema();
    }
  }

  private static Schema createDefaultSchema() {
    // String value =
    // resultSet.getObject(getRealColumnIndex(currentColumnIndex)).toString();
    // long id = resultSet.getLong(getIncremetingColumnIndex());
    // Timestamp latest = resultSet.getTimestamp(getTimestampColumnIndex());
    // record.put("id", id);
    // record.put("ts", latest);
    // record.put("name", name);
    // record.put("value", value);

    SchemaBuilder builder = SchemaBuilder.struct().name("sensorvalues");
    builder.field("id", Schema.INT64_SCHEMA)
           .field("ts", org.apache.kafka.connect.data.Timestamp.builder().build())
           .field("machinename", Schema.STRING_SCHEMA)
           .field("sensorname", Schema.STRING_SCHEMA)
           .field("value", Schema.FLOAT64_SCHEMA);

    return builder.build();
  }
  
  private static Schema createKeySchema() {
    SchemaBuilder builder = SchemaBuilder.struct().name("sensorvaluesKey");
    builder.field("key", Schema.STRING_SCHEMA);
    return builder.build();
  }

  @Override
  public boolean next() throws SQLException {
    boolean superNextFalse = false;
    if (columnIterator == null || !columnIterator.hasNext()) {
      superNextFalse = !super.next();
      if (!superNextFalse) {
        resetColumnSetAndIterator();
      }
    }
    
    return !superNextFalse
         && (columnIterator.hasNext())
         && (resultSet != null)
         && !(resultSet.isBeforeFirst() || resultSet.isAfterLast());
  }

  protected Set<String> ignoredColumns() {
    return Arrays.stream(new String[] {
        timestampColumn,
        incrementingColumn
    }).collect(Collectors.<String>toSet());
  }

  private void resetColumnSetAndIterator() throws SQLException {
    columnLabels = new HashSet<>();
    ResultSetMetaData metaData = resultSet.getMetaData();
    for (int currentColumnIndex = 1; currentColumnIndex <= metaData.getColumnCount(); currentColumnIndex++) {
      String currentColumnLabel = metaData.getColumnLabel(currentColumnIndex);
      if (!ignoredColumns().contains(currentColumnLabel) &&
          (metaData.getColumnType(currentColumnIndex) == Types.DOUBLE)) {
        columnLabels.add(currentColumnLabel);
      }
    }
    columnIterator = columnLabels.iterator();
  }

  private final static Pattern PATTERN_SPLIT_NAME = Pattern.compile("^(AVT_\\d+)_(.+)$");
  
  @Override
  public SourceRecord extractRecord() throws SQLException {
    String currentColumn = columnIterator.next();
    
    Struct record = new Struct(schema);
    Double value = resultSet.getDouble(currentColumn);
    String valueColumnName = currentColumn;
    long id = resultSet.getLong(incrementingColumn);
    Timestamp latest = resultSet.getTimestamp(timestampColumn);
    
    record.put("id", id);
    record.put("ts", latest);
    // AVT_03_008_4NBL_I1_A
    // machinename ist dann AVT_03
    // sensorname ist 008_4NBL_I1_A
    
    // TODO factor out into subroutine
    Matcher matcher = PATTERN_SPLIT_NAME.matcher(valueColumnName);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Illegal column name: " + valueColumnName);
    }
    
    String machineName = matcher.group(1);
    String sensorName = matcher.group(2);
    
    record.put("machinename", machineName);
    record.put("sensorname", sensorName);
    record.put("value", value);

    long incrementingOffset = offset.getIncrementingOffset();
    assert (incrementingOffset == -1 || id > incrementingOffset) || timestampColumn != null;

    Timestamp timestampOffset = offset.getTimestampOffset();
    assert timestampOffset != null && timestampOffset.compareTo(latest) <= 0;
    offset = new TimestampIncrementingOffset(latest, id);

    final Map<String, String> partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);

    Struct key = new Struct(keySchema);
    key.put("key", sensorName);
    
    return new SourceRecord(partition, offset.toMap(), topicPrefix, keySchema, key, record.schema(), record);
  }

}
