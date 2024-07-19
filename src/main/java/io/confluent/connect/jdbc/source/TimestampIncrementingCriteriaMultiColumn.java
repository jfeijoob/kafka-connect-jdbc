/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

public class TimestampIncrementingCriteriaMultiColumn extends TimestampIncrementingCriteriaBase<IncrementingOffset> {

  protected DatabaseDialect dialect;

  public TimestampIncrementingCriteriaMultiColumn(List<ColumnId> incrementingColumns, List<ColumnId> timestampColumns,
      TimeZone timeZone, DatabaseDialect dialect) {
    super(incrementingColumns, timestampColumns, timeZone);
    this.dialect = dialect;
  }

  // TODO: JOSU use similar technic used in Sink Connector using Bind field in
  // Database Dialect
  protected int setQueryParametersIncrementing(PreparedStatement stmt, Map<String, Object> incOffset,
      int parameterIndex) throws SQLException {

    for (Entry<String, Object> e : incOffset.entrySet()) {
      dialect.bindField(stmt, parameterIndex, e.getValue());
    }

    return parameterIndex;
  }

  // TODO: JOSU SET QUERY PARAMETERS has to be changed for the new format of query
  @Override
  protected void setQueryParametersTimestampIncrementing(PreparedStatement stmt,
      CriteriaValues<IncrementingOffset> values) throws SQLException {
    
    int nextParameterIndex = 0;
    Timestamp beginTime = values.beginTimestampValue();
    IncrementingOffset incOffset = values.lastIncrementedValue();
    
    for (int parameterIndex = 0; parameterIndex < timestampColumns.size(); parameterIndex++) {
      nextParameterIndex = parameterIndex + 1; 
      stmt.setTimestamp(nextParameterIndex, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    }
    
    nextParameterIndex++;

    setQueryParametersIncrementing(stmt, incOffset.asMap(), nextParameterIndex);
    
    log.debug("Executing prepared statement with start time value = {} and incrementing" + " value = {}",
        DateTimeUtils.formatTimestamp(beginTime, timeZone), incOffset);
  }

  @Override
  protected void setQueryParametersIncrementing(PreparedStatement stmt, CriteriaValues<IncrementingOffset> values)
      throws SQLException {
    IncrementingOffset incOffset = values.lastIncrementedValue();
    setQueryParametersIncrementing(stmt, incOffset.asMap(), 1);
    log.debug("Executing prepared statement with incrementing value = {}", incOffset);
  }

  @Override
  protected void setQueryParametersTimestamp(PreparedStatement stmt, CriteriaValues<IncrementingOffset> values)
      throws SQLException {
    Timestamp beginTime = values.beginTimestampValue();
    Timestamp endTime = values.endTimestampValue();
    stmt.setTimestamp(1, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    stmt.setTimestamp(2, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    log.debug("Executing prepared statement with timestamp value = {} end time = {}",
        DateTimeUtils.formatTimestamp(beginTime, timeZone), DateTimeUtils.formatTimestamp(endTime, timeZone));
  }

  /**
   * Extract the offset values from the row.
   *
   * @param schema               the record's schema; never null
   * @param record               the record's struct; never null
   * @param previousOffset       a previous timestamp offset if the table has
   *                             timestamp columns
   * @param timestampGranularity defines the configured granularity of the
   *                             timestamp field
   * @return the timestamp for this row; may not be null
   */
  @Override
  public TimestampIncrementingOffsetBase<IncrementingOffset> extractValues(Schema schema, Struct record,
      TimestampIncrementingOffsetBase<IncrementingOffset> previousOffset,
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity) {
    Timestamp extractedTimestamp = null;
    if (hasTimestampColumns()) {
      extractedTimestamp = extractOffsetTimestamp(schema, record, timestampGranularity);
      assert previousOffset == null || (previousOffset.getTimestampOffset() != null
          && previousOffset.getTimestampOffset().compareTo(extractedTimestamp) <= 0);
    }
    IncrementingOffset extractedId = null;
    if (hasIncrementedColumn()) {
      extractedId = extractOffsetIncrementedId(schema, record);

      // If we are only using an incrementing column, then this must be incrementing.
      // If we are also using a timestamp, then we may see updates to older rows.
      assert previousOffset == null || previousOffset.getIncrementingOffset() == IncrementingOffset.EMPTY_OFFSET
          || extractedId.compareTo(previousOffset.getIncrementingOffset()) == 1 || hasTimestampColumns();
    }
    return new TimestampIncrementingOffsetMultiColumn(extractedTimestamp, extractedId);
  }

  /**
   * Extract the incrementing column value from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @return the incrementing ID for this row; may not be null
   */
  @SuppressWarnings("unchecked")
  @Override
  protected IncrementingOffset extractOffsetIncrementedId(Schema schema, Struct record) {
    final IncrementingOffset extractedId = new IncrementingOffset();

    for (ColumnId incrementingColumn : incrementingColumns) {
      final Field field = schema.field(incrementingColumn.name());
      if (field == null) {
        throw new DataException("Incrementing column " + incrementingColumn.name() + " not found in "
            + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")));
      }
      final Schema incrementingColumnSchema = field.schema();
      Object incrementingColumnValue = record.get(incrementingColumn.name());
      if (incrementingColumnValue == null) {
        throw new ConnectException("Null value for incrementing column of type: " + incrementingColumnSchema.type());
      } else if (incrementingColumnSchema.name() != null
          && incrementingColumnSchema.name().equals(Decimal.LOGICAL_NAME)) {
        incrementingColumnValue = extractDecimalId(incrementingColumnValue);
      }

      extractedId.put(incrementingColumn.name(), (Comparable<Object>) incrementingColumnValue);

      log.trace("Extracted column name: {} value: {}", incrementingColumn.name(), incrementingColumnValue);
    }

    log.trace("Extracted id value: {}", extractedId);
    return extractedId;
  }

  protected Long extractDecimalId(Object incrementingColumnValue) {
    final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
    if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
      throw new ConnectException("Decimal value for incrementing column exceeded Long.MAX_VALUE");
    }
    if (decimal.scale() != 0) {
      throw new ConnectException("Scale of Decimal value for incrementing column must be 0");
    }
    return decimal.longValue();
  }

  protected boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
    return incrementingColumnValue instanceof Long || incrementingColumnValue instanceof Integer
        || incrementingColumnValue instanceof Short || incrementingColumnValue instanceof Byte;
  }

  @Override
  protected void timestampIncrementingWhereClause(ExpressionBuilder builder) {
    // This version combines two possible conditions. The first checks timestamp ==
    // last
    // timestamp and incrementing > last incrementing. The timestamp alone would
    // include
    // duplicates, but adding the incrementing condition ensures no duplicates, e.g.
    // you would
    // get only the row with id = 23:
    // timestamp 1234, id 22 <- last
    // timestamp 1234, id 23
    // The second check only uses the timestamp > last timestamp. This covers
    // everything new,
    // even if it is an update of the existing row. If we previously had:
    // timestamp 1234, id 22 <- last
    // and then these rows were written:
    // timestamp 1235, id 22
    // timestamp 1236, id 23
    // We should capture both id = 22 (an update) and id = 23 (a new row)

    builder.append(" WHERE ");
    builder.appendNewLine();
    builder.append("(");
    builder.appendNewLine();
    whereForTimeStampFields(builder);
    builder.appendNewLine();
    builder.append(")");
    builder.appendNewLine();
    builder.append(" AND ");
    builder.appendNewLine();
    builder.append("(");
    builder.appendNewLine();
    whereForIncrementingFields(builder);
    builder.appendNewLine();
    builder.append(")");

  }

  @Override
  protected void incrementingWhereClause(ExpressionBuilder builder) {
    builder.append(" WHERE ");
    builder.appendNewLine();
    whereForIncrementingFields(builder);
  }

  @Override
  protected void timestampWhereClause(ExpressionBuilder builder) {
    builder.append(" WHERE ");
    builder.appendNewLine();
    whereForTimeStampFields(builder);
  }

  protected void whereForTimeStampFields(ExpressionBuilder builder) {
    builder.append("    ");
    if (timestampColumns.size() == 1) {
      builder.append(timestampColumns.get(0));
      builder.append(" > ?");
      return;
    }

    boolean first = true;
    for (ColumnId columnId : timestampColumns) {
      if (!first)
        builder.append(" OR ");
      builder.append(columnId);
      builder.append(" > ?");
      first = false;
    }
  }

  protected void whereForIncrementingFields(ExpressionBuilder builder) {
    whereForIncrementingFields(builder, incrementingColumns);
  }

  protected void whereForIncrementingFields(ExpressionBuilder builder, List<ColumnId> incColumns) {
    builder.append("    ( ");
    if (incColumns.size() == 1) {
      builder.append(incColumns.get(0));
      builder.append(" > ?");
      builder.append(" )");
      return;
    }

    for (int i = 0; i < incColumns.size(); i++) {
      if (i > 0)
        builder.append(" AND ");

      builder.append(incColumns.get(i));

      if (i == incColumns.size() - 1) {
        builder.append(" > ?");
      } else {
        builder.append(" = ?");
      }
    }

    builder.append(" ) OR ");
    builder.appendNewLine();
    whereForIncrementingFields(builder, incColumns.subList(0, incColumns.size() - 1));
  }

}
