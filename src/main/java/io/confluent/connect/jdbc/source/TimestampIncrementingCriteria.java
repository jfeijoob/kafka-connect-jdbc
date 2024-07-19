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

import java.util.Arrays;
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

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

public class TimestampIncrementingCriteria extends TimestampIncrementingCriteriaBase<Long>{

  public TimestampIncrementingCriteria(
      ColumnId incrementingColumn,
      List<ColumnId> timestampColumns,
      TimeZone timeZone
  ) {
	  super( incrementingColumn == null?null:Arrays.asList(incrementingColumn),
			  timestampColumns, timeZone);	  
  }

  protected ColumnId getColumnId() {
	  return (incrementingColumns == null || 
			  incrementingColumns.isEmpty()) ? 
					  null:incrementingColumns.get(0);
  }
  
  @Override
  protected void setQueryParametersTimestampIncrementing(
      PreparedStatement stmt,
      CriteriaValues<Long> values
  ) throws SQLException {
    Timestamp beginTime = values.beginTimestampValue();
    Timestamp endTime = values.endTimestampValue();
    Long incOffset = values.lastIncrementedValue();
    stmt.setTimestamp(1, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    stmt.setTimestamp(2, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    stmt.setLong(3, incOffset);
    stmt.setTimestamp(4, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    log.debug(
        "Executing prepared statement with start time value = {} end time = {} and incrementing"
        + " value = {}", DateTimeUtils.formatTimestamp(beginTime, timeZone),
        DateTimeUtils.formatTimestamp(endTime, timeZone), incOffset
    );
  }

  @Override
  protected void setQueryParametersIncrementing(
      PreparedStatement stmt,
      CriteriaValues<Long> values
  ) throws SQLException {
    Long incOffset = values.lastIncrementedValue();
    stmt.setLong(1, incOffset);
    log.debug("Executing prepared statement with incrementing value = {}", incOffset);
  }

  @Override
  protected void setQueryParametersTimestamp(
      PreparedStatement stmt,
      CriteriaValues<Long> values
  ) throws SQLException {
    Timestamp beginTime = values.beginTimestampValue();
    Timestamp endTime = values.endTimestampValue();
    stmt.setTimestamp(1, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    stmt.setTimestamp(2, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    log.debug("Executing prepared statement with timestamp value = {} end time = {}",
        DateTimeUtils.formatTimestamp(beginTime, timeZone),
        DateTimeUtils.formatTimestamp(endTime, timeZone)
    );
  }

  /**
   * Extract the offset values from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @param previousOffset a previous timestamp offset if the table has timestamp columns
   * @param timestampGranularity defines the configured granularity of the timestamp field
   * @return the timestamp for this row; may not be null
   */
  @Override
  public TimestampIncrementingOffsetBase<Long> extractValues(
      Schema schema,
      Struct record,
      TimestampIncrementingOffsetBase<Long> previousOffset,
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity
  ) {
    Timestamp extractedTimestamp = null;
    if (hasTimestampColumns()) {
      extractedTimestamp = extractOffsetTimestamp(schema, record, timestampGranularity);
      assert previousOffset == null || (previousOffset.getTimestampOffset() != null
                                        && previousOffset.getTimestampOffset().compareTo(
          extractedTimestamp) <= 0
      );
    }
    Long extractedId = null;
    if (hasIncrementedColumn()) {
      extractedId = extractOffsetIncrementedId(schema, record);

      // If we are only using an incrementing column, then this must be incrementing.
      // If we are also using a timestamp, then we may see updates to older rows.
      assert previousOffset == null || previousOffset.getIncrementingOffset() == -1L
             || extractedId > previousOffset.getIncrementingOffset() || hasTimestampColumns();
    }
    return new TimestampIncrementingOffset(extractedTimestamp, extractedId);
  }

  /**
   * Extract the incrementing column value from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @return the incrementing ID for this row; may not be null
   */
  @Override
  protected Long extractOffsetIncrementedId(
      Schema schema,
      Struct record
  ) {
    final Long extractedId;
    final ColumnId incrementingColumn = getColumnId();
    final Field field = schema.field(incrementingColumn.name());
    if (field == null) {
      throw new DataException("Incrementing column " + incrementingColumn.name() + " not found in "
              + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")));
    }

    final Schema incrementingColumnSchema = field.schema();
    final Object incrementingColumnValue = record.get(incrementingColumn.name());
    if (incrementingColumnValue == null) {
      throw new ConnectException(
          "Null value for incrementing column of type: " + incrementingColumnSchema.type());
    } else if (isIntegralPrimitiveType(incrementingColumnValue)) {
      extractedId = ((Number) incrementingColumnValue).longValue();
    } else if (incrementingColumnSchema.name() != null && incrementingColumnSchema.name().equals(
        Decimal.LOGICAL_NAME)) {
      extractedId = extractDecimalId(incrementingColumnValue);
    } else {
      throw new ConnectException(
          "Invalid type for incrementing column: " + incrementingColumnSchema.type());
    }
    log.trace("Extracted incrementing column value: {}", extractedId);
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
    // This version combines two possible conditions. The first checks timestamp == last
    // timestamp and incrementing > last incrementing. The timestamp alone would include
    // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
    // get only the row with id = 23:
    //  timestamp 1234, id 22 <- last
    //  timestamp 1234, id 23
    // The second check only uses the timestamp > last timestamp. This covers everything new,
    // even if it is an update of the existing row. If we previously had:
    //  timestamp 1234, id 22 <- last
    // and then these rows were written:
    //  timestamp 1235, id 22
    //  timestamp 1236, id 23
    // We should capture both id = 22 (an update) and id = 23 (a new row)
	  
	ColumnId incrementingColumn = getColumnId();
    builder.append(" WHERE ");
    coalesceTimestampColumns(builder);
    builder.append(" < ? AND ((");
    coalesceTimestampColumns(builder);
    builder.append(" = ? AND ");
    builder.append(incrementingColumn);
    builder.append(" > ?");
    builder.append(") OR ");
    coalesceTimestampColumns(builder);
    builder.append(" > ?)");
    builder.append(" ORDER BY ");
    coalesceTimestampColumns(builder);
    builder.append(",");
    builder.append(incrementingColumn);
    builder.append(" ASC");
  }

  @Override
  protected void incrementingWhereClause(ExpressionBuilder builder) {
	ColumnId incrementingColumn = getColumnId();
    builder.append(" WHERE ");
    builder.append(incrementingColumn);
    builder.append(" > ?");
    builder.append(" ORDER BY ");
    builder.append(incrementingColumn);
    builder.append(" ASC");
  }
  
  @Override
  protected void timestampWhereClause(ExpressionBuilder builder) {
	    builder.append(" WHERE ");
	    coalesceTimestampColumns(builder);
	    builder.append(" > ? AND ");
	    coalesceTimestampColumns(builder);
	    builder.append(" < ? ORDER BY ");
	    coalesceTimestampColumns(builder);
	    builder.append(" ASC");
	  }

}
