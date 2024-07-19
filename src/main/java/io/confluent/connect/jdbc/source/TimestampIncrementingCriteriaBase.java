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

import java.util.ArrayList;
import java.util.Map;
import java.util.TimeZone;

import io.confluent.connect.jdbc.util.LruCache;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

public abstract class TimestampIncrementingCriteriaBase<T> {

  /**
   * The values that can be used in a statement's WHERE clause.
   */
  public interface CriteriaValues<T> {

    /**
     * Get the beginning of the time period.
     *
     * @return the beginning timestamp; may be null
     * @throws SQLException if there is a problem accessing the value
     */
    Timestamp beginTimestampValue() throws SQLException;

    /**
     * Get the end of the time period.
     *
     * @return the ending timestamp; never null
     * @throws SQLException if there is a problem accessing the value
     */
    Timestamp endTimestampValue() throws SQLException;

    /**
     * Get the last incremented value seen.
     *
     * @return the last incremented value from one of the rows
     * @throws SQLException if there is a problem accessing the value
     */
    T lastIncrementedValue() throws SQLException;
  }

  protected static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final List<ColumnId> timestampColumns;
  protected final List<ColumnId> incrementingColumns;
  protected final TimeZone timeZone;
  private final LruCache<Schema, List<String>> caseAdjustedTimestampColumns;


  public TimestampIncrementingCriteriaBase(
      List<ColumnId> incrementingColumns,
      List<ColumnId> timestampColumns,
      TimeZone timeZone
  ) {
    this.timestampColumns =
        timestampColumns != null ? timestampColumns : Collections.<ColumnId>emptyList();
    this.incrementingColumns = incrementingColumns;
    this.timeZone = timeZone;
    this.caseAdjustedTimestampColumns =
        timestampColumns != null ? new LruCache<>(16) : null;
  }

  protected boolean hasTimestampColumns() {
    return !timestampColumns.isEmpty();
  }

  protected boolean hasIncrementedColumn() {
    return incrementingColumns != null && !incrementingColumns.isEmpty() ;
  }

  /**
   * Build the WHERE clause for the columns used in this criteria.
   *
   * @param builder the string builder to which the WHERE clause should be appended; never null
   */
  public void whereClause(ExpressionBuilder builder) {
    if (hasTimestampColumns() && hasIncrementedColumn()) {
      timestampIncrementingWhereClause(builder);
    } else if (hasTimestampColumns()) {
      timestampWhereClause(builder);
    } else if (hasIncrementedColumn()) {
      incrementingWhereClause(builder);
    }
  }

  /**
   * Set the query parameters on the prepared statement whose WHERE clause was generated with the
   * previous call to {@link #whereClause(ExpressionBuilder)}.
   *
   * @param stmt   the prepared statement; never null
   * @param values the values that can be used in the criteria parameters; never null
   * @throws SQLException if there is a problem using the prepared statement
   */
  public void setQueryParameters(
      PreparedStatement stmt,
      CriteriaValues<T> values
  ) throws SQLException {
    if (hasTimestampColumns() && hasIncrementedColumn()) {
      setQueryParametersTimestampIncrementing(stmt, values);
    } else if (hasTimestampColumns()) {
      setQueryParametersTimestamp(stmt, values);
    } else if (hasIncrementedColumn()) {
      setQueryParametersIncrementing(stmt, values);
    }
  }

  protected abstract void setQueryParametersTimestampIncrementing(
      PreparedStatement stmt,
      CriteriaValues<T> values
  ) throws SQLException;
  
  protected abstract void setQueryParametersIncrementing(
      PreparedStatement stmt,
      CriteriaValues<T> values
  ) throws SQLException;
  
  protected abstract void setQueryParametersTimestamp(
      PreparedStatement stmt,
      CriteriaValues<T> values
  ) throws SQLException;

  /**
   * Extract the offset values from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @param previousOffset a previous timestamp offset if the table has timestamp columns
   * @param timestampGranularity defines the configured granularity of the timestamp field
   * @return the timestamp for this row; may not be null
   */
  public abstract TimestampIncrementingOffsetBase<T> extractValues(
      Schema schema,
      Struct record,
      TimestampIncrementingOffsetBase<T> previousOffset,
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity
  );
  /**
   * Extract the timestamp from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @param timestampGranularity defines the configured granularity of the timestamp field
   * @return the timestamp for this row; may not be null
   */
  protected Timestamp extractOffsetTimestamp(
      Schema schema,
      Struct record,
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity
  ) {
    caseAdjustedTimestampColumns.computeIfAbsent(schema, this::findCaseSensitiveTimestampColumns);
    for (String timestampColumn : caseAdjustedTimestampColumns.get(schema)) {
      Timestamp ts = timestampGranularity.toTimestamp.apply(record.get(timestampColumn), timeZone);
      if (ts != null) {
        return ts;
      }
    }
    return null;
  }

  /**
   * Extract the incrementing column value from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @return the incrementing ID for this row; may not be null
   */
  protected abstract T extractOffsetIncrementedId(
      Schema schema,
      Struct record
  );
 
  protected String coalesceTimestampColumns(ExpressionBuilder builder) {
    if (timestampColumns.size() == 1) {
      builder.append(timestampColumns.get(0));
    } else {
      builder.append("COALESCE(");
      builder.appendList().delimitedBy(",").of(timestampColumns);
      builder.append(")");
    }
    return builder.toString();
  }

  protected abstract void timestampIncrementingWhereClause(ExpressionBuilder builder);

  protected abstract void incrementingWhereClause(ExpressionBuilder builder);

  protected abstract void timestampWhereClause(ExpressionBuilder builder);

  private List<String> findCaseSensitiveTimestampColumns(Schema schema) {
    Map<String, List<String>> caseInsensitiveColumns = schema.fields().stream()
        .map(Field::name)
        .collect(Collectors.groupingBy(String::toLowerCase));

    List<String> result = new ArrayList<>();
    for (ColumnId timestampColumn : timestampColumns) {
      String columnName = timestampColumn.name();
      if (schema.field(columnName) != null) {
        log.trace(
            "Timestamp column name {} case-sensitively matches column read from database",
            columnName
        );
        result.add(columnName);
      } else {
        log.debug(
            "Timestamp column name {} not found in columns read from database; "
                + "falling back to a case-insensitive search",
            columnName
        );
        List<String> caseInsensitiveMatches = caseInsensitiveColumns.get(columnName.toLowerCase());
        if (caseInsensitiveMatches == null || caseInsensitiveMatches.isEmpty()) {
          throw new DataException("Timestamp column " + columnName + " not found in "
              + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")));
        } else if (caseInsensitiveMatches.size() > 1) {
          throw new DataException("Timestamp column " + columnName
              + " not found in columns read from database: "
              + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")) + ". "
              + "Could not fall back to case-insensitively selecting a column "
              + "because there were multiple columns whose names "
              + "case-insensitively matched the specified name: "
              + String.join(",", caseInsensitiveMatches) + ". "
              + "To force the connector to choose between these columns, "
              + "specify a value for the timestamp column configuration property "
              + "that matches the desired column case-sensitively."
          );
        } else {
          String caseAdjustedColumnName = caseInsensitiveMatches.get(0);
          log.debug(
              "Falling back on column {} for user-specified timestamp column {} "
                  + "(this is the only column that case-insensitively matches)",
              caseAdjustedColumnName,
              columnName
          );
          result.add(caseAdjustedColumnName);
        }
      }
    }

    return result;
  }

}
