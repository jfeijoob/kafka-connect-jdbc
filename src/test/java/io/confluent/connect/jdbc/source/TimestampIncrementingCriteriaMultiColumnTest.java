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

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.TimeZone;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class TimestampIncrementingCriteriaMultiColumnTest {

  private static final TableId TABLE_ID = new TableId(null, null,"myTable");
  private static final ColumnId INCREMENTING1_COLUMN = new ColumnId(TABLE_ID, "id1");
  private static final ColumnId INCREMENTING2_COLUMN = new ColumnId(TABLE_ID, "id2");
  private static final ColumnId INCREMENTING3_COLUMN = new ColumnId(TABLE_ID, "id3");
  private static final List<ColumnId> INCREMENTING_COLUMNS = Arrays.asList(INCREMENTING1_COLUMN, INCREMENTING2_COLUMN, INCREMENTING3_COLUMN);
  private static final ColumnId TS1_COLUMN = new ColumnId(TABLE_ID, "ts1");
  private static final ColumnId TS2_COLUMN = new ColumnId(TABLE_ID, "ts2");
  private static final List<ColumnId> TS_COLUMNS = Arrays.asList(TS1_COLUMN, TS2_COLUMN);
  private static final java.sql.Timestamp TS0 = new java.sql.Timestamp(0);
  private static final java.sql.Timestamp TS1 = new java.sql.Timestamp(4761);
  private static final java.sql.Timestamp TS2 = new java.sql.Timestamp(176400L);

  private IdentifierRules rules;
  private QuoteMethod identifierQuoting;
  private ExpressionBuilder builder;
  private TimestampIncrementingCriteriaMultiColumn criteria;
  private TimestampIncrementingCriteriaMultiColumn criteriaInc;
  private TimestampIncrementingCriteriaMultiColumn criteriaTs;
  private TimestampIncrementingCriteriaMultiColumn criteriaIncTs;
  private Schema schema;
  private Struct record;
  private TimeZone utcTimeZone = TimeZone.getTimeZone(ZoneOffset.UTC);

  @Before
  public void beforeEach() {
    criteria = new TimestampIncrementingCriteriaMultiColumn(null, null, utcTimeZone, null);
    criteriaInc = new TimestampIncrementingCriteriaMultiColumn(INCREMENTING_COLUMNS, null, utcTimeZone, null);
    criteriaTs = new TimestampIncrementingCriteriaMultiColumn(null, TS_COLUMNS, utcTimeZone, null);
    criteriaIncTs = new TimestampIncrementingCriteriaMultiColumn(INCREMENTING_COLUMNS, TS_COLUMNS, utcTimeZone, null);
    identifierQuoting = null;
    rules = null;
    builder = null;
  }

  @Test
  public void createIncrementingWhereClause() {
      StringBuffer expectedResult = new StringBuffer();
      expectedResult.append(" WHERE ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    ( \"myTable\".\"id1\" = ? AND \"myTable\".\"id2\" = ? AND \"myTable\".\"id3\" > ? ) OR ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    ( \"myTable\".\"id1\" = ? AND \"myTable\".\"id2\" > ? ) OR ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    ( \"myTable\".\"id1\" > ? )");

      builder = builder();
      criteriaInc.incrementingWhereClause(builder);
   
      assertEquals(expectedResult.toString(),builder.toString());
  }

  @Test
  public void createTimestampWhereClause() {
      StringBuffer expectedResult = new StringBuffer();
      expectedResult.append(" WHERE ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    \"myTable\".\"ts1\" > ? OR \"myTable\".\"ts2\" > ?");
      
      builder = builder();
      criteriaTs.timestampWhereClause(builder);
      assertEquals(expectedResult.toString(),builder.toString());
  }

  @Test
  public void createTimestampIncrementingWhereClause() {
      StringBuffer expectedResult = new StringBuffer();
      expectedResult.append(" WHERE ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("(");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    \"myTable\".\"ts1\" > ? OR \"myTable\".\"ts2\" > ?");
      expectedResult.append(System.lineSeparator());
      expectedResult.append(")");
      expectedResult.append(System.lineSeparator());
      expectedResult.append(" AND ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("(");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    ( \"myTable\".\"id1\" = ? AND \"myTable\".\"id2\" = ? AND \"myTable\".\"id3\" > ? ) OR ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    ( \"myTable\".\"id1\" = ? AND \"myTable\".\"id2\" > ? ) OR ");
      expectedResult.append(System.lineSeparator());
      expectedResult.append("    ( \"myTable\".\"id1\" > ? )");
      expectedResult.append(System.lineSeparator());
      expectedResult.append(")");

      builder = builder();
      criteriaIncTs.timestampIncrementingWhereClause(builder);
      assertEquals(expectedResult.toString(),builder.toString());
  }

  protected ExpressionBuilder builder() {
      ExpressionBuilder result = new ExpressionBuilder(rules);
      result.setQuoteIdentifiers(identifierQuoting);
      return result;
  }
}