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

import java.sql.Timestamp;
import java.util.Map;

public class TimestampIncrementingOffset extends TimestampIncrementingOffsetBase<Long> {
  /**
   * @param timestampOffset the timestamp offset.
   *                        If null, {@link #getTimestampOffset()} will return
   *                        {@code new Timestamp(0)}.
   * @param incrementingOffset the incrementing offset.
   *                           If null, {@link #getIncrementingOffset()} will return -1.
   */
  public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset) {
	super( timestampOffset, incrementingOffset );
  }

  @Override
  public Long getIncrementingOffset() {
    return incrementingOffset == null ? -1 : incrementingOffset;
  }
 
  public static TimestampIncrementingOffset fromMap(Map<String, ?> map) {
    if (map == null || map.isEmpty()) {
    	return new TimestampIncrementingOffset(null, null);
	}

    Long incr = (Long) map.get(INCREMENTING_FIELD);
    Long millis = (Long) map.get(TIMESTAMP_FIELD);
    Timestamp ts = null;
    if (millis != null) {
      log.trace("millis is not null");
      ts = new Timestamp(millis);
      Long nanos = (Long) map.get(TIMESTAMP_NANOS_FIELD);
      if (nanos != null) {
        log.trace("Nanos is not null");
        ts.setNanos(nanos.intValue());
      }
    }
    return new TimestampIncrementingOffset(ts, incr);
  }
}
