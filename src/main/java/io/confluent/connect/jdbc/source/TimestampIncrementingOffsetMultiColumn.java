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

public class TimestampIncrementingOffsetMultiColumn extends TimestampIncrementingOffsetBase<IncrementingOffset> {
	
	
	public TimestampIncrementingOffsetMultiColumn(Timestamp timestampOffset, IncrementingOffset incrementingOffset) {
		super(timestampOffset, incrementingOffset);
	}
	
	@Override
	public IncrementingOffset getIncrementingOffset() {
		return incrementingOffset == null?IncrementingOffset.EMPTY_OFFSET:incrementingOffset;
	}

	public Map<String, Object> toMap() {
	    return getIncrementingOffset().asMap();
	  }
	
	@SuppressWarnings("unchecked")
	public static TimestampIncrementingOffsetMultiColumn fromMap(Map<String, ?> map) {
			IncrementingOffset offset = null;
		    if (map == null || map.isEmpty()) {
		    	return new TimestampIncrementingOffsetMultiColumn(null, null);
			}

			Map<String,Object> incr = (Map<String,Object>) map.get(INCREMENTING_FIELD);
		    if (incr != null && incr.size() > 0 ) {
		    	offset = new IncrementingOffset();
		    	for (Map.Entry<String, Object> e : incr.entrySet()) {
		    		offset.put(e.getKey(), (Comparable<Object>)e.getValue());
				}
		    }
		    
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
		    return new TimestampIncrementingOffsetMultiColumn(ts, offset);
		  }
}
