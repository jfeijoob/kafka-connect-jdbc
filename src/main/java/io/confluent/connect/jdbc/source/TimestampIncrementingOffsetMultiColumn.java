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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TimestampIncrementingOffsetMultiColumn extends TimestampIncrementingOffsetBase<Timestamp,IncrementingOffset> {
	
	
	public TimestampIncrementingOffsetMultiColumn(Timestamp timestampOffset, IncrementingOffset incrementingOffset) {
		super(timestampOffset, incrementingOffset);
	}
	
	@Override
	public IncrementingOffset getIncrementingOffset() {
		return incrementingOffset == null?IncrementingOffset.EMPTY_OFFSET:incrementingOffset;
	}
	
	@Override
	public Timestamp getTimestampOffset(){
	  return timestampOffset != null ? timestampOffset : new Timestamp(0);
	}

	public Map<String, Object> toMap() {    
    Map<String, Object> map = new HashMap<>(3);
    if (incrementingOffset != null) {
      for (Entry<String, Object> e : incrementingOffset.asMap().entrySet()) {
        String key = INCREMENTING_FIELD+"."+e.getKey();
        map.put(key, e.getValue());
      }
    }
    
    if (timestampOffset != null) {
      map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
      map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
    }
    
    return map;
	  }
	
	public static TimestampIncrementingOffsetMultiColumn fromMap(Map<String, ?> map) {
		  if (map == null || map.isEmpty()) {
	    	return new TimestampIncrementingOffsetMultiColumn(null, null);
		}

	  IncrementingOffset offset = new IncrementingOffset();
	  
    for (Entry<String, ?> e : map.entrySet()) {
      if( e.getKey().startsWith(INCREMENTING_FIELD+".") ) {
        String key = e.getKey().split("\\.")[1];
        offset.put(key, ((Comparable<?>)e.getValue()));
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
