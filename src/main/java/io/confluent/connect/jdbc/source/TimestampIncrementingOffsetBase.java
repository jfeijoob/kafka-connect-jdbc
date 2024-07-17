package io.confluent.connect.jdbc.source;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TimestampIncrementingOffsetBase<T> {

	protected static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
	protected static final String INCREMENTING_FIELD = "incrementing";
	protected static final String TIMESTAMP_FIELD = "timestamp";
	protected static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";
	
	protected final T incrementingOffset;
	protected final Timestamp timestampOffset;

	public TimestampIncrementingOffsetBase(Timestamp timestampOffset, T incrementingOffset) {
		this.timestampOffset = timestampOffset;
		this.incrementingOffset = incrementingOffset;
	}

	public abstract T getIncrementingOffset();
	
	public Timestamp getTimestampOffset() {
	    return timestampOffset != null ? timestampOffset : new Timestamp(0L);
	  }

	public boolean hasTimestampOffset() {
	    return timestampOffset != null;
	  }

	public Map<String, Object> toMap() {
	    Map<String, Object> map = new HashMap<>(3);
	    if (incrementingOffset != null) {
	      map.put(INCREMENTING_FIELD, incrementingOffset);
	    }
	    if (timestampOffset != null) {
	      map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
	      map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
	    }
	    return map;
	  }

	@Override
	public boolean equals(Object o) {
	    if (this == o) {
	      return true;
	    }
	    if (o == null || getClass() != o.getClass()) {
	      return false;
	    }
	
	    @SuppressWarnings("unchecked")
		TimestampIncrementingOffsetBase<T> that = (TimestampIncrementingOffsetBase<T>) o;
	
	    return Objects.equals(incrementingOffset, that.incrementingOffset)
	        && Objects.equals(timestampOffset, that.timestampOffset);
	  }

	@Override
	public int hashCode() {
	    int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
	    result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
	    return result;
	  }
}
