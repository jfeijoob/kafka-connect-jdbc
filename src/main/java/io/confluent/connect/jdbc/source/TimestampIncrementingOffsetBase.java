package io.confluent.connect.jdbc.source;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TimestampIncrementingOffsetBase<T,O> {

	protected static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
	protected static final String INCREMENTING_FIELD = "incrementing";
	protected static final String TIMESTAMP_FIELD = "timestamp";
	protected static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";
	
	protected final O incrementingOffset;
	protected final T timestampOffset;

	public TimestampIncrementingOffsetBase(T timestampOffset, O incrementingOffset) {
		this.timestampOffset = timestampOffset;
		this.incrementingOffset = incrementingOffset;
	}

	public abstract O getIncrementingOffset();
	
	public abstract T getTimestampOffset();

	public boolean hasTimestampOffset() {
	    return timestampOffset != null;
	  }

	public abstract Map<String, Object> toMap();

	@Override
	public boolean equals(Object o) {
	    if (this == o) {
	      return true;
	    }
	    if (o == null || getClass() != o.getClass()) {
	      return false;
	    }
	
	    @SuppressWarnings("unchecked")
		TimestampIncrementingOffsetBase<T,O> that = (TimestampIncrementingOffsetBase<T,O>) o;
	
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
