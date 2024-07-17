package io.confluent.connect.jdbc.source;

import java.util.LinkedHashMap;
import java.util.Map;

public class IncrementingOffset implements Comparable<IncrementingOffset> {
	
	public static final IncrementingOffset EMPTY_OFFSET = new IncrementingOffset(); 
	
	protected LinkedHashMap<String, Comparable<?>> offset;
	
	public IncrementingOffset() {
		offset = new LinkedHashMap<String, Comparable<?>>();
	}

	public void put( String key, Comparable<Object> value ) {
		offset.put(key, value);
	}
	
	public Map<String, Object> asMap(){
		LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
		for (Map.Entry<String, Comparable<?>> entry : offset.entrySet()) {
			result.put( entry.getKey(), entry.getValue());
		}
		return result;
	}
	
	public Comparable<?> getValue( String key ) {
		return offset.get(key);
	}

	@Override
	public int compareTo(IncrementingOffset o) {
		int result = 0; 
		for (Map.Entry<String, Comparable<?>> entry : offset.entrySet()) {
			@SuppressWarnings("unchecked")
			Comparable<Object> myValue = (Comparable<Object>) entry.getValue();
			@SuppressWarnings("unchecked")
			Comparable<Object> otherValue = (Comparable<Object>) o.getValue(entry.getKey());
			result = myValue.compareTo(otherValue);
			if( result != 0 ) {
				break;
			}
		}
		
		return result;
	}
}
