package sndml.servicenow;

import java.util.Comparator;
import java.util.regex.Pattern;

/**
 * Thin wrapper for a <b>sys_id</b> (GUID).  
 * This class is used to ensure proper parameter type resolution 
 * for various methods.
 */
public class RecordKey implements Comparable<RecordKey>, Comparator<RecordKey> {

	static final Pattern pattern = Pattern.compile("[0-9a-f]{32}");
	static final int LENGTH = 32;
	
	final String value;	
	
	public RecordKey(String value) {
		assert value != null;
		this.value = value;
	}
	
	public String toString() {
		return this.value;
	}
	
	@Override
	public int compareTo(RecordKey other) {
		return this.value.compareTo(other.value);
	}
	
	@Override
	public int compare(RecordKey key1, RecordKey key2) {
		return key1.value.compareTo(key2.value);
	}
	
	@Override
	public boolean equals(Object other) {
		if (other == null) return false;
		return this.value.equals(other.toString());
	}
	
	public boolean greaterThan(RecordKey other) {
		// Note: Any value is greater than null
		if (other == null) return true;
		if (this.value.compareTo(other.value) > 0) return true;
		return false;		
	}
	
	public boolean lessThan(RecordKey other) {
		// Note: Any value is less than null
		if (other == null) return true;
		if (this.value.compareTo(other.value)< 0) return true;
		return false;
	}
	
	public int hashCode() {
		return this.value.hashCode();
	}
	
	public boolean isGUID() {
		return isGUID(value);
	}

	static public boolean isGUID(String v) {
		if (v == null) return false;
		return pattern.matcher(v).matches();
	}
	
}

