package servicenow.api;

import java.util.Comparator;
import java.util.regex.Pattern;

/**
 * Thin wrapper for a <b>sys_id</b> (GUID).  
 * This class is used to ensure proper parameter type resolution 
 * for various methods.
 */
public final class Key implements Comparator<Key> {

	static final Pattern pattern = Pattern.compile("[0-9a-f]{32}");
	static final int LENGTH = 32;
	
	final String value;	
	
	public Key(String value) {
		assert value != null;
		this.value = value;
	}
	
	public String toString() {
		return this.value;
	}
	
	public boolean equals(Key other) {
		if (other == null) return false;
		return this.value.equals(other.value);
	}
	
	public boolean greaterThan(Key other) {
		if (other == null) return true;
		if (this.value.compareTo(other.value) > 0) return true;
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

	public int compare(Key key1, Key key2) {
		return key1.value.compareTo(key2.value);
	}
	
}

