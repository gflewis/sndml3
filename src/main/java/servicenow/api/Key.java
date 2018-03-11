package servicenow.api;

import java.util.regex.Pattern;

/**
 * Thin wrapper for a <b>sys_id</b> (GUID).  
 * This class is used to ensure proper parameter type resolution 
 * for various methods.
 */
public final class Key {

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
	
	public boolean equals(Object other) {
		return this.value.equals(other.toString());
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

