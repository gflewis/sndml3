package sndml.util;

import java.util.Properties;

@SuppressWarnings("serial")
/**
 * A subset of properties with a given prefix (with the prefix removed)
 *
 */
public class PropertySet extends java.util.Properties {

	private final Properties parent;
	private final String prefix;

	public PropertySet(Properties parent, String prefix) {
		super();
		assert prefix != null && prefix.length() > 0;
		this.parent = parent;
		this.prefix = prefix;
		for (String key : parent.stringPropertyNames()) {
			String parts[] = key.split("\\.", 2);
			if (parts.length == 2) {
				if (prefix.equals(parts[0])) {
					this.setProperty(parts[1], parent.getProperty(key));
				}
			}			
		}		
	}
		
	public Properties getParent() {
		return this.parent;
	}
	
	public String getPrefix() {
		return this.prefix;
	}
	
	public boolean hasProperty(String name) {
		return containsKey(name);
	}
		
	public String getString(String name) {
		return getProperty(name);
	}

	public String getString(String name, String defaultValue) {
		return getProperty(name, defaultValue);
	}
		
	public boolean getBoolean(String name, boolean defaultValue) {
		String stringValue = getProperty(name);
		if (stringValue == null) return defaultValue;
		return Boolean.valueOf(stringValue);
	}

	public int getInt(String name, int defaultValue) {
		String stringValue = getProperty(name);
		if (stringValue == null) return defaultValue;
		return Integer.valueOf(stringValue);
	}
	
}
