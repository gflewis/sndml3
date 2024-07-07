package sndml.util;

import java.util.Properties;

/**
 * A subset of properties with a given prefix.
 * The prefix is removed when the properties are added by the constructor.
 *
 */
@SuppressWarnings("serial")
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
	
	public void assertNotEmpty(String name) throws MissingPropertyException {
		if (!hasProperty(name)) alertMissingProperty(name);
		String value = getString(name);
		if (value == null) alertMissingProperty(name);
		if (value.length() == 0) alertMissingProperty(name);
	}
	
	public void alertMissingProperty(String name) throws MissingPropertyException {
		String errmsg = String.format("missing property: %s.%s", prefix, name); 
		throw new MissingPropertyException(errmsg);		
	}
	
	public Properties getParent() {
		return this.parent;
	}
	
	public String getPrefix() {
		return this.prefix;
	}
	
	/**
	 * Return true if there is a non-blank property.
	 */
	public boolean hasProperty(String name) {
		String value = getProperty(name);
		return (value != null && value != "");
	}
		
	public String getString(String name) {
		return getProperty(name);
	}
	
	public String getNotEmpty(String name) throws MissingPropertyException {
		assertNotEmpty(name);
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
