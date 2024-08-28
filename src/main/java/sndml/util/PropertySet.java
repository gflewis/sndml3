package sndml.util;

import java.util.Collection;
import java.util.Properties;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A subset of properties with a given prefix.
 * The prefix is removed when the properties are added by the constructor.
 *
 */
@SuppressWarnings("serial")
public class PropertySet extends java.util.Properties {

	private final Properties parent;	
	private final String prefix;
	private final Collection<String> validNames;
	
	private static final Logger logger = LoggerFactory.getLogger(PropertySet.class);
	
	public PropertySet(Properties parent, String prefix, 
			Collection<String> validNames, Properties parentDefaults) {
		super();
		logger.debug(Log.INIT, String.format("propertyset %s", prefix));
		assert parent != null;
		assert prefix != null && prefix.length() > 0;
		assert parentDefaults != null;
		this.parent = parent;
		this.prefix = prefix;
		for (String parentKey : parent.stringPropertyNames()) {
			String parts[] = parentKey.split("\\.", 2);
			if (parts.length == 2) {
				if (prefix.equals(parts[0])) {					
					String key = parts[1];
					this.setProperty(key, parent.getProperty(parentKey));
				}
			}
		}
		this.validNames = new TreeSet<String>();
		for (String parentKey : validNames) {
			String parts[] = parentKey.split("\\.", 2);
			if (parts.length == 2) {
				if (prefix.equals(parts[0])) {					
					String key = parts[1];
					this.validNames.add(key);
				}
			}
		}
		for (String parentKey : parentDefaults.stringPropertyNames()) {
			String parts[] = parentKey.split("\\.", 2);
			if (parts.length == 2) {
				if (prefix.equals(parts[0])) {
					String key = parts[1];
					if (!this.containsKey(key)) {
						String defaultValue = parentDefaults.getProperty(parentKey);
						logger.debug(Log.INIT, String.format("default %s=%s", key, defaultValue));
						this.setProperty(key, defaultValue);
					}
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
	
	public boolean isValidName(String name) {
		return validNames.contains(name);
	}
	
	@Override
	public String getProperty(String name) {
		if (!isValidName(name)) 
			throw new IllegalArgumentException("Invalid property name: " + getFullName(name));
		return super.getProperty(name);		
	}
	
	public String getFullName(String name) {
		return prefix + "." + name;
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
