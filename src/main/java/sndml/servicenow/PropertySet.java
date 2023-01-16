package sndml.servicenow;

import java.util.Properties;

@SuppressWarnings("serial")
/**
 * A subset of properties with a given prefix (with the prefix removed)
 *
 */
public class PropertySet extends java.util.Properties {

	private final Properties parent;
	private final String prefix;
//	private final static Logger logger = LoggerFactory.getLogger(PropertySet.class);

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

//	@Deprecated
//	private void addSubset(Properties parent, String prefix) {
//		assert prefix != null && prefix.length() > 0;
//		for (String key : parent.stringPropertyNames()) {
//			String parts[] = key.split("\\.", 2);
//			if (parts.length == 2) {
//				if (prefix.equals(parts[0])) {
//					this.setProperty(parts[1], parent.getProperty(key));
//				}
//			}			
//		}		
//	}
	
	/**
	 * Load properties from an InputStream.
	 * Any value ${name} will be replaced with a system property.
	 * Any value inclosed in backticks will be passed to Runtime.exec() for evaluation.
	 */
//	@Deprecated
//	private synchronized void loadWithSubstitutions(InputStream stream) throws IOException {
//		final Pattern cmdPattern = Pattern.compile("^`(.+)`$");
//		assert stream != null;
//		Properties raw = new Properties();
//		raw.load(stream);
//		for (String name : raw.stringPropertyNames()) {
//			String value = raw.getProperty(name);
//			// Replace any environment variables
//			StringSubstitutor envMap = 
//					new org.apache.commons.text.StringSubstitutor(System.getenv());
//			value = envMap.replace(value);
//			// If property is in backticks then evaluate as a command 
//			Matcher cmdMatcher = cmdPattern.matcher(value); 
//			if (cmdMatcher.matches()) {
//				logger.info(Log.INIT, "evaluate " + name);
//				String command = cmdMatcher.group(1);
//				value = evaluate(command);
//				if (value == null || value.length() == 0)
//					throw new AssertionError(String.format("Failed to evaluate \"%s\"", command));
//				logger.debug(Log.INIT, value);
//			}
//			this.setProperty(name, value);
//		}
//	}
//
//	/**
//	 * Pass a string to Runtime.exec() for evaluation
//	 * @param command - Command to be executed
//	 * @return Result of command with whitespace trimmed
//	 * @throws IOException
//	 */
//	public static String evaluate(String command) throws IOException {
//		Process p = Runtime.getRuntime().exec(command);
//		String output = IOUtils.toString(p.getInputStream(), "UTF-8").trim();
//		return output;
//	}

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
