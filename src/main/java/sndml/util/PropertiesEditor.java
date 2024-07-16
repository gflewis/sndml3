package sndml.util;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesEditor {

	private static final Logger logger = LoggerFactory.getLogger(PropertiesEditor.class);

	/**
	 * Substitute environment variables found in property values
	 */
	public Properties replaceEnvVariables(Properties props) {
		StringSubstitutor envMap = 
			new org.apache.commons.text.StringSubstitutor(System.getenv());
		for (String name : props.stringPropertyNames()) {
			String value = props.getProperty(name);
			String newValue = envMap.replace(value);
			if (!newValue.equals(value)) props.setProperty(name, newValue);			
		}
		return props;		
	}
	
	/**
	 * If property value is enclosed in backtics then evaluate as a command
	 */
	public Properties replaceCommands(Properties props) throws IOException {
		final Pattern cmdPattern = Pattern.compile("^`(.+)`$");
		for (String name : props.stringPropertyNames()) {
			String value = props.getProperty(name);
			// If property is in backticks then evaluate as a command 
			Matcher cmdMatcher = cmdPattern.matcher(value); 
			if (cmdMatcher.matches()) {
				logger.info(Log.INIT, "evaluate " + name);
				String command = cmdMatcher.group(1);
				value = evaluate(command);
				if (value == null || value.length() == 0)
					throw new AssertionError(String.format("Failed to evaluate \"%s\"", command));
				logger.debug(Log.INIT, value);
				props.setProperty(name, value);
			}
		}
		return props;		
	}
	
	/**
	 * Pass a string to Runtime.exec() for evaluation
	 * @param command - Command to be executed
	 * @return Result of command with whitespace trimmed
	 * @throws IOException
	 */
	public static String evaluate(String command) throws IOException {
		Process p = Runtime.getRuntime().exec(command);
		String output = IOUtils.toString(p.getInputStream(), "UTF-8").trim();
		return output;
	}
	
}
