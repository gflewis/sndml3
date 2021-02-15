package sndml.datamart;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.Log;
import sndml.servicenow.Session;

/**
 * <p>A {@link ConnectionProfile} holds connection credentials for a ServiceNow instance
 * and a JDBC database as read from Properties file.</p>
 * 
 * <p>When this object is initialized,
 * and value which is enclosed in backticks will be passed to <tt>Runtime.exec()</tt>
 * for evaluation.</p>
 *
 */
public class ConnectionProfile {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final Properties properties = new Properties();
	private String pathname; // file used to initialize this object
	private Session session = null; // initialized on request
	private Database database = null; // initialized on request
	
	public ConnectionProfile(File profile) throws IOException {
		pathname = profile.getPath();
		logger.info(Log.INIT, "ConnectionProfile: " + pathname);
		loadProperties(new FileInputStream(profile));
	}

	/**
	 * Load properties from an InputStream into this {@link ConnectionProfile}.
	 * Any value which is inclosed in backticks will be passed to Runtime.exec()
	 * for evaluation.
	 * 
	 */
	void loadProperties(InputStream stream) throws IOException {
		assert stream != null;
		Pattern cmdPattern = Pattern.compile("^`(.+)`$");
		Properties raw = new Properties();
		raw.load(stream);
		for (String name : raw.stringPropertyNames()) {
			String value = raw.getProperty(name);
			// Replace any environment variables
			StringSubstitutor envMap = new StringSubstitutor(System.getenv());
			value = envMap.replace(value);
			// If property is in backticks then evaluate as a command 
			Matcher cmdMatcher = cmdPattern.matcher(value); 
			if (cmdMatcher.matches()) {
				logger.info(Log.INIT, "evaluate " + name);
				String command = cmdMatcher.group(1);
				value = evaluate(command);
				if (value == null || value.length() == 0)
					throw new AssertionError(String.format("Failed to evaluate \"%s\"", command));
				logger.debug(Log.INIT, value);
			}
			properties.setProperty(name, value);			
		}
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

	/**
	 * Return all properties stored in this object.
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Return a property value.
	 */
	public String getProperty(String name) {
		return properties.getProperty(name);
	}
	
	public String getProperty(String name, String defaultValue) {
		String value = getProperty(name);
		return value == null ? defaultValue : value;
	}
	
	public boolean getPropertyBoolean(String name, boolean defaultValue) {
		String stringValue = getProperty(name);
		if (stringValue == null) return defaultValue;
		return new Boolean(stringValue);
	}
	
	public int getPropertyInt(String name, int defaultValue) {
		String stringValue = getProperty(name);
		if (stringValue == null) return defaultValue;
		return new Integer(stringValue);
	}
	
	/** 
	 * Opens and returns a connection to the ServiceNow instance.
	 * If a connection has already been opened, then it will be returned.
	 * @return
	 */
	public Session getSession() throws ResourceException {
		if (session == null) {
			try {
				session = new Session(properties);
			} catch (IOException e) {
				throw new ResourceException(e);
			}
		}
		return session;
	}

	/**
	 * Opens and returns a connection to the JDBC database.
	 * If a connection has already been opened, then it will be returned.
	 * If {@link #close()} has been called, then a new connection will be opened and returned.
	 */
	public Database getDatabase() throws ResourceException {
		if (database == null) {
			try {
				database = new Database(properties);
			} catch (SQLException | URISyntaxException e) {
				throw new ResourceException(e);
			}
		}
		return database;
	}
	
	/**
	 * Closes the connection to the {@link Database}. 
	 */
	public void close() {
		logger.info(Log.FINISH, "Close profile " + pathname);
		try {
			if (database != null) database.close();
			if (session != null) session.close();
		} catch (IOException | SQLException e) {
			logger.error(Log.FINISH, "ConnectionProfile.close() failure");;
		}
		database = null;
		session = null;
	}
	
	@Override
	/**
	 * Returns the absolute path of the properties file used to initialize this object.
	 */
	public String toString() {
		return pathname;
	}
		
}
