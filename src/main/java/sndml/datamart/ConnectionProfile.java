package sndml.datamart;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.daemon.AgentDaemon;
import sndml.servicenow.Instance;
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
	private final File profile;
	private final Properties properties = new Properties();
	private long lastModified;
	private final Pattern cmdPattern = Pattern.compile("^`(.+)`$");
	
	public ConnectionProfile(File profile) throws IOException {
		this.profile = profile;
		logger.info(Log.INIT, "ConnectionProfile: " + getPathName());
		this.loadProperties();
	}

	public String getPathName() {
		return profile.getPath();
	}
	
	/**
	 * Return true if the file has been modified since it was loaded.
	 * @return
	 */
	public boolean hasChanged() {
		return (profile.lastModified() > this.lastModified);	
	}
	
	/**
	 * Reload all properties if the file has been changed.
	 */
	public synchronized void reloadIfChanged() throws IOException {
		if (hasChanged()) {
			logger.info(Log.INIT, "Reloading " + getPathName());
			loadProperties();
		}		
	}
	
	synchronized void loadProperties() throws IOException {
		this.loadProperties(new FileInputStream(profile));
		this.lastModified = profile.lastModified();		
	}
	
	/**
	 * Load properties from an InputStream into this {@link ConnectionProfile}.
	 * Any value which is inclosed in backticks will be passed to Runtime.exec()
	 * for evaluation.
	 * Any property which is not a password will become a System property
	 * with a "sndml." prefix.
	 */
	synchronized void loadProperties(InputStream stream) throws IOException {
		assert stream != null;
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

	public boolean hasProperty(String name) {
		return properties.getProperty(name) != null;
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
		return Boolean.valueOf(stringValue);
	}
	
	public int getPropertyInt(String name, int defaultValue) {
		String stringValue = getProperty(name);
		if (stringValue == null) return defaultValue;
		return Integer.valueOf(stringValue);
	}
	
	/**
	 * Return the URL of the ServiceNow
	 */
	public Instance getInstance() {
		return new Instance(properties);
	}
	
	/** 
	 * Opens and returns a new connection to the ServiceNow instance.
	 * @return
	 */
	public synchronized Session getSession() throws ResourceException {
		Session session;
		try {
			session = new Session(properties);
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		return session;
	}

	/**
	 * Opens and returns a new connection to the JDBC database.
	 */
	public synchronized Database getDatabase() throws SQLException {
		Database database;
		try {
			database = new Database(this);
		} catch (URISyntaxException e) {
			throw new ResourceException(e);
		}
		return database;
	}

	/**
	 * Return the URI of an API. This will be dependent on the application scope
	 * which is available from the property daemon.scope.
	 */
	public URI getAPI(String apiName) {
		return getAPI(apiName, null);
	}
	
	public URI getAPI(String apiName, String parameter) {
		Instance instance = new Instance(getProperty("servicenow.instance"));
		ConnectionProfile profile = AgentDaemon.getConnectionProfile();
		assert profile != null;		
		String appScope = getProperty("daemon.scope", "x_108443_sndml");
		String apiPath = "api/" + appScope + "/" + apiName;
		if (parameter != null) apiPath += "/" + parameter;
		return instance.getURI(apiPath);		
	}
		
	@Override
	/**
	 * Returns the absolute path of the properties file used to initialize this object.
	 */
	public String toString() {
		return getPathName();
	}
		
}
