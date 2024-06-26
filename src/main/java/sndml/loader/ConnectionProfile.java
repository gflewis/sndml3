package sndml.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
//import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.AppSession;
import sndml.servicenow.Instance;
import sndml.util.Log;
import sndml.util.PropertySet;
import sndml.util.ResourceException;

/**
 * <p>A {@link ConnectionProfile} holds connection credentials 
 * which have been read from Properties file.
 * The properties are split among several {@link PropertySet} collections.</p>
 * 
 * <p>When this object is initialized,</p>
 * <ul>
 * <li><b>${name}</b> in a property value will be replaced with the the value of 
 * the corresponding system property</li>
 * <li>any value which is surrounded by backticks will be passed to <code>Runtime.exec()</code>
 * for evaluation</li>
 * </ul>
 *
 */

public class ConnectionProfile {
	
	// public static final String DEFAULT_APP_SCOPE = "x_108443_sndml";
	// public static final String DEFAULT_AGENT_NAME = "main";

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final File file;
	private final Properties allProperties;
	public final PropertySet reader; // Properties for ServiceNow instance that is source of data
	public final PropertySet dict; // Properties for ServiceNow instance used for schema
	public final PropertySet database; // Properties for SQL Database
	public final PropertySet agent; // Properties for ServiceNow instance that contains scopped app
	public final PropertySet loader; // Is this still used for anything?
	public final PropertySet server; // Properties for HTTP Server
	public final PropertySet daemon; // Properties for Agent Daemon
	static AppSession lastAppSession = null; // Last AppSession obtained
	static ReaderSession lastReaderSession = null; // last ReaderSession obtained

	enum SchemaSource {
		APP,    // Use app instance and {@link AppSchemaReader}
		READER, // Use reader instance and {@link TableSchemaReader}
		SCHEMA  // Use schema instance and {@link TableSchemaReader}
	}
	public final SchemaSource schemaSource;
	
	public ConnectionProfile(File profile) throws IOException {
		this.file = profile;
		this.allProperties = new Properties();
		FileInputStream stream = new FileInputStream(profile);
		this.loadWithSubstitutions(stream);
		logger.info(Log.INIT, "ConnectionProfile: " + getPathName());

		// This code is for backward compatiblity with old connection profiles
		this.reader = 
			hasProperty("reader.instance") ? getSubset("reader") : getSubset("servicenow");
		this.agent =
			hasProperty("app.agent") ? getSubset("app") :
			hasProperty("daemon.agent") ? getSubset("daemon") :
			this.reader;
		this.dict =
			hasProperty("dict.instance") ? getSubset("dict") : this.reader;		
		this.database = 
			hasProperty("database.url") ? getSubset("database") : getSubset("datamart");
		this.loader = getSubset("loader");
		this.server = getSubset("server");
		this.daemon = getSubset("daemon");
		
		if (hasProperty("app.instance"))
			this.schemaSource = SchemaSource.APP;
		else if (hasProperty("schema.instance"))
			this.schemaSource = SchemaSource.SCHEMA;
		else 
			this.schemaSource = SchemaSource.READER;	
	}

	public String getPathName() {
		return file.getPath();
	}

	private PropertySet getSubset(String prefix) {
		PropertySet result = new PropertySet(allProperties, prefix);
		return result;
	}

	private boolean hasProperty(String name) {
		return allProperties.containsKey(name);
	}
	
	public String getMetricsFolder() { return loader.getProperty("metrics_folder"); }
	public boolean getWarnOnTruncate() { return loader.getBoolean("warn_on_truncate", true); }
	/**
	 * Load properties from an InputStream.
	 * Any value ${name} will be replaced with a system property.
	 * Any value inclosed in backticks will be passed to Runtime.exec() for evaluation.
	 */
	public synchronized void loadWithSubstitutions(InputStream stream) throws IOException {
		final Pattern cmdPattern = Pattern.compile("^`(.+)`$");
		assert stream != null;
		Properties raw = new Properties();
		raw.load(stream);
		for (String name : raw.stringPropertyNames()) {
			String value = raw.getProperty(name);
			// Replace any environment variables
			StringSubstitutor envMap = 
					new org.apache.commons.text.StringSubstitutor(System.getenv());
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
			allProperties.setProperty(name, value);
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
	 * Return the URL of the ServiceNow
	 */
	public Instance getInstance() {
		return new Instance(reader);
	}
	
	/** 
	 * Opens and returns a new connection to the ServiceNow instance.
	 * @return
	 */
	public synchronized ReaderSession newReaderSession() throws ResourceException {
		ReaderSession session = new ReaderSession(reader);
		lastReaderSession = session;
		return session;
	}
	
	public synchronized ReaderSession getReaderSession() throws ResourceException {
		if (lastReaderSession == null) newReaderSession();
		assert lastReaderSession != null;
		return lastReaderSession;		
	}
	
	public synchronized AppSession newAppSession() throws ResourceException {
		AppSession appSession = null;
		if (agent.containsKey("instance"))
			appSession = new AppSession(agent);
		else
			agent.alertMissingProperty("instance");
		lastAppSession = appSession;
		return appSession;
	}
	
	public synchronized AppSession getAppSession() throws ResourceException {
		if (lastAppSession == null) newAppSession();
		assert lastAppSession != null;
		return lastAppSession;
	}
	
	@Deprecated
	public Instance getAppInstance() throws ResourceException {
		Instance instance = null;
		if (agent.containsKey("instance"))
			instance = new Instance(agent);
		else if (reader.containsKey("instance"))
			instance = new Instance(reader);
		else
			reader.alertMissingProperty("instance");
		return instance;		
	}

	/**
	 * Opens and returns a new connection to the JDBC database.
	 * Throw an unchecked ResourceException if unsuccessful.
	 */
	public synchronized DatabaseConnection newDatabaseConnection() {
		DatabaseConnection database;
		try {
			database = new DatabaseConnection(this);
		} catch (URISyntaxException e) {
			throw new ResourceException(e);
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
		return database;
	}

//	public int getThreadCount() {
//		return server.getInt("threads", 3);
//	}
	
	public String getAgentName() {
		return agent.getNotEmpty("agent");
	}
		
	@Override
	/**
	 * Returns the absolute path of the properties file used to initialize this object.
	 */
	public String toString() {
		return getPathName();
	}
		
}
