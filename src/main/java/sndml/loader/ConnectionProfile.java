package sndml.loader;

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

import sndml.servicenow.Instance;
import sndml.servicenow.Session;
import sndml.util.Log;
import sndml.util.PropertySet;

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
	
	public static final String DEFAULT_APP_SCOPE = "x_108443_sndml";
	public static final String DEFAULT_AGENT_NAME = "main";

	enum SchemaSource {
		APP,    // Use app instance and {@link AppSchemaReader}
		READER, // Use reader instance and {@link TableSchemaReader}
		SCHEMA  // Use schema instance and {@link TableSchemaReader}
	}
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final File file;
	private final Properties allProperties;
	public final PropertySet reader; // Properties for ServiceNow instance that is source of data
	public final PropertySet dict; // Properties for ServiceNow instance used for schema
	public final PropertySet database; // Properties for SQL Database
	public final PropertySet agent; // Properties for ServiceNow instance that contains scopped app
	public final PropertySet loader;
	// TODO Implement AgentServer
	@Deprecated public final PropertySet httpserver;
	public final SchemaSource schemaSource;
	
	public ConnectionProfile(File profile) throws IOException {
		this.file = profile;
		this.allProperties = new Properties();
		FileInputStream stream = new FileInputStream(profile);
		this.loadWithSubstitutions(stream);
		logger.info(Log.INIT, "ConnectionProfile: " + getPathName());
		this.reader = 
			hasProperty("reader.instance") ? getSubset("reader") : getSubset("servicenow");
		this.dict =
			hasProperty("dict.instance") ? getSubset("dict") : this.reader;
		this.agent =
			hasProperty("app.agent") ? getSubset("app") :
			hasProperty("daemon.agent") ? getSubset("daemon") :
			this.reader;
		this.database = 
			hasProperty("database.url") ? getSubset("database") : getSubset("datamart");
		this.loader = getSubset("loader");
		this.httpserver = getSubset("server");
		
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
	public synchronized Session newReaderSession() throws ResourceException {
		Session session = new Session(reader);
		return session;
	}
	
	public synchronized Session newAppSession() throws ResourceException {
		Session session = null;
		if (agent.containsKey("instance"))
			session = new Session(agent);
		else if (reader.containsKey("instance"))
			session = new Session(reader);
		else
			reader.missingProperty("instance");
		return session;
	}
	
	public Instance getAppInstance() throws ResourceException {
		Instance instance = null;
		if (agent.containsKey("instance"))
			instance = new Instance(agent);
		else if (reader.containsKey("instance"))
			instance = new Instance(reader);
		else
			reader.missingProperty("instance");
		return instance;		
	}

	/**
	 * Opens and returns a new connection to the JDBC database.
	 */
	public synchronized DatabaseConnection newDatabaseConnection() throws SQLException {
		DatabaseConnection database;
		try {
			database = new DatabaseConnection(this);
		} catch (URISyntaxException e) {
			throw new ResourceException(e);
		}
		return database;
	}

	public String getAgentName() {
		return agent.getString("agent", DEFAULT_AGENT_NAME);
	}
	/**
	 * Return the URI of an API. This will be dependent on the application scope
	 * which is available from the property daemon.scope.
	 */
	public URI getAPI(String apiName) {
		return getAPI(apiName, null);
	}
	
	public URI getAPI(String apiName, String parameter) {
		Instance instance = getAppInstance();
		// ConnectionProfile profile = AgentDaemon.getConnectionProfile();
		// assert profile != null;		
		String appScope = agent.getString("scope", DEFAULT_APP_SCOPE);
		String apiPath = "api/" + appScope + "/" + apiName;
		if (parameter != null) apiPath += "/" + parameter;
		return instance.getURI(apiPath);		
	}
	
	/**
	 * Return true if this process is connected to a scoped app 
	 * in the ServiceNow instance. 
	 * 
	 * @return true if using scoped app, otherwise false
	 */	
	public boolean isAgent() {
		return Main.isAgent();
	}
	
	@Override
	/**
	 * Returns the absolute path of the properties file used to initialize this object.
	 */
	public String toString() {
		return getPathName();
	}
		
}
