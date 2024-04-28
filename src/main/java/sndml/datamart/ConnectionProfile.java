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

import sndml.agent.AgentDaemon;
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
@SuppressWarnings("serial")
public class ConnectionProfile extends java.util.Properties {

	enum SchemaSource {
		APP,    // Use app instance and {@link AppSchemaReader}
		READER, // Use reader instance and {@link TableSchemaReader}
		SCHEMA  // Use schema instance and {@link TableSchemaReader}
	}
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final File profile;
	public final PropertySet reader; // Properties for ServiceNow instance that is source of data
	public final PropertySet schema; // Properties for ServiceNow instance used for schema
	public final PropertySet app;    // Properties for ServiceNow instance that contains scopped app
	public final PropertySet database; // Properties for SQL Database
	public final PropertySet daemon; 
	public final PropertySet loader;
	public final SchemaSource schemaSource;
	@Deprecated public final PropertySet httpserver;
	
	public ConnectionProfile(File profile) throws IOException {
		this.profile = profile;
		FileInputStream stream = new FileInputStream(profile);
		this.loadWithSubstitutions(stream);
		logger.info(Log.INIT, "ConnectionProfile: " + getPathName());
		this.reader = 
			hasProperty("reader.instance") ? getSubset("reader") : getSubset("servicenow");
		this.schema =
			hasProperty("schema.instance") ? getSubset("schema") : this.reader;
		this.app = 
			hasProperty("app.instance") ? getSubset("app") : this.reader;
		this.database = 
			hasProperty("database.url") ? getSubset("database") : getSubset("datamart");
		if (hasProperty("app.instance"))
			this.schemaSource = SchemaSource.APP;
		else if (hasProperty("schema.instance"))
			this.schemaSource = SchemaSource.SCHEMA;
		else 
			this.schemaSource = SchemaSource.READER;
		
		this.loader = getSubset("loader");
		this.daemon = getSubset("daemon");			
		this.httpserver = getSubset("http"); // HTTP Server
		
	}

	public String getPathName() {
		return profile.getPath();
	}

	public PropertySet getSubset(String prefix) {
		PropertySet result = new PropertySet(this, prefix);
		return result;
	}

	public boolean hasProperty(String name) {
		return containsKey(name);
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
			this.setProperty(name, value);
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
	public synchronized Session getReaderSession() throws ResourceException {
		Session session = new Session(reader);
		return session;
	}
	
	public synchronized Session getAppSession() throws ResourceException {
		Session session = new Session(app);
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
		Instance instance = new Instance(app.getString("instance"));
		ConnectionProfile profile = AgentDaemon.getConnectionProfile();
		assert profile != null;		
		String appScope = daemon.getString("scope", "x_108443_sndml");
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
