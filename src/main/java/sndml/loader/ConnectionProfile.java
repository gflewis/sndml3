package sndml.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.jdom2.JDOMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.AppSchemaReader;
import sndml.agent.AppSession;
import sndml.servicenow.Instance;
import sndml.servicenow.SchemaReader;
import sndml.servicenow.TableSchemaReader;
import sndml.util.Log;
import sndml.util.PropertiesEditor;
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

//	private File file = null;
	private String pathName = null;
	
	private final Properties allProperties;
	public final PropertySet app; // Properties for ServiceNow instance that contains scopped app
	public final PropertySet reader; // Properties for ServiceNow instance that is source of data
	public final PropertySet dict; // Properties for ServiceNow instance used for schema
	public final PropertySet database; // Properties for SQL Database
	private final PropertySet loader; // Is this still used for anything?
	private final PropertySet server; // Properties for HTTP Server
	private final PropertySet daemon; // Properties for Agent Daemon
	static AppSession lastAppSession = null; // Last AppSession obtained
	static ReaderSession lastReaderSession = null; // last ReaderSession obtained

	private static final Logger logger = LoggerFactory.getLogger(ConnectionProfile.class);
	
	enum SchemaSource {
		APP,    // Use app instance and {@link AppSchemaReader}
		READER, // Use reader instance and {@link TableSchemaReader}
		SCHEMA  // Use schema instance and {@link TableSchemaReader}
	}
	public final SchemaSource schemaSource;
	
	/*
	public ConnectionProfile(File profile) throws IOException {
		this(propertiesWithSubstitutions(profile));
		this.file = profile;
		this.pathName = file.getPath();
		logger.info(Log.INIT, "ConnectionProfile: " + pathName);
	}
	*/
	
	public ConnectionProfile(File profile) throws ResourceException {
		this(propertiesFromFile(profile));
		this.pathName = profile.getPath();
	}

	private static Properties propertiesFromFile(File profile) throws ResourceException {
		logger.info(Log.INIT, "ConnectionProfile: " + profile.getPath());
		Properties newProperties;
		try {
			FileInputStream inputStream = new FileInputStream(profile);
			Properties origProperties = new Properties();
			origProperties.load(inputStream);				
			newProperties = PropertiesEditor.replacePropertyNames(origProperties, true);
			PropertiesEditor.replaceEnvVariables(newProperties);
			PropertiesEditor.replaceCommands(newProperties);			
		}
		catch (IOException e) {
			throw new ResourceException(e);
		}
		catch (JDOMException e) {
			throw new ResourceException(e);
		}
		return newProperties;
	}
	
	public ConnectionProfile(Properties properties) {
		this.allProperties = properties;

		// This code is for backward compatiblity with old connection profiles
		/*
		this.reader = 
			hasProperty("reader.instance") ? getSubset("reader") : getSubset("servicenow");
		this.app =
			hasProperty("app.agent") ? getSubset("app") :
			hasProperty("daemon.agent") ? getSubset("daemon") :
			this.reader;
		this.dict =
			hasProperty("dict.instance") ? getSubset("dict") : this.reader;		
		this.database = 
			hasProperty("database.url") ? getSubset("database") : getSubset("datamart");
		*/
		this.app      = getSubset("app");
		this.reader   = getSubset("reader");
		this.database = getSubset("database");
		this.dict     = getSubset("dict");
		this.loader   = getSubset("loader");
		this.server   = getSubset("server");
		this.daemon   = getSubset("daemon");
		
		if (hasProperty("app.instance"))
			this.schemaSource = SchemaSource.APP;
		else if (hasProperty("schema.instance"))
			this.schemaSource = SchemaSource.SCHEMA;
		else 
			this.schemaSource = SchemaSource.READER;			
	}
	
	public PropertySet appProperties() {
		return this.app; 
	}
	
	public PropertySet daemonProperties() {
		return this.daemon; 
	}
	public PropertySet databaseProperties() {
		return this.database; 
	}
	
	public PropertySet dictProperties() {
		return this.dict; 
	}
	
	public PropertySet loaderProperties() {
		return this.loader; 	
	}
	
	public PropertySet readerProperties() { 
		return this.reader; 
	}
	
	public PropertySet serverProperties() { 
		return this.server; 
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
	 * Return the profile name if it is known, otherwise null
	 */
	public String getFileName() {
		return pathName;
	}

	/*
	private static Properties propertiesWithSubstitutions(File profile) throws IOException {
		FileInputStream stream = new FileInputStream(profile);
		Properties properties = propertiesWithSubstitutions(stream);
		return properties; 		
	}
	
	private static Properties propertiesWithSubstitutions(InputStream stream) throws IOException {
		Properties properties = new Properties();
		loadWithSubstitutions(properties, stream);
		return properties;
	}
	

	/**
	 * Load properties from an InputStream.
	 * Any value ${name} will be replaced with a system property.
	 * Any value inclosed in backticks will be passed to Runtime.exec() for evaluation.
	 *
	private static void loadWithSubstitutions(Properties properties, InputStream stream) throws IOException {
		final Pattern cmdPattern = Pattern.compile("^`(.+)`$");
		assert stream != null;
		StringSubstitutor envMap = 
			new org.apache.commons.text.StringSubstitutor(System.getenv());
		Properties raw = new Properties();
		raw.load(stream);
		for (String name : raw.stringPropertyNames()) {
			String value = raw.getProperty(name);
			// Replace any environment variables
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
	 *
	public static String evaluate(String command) throws IOException {
		Process p = Runtime.getRuntime().exec(command);
		String output = IOUtils.toString(p.getInputStream(), "UTF-8").trim();
		return output;
	}
	*/
		
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
		if (app.containsKey("instance"))
			appSession = new AppSession(app);
		else
			app.alertMissingProperty("instance");
		lastAppSession = appSession;
		return appSession;
	}
	
	public synchronized AppSession getAppSession() throws ResourceException {
		if (lastAppSession == null) newAppSession();
		assert lastAppSession != null;
		return lastAppSession;
	}
	
	public synchronized SchemaReader newSchemaReader() {
		SchemaReader result;
		result = hasAgent() ?
			new AppSchemaReader(getAppSession()) :
			new TableSchemaReader(getReaderSession());
		return result;		
	}

	@Deprecated
	public Instance getAppInstance() throws ResourceException {
		Instance instance = null;
		if (app.containsKey("instance"))
			instance = new Instance(app);
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
	public synchronized DatabaseWrapper newDatabaseWrapper() {
		DatabaseWrapper database;
		try {
			database = new DatabaseWrapper(this);
//		} catch (URISyntaxException e) {
//			throw new ResourceException(e);
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
		return database;
	}

	private final int DEFAULT_THREAD_COUNT = 3;
	
	public int getThreadCount() {
		return server.getInt("threads", DEFAULT_THREAD_COUNT);
	}
	
	public boolean hasAgent() {
		return app.hasProperty("agent");
		
	}
	
	public String getAgentName() {
		return app.getProperty("agent");
	}
				
}
