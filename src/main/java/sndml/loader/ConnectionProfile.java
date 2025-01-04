package sndml.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

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
		
	private final Properties allProperties;
	private final String pathName; // null if unknown
	public final PropertySet app; // Properties for ServiceNow instance that contains scopped app
	public final PropertySet reader; // Properties for ServiceNow instance that is source of data
	public final PropertySet dict; // Properties for ServiceNow instance used for schema
	public final PropertySet database; // Properties for SQL Database
	private final PropertySet loader; // Is this still used for anything?
	private final PropertySet server; // Properties for HTTP Server
	private final PropertySet daemon; // Properties for Agent Daemon
	private final PropertiesSchema schema;

	static AppSession lastAppSession = null; // Last AppSession obtained
	static ReaderSession lastReaderSession = null; // last ReaderSession obtained

	private static final Logger logger = LoggerFactory.getLogger(ConnectionProfile.class);
	
	// TODO move SchemaSource implementation to Resources
	enum SchemaSource {
		APP,    // Use app instance and {@link AppSchemaReader}
		READER, // Use reader instance and {@link TableSchemaReader}
		SCHEMA  // Use schema instance and {@link TableSchemaReader}
	}
	public final SchemaSource schemaSource;
		
	public ConnectionProfile(Properties properties) {
		this.schema = new PropertiesSchema();
		this.allProperties = properties;
		this.pathName = null;
		
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
	
	public ConnectionProfile(File file) throws ResourceException {
		this.schema = new PropertiesSchema();
		this.allProperties = propertiesFromFile(file, schema);
		this.pathName = file.getPath();
		
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

	private static Properties propertiesFromFile(File file, PropertiesSchema schema) throws ResourceException {
		logger.info(Log.INIT, "ConnectionProfile: " + file.getPath());
		PropertiesEditor editor = new PropertiesEditor();
		Properties newProperties;
		try {
			FileInputStream inputStream = new FileInputStream(file);
			Properties origProperties = new Properties();
			origProperties.load(inputStream);				
			newProperties = schema.replacePropertyNames(origProperties, true);
			editor.replaceEnvVariables(newProperties);
			editor.replaceCommands(newProperties);			
			if (logger.isDebugEnabled()) dump(newProperties, null);
		}
		catch (IOException | JDOMException e) {
			throw new ResourceException(e);
		}
		return newProperties;
	}
	
	public boolean isValidName(String name) {
		return schema.hasName(name);
	}

	/*
	 * Return a property value from the profile.
	 * If name is not in property_names.xml then throw IllegalArgumentException.
	 * If value is not found, then return default value from property_names.xml.
	 */
	public String getProperty(String name) {
		if (!isValidName(name)) 
			throw new IllegalArgumentException("Invalid property name: " + name);
		String value = allProperties.getProperty(name);
		if (value == null)
			if (schema.hasDefault(name)) value = schema.getDefault(name);
		return value;
	}
		
	private boolean hasProperty(String name) {
		return allProperties.containsKey(name);
	}
	
	public PropertySet appProperties() {
		return this.app; 
	}
	
	@Deprecated
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
	
	@Deprecated
	public PropertySet serverProperties() { 
		return this.server; 
	}
	
	private PropertySet getSubset(String prefix) {
		PropertySet result = new PropertySet(
				allProperties, prefix, schema.getValidNames(), schema.getDefaultValues());
		return result;
	}

	public String getMetricsFolder() { 
		return loader.getProperty("metrics_folder"); 
	}

	/**
	 * Return the profile name if it is known, otherwise null
	 */
	public String getFileName() {
		return pathName;
	}
	
	/**
	 * Return the URL of the ServiceNow
	 */
	public Instance getInstance() {
		return new Instance(reader);
	}
	
	/** 
	 * Opens and returns a new connection to the ServiceNow instance.
	 * Used for JUnit tests.
	 */
	public synchronized ReaderSession newReaderSession() throws ResourceException {
		ReaderSession session = new ReaderSession(reader);
		lastReaderSession = session;
		return session;
	}
	
	@Deprecated
	public synchronized ReaderSession getReaderSession() throws ResourceException {
		if (lastReaderSession == null) newReaderSession();
		assert lastReaderSession != null;
		return lastReaderSession;		
	}
	
	@Deprecated
	public synchronized AppSession newAppSession() throws ResourceException {
		logger.info(Log.INIT, "newAppSession deprecated");		
		AppSession appSession = null;
		if (app.containsKey("instance"))
			appSession = new AppSession(app);
		else
			app.alertMissingProperty("instance");
		lastAppSession = appSession;
		return appSession;
	}
	
	@Deprecated
	public synchronized AppSession getAppSession() throws ResourceException {
		logger.info(Log.INIT, "getAppSession deprecated");
		if (lastAppSession == null) newAppSession();
		assert lastAppSession != null;
		return lastAppSession;
	}
	
	@Deprecated
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
	@Deprecated
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

	public static final String APP_AGENT_PROPERTY = "app.agent";
	
	public boolean hasAgent() {
		return hasProperty(APP_AGENT_PROPERTY);		
	}
	
	static private String agentName = null;
	public String getAgentName() {
		if (agentName == null && hasProperty(APP_AGENT_PROPERTY)) 
			agentName = getProperty(APP_AGENT_PROPERTY);
		return agentName;
	}

	/**
	 * Get the size of the worker pool.
	 */
	public int getThreadCount() {
		return Integer.parseInt(getPropertyNotNull("server.threads"));
	}
	
	public String getPidFileName() {
		return getProperty("server.pidfile");
	}
	
	public int getInterval() {
		return Integer.parseInt(getPropertyNotNull("daemon.interval"));
	}
	
	protected String getPropertyNotNull(String name) {
		String value = getProperty(name);
		if (value == null) throw new AssertionError("No value for property: " + name);
		return value;
	}
	
	/**
	 * Print all property names and values to the logger.
	 * Used for debugging.
	 */
	private static void dump(Properties props, String label) {
		if (label != null) logger.debug(Log.INIT, label);
		SortedSet<String> names = new TreeSet<String>(props.stringPropertyNames());
		for (String name : names) {
			String value = props.getProperty(name);
			logger.debug(Log.INIT, name + "=" + value);
		}
	}

}
