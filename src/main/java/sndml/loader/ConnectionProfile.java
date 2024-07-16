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
	private final static PropertiesSchema schema = new PropertiesSchema();
	private final static PropertiesEditor editor = new PropertiesEditor();

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
		this.allProperties = properties;

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
