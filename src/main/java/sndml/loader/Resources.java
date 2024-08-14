package sndml.loader;

import java.io.InputStream;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.AppSchemaReader;
import sndml.agent.AppSession;
import sndml.agent.WorkerPool;
import sndml.servicenow.SchemaReader;
import sndml.servicenow.TableSchemaReader;
import sndml.util.Log;
import sndml.util.PropertySet;
import sndml.util.ResourceException;

public class Resources {

	Resources parent; // null if no parent
	private ConnectionProfile profile;
	private boolean hasAppSession;
	private String agentName; // null if no agent
	private ReaderSession readerSession;
	private AppSession appSession;
	private SchemaReader schemaReader;
	private Generator generator;	
	private DatabaseWrapper dbWrapper;
	private java.sql.Connection sqlConnection;
	private WorkerPool workerPool;	

	private static final Logger logger = LoggerFactory.getLogger(Resources.class);
	
	public Resources(ConnectionProfile profile, boolean requiresAppSession) throws ResourceException {
		assert profile != null;
		this.profile = profile;
		this.agentName = profile.getAgentName(); // null if no agent
		if (requiresAppSession) profile.app.assertNotEmpty("agent");
		this.hasAppSession = profile.hasAgent();
	}
	
	public Resources(ConnectionProfile profile) throws ResourceException {
		this.profile = profile;
		this.agentName = profile.getAgentName();
		this.hasAppSession = profile.hasAgent();		
	}
	
	void setProfile(ConnectionProfile profile) throws ResourceException {
		this.profile = profile;
		this.agentName = profile.getAgentName();
		this.hasAppSession = profile.hasAgent();
		this.readerSession = null;
		this.appSession = null;
		this.schemaReader = null;
		this.generator = null;
		this.dbWrapper = null;
		this.sqlConnection = null;
		this.workerPool = null;
	}
	
	/**
	 * Return app agent name as a string or null if there is no app.
	 */
	public String getAgentName() {
		return this.agentName;
	}
		
	public ConnectionProfile getProfile() {
		return this.profile;
	}
	
	public boolean isWorkerCopy() {
		return (parent != null);
	}
	
	public ReaderSession getReaderSession() throws ResourceException {
		logger.debug(Log.INIT, "getReaderSession");
		if (this.readerSession == null) {
			// this.readerSession = profile.newReaderSession();
			this.readerSession = new ReaderSession(profile.readerProperties());
		}
		return this.readerSession;
	}
	
	public AppSession getAppSession() throws ResourceException {
		logger.debug(Log.INIT, "getAppSession");
		if (this.appSession == null) {
			// this.appSession = profile.newAppSession();
			this.appSession = new AppSession(profile.appProperties());
		}
		return this.appSession;	
	}
	
	public SchemaReader getSchemaReader() {
		logger.debug(Log.INIT, "getSchemaReader");
		if (this.schemaReader == null) {
			this.schemaReader = hasAppSession ?
					new AppSchemaReader(getAppSession()) :
					new TableSchemaReader(getReaderSession());			
			logger.info(Log.INIT, String.format(
					"schemaReader=%s", 
					schemaReader.getClass().getSimpleName()));
		}
		return schemaReader;
	}
	
	public void setSqlConnection(java.sql.Connection connection) {
		assert connection != null;
		this.sqlConnection = connection;
	}
	
	public java.sql.Connection getSqlConnection() throws ResourceException {
		assert this.sqlConnection != null;
		return this.sqlConnection;
	}
	
	public Generator getGenerator() throws ResourceException {
		logger.debug(Log.INIT, "getGenerator");	
		
		if (this.generator == null) {
			PropertySet dbprops = profile.databaseProperties();
			InputStream templatesStream = Generator.getTemplatesStream(dbprops);
			this.generator = new Generator(templatesStream, dbprops, getSchemaReader());
		}
		return this.generator;
	}
	
	public DatabaseWrapper getDatabaseWrapper() throws ResourceException {
		if (dbWrapper == null) {
			PropertySet props = profile.databaseProperties();
			try {
				if (props.hasProperty("url")) {
					dbWrapper = new DatabaseWrapper(profile, getGenerator());
					sqlConnection = dbWrapper.getConnection();
				}
				else {
					assert sqlConnection != null;
					dbWrapper = new DatabaseWrapper(sqlConnection, getGenerator(), props);								
				}
			} catch (SQLException e) {
				throw new ResourceException(e);
			}
		}
		return dbWrapper;
	}
	
	public WorkerPool getWorkerPool() throws ResourceException {
		if (this.workerPool == null) {
			int threadCount = Integer.parseInt(profile.getProperty("server.threads"));
			this.workerPool = new WorkerPool(threadCount);			
		}
		return this.workerPool;
	}
	
	/**
	 * Make a copy of these resources for use by a worker thread.
	 * Everything is set to null, so the worker has to create their own sessions.
	 */
	public Resources workerCopy() {
		Resources copy = new Resources(profile, hasAppSession);
		copy.parent = this;
		return copy;
	}
	
	public void close() throws ResourceException {
		try {
			if (dbWrapper != null) 
				dbWrapper.close();
			if (sqlConnection != null && !sqlConnection.isClosed()) 
				sqlConnection.close();
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
		sqlConnection = null;
		dbWrapper = null;
		appSession = null;
		readerSession = null;
	}

}
