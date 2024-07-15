package sndml.loader;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.AppSchemaReader;
import sndml.agent.AppSession;
import sndml.servicenow.SchemaReader;
import sndml.servicenow.TableSchemaReader;
import sndml.util.Log;
import sndml.util.PropertySet;
import sndml.util.ResourceException;

public class Resources {

	protected final ConnectionProfile profile;
	protected final boolean requiresAppSession;
	protected final String agentName; // null if no agent
	protected ReaderSession readerSession;
	protected AppSession appSession;
	protected SchemaReader schemaReader;
	protected Generator generator;	
	protected DatabaseWrapper dbWrapper;
	protected java.sql.Connection sqlConnection;

	private static final Logger logger = LoggerFactory.getLogger(Resources.class);
	
	public Resources(ConnectionProfile profile, boolean requiresAppSession) throws ResourceException {
		assert profile != null;
		this.profile = profile;
		this.agentName = profile.getAgentName();
		this.requiresAppSession = requiresAppSession;
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
	
	public ReaderSession getReaderSession() throws ResourceException {
		if (this.readerSession == null) {
			this.readerSession = profile.newReaderSession();			
		}
		return this.readerSession;
	}
	
	public AppSession getAppSession() throws ResourceException {
		if (this.appSession == null) {
			this.appSession = profile.newAppSession();			
		}
		return this.appSession;	
	}
	
	public SchemaReader getSchemaReader() {
		if (this.schemaReader == null) {
			this.schemaReader = requiresAppSession ?
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
		if (this.generator == null) {
			this.generator = new Generator(profile);
		}
		return this.generator;
	}
	
	public DatabaseWrapper getDatabaseWrapper() throws ResourceException {
		if (dbWrapper == null) {
			PropertySet props = profile.databaseProperties();
			try {
				if (props.hasProperty("url")) {
					dbWrapper = new DatabaseWrapper(profile);
					generator = dbWrapper.getGenerator();
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
	
	/**
	 * Make a copy of these resources for use by a worker thread.
	 * Everything is set to null, so the worker has to create their own sessions.
	 */
	public Resources workerCopy() {
		Resources copy = new Resources(profile, requiresAppSession);
		return copy;
	}

}
