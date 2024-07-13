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
	protected final boolean hasAppConnection;
	protected final ReaderSession readerSession;
	protected final AppSession appSession;
	protected final SchemaReader schemaReader;
	protected Generator generator;	
	protected DatabaseWrapper dbWrapper;
	protected java.sql.Connection sqlConnection;

	private static final Logger logger = LoggerFactory.getLogger(Resources.class);
	
	public Resources(ConnectionProfile profile, boolean hasAppConnection) throws ResourceException {
		assert profile != null;
		this.profile = profile;
		this.hasAppConnection = hasAppConnection;
		this.readerSession = profile.newReaderSession();
		this.appSession = profile.newAppSession();
		this.schemaReader = hasAppConnection ?
				new AppSchemaReader(appSession) :
				new TableSchemaReader(readerSession);
		logger.info(Log.INIT, String.format(
				"profile=%s schemaReader=%s", 
				profile.getFileName(), schemaReader.getClass().getSimpleName()));
	}
	
	public ReaderSession getReaderSession() {
		return readerSession;
	}
	
	public AppSession getAppSession() {
		return appSession;	
	}
	
	public SchemaReader getSchemaReader() {
		return schemaReader;
	}
	
	public void setSqlConnection(java.sql.Connection connection) {
		this.sqlConnection = connection;
	}
	
	public java.sql.Connection getSqlConnection() {
		assert this.sqlConnection != null;
		return this.sqlConnection;
	}
	
	public Generator getGenerator() {
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

}
