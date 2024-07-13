package sndml.loader;

import java.sql.SQLException;

import sndml.agent.AppSchemaReader;
import sndml.agent.AppSession;
import sndml.servicenow.SchemaReader;
import sndml.servicenow.TableSchemaReader;
import sndml.util.ResourceException;
public class Resources {

	protected final ConnectionProfile profile;
	protected final boolean hasAppConnection;
	protected final ReaderSession readerSession;
	protected final AppSession appSession;
	protected final Generator generator;	
	protected final SchemaReader schemaReader;
	protected DatabaseWrapper dbWrapper;
	protected java.sql.Connection sqlConnection;
		
	public Resources(ConnectionProfile profile, boolean hasAppConnection) throws ResourceException {
		this.profile = profile;
		this.hasAppConnection = hasAppConnection;
		this.generator = new Generator(profile);
		this.readerSession = profile.newReaderSession();
		this.appSession = profile.newAppSession();
		this.schemaReader = hasAppConnection ?
				new AppSchemaReader(appSession) :
				new TableSchemaReader(readerSession);
		if (profile.database.hasProperty("url"))
			try {
				this.dbWrapper = new DatabaseWrapper(profile);
			} catch (SQLException e) {
				throw new ResourceException(e);
			}						
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
		return this.sqlConnection;
	}
	
	public Generator getGenerator() {
		return this.generator;
	}
	
	public DatabaseWrapper getDatabaseWrapper() {
		return new DatabaseWrapper(sqlConnection, generator, profile.database);
	}
	

}
