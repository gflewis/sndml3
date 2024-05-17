package sndml.loader;

import java.net.URISyntaxException;
import java.sql.SQLException;

import sndml.agent.AppSession;
import sndml.util.ResourceException;

public class ResourceManager {

	static ResourceManager instance;
	
	private ConnectionProfile profile;
	private AppSession firstAppSession = null;  
	private AppSession lastAppSession = null; 
	private ReaderSession lastReaderSession = null; 
	private ReaderSession firstReaderSession = null;
	private DatabaseConnection lastDBC = null;
	
	private ResourceManager(ConnectionProfile profile) {
		instance = this;
		this.profile = profile;
	}
	
	static ResourceManager init(ConnectionProfile profile) {
		if (instance != null)
			throw new IllegalStateException("ResourceManager already initialized");
		instance = new ResourceManager(profile);
		return instance;
	}
	
	public static ResourceManager getManager() {
		assert instance != null;
		return instance;
	}
	
	public static ConnectionProfile getProfile() {
		assert instance.profile != null;
		return instance.profile;
	}

	/** 
	 * Opens and returns a new connection to the ServiceNow instance.
	 * @return
	 */
	public static synchronized ReaderSession newReaderSession() throws ResourceException {
		instance.lastReaderSession = new ReaderSession(instance.profile.reader);
		if (instance.firstReaderSession == null)
			instance.firstReaderSession = instance.lastReaderSession;
		return instance.lastReaderSession;
	}
	
	public static synchronized ReaderSession getReaderSession() throws ResourceException {
		if (instance.lastReaderSession == null) newReaderSession();
		return instance.lastReaderSession;		
	}
	
	public static synchronized AppSession newAppSession() throws ResourceException {
		instance.lastAppSession = instance.profile.newAppSession();
		if (instance.firstAppSession == null)
			instance.firstAppSession = instance.lastAppSession;
		return instance.lastAppSession;
	}
	
	public static synchronized AppSession getAppSession() throws ResourceException {
		if (instance.lastAppSession == null) newAppSession();
		return instance.lastAppSession;
	}
	
	/**
	 * Opens and returns a new connection to the JDBC database.
	 * Throw an unchecked ResourceException if unsuccessful.
	 */
	public static synchronized DatabaseConnection newDatabaseConnection() throws ResourceException {
		DatabaseConnection dbc;
		try {
			dbc = new DatabaseConnection(instance.profile);
		} catch (URISyntaxException e) {
			throw new ResourceException(e);
		} catch (SQLException e) {
			throw new ResourceException(e);
		}
		instance.lastDBC = dbc;
		return dbc;
	}
	
	public static synchronized DatabaseConnection getDatabaseConnection() throws ResourceException {
		if (instance.lastDBC == null) newDatabaseConnection();
		return instance.lastDBC;
	}
	
	
}
