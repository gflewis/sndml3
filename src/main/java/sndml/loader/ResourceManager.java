package sndml.loader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.AppSession;
import sndml.util.Log;
import sndml.util.PropertySet;
import sndml.util.ResourceException;

public class ResourceManager {

	static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);
	static final ResourceManager instance = new ResourceManager();
	
	private ConnectionProfile profile = null;
	private boolean app_agent = false;
	private AppSession firstAppSession = null;  
	private AppSession lastAppSession = null; 
	private ReaderSession lastReaderSession = null; 
	private ReaderSession firstReaderSession = null;
	private DatabaseConnection lastDBC = null;
	
	
	public static void  setProfile(ConnectionProfile profile) {
		if (instance.profile != null) {
			logger.warn(Log.INIT, "Profile already set");
		}
		instance.profile = profile;
	}
	
	public static ConnectionProfile getProfile() {
		assert instance.profile != null;
		return instance.profile;
	}
	
	public static void setAppAgent(boolean value) {
		instance.app_agent = value;		
	}
	
	/**
	 * Return true if this process is connected to a scoped app 
	 * in the ServiceNow instance. 
	 * 
	 * @return true if using scoped app, otherwise false
	 */	
	public static boolean isAppAgent() {
		return instance.app_agent;
	}

	public static ResourceManager getManager() {
		assert instance != null;
		return instance;
	}
	
	/** 
	 * Opens and returns a new connection to the ServiceNow instance.
	 * @return
	 */
	public static synchronized ReaderSession newReaderSession() throws ResourceException {
		PropertySet propset = instance.profile.reader;
		ReaderSession session;
		try {
			session = new ReaderSession(propset);
			if (propset.hasProperty("verify_user") || propset.hasProperty("verify_timezone"))
				session.verifyUser();
			
		} catch (IOException e) {
			// verifyUser has failed
			throw new ResourceException(e);
		}
		instance.lastReaderSession = session;
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
