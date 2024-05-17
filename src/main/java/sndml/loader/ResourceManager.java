package sndml.loader;

import sndml.agent.AppSession;
import sndml.util.ResourceException;

public class ResourceManager {

	static ResourceManager instance;
	
	private ConnectionProfile profile;
	private AppSession lastAppSession = null; // Last AppSession obtained
	private ReaderSession lastReaderSession = null; // last ReaderSession obtained
	
	private ResourceManager(ConnectionProfile profile) {
		instance = this;
		this.profile = profile;
	}
	
	static ResourceManager createResourceManager(ConnectionProfile profile) {
		if (instance != null)
			throw new IllegalStateException("ResourceManager already instantiated");
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
		return instance.lastReaderSession;
	}
	
	public static synchronized ReaderSession getReaderSession() throws ResourceException {
		if (instance.lastReaderSession == null) newReaderSession();
		return instance.lastReaderSession;		
	}
	
	public static synchronized AppSession newAppSession() throws ResourceException {
		instance.lastAppSession = instance.profile.newAppSession();
		return instance.lastAppSession;
	}
	
	public static synchronized AppSession getAppSession() throws ResourceException {
		if (instance.lastAppSession == null) newAppSession();
		assert instance.lastAppSession != null;
		return instance.lastAppSession;
	}
	

}
