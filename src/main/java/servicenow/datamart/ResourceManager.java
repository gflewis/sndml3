package servicenow.datamart;

import java.util.Properties;

import servicenow.core.*;

public class ResourceManager {

	private static Database db;
	private static Session session;

	/**
	 * Initialize global resources using properties.
	 */
	public static void initialize(Properties props) throws ResourceException {
		try {
			setSession(new Session(props));
			setDatabase(new Database(props));			
		}
		catch (Exception e) {
			throw new ResourceException(e);
		}
	}
	
	public static void setDatabase(Database w) {
		db = w;
	}
	
	public static void setSession(Session s) {
		session = s;
	}
	
	public static Database getDatabaseWriter() {
		assert db != null;
		return db;
	}
	
	public static Session getSession() {
		assert session != null;
		return session;
	}

}
