package sndml.servicenow;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;

import sndml.agent.AppSession;
import sndml.util.Log;
import sndml.util.Parameters;
import sndml.util.PropertySet;
import sndml.util.ResourceException;

/**
 * Holds a ServiceNow URL, connection credentials, a cookie store with a session ID
 * and a cache of {@link TableSchema} definitions.
 */
public class Session {

	protected final Instance instance;
	protected final PropertySet propset;
	protected final AuthScope authScope;
	protected final String username;
	protected final Domain domain;
	protected final UsernamePasswordCredentials userPassCreds;
	protected final CredentialsProvider credsProvider;
	protected final BasicCookieStore cookieStore = new BasicCookieStore();
	protected final PoolingHttpClientConnectionManager connectionManager;	
	protected final ConcurrentHashMap<String,TableWSDL> wsdlCache = 
			new ConcurrentHashMap<String,TableWSDL>();
	protected CloseableHttpClient client = null; // created on request
	protected SchemaReader schemaReader = null;

	protected final Logger logger = Log.getLogger(this.getClass());
		
	public Session(PropertySet propset) {
		this.propset = propset;
		String instancename = propset.getProperty("instance");
		String username = propset.getProperty("username");
		String password = propset.getProperty("password");
		assert instancename != null;
		assert username != null;
		assert password != null;
		String domainname = this instanceof AppSession ? null : propset.getProperty("domain");
		this.instance = new Instance(instancename);
		this.username = username;
		this.domain = (domainname == null || domainname.length() == 0) ? 
			null : new Domain(domainname);		
		this.logInitInfo();
		this.authScope = new AuthScope(instance.getHost());
		this.credsProvider = new BasicCredentialsProvider();
		this.userPassCreds = new UsernamePasswordCredentials(username, password);		
		this.credsProvider.setCredentials(this.authScope, this.userPassCreds);	
		this.connectionManager = new PoolingHttpClientConnectionManager();
		// Note that HTTP Client is created on initial request by createClient method below
	}
	
	private void logInitInfo() {
		String msg = "instance=" + instance.getURL() + " user=" + username;
		if (getDomain() != null) msg += " domain=" + getDomain();		
		logger.info(Log.INIT, msg);
	}

	@Deprecated
	// Use {@link Resources.getSchemaReader()}
	public SchemaReader getSchemaReader() {
		if (schemaReader == null) {
			schemaReader = new TableSchemaReader(this);
		}
		return schemaReader;		
	}
	
	/**
	 * Create a new Session with the same properties as this one. 
	 * The URL and credentials will be the same, but the Session ID will be different.
	 */
	public Session duplicate() throws IOException {
		return new Session(this.propset);
	}

	public int defaultPageSize() {
		int pageSize = Integer.valueOf(propset.getProperty("pagesize"));
		assert pageSize > 0;
		return pageSize;
	}
	
	public int defaultPageSize(Table table) {
		return defaultPageSize();
	}
		
	public void close() {
		if (client != null) closeClient();
	}
		
	public URI getURI(String path) {
		return instance.getURI(path);
	}

	public URI getURI(String path, Parameters params) {
		return instance.getURI(path, params);
	}

	public CloseableHttpClient getClient() {
		if (client == null) createClient();
		return client;
	}

	public void reset() {
		closeClient();
	}
	
	private void createClient() {
		client = HttpClients.custom().
			setConnectionManager(connectionManager).
			setDefaultCredentialsProvider(credsProvider).
			setDefaultCookieStore(cookieStore).
			build();			
	}
	
	private void closeClient() {
		try {
			client.close();
		} catch (IOException e) {
			logger.warn(Log.ERROR, "Unable to close HTTP client", e);
		}
		this.client = null;
	}

	public Instance getInstance() {
		return this.instance;
	}

	public String getUsername() {
		return this.username;
	}
	
	public HttpHost getHost() {
		return getInstance().getHost();
	}

	public Domain getDomain() {
		return this.domain;
	}
	
	/**
	 * Generate {@link TableWSDL} or retrieve from cache.
	 */
	public TableWSDL getWSDL(String tablename) throws IOException {
		if (wsdlCache.containsKey(tablename)) 
			return wsdlCache.get(tablename);
		String saveJob = Log.getJobContext();
		Log.setJobContext(tablename + ".wsdl");		
		TableWSDL wsdl = new TableWSDL(this, tablename);
		wsdlCache.put(tablename, wsdl);
		Log.setJobContext(saveJob);						
		return wsdl;
	}
	
	/**
	 * Get the sys_user record associated with this session.
	 * We assume that the user can read their own sys_user record;
	 */
	public TableRecord getUserProfile() throws IOException {
		assert this.username != null;
		Table user = this.table("sys_user");
		TableRecord userProfile = user.api().getRecord("user_name", this.username);
		if (userProfile == null) 
			// Should be impossible.
			// If the user did not exist, then we would have previously thrown
			// InsufficientRightsException
			throw new NoSuchRecordException(
					String.format("user not found: %s", this.username));
		return userProfile;
	}
	
	protected void verifySession(PropertySet propset) throws ResourceException {
		boolean verifySession = propset.getBoolean("verify_session");
		boolean verifyTimeZone = propset.getBoolean("verify_timezone");
		if (verifySession || verifyTimeZone) {
			try {
				this.verifyUser(verifyTimeZone);
			} catch (IOException e) {
				String errmsg = String.format(
						"verifySession %s user=%s", this.instance.getURL(), this.username);
				logger.error(Log.INIT, errmsg, e);				
				throw new ResourceException(e);
			}
		}
	}
		
	/**
	 * Verify that this Session is valid by retrieving the users's record from sys_user.
	 * Verify that the time zone is GMT.
	 * If abort is true and the time zone is not GMT then an exception will be thrown.
	 */
	public Session verifyUser(boolean abort) throws IOException, ServiceNowException {
		TableRecord userProfile = this.getUserProfile();
		String timezone = userProfile.getValue("time_zone");
		if (!"GMT".equals(timezone)) { 
			String message = "Time zone not GMT for user " + this.username;
			if (abort) {
				logger.error(Log.INIT, message);				
				throw new ServiceNowException(message);				
			}
			else {
				logger.warn(Log.INIT, message);				
			}
		}
		return this;
	}
	
	/**
	 * Verify that the Schema for a table can be retrieved.
	 */
	public Table verifyTable(String tablename) throws IOException, InterruptedException {
		Table table = this.table(tablename);
		TableWSDL wsdl;
		try {
			wsdl = table.getWSDL();			
		}
		catch (IOException e) {
			logger.error(Log.INIT, "Unable to access WSDL for table " + tablename);
			throw e;
		}
		TableSchema schema = getSchemaReader().getSchema(tablename);
		if (wsdl.getReadFieldNames().size() != schema.numFields())
			throw new AssertionError("field count mismatch");
		return table;
	}

	
	/**
	 * Returns a {@link Table} object which can be used for 
	 * get, insert, update and delete operations.
	 * 
	 * @param name 
	 * The internal name of the table (not the label that appears on the forms). 
	 * The table name is in all lower case letters and may contain underscores.
	 * @return {@link Table} object
	 */
	public Table table(String name) {
		return new Table(this, name);
	}

	public String getSessionID() {
		for (Cookie cookie : cookieStore.getCookies() ) {
			String name = cookie.getName();
			String value = cookie.getValue();
			if ("JSESSIONID".equals(name)) return value;
		}
		return null;
	}
		
}
