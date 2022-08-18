package sndml.servicenow;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
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

import sndml.daemon.AgentDaemon;
import sndml.daemon.AppSchemaFactory;

/**
 * Holds a ServiceNow URL, connection credentials, a cookie store with a session ID
 * and a cache of {@link TableSchema} definitions.
 */
public class Session {

	private final Instance instance;
	private final Properties properties;
	private final AuthScope authScope;
	private final String username;
	private final Domain domain;
	private final UsernamePasswordCredentials userPassCreds;
	private final CredentialsProvider credsProvider;
	final private BasicCookieStore cookieStore = new BasicCookieStore();
	final private PoolingHttpClientConnectionManager connectionManager;	
	private final ConcurrentHashMap<String,TableSchema> schemaCache = 
			new ConcurrentHashMap<String,TableSchema>();
	private final ConcurrentHashMap<String,TableWSDL> wsdlCache = 
			new ConcurrentHashMap<String,TableWSDL>();
	private CloseableHttpClient client = null; // created on request
	private SchemaFactory schemaFactory;

	final private Logger logger = Log.logger(this.getClass());

	public Session(Properties props) throws IOException {
		this(props, false);
	}
	
	public Session(Properties props, boolean agentApp) throws IOException  {
		this.properties = props;
		String instancename, username, password;
		if (agentApp) {
			instancename = this.getAppProperty("instance");
			username = this.getAppProperty("username");
			password = this.getAppProperty("password");
		}
		else {
			instancename = this.getProperty("instance");
			username = this.getProperty("username");
			password = this.getProperty("password");			
		}
		String domainname = this.getProperty("domain");
		assert instancename != null; 
		assert instancename != "";
		assert username != null;
		assert username != "";
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
//		client is now created on initial request
//		this.client = HttpClients.custom().
//				setConnectionManager(connectionManager).
//				setDefaultCredentialsProvider(credsProvider).
//				setDefaultCookieStore(cookieStore).
//				build();			
		if (this.getPropertyBoolean("verify_session", false)) this.verifyUser();
	}
	
	private void logInitInfo() {
		String msg = "instance=" + instance.getURL() + " user=" + username;
		if (getDomain() != null) msg += " domain=" + getDomain();		
		logger.info(Log.INIT, msg);
	}

	/**
	 * Create a new Session with the same properties as this one. 
	 * The URL and credentials will be the same, but the Session ID will be different.
	 */
	public Session duplicate() throws IOException {
		return new Session(this.properties);
	}
		
	/**
	 * Return the value of a property with the name "servicenow." + propname
	 * if it is defined, otherwise return null.
	 */
	public String getProperty(String propname) {
		String value =  getPrefixProperty("servicenow", propname);
		// TODO Why is this here?
		if (value == null && properties != null)	
			value = properties.getProperty(propname);
		return value;
	}
	
	private String getAppProperty(String propname) {
		String value = getPrefixProperty("app", propname);
		if (value == null) value = getPrefixProperty("servicenow", propname);
		return value;
	}
	
	private String getPrefixProperty(String prefix, String propname) {
		propname = prefix + "." + propname;
		String value = System.getProperty(propname);
		if (value == null && properties != null)	
			value = properties.getProperty(propname);
		return value;		
	}
	
	private boolean getPropertyBoolean(String propname, boolean defaultValue) {
		String propvalue = getProperty(propname);
		if (propvalue == null) return defaultValue;
		return Boolean.parseBoolean(propvalue);
	}
	
	private boolean getPropertyBoolean(String propname) {
		return getPropertyBoolean(propname, false);
	}
	
	public int getPropertyInt(String name, int defaultValue) {
		String stringValue = getProperty(name);
		if (stringValue == null) return defaultValue;
		return Integer.valueOf(stringValue);
	}
	
	public int defaultPageSize() {
		int pageSize = getPropertyInt("pagesize", 200);
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
	 * Generate {@link TableSchema} or retrieve from cache.
	 */
	public TableSchema getSchema(String tablename) 
			throws InvalidTableNameException, IOException, InterruptedException {
		// TODO: Session should not be referencing a different class. Move cache into SchemaFactory class.
		if (schemaFactory == null) {
			schemaFactory =	AgentDaemon.isRunning() ?
				new AppSchemaFactory(this) : 
				new TableSchemaFactory(this);
		}
		if (schemaCache.containsKey(tablename)) 
			return schemaCache.get(tablename);
		String saveJob = Log.getJobContext();
		Log.setJobContext(tablename + ".schema");		
		TableSchema schema = schemaFactory.getSchema(tablename);
		if (schema.isEmpty()) throw new InvalidTableNameException(tablename);
		schemaCache.put(tablename, schema);
		Log.setJobContext(saveJob);
		return schema;
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
	 * Verify that this Session is valid by retrieving the users's record from sys_user.
	 * If the time zone is not GMT then an exception will be thrown.
	 */
	public Session verifyUser() throws IOException {
		Table user;
		try {
			user = verifyTable("sys_user");
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		assert this.username != null;
		TableRecord profile = user.api().getRecord("user_name", this.username);
		String timezone = profile.getValue("time_zone");
		if (!"GMT".equals(timezone)) { 
			String message = "Time zone not GMT for user " + this.username;
			if (getPropertyBoolean("verify_timezone")) {
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
		TableSchema schema = table.getSchema();
		if (wsdl.getReadFieldNames().size() != schema.numFields())
			throw new AssertionError("field count mismatch");
		return table;
	}

	
	/**
	 * <p>Returns a {@link Table} object which can be used for 
	 * get, insert, update and delete operations.</p>
	 * 
	 * <p>The full schema will be retrieved from sys_dictionary 
	 * only if you call {@link Table#getSchema()}.</p>
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
