package servicenow.api;

import java.io.IOException;
import java.net.MalformedURLException;
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
import org.slf4j.Logger;

public class Session {

	private final Instance instance;
	private final Properties properties;
	private final AuthScope authScope;
	private String username;
	private UsernamePasswordCredentials userPassCreds;
	private CredentialsProvider credsProvider = null;
	private ConcurrentHashMap<String,TableSchema> schemaCache = 
			new ConcurrentHashMap<String,TableSchema>();
	private ConcurrentHashMap<String,TableWSDL> wsdlCache = 
			new ConcurrentHashMap<String,TableWSDL>();
	final private BasicCookieStore cookieStore = new BasicCookieStore();
	CloseableHttpClient client;
	

	final private Logger logger = Log.logger(this.getClass());
		
	public Session(String instancename, String username, String password, Properties props) 
			throws MalformedURLException {
		this(new Instance(instancename), username, password);
	}

	public Session(Instance instance, String username, String password) {
		this(instance);
		this.setCredentials(username, password);
		logger.info(Log.INIT, "instance=" + this.instance.getURL() + " user=" + this.username);		
	}

	public Session(Instance instance) {
		assert instance != null;
		this.instance = instance;
		this.properties = null;
		this.authScope = new AuthScope(instance.getHost());		
		logger.info(Log.INIT, "instance=" + this.instance.getURL() + " user=" + this.username);		
	}

	public Session(Properties props) {
		this.properties = props;
		this.instance = new Instance(getProperty("instance"));
		this.authScope = new AuthScope(instance.getHost());
		this.setCredentials(getProperty("username"), getProperty("password"));
		logger.info(Log.INIT, "instance=" + this.instance.getURL() + " user=" + this.username);
	}

	/**
	 * Return the value of a property with the name "servicenow." + propname
	 * if it is defined, otherwise return null.
	 */
	public String getProperty(String propname) {
		propname = "servicenow." + propname;
		String value = System.getProperty(propname);
		if (value == null && properties != null)	value = properties.getProperty(propname);
		return value;
	}
	
	@Deprecated
	private boolean getPropertyBoolean(String propname, boolean defaultValue) {
		String propvalue = getProperty(propname);
		if (propvalue == null) return defaultValue;
		return Boolean.parseBoolean(propvalue);
	}
	
	@Deprecated
	private boolean getPropertyBoolean(String propname) {
		return getPropertyBoolean(propname, false);
	}
	
	private Session setCredentials(String username, String password) {
		assert username != null;
		this.username = username;
		this.credsProvider = new BasicCredentialsProvider();
		this.userPassCreds = new UsernamePasswordCredentials(username, password);
		this.credsProvider.setCredentials(this.authScope, this.userPassCreds);
		return this;
	}
	
	public void close() throws IOException {
		client.close();
	}
	
	public URI getURI(String path) {
		return instance.getURI(path);
	}

	public URI getURI(String path, Parameters params) {
		return instance.getURI(path, params);
	}

	public CloseableHttpClient getClient() {
		assert this.credsProvider != null;
		assert this.cookieStore != null;
		if (this.client == null) {
			this.client = HttpClients.custom().
					setDefaultCredentialsProvider(credsProvider).
					setDefaultCookieStore(cookieStore).
					build();			
		}
		return this.client;
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

	/**
	 * Generate {@link TableSchema} or retrieve from cache.
	 */
	public TableSchema getSchema(String tablename) 
			throws InvalidTableNameException, IOException, InterruptedException {
		if (schemaCache.containsKey(tablename)) 
			return schemaCache.get(tablename);
		String saveJob = Log.getJobContext();
		Log.setJobContext(tablename + ".schema");		
		Table table = table(tablename);
		TableSchema schema = new TableSchema(table);
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
	
	public Session verify() throws IOException, InterruptedException {
		Table user = verify("sys_user");
		assert this.username != null;
		Record profile = user.api().getRecord("user_name", this.username);
		String timezone = profile.getValue("time_zone");
		if (!"GMT".equals(timezone)) { 
			String message = "Time zone not GMT for user " + this.username;
			if (getPropertyBoolean("verify_session")) {
				logger.error(Log.INIT, message);				
				throw new ServiceNowException(message);				
			}
			else {
				logger.warn(Log.INIT, message);				
			}
		}
		return this;
	}
	
	public Table verify(String tablename) throws IOException, InterruptedException {
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
