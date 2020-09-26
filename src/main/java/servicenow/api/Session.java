package servicenow.api;

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
import org.slf4j.Logger;

public class Session {

	private final Instance instance;
	private final Properties properties;
	private final AuthScope authScope;
	private final String username;
	private final Domain domain;
	private final UsernamePasswordCredentials userPassCreds;
	private final CredentialsProvider credsProvider;
	private final ConcurrentHashMap<String,TableSchema> schemaCache = 
			new ConcurrentHashMap<String,TableSchema>();
	private final ConcurrentHashMap<String,TableWSDL> wsdlCache = 
			new ConcurrentHashMap<String,TableWSDL>();
	final private BasicCookieStore cookieStore = new BasicCookieStore();
	CloseableHttpClient client;	

	final private Logger logger = Log.logger(this.getClass());

	public Session(Properties props) throws IOException {
		this.properties = props;
		String instancename = this.getProperty("instance");
		String username = this.getProperty("username");
		String password = this.getProperty("password");
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
		if (this.getPropertyBoolean("verify_session", false)) this.verify();
	}

	private void logInitInfo() {
		String msg = "instance=" + instance.getURL() + " user=" + username;
		if (getDomain() != null) msg += " domain=" + getDomain();		
		logger.info(msg);
	}
		
	/**
	 * Return the value of a property with the name "servicenow." + propname
	 * if it is defined, otherwise return null.
	 */
	public String getProperty(String propname) {
		propname = "servicenow." + propname;
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

	public Domain getDomain() {
		return this.domain;
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
	
	public Session verify() throws IOException {
		Table user;
		try {
			user = verify("sys_user");
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		assert this.username != null;
		Record profile = user.api().getRecord("user_name", this.username);
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
