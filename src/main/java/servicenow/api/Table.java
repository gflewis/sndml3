package servicenow.api;

import java.io.IOException;

public class Table {

	protected final Instance instance;
	protected final Session session;
	protected final String tablename;
	
	private SoapTableAPI apiSOAP;
	private RestTableAPI apiREST;
	private JsonTableAPI apiJSON;
	private TableAPI api;
			
	Table(Session session, String tablename) {
		this.session = session;
		this.tablename = tablename;
		this.instance = session.getInstance();
		String apiName = session.getProperty("api");
		if (apiName == null)
			this.api = rest();
		else {
			switch (apiName) {
			case "soap" : this.api = soap(); break;
			case "rest" : this.api = rest(); break;
			case "json" : this.api = json(); break;
			default:
				throw new IllegalArgumentException(String.format("api=\"%s\"",  apiName));
			}
		}
	}

	public Session getSession() {
		return this.session;
	}
	
	public String getName() {
		return this.tablename;
	}
	
	/**
	 * Return the SOAP API.
	 */
	public SoapTableAPI soap() {
		if (this.apiSOAP == null) this.apiSOAP = new SoapTableAPI(this);
		return this.apiSOAP;
	}
	
	/**
	 * return the REST API.
	 */
	public RestTableAPI rest() {
		if (this.apiREST == null) this.apiREST = new RestTableAPI(this);
		return this.apiREST;
	}
	
	/**
	 * Return the JSONv2 API
	 */
	public JsonTableAPI json() {
		if (this.apiJSON == null) this.apiJSON = new JsonTableAPI(this);
		return this.apiJSON;
	}
		
	/**
	 * Return the default API.
	 */
	public TableAPI api() {
		assert api != null;
		return api;
	}
	
	@Deprecated
	public TableReader getDefaultReader() throws IOException {
		if (api == apiJSON) return apiJSON.getDefaultReader();
		if (api == apiREST) return apiREST.getDefaultReader();
		if (api == apiSOAP) return apiSOAP.getDefaultReader();
		throw new IllegalStateException();
	}
			
	public Record getRecord(Key key) throws IOException {
		return api.getRecord(key);
	}
			
	public TableWSDL getWSDL() throws IOException {
		return session.getWSDL(getName());
	}
	
	public TableSchema getSchema() throws IOException, InterruptedException {
		return session.getSchema(getName());
	}
		
}

