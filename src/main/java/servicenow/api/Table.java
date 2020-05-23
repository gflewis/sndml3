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
	
	/*
	 * Set the default API to SOAP Web Services (XML).
	 */
	@Deprecated
	public TableAPI setSOAP() {
		this.api = soap();
		return api;
	}
	
	/*
	 * Set the default API to the REST Table API (JSON).
	 */
	@Deprecated
	public TableAPI setREST() {
		this.api = rest();
		return api;
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
		
	/*
	 * Retrieves a single record based on a unique field such as "name" or "number".  
	 * This method should be used in cases where the field value is known to be unique.
	 * If no qualifying records are found this function will return null.
	 * If one qualifying record is found it will be returned.
	 * If multiple qualifying records are found this method 
	 * will throw an {@link RowCountExceededException}.
	 * <pre>
	 * {@link Record} grouprec = session.table("sys_user_group").get("name", "Network Support");
	 * </pre>
	 * 
	 * @param fieldname Field name, e.g. "number" or "name"
	 * @param fieldvalue Field value
	 */
	@Deprecated
	public Record getRecord(String fieldname, String fieldvalue)
			throws IOException, RowCountExceededException {
		return api.getRecord(fieldname, fieldvalue);
	}
	
	public TableWSDL getWSDL() throws IOException {		
		String saveJob = Log.getJobContext();
		Log.setJobContext(getName() + ".WSDL");
		TableWSDL result = soap().getWSDL();
		Log.setJobContext(saveJob);
		return result;
	}
	
	public TableSchema getSchema() throws IOException, InterruptedException {
		String saveJob = Log.getJobContext();
		Log.setJobContext(getName() + ".schema");
		TableSchema result = new TableSchema(this);
		Log.setJobContext(saveJob);
		return result;
	}
		
}

