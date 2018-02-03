package servicenow.core;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import servicenow.json.JsonTableAPI;
import servicenow.rest.RestTableAPI;
import servicenow.soap.SoapTableAPI;
import servicenow.soap.TableWSDL;

public class Table {

	protected final Instance instance;
	protected final Session session;
	protected final String tablename;
	
	// enum APIType {SOAP, REST};
	// private EnumMap<APIType, TableAPI> apiset;
	
	private SoapTableAPI apiSOAP;
	private RestTableAPI apiREST;
	private JsonTableAPI apiJSON;
	private TableAPI api;
		
	final private Logger log = LoggerFactory.getLogger(this.getClass());
	final Marker mrkRequest = MarkerFactory.getMarker("REQUEST");
	final Marker mrkResponse = MarkerFactory.getMarker("RESPONSE");
	
	Table(Session session, String tablename) {
		this.session = session;
		this.tablename = tablename;
		this.instance = session.getInstance();
		String apiName = session.getProperty("api");
		if (apiName == null)
			this.api = json();
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

	/*
	public TableAPI getAPI(APIType t) {
		if (!apiset.containsKey(t)) {
			switch(t) {
			case SOAP: apiset.put(t,  new SoapTableAPI(this)); break;
			case REST: apiset.put(t,  new RestTableAPI(this)); break;
			default: throw new AssertionError();
			}
		}
		TableAPI api = apiset.get(t);
		assert api != null;
		return api;
	}
	*/
	
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
	 * Set the default API to SOAP Web Services (XML).
	 */
	@Deprecated
	public TableAPI setSOAP() {
		this.api = soap();
		return api;
	}
	
	/**
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
	
	public TableReader getDefaultReader() throws IOException {
		if (api == apiJSON) return apiJSON.getDefaultReader();
		if (api == apiREST) return apiREST.getDefaultReader();
		if (api == apiSOAP) return apiSOAP.getDefaultReader();
		throw new IllegalStateException();
	}
		
//	public KeySet getKeys() throws IOException {
//		return api.getKeys((EncodedQuery) null);
//	}
//	
//	public KeySet getKeys(EncodedQuery filter) throws IOException {
//		return api.getKeys(filter);
//	}
	
	public Record getRecord(Key key) throws IOException {
		return api.getRecord(key);
	}
	
	public Record getRecord(String fieldname, String fieldvalue, boolean displayValue)  throws IOException {
		return api.getRecord(fieldname, fieldvalue, displayValue);
	}
		
	/**
	 * Retrieves a single record based on a unique field such as "name" or "number".  
	 * This method should be used in cases where the field value is known to be unique.
	 * If no qualifying records are found this function will return null.
	 * If one qualifying record is found it will be returned.
	 * If multiple qualifying records are found this method 
	 * will throw an RowCountExceededException.
	 * <pre>
	 * {@link Record} grouprec = session.table("sys_user_group").get("name", "Network Support");
	 * </pre>
	 * 
	 * @param fieldname Field name, e.g. "number" or "name"
	 * @param fieldvalue Field value
	 */
	public Record getRecord(String fieldname, String fieldvalue)
			throws IOException, RowCountExceededException {
		RecordList result = api.getRecords(fieldname, fieldvalue, true);
		int size = result.size();
		String msg = 
			"get " + fieldname + "=" + fieldvalue +	" returned " + size + " records";
		log.info(mrkResponse, msg);
		if (size == 0) return null;
		if (size > 1) throw new RowCountExceededException(this, msg);
		return result.get(0);
	}
	
	public TableWSDL getWSDL() throws IOException {
		return apiSOAP.getWSDL();
	}
	
	public TableSchema getSchema() throws IOException, InterruptedException {
		return new TableSchema(this);
	}
		
}

