package servicenow.core;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import servicenow.rest.RestTableAPI;
import servicenow.soap.SoapTableAPI;
import servicenow.soap.TableWSDL;

public class Table {

	protected final Instance instance;
	protected final Session session;
	protected final String tablename;
	
	private final SoapTableAPI apiSOAP;
	private final RestTableAPI apiREST;
	TableAPI api;
		
	final private Logger log = LoggerFactory.getLogger(this.getClass());
	final Marker mrkRequest = MarkerFactory.getMarker("REQUEST");
	final Marker mrkResponse = MarkerFactory.getMarker("RESPONSE");
	
	Table(Session session, String tablename) {
		this.session = session;
		this.tablename = tablename;
		this.instance = session.getInstance();
		this.apiSOAP = new SoapTableAPI(this);
		this.apiREST = new RestTableAPI(this);
		if ("soap".equals(session.getProperty("api"))) 
			this.api = apiSOAP;
		else
			this.api = apiREST;
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
		return this.apiSOAP;
	}
	
	/**
	 * return the REST API.
	 */
	public RestTableAPI rest() {
		return this.apiREST;
	}
	
	/**
	 * Set the default API to SOAP Web Services (XML).
	 */
	public TableAPI setSOAP() {
		this.api = apiSOAP;
		return api;
	}
	
	/**
	 * Set the default API to the REST Table API (JSON).
	 */
	public TableAPI setREST() {
		this.api = apiREST;
		return api;
	}
	
	/**
	 * Return the default API.
	 */
	public TableAPI api() {
		return api;
	}
	
	public TableReader getDefaultReader() throws IOException {
		return api().getDefaultReader();
	}
		
	public KeySet getKeys() throws IOException {
		return api.getKeys((EncodedQuery) null);
	}
	
	public KeySet getKeys(EncodedQuery filter) throws IOException {
		return api.getKeys(filter);
	}
	
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

