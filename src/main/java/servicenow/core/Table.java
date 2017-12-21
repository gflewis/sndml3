package servicenow.core;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import servicenow.rest.TableImplRest;
import servicenow.soap.TableImplSoap;
import servicenow.soap.TableWSDL;

public class Table {

	protected final Instance instance;
	protected final Session session;
	protected final String tablename;
	
	private final TableImplSoap implSOAP;
	private final TableImplRest implREST;
	TableImpl implDefault;
		
	final private Logger log = LoggerFactory.getLogger(this.getClass());
	final Marker mrkRequest = MarkerFactory.getMarker("REQUEST");
	final Marker mrkResponse = MarkerFactory.getMarker("RESPONSE");
	
	Table(Session session, String tablename) {
		this.session = session;
		this.tablename = tablename;
		this.instance = session.getInstance();
		this.implSOAP = new TableImplSoap(this);
		this.implREST = new TableImplRest(this);
		if ("soap".equals(session.getProperty("api"))) 
			this.implDefault = implSOAP;
		else
			this.implDefault = implREST;
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
	public TableImplSoap soap() {
		return this.implSOAP;
	}
	
	/**
	 * return the REST API.
	 */
	public TableImplRest rest() {
		return this.implREST;
	}
	
	/**
	 * Set the default API to SOAP Web Services (XML).
	 */
	public TableImpl setSOAP() {
		this.implDefault = implSOAP;
		return implDefault;
	}
	
	/**
	 * Set the default API to the REST Table API (JSON).
	 */
	public TableImpl setREST() {
		this.implDefault = implREST;
		return implDefault;
	}
	
	/**
	 * Return the default API.
	 */
	public TableImpl getDefaultImpl() {
		return implDefault;
	}
	
	public TableReader getDefaultReader() throws IOException {
		return getDefaultImpl().getDefaultReader();
	}
		
	public KeyList getKeys() throws IOException {
		return implDefault.getKeys((EncodedQuery) null);
	}
	
	public KeyList getKeys(EncodedQuery filter) throws IOException {
		return implDefault.getKeys(filter);
	}
	
	public Record getRecord(Key key) throws IOException {
		return implDefault.getRecord(key);
	}
	
	public Record getRecord(String fieldname, String fieldvalue, boolean displayValue)  throws IOException {
		return implDefault.getRecord(fieldname, fieldvalue, displayValue);
	}

	@Deprecated
	public RecordList getRecords() throws IOException {
		return implDefault.getRecords();
	}
	
	@Deprecated
	public RecordList getRecords(String fieldname, String fieldvalue) throws IOException {
		return implDefault.getRecords(fieldname, fieldvalue);
	}
	
	@Deprecated
	public RecordList getRecords(String fieldname, String fieldvalue, boolean displayValues) throws IOException {
		return implDefault.getRecords(fieldname, fieldvalue, displayValues);
	}
	
	@Deprecated
	public RecordList getRecords(EncodedQuery query) throws IOException {
		return implDefault.getRecords(query);
	}
	
	@Deprecated
	public RecordList getRecords(EncodedQuery query, boolean displayValues) throws IOException {
		return implDefault.getRecords(query, displayValues);
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
		RecordList result = getRecords(fieldname, fieldvalue, true);
		int size = result.size();
		String msg = 
			"get " + fieldname + "=" + fieldvalue +	" returned " + size + " records";
		log.info(mrkResponse, msg);
		if (size == 0) return null;
		if (size > 1) throw new RowCountExceededException(this, msg);
		return result.get(0);
	}
	
	public TableWSDL getWSDL() throws IOException {
		return implSOAP.getWSDL();
	}
	
	public TableSchema getSchema() throws IOException, InterruptedException {
		return new TableSchema(this);
	}
		
}

