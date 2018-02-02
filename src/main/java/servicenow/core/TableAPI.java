package servicenow.core;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.soap.SoapResponseException;

public abstract class TableAPI {

	final protected Table table;
	final protected Session session;
	
	final private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public TableAPI(Table table) {
		this.table = table;
		this.session = table.getSession();
	}

	public Table getTable() {
		return this.table;
	}

	public Session getSession() {
		return this.session;
	}

	public String getTableName() {
		return table.getName();
	}
		
 	public abstract KeySet getKeys(EncodedQuery query) throws IOException;
 	
 	public abstract Record getRecord(Key sys_id) throws IOException;
 	 	
 	public RecordList getRecords() throws IOException {
 		return getRecords(false);
 	}
 	
 	public RecordList getRecords(boolean displayValue) throws IOException {
 		return getRecords((EncodedQuery) null, displayValue);
 	}
 	
 	public RecordList getRecords(String fieldname, String fieldvalue) throws IOException {
 		return getRecords(fieldname, fieldvalue, false);
 	}
 	
 	public RecordList getRecords(EncodedQuery query) throws IOException {
 		return getRecords(query, false);
 	}
 	
 	public abstract RecordList getRecords(String fieldname, String fieldvalue, boolean displayVaue) throws IOException;

 	public abstract RecordList getRecords(EncodedQuery query, boolean displayValue) throws IOException;

 	public abstract TableReader getDefaultReader() throws IOException;

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
	public Record getRecord(String fieldname, String fieldvalue, boolean displayValues)
			throws IOException, SoapResponseException {
		RecordList result = getRecords(fieldname, fieldvalue, displayValues);
		int size = result.size();
		String msg = String.format("get %s=%s returned %d records", fieldname, fieldvalue, size);
		logger.info(Log.RESPONSE, msg);
		if (size == 0) return null;
		if (size > 1) throw new RowCountExceededException(getTable(), msg);
		return result.get(0);
	}
	
 	
}
