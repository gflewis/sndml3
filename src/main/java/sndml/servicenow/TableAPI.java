package sndml.servicenow;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

		
	/**
	 * Gets a record using the sys_id. 
	 * If the record is not found then null is returned.
	 * 
	 * @param sys_id
	 * @return
	 * @throws IOException
	 */
 	public abstract TableRecord getRecord(RecordKey sys_id) throws IOException;
 	
 	public abstract RecordList getRecords(EncodedQuery query, boolean displayValue) throws IOException;
 	
 	public abstract InsertResponse insertRecord(Parameters fields) throws IOException;
 	
 	public abstract void updateRecord(RecordKey key, Parameters fields) throws IOException;
 	
 	public abstract boolean deleteRecord(RecordKey key) throws IOException;

 	public abstract TableReader getDefaultReader() throws IOException;

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
 	
	public RecordList getRecords(String fieldname, String fieldvalue, boolean displayValue) throws IOException {
		EncodedQuery query = new EncodedQuery(table);
		query.addQuery(fieldname, EncodedQuery.EQUALS, fieldvalue);
		return getRecords(query, displayValue);
	}

	/**
	 * Retrieves a single record based on a unique field such as "name" or "number".  
	 * This method should be used in cases where the field value is known to be unique.
	 * If no qualifying records are found this function will return null.
	 * If one qualifying record is found it will be returned.
	 * If multiple qualifying records are found this method 
	 * will throw an RowCountExceededException.
	 * <pre>
	 * {@link TableRecord} grouprec = session.table("sys_user_group").get("name", "Network Support");
	 * </pre>
	 * 
	 * @param fieldname Field name, e.g. "number" or "name"
	 * @param fieldvalue Field value
	 */
	public TableRecord getRecord(String fieldname, String fieldvalue) throws IOException {
		return getRecord(fieldname, fieldvalue, false);
	}
	
	public TableRecord getRecord(String fieldname, String fieldvalue, boolean displayValues)
			throws IOException, SoapResponseException {
		RecordList result = getRecords(fieldname, fieldvalue, displayValues);
		int size = result.size();
		String msg = String.format("get %s=%s returned %d records", fieldname, fieldvalue, size);
		logger.debug(Log.RESPONSE, msg);
		if (size == 0) return null;
		if (size > 1) throw new TooManyRowsException(getTable(), 1, size); 
		return result.get(0);
	}
	
}
