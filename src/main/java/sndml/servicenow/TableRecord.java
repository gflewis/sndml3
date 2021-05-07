package sndml.servicenow;

import java.util.Iterator;

import org.slf4j.Logger;

public abstract class TableRecord implements InsertResponse {

	static final Logger logger = Log.logger(TableRecord.class);
	
	protected Table table;

	/**
	 * The table from which this record was retrieved.
	 */
	public Table getTable() { 
		return this.table; 
	}	
	
	/**
	 * Get the value of a field from a Record.
	 * 
	 * @param fieldname Name of Record field
	 * @return Null if the field is missing or has zero length, 
	 * otherwise the field value as a string
	 */
	abstract public String getValue(String fieldname);

	/**
	 * Get the display value of a field if it was included in the record
	 * 
	 * @param fieldname Name of Record field
	 * @return Null if the field does not exist or the display value
	 * was not included in the record or the display value has zero length
	 */
	abstract public String getDisplayValue(String fieldname);

	abstract public Iterator<String> keys();
	
	/**
	 * Get a list of all the fields in the record.
	 */
	abstract public FieldNames getFieldNames();
	
	/**
	 * Get String representation of this document.
	 */
	abstract public String asText(boolean pretty);
	
	public String asText() {
		return asText(false);
	}
	
	/**
	 * Get the sys_id (primary key) of a Record.  
	 */
	public RecordKey getKey() {
		String fieldvalue = getValue("sys_id");
		assert fieldvalue != null;
		return new RecordKey(fieldvalue);
	}

	/**
	 * Get the value of a reference field.
	 */
	public RecordKey getKey(String fieldname) {
		String fieldvalue = getValue(fieldname);
		if (fieldvalue==null) return null;
		return new RecordKey(fieldvalue);
	}

	/**
	 * Return number if it exists otherwise null
	 */
	public String getNumber() {
		return getValue("number");
	}
	
	/**
	 * Get sys_updated_on from a Record object.
	 */
	public DateTime getUpdatedTimestamp() {
		return getDateTime("sys_updated_on");
	}
	
	/**
	 * Get sys_created_on from a Record object.
	 */
	public DateTime getCreatedTimestamp() {
		return getDateTime("sys_created_on");
	}
	
	/**
	 * Get the value of a DateTime field.
	 * For a Java date use getDateTime(fieldname).toDate().
	 * @throws InvalidDateTimeException 
	 */
	public DateTime getDateTime(String fieldname) throws InvalidDateTimeException {
		String value = getValue(fieldname);
		if (value == null) return null;
		DateTime result = new DateTime(value, DateTime.DATE_TIME);
		return result;
	}
	
	/**
	 * Get the value of a Date field.
	 * For a Java date use getDate(fieldname).toDate().
	 * @throws InvalidDateTimeException 
	 */
	public DateTime getDate(String fieldname) throws InvalidDateTimeException {
		String value = getValue(fieldname);
		if (value == null) return null;
		DateTime result = new DateTime(value, DateTime.DATE_ONLY);
		return result;
	}

	/**
	 * Get the value of an integer field.
	 */
	public Integer getInteger(String fieldname) {
		String value = getValue(fieldname);
		if (value == null) return null;
		return Integer.valueOf(value);
	}	   	

	/**
	 * Get the value of a boolean field.
	 * @throws ServiceNowException
	 */
	public Boolean getBoolean(String fieldname) throws ServiceNowException {
		String value = getValue(fieldname);
		if (value == null) return null;
		if (value.equals("0") || value.equals("false")) return Boolean.FALSE;
		if (value.equals("1") || value.equals("true")) return Boolean.TRUE;
		throw new ServiceNowException("getBoolean " + fieldname + "=" + value);
	}

	/**
	 * Get the value of a duration field converted to seconds.
	 * @throws ServiceNowException 
	 */
	public Integer getDuration(String fieldname) throws ServiceNowException {
		String value = getValue(fieldname);
		if (value == null) return null;
		// value will be stored as yyyy-mm-dd hh:mm:ss
		DateTime dt;
		try {
			dt = new DateTime(value, DateTime.DATE_TIME);
		}
		catch (InvalidDateTimeException e) {
			throw new ServiceNowException("getDuration " + fieldname + "=" + value);
		}
		long millis = dt.getMillisec();
		return Integer.valueOf((int) (millis / 1000));
	}
		
}
