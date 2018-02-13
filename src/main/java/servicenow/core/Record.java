package servicenow.core;

import java.io.StringWriter;
import java.util.Iterator;
import org.json.JSONWriter;

public abstract class Record implements InsertResponse {

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
	
	/*
	 * Get a list of all the fields in the record.
	 */
	abstract public FieldNames getFieldNames();
	
	/**
	 * Get the sys_id (primary key) of a Record.  
	 */
	public Key getKey() {
		String fieldvalue = getValue("sys_id");
		assert fieldvalue != null;
		return new Key(fieldvalue);
	}

	/**
	 * Get the value of a reference field.
	 */
	public Key getKey(String fieldname) {
		String fieldvalue = getValue(fieldname);
		if (fieldvalue==null) return null;
		return new Key(fieldvalue);
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
		String fieldvalue = getValue("sys_updated_on");
		assert fieldvalue != null;
		return new DateTime(fieldvalue, DateTime.DATE_TIME);
	}
	
	/**
	 * Get sys_created_on from a Record object.
	 */
	public DateTime getCreatedTimestamp() {
		String fieldvalue = getValue("sys_created_on");
		assert fieldvalue != null;
		return new DateTime(fieldvalue, DateTime.DATE_TIME);
	}
	
	/**
	 * Get the value of a DateTime field.
	 * For a Java date use getDateTime(fieldname).toDate().
	 * @throws InvalidDateTimeException 
	 */
	public DateTime getDateTime(String fieldname) {
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
		return new Integer(value);
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
		return new Integer((int) (millis / 1000));
	}
	
	public String toJSON() {
		StringWriter writer = new StringWriter();
		JSONWriter json = new JSONWriter(writer);
		json.object();
		for (String name : this.getFieldNames()) {
			String value = this.getValue(name);
			if (value != null && value.length() > 0) {
				json.key(name);
				json.value(value);				
			}
		}
		json.endObject();
		return writer.toString();
	}
	
}
