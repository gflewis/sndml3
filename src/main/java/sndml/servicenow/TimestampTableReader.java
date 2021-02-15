package sndml.servicenow;

public class TimestampTableReader extends RestTableReader {

	final FieldNames fieldNames = new FieldNames("sys_id,sys_created_on,sys_updated_on");

	public TimestampTableReader(Table table) {
		super(table);
		super.setFields(fieldNames);		
		super.setPageSize(10000);
	}
	
	public TimestampTableReader setFields(FieldNames names) {
		throw new UnsupportedOperationException();
	}
	
}
