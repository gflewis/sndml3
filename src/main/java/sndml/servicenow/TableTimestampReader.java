package sndml.servicenow;

import sndml.util.FieldNames;

/**
 * Class to read values of sys_id, sys_created_on and sys_updated_on
 * from a ServiceNow table.
 *
 */
public class TableTimestampReader extends RestTableReader {

	final FieldNames fieldNames = new FieldNames("sys_id,sys_created_on,sys_updated_on");

	public TableTimestampReader(Table table) {
		super(table);
		super.setFields(fieldNames);		
		super.setPageSize(10000);
	}
	
	public TableTimestampReader setFields(FieldNames names) {
		throw new UnsupportedOperationException();
	}
	
}
