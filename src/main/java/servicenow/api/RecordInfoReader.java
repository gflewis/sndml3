package servicenow.api;

public class RecordInfoReader extends RestTableReader {

	final FieldNames fieldNames = new FieldNames("sys_id,sys_created_on,sys_updated_on");

	public RecordInfoReader(Table table) {
		super(table);
		super.setFields(fieldNames);		
		super.setPageSize(10000);
	}
	
	public RecordInfoReader setFields(FieldNames names) {
		throw new UnsupportedOperationException();
	}
	
}
