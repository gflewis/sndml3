package sndml.servicenow;

@SuppressWarnings("serial")
public class TooManyRowsException extends ServiceNowException {
	
	public TooManyRowsException(Table table, int maximum, int current) {
		super(String.format(
				"Error processing %s: processed %d rows; maximum is %d", 
				table.getName(), current, maximum));
	}

}
