package sndml.servicenow;

@SuppressWarnings("serial")
public class TooFewRowsException extends ServiceNowException {
	
	public TooFewRowsException(Table table, int minimum, int actual) {
		super(String.format(
			"Error loading from %s: processed %d rows; minimum is %d", 
			table.getName(), actual, minimum));

	}

}
