package sndml.servicenow;

@SuppressWarnings("serial")
public class InvalidTableNameException extends ServiceNowException {
	
	public InvalidTableNameException(String name) {
		super("InvalidTableName: " + name);
	}

}
