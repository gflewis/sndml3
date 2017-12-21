package servicenow.core;

@SuppressWarnings("serial")
public class InvalidTableNameException extends ServiceNowException {
	
	public InvalidTableNameException(String name) {
		super("InvalidTableName: " + name);
	}

}
