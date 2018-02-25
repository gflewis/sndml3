package servicenow.api;

@SuppressWarnings("serial")
public class InvalidTableNameException extends ServiceNowException {
	
	public InvalidTableNameException(String name) {
		super("InvalidTableName: " + name);
	}

}
