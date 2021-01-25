package sndml.servicenow;

@SuppressWarnings("serial")
public class InvalidFieldNameException extends ServiceNowException {

	public InvalidFieldNameException(String message) {
		super(message);
	}

}
