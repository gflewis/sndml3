package sndml.servicenow;

@SuppressWarnings("serial")
public class NoSuchRecordException extends ServiceNowException {
	
	public NoSuchRecordException(String message) {
		super(message);
	}

	public NoSuchRecordException(ServiceNowRequest request) {
		super(request);
	}
	
}
