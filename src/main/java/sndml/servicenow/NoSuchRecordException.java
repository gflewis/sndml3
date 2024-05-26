package sndml.servicenow;

@SuppressWarnings("serial")
public class NoSuchRecordException extends ServiceNowException {
	
	public NoSuchRecordException(ServiceNowRequest request, String message) {
		super(request, message);
	}

	public NoSuchRecordException(ServiceNowRequest request) {
		super(request);
	}
	
	public NoSuchRecordException(String message) {
		super(message);
	}
	
}
