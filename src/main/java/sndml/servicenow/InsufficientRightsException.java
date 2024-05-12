package sndml.servicenow;

@SuppressWarnings("serial")
public class InsufficientRightsException extends ServiceNowException {
	
	public InsufficientRightsException(String message) {
		super(message);
	}
	
	public InsufficientRightsException(ServiceNowRequest request) {
		super(request);
	}
	
}
