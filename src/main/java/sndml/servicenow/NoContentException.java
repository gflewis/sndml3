package sndml.servicenow;

@SuppressWarnings("serial")
public class NoContentException extends ServiceNowException {

	public NoContentException(ServiceNowRequest request) {
		super(request);
	}
			
}
