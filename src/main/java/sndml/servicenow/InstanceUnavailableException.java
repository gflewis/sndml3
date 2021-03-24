package sndml.servicenow;

public class InstanceUnavailableException extends ServiceNowException {

	private static final long serialVersionUID = 1L;

	public InstanceUnavailableException(ServiceNowRequest request) {
		super(request);
	}
	
}
