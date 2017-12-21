package servicenow.rest;

import servicenow.core.ServiceNowError;

public class JsonResponseError extends ServiceNowError {

	private static final long serialVersionUID = 1L;

	JsonResponseError(String message) {
		super(message);
	}

}
