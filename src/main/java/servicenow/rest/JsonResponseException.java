package servicenow.rest;

import org.json.JSONObject;

import servicenow.core.ServiceNowException;

public class JsonResponseException extends ServiceNowException {

	private static final long serialVersionUID = 1L;
	
	final JSONObject obj;

	JsonResponseException(JSONObject obj) {
		super(obj.toString());
		this.obj = obj;
	}
	
	JSONObject getObject() {
		return this.obj;
	}

}
