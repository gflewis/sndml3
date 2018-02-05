package servicenow.core;

import org.json.JSONObject;

public class JsonResponseException extends ServiceNowException {

	private static final long serialVersionUID = 1L;
	
	final JSONObject obj;

	public JsonResponseException(JSONObject obj) {
		super(obj.toString());
		this.obj = obj;
	}
	
	public JSONObject getObject() {
		return this.obj;
	}

}
