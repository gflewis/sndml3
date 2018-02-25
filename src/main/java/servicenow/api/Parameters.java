package servicenow.api;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class holds a list of name/value pairs.
 * 
 * @author Giles Lewis
 */
public class Parameters extends LinkedHashMap<String, String> {

	private static final long serialVersionUID = 1L;

	public Parameters() {
		super();
	}
	
	public Parameters(Parameters params) {
		super();
		if (params != null) this.putAll(params);
	}
	
	public Parameters(String name, String value) {
		super();
		super.put(name, value);
	}
	
	public Parameters(List<NameValuePair> list) {
		super();
		for (NameValuePair nvp : list) {
			super.put(nvp.getName(), nvp.getValue());
		}
	}

	public void add(String name, String value) {
		super.put(name, value);
	}
	
	public void add(Parameters params) {
		if (params == null) return;
		super.putAll(params);
	}
	
	public boolean contains(String name) {
		return super.containsKey(name);
	}
	
	public Key getSysId() {
		String sysid = get("sys_id");
		return (sysid == null) ? null : new Key(sysid);
	}
	
	public String getNumber() {
		return get("number");
	}

	public void add(NameValuePair nvp) {
		super.put(nvp.getName(), nvp.getValue());
	}
	
	public List<NameValuePair> nvpList() {
		List<NameValuePair> result = new ArrayList<NameValuePair>(this.size());
		for (Map.Entry<String,String> entry : this.entrySet()) {
			NameValuePair nvp = new BasicNameValuePair(entry.getKey(), entry.getValue());
			result.add(nvp);
		}
		return result;		
	}
	
	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();
		for (Map.Entry<String,String> entry : this.entrySet()) {
			obj.put(entry.getKey(), entry.getValue());
		}
		return obj;		
	}
}
