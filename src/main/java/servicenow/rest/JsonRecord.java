package servicenow.rest;

import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;

import servicenow.core.*;

public class JsonRecord extends Record {

	final JSONObject obj;
	
	protected JsonRecord(Table table, JSONObject obj) {
		this.table = table;
		this.obj = obj;		
	}
	
	@Override
	public String getValue(String fieldname) {
		if (obj.has(fieldname)) {
			Object field = obj.get(fieldname);
			if (field instanceof String) {
				String value = (String) field;
				if (value.length() == 0) return null;
				return value;
			}
			if (field instanceof JSONObject) {
				String value = ((JSONObject) field).getString("value");
				if (value.length() == 0) return null;
				return value;			
			}
			throw new JsonResponseError("table=" + table.getName() + "; value not string: " + fieldname);			
		}
		else return null;
	}

	@Override
	public String getDisplayValue(String fieldname) {
		if (obj.has(fieldname)) {
			try {
				JSONObject field = obj.getJSONObject(fieldname);
				String displayValue = field.getString("display_value");
				if (displayValue.length() == 0) return null;
				return displayValue;
			}
			catch (JSONException e) {
				return null;
			}			
		}
		else return null;
	}

	@Override
	public Iterator<String> keys() {
		return obj.keys();
	}

	@Override
	public FieldNames getFieldNames() {
		FieldNames names = new FieldNames();
		Iterator<String> iter = obj.keys();
		while (iter.hasNext()) {
			names.add(iter.next());
		}
		return names;
	}

}
