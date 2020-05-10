package servicenow.api;

import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;

public class JsonRecord extends Record {

	final JSONObject obj;
	
	public JsonRecord(Table table, JSONObject obj) {
		this.table = table;
		this.obj = obj;		
	}
	
	@Override
	public String getValue(String fieldname) {
		if (obj.has(fieldname)) {
			if (obj.isNull(fieldname)) {
				return null;
			}
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
			String msg = table.getName() + 
					"." + this.getKey() + "." + fieldname + 
					" type is " + field.getClass().getName();
			Logger logger = Log.logger(this.getClass());
			logger.error(Log.RESPONSE, obj.toString());
			logger.error(Log.RESPONSE, msg);
			throw new JsonResponseError(msg);
		}
		else return null;
	}

	@Override
	public String getDisplayValue(String fieldname) {
		if (obj.has("dv_" + fieldname)) {
			// JSONv2 API
			String displayValue = obj.getString("dv_" + fieldname);
			return displayValue;
		}
		if (obj.has(fieldname)) {
			// REST Table API
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
		return null;
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
