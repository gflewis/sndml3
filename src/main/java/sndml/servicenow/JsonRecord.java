package sndml.servicenow;

import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonRecord extends TableRecord {

	final ObjectNode root;
	
	public JsonRecord(Table table, ObjectNode obj) {
		this.table = table;
		this.root = obj;		
	}
	
	@Override
	public String getValue(String fieldname) {
		JsonNode node = root.get(fieldname);
		if (node == null) return null;
		JsonNodeType nodetype = node.getNodeType();
		switch (nodetype) {
		case MISSING:
		case NULL:
			return null;
		case STRING:
			String value = node.asText();
			if (value.length() == 0) return null;
			return value;
		case OBJECT:
			if (node.has("value")) {
				value = node.get("value").asText();
				if (value.length() == 0) return null;
				return value;
			}
			// fall through to error
		default:
			String msg = table.getName() + 
				"." + this.getKey() + "." + fieldname + 
				" type is " + nodetype.toString();
			throw new JsonResponseError(msg);				
		}			
	}

	@Override
	public String getDisplayValue(String fieldname) {
		JsonNode node = root.get(fieldname);
		// REST Table API
		if (node.isObject()) {
			return node.get("display_value").asText();
		}
		// JSONv2 API
		JsonNode dvNode = root.get("dv_" + fieldname);
		if (dvNode != null) {
			return dvNode.asText();
		}
		return null;
	}

	@Override
	public Iterator<String> keys() {
		return root.fieldNames();
	}

	@Override
	public FieldNames getFieldNames() {
		FieldNames names = new FieldNames();
		Iterator<String> iter = root.fieldNames();
		while (iter.hasNext()) {
			names.add(iter.next());
		}
		return names;
	}

	@Override
	public String toString() {
		return root.toString();
	}

	@Override
	public String asText(boolean pretty) {
		if (pretty) 
			return root.toPrettyString();
		else
			return root.toString();
	}
			
}
