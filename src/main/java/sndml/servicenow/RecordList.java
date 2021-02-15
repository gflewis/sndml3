package sndml.servicenow;

import java.util.*;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An array of Records.
 */
public class RecordList extends ArrayList<Record> {
	
	private static final long serialVersionUID = 1L;
	
	final protected Table table;
	
	public RecordList(Table table) {
		super();
		this.table = table;
	}

	public RecordList(Table table, int size) {
		super(size);
		this.table = table;
	}

	public RecordList(Table table, ArrayNode array) {
		this(table, array.size());
		for (int i = 0; i < array.size(); ++i) {
			ObjectNode entry = (ObjectNode) array.get(i);
			JsonRecord rec = new JsonRecord(table, entry);
			this.add(rec);
		}
	}
	
	/*
	public RecordList(Table table, JSONObject obj, String fieldname) {
		this(table, (JSONArray) obj.get(fieldname));
	}
	*/
	
	public RecordIterator iterator() {
		return new RecordIterator(this);
	}

	public Key maxKey() {
		Key result = null;
		for (Record rec : this) {			
			Key key = rec.getKey();
			if (result == null || key.greaterThan(result)) result = key;
		}
		return result;
	}
	
	public Key minKey() {
		Key result = null;
		for (Record rec : this) {			
			Key key = rec.getKey();
			if (result == null || key.lessThan(result)) result = key;
		}
		return result;
	}
	
	/**
	 * Extract all the values of a reference field from a list of records.
	 * Null keys are not included in the list.
	 * @param fieldname Name of a reference field
	 * @return A list keys
	 */
	public KeySet extractKeys(String fieldname) {
		KeySet result = new KeySet(this.size());
		if (this.size() == 0) return result;
		for (Record rec : this) {
			String value = rec.getValue(fieldname);
			if (value != null) {
				assert Key.isGUID(value);
				result.add(new Key(value));
			}
		}		
		return result;
	}

	/**
	 * Extract the primary keys (sys_ids) from this list.
	 */
	public KeySet extractKeys()  {
		return extractKeys("sys_id");
	}
	
}
