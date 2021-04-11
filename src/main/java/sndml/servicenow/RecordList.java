package sndml.servicenow;

import java.util.*;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An array of Records.
 */
public class RecordList extends ArrayList<TableRecord> {
	
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
		
	public RecordIterator iterator() {
		return new RecordIterator(this);
	}

	public RecordKey maxKey() {
		RecordKey result = null;
		for (TableRecord rec : this) {			
			RecordKey key = rec.getKey();
			if (result == null || key.greaterThan(result)) result = key;
		}
		return result;
	}
	
	public RecordKey minKey() {
		RecordKey result = null;
		for (TableRecord rec : this) {			
			RecordKey key = rec.getKey();
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
		for (TableRecord rec : this) {
			String value = rec.getValue(fieldname);
			if (value != null) {
				assert RecordKey.isGUID(value);
				result.add(new RecordKey(value));
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
