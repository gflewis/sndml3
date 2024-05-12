package sndml.servicenow;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Holds a list of {@link RecordKey} (<i>i.e.</i> <b>sys_id</b>) values 
 * as returned from a <b>getKeys</b> SOAP Web Services call. 
 * 
 */
public class RecordKeySet extends ArrayList<RecordKey> {

	private static final long serialVersionUID = 1L;

	public RecordKeySet() {
		super();
	}
	
	public RecordKeySet(int size) {
		super(size);
	}
	
	public RecordKeySet(ArrayNode array) {		
		this(array.size());		
		for (int i = 0; i < array.size(); ++i) {
			JsonNode ele = array.get(i);
			this.add(new RecordKey(ele.asText()));
		}		
	}
		
	public RecordKeySet(Set<RecordKey> set) {
		super(set.size());
		for (RecordKey key : set) {
			this.add(key);
		}
	}
	
	/**
	 * @return the complete list as a comma separated list of sys_ids.
	 */
	public String toString() {	
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < size(); ++i) {
			if (i > 0) result.append(",");
			result.append(get(i).toString());
		}
		return result.toString();				
	}
	
	/**
	 * Returns a subset of the list as comma separated string.
	 * Used to construct encoded queries.
	 * The number of entries returned is (toIndex - fromIndex).
	 * An exception may occur if toIndex less than 0 or fromIndex greater than size().
	 * 
	 * @param startIndex Zero based starting index (inclusive).
	 * @param endIndex Zero based ending index (exclusive).
	 * @return A list of keys.
	 */
	public RecordKeySet getSlice(int startIndex, int endIndex) {
		RecordKeySet result = new RecordKeySet(endIndex - startIndex);
		int size = size();
		for (int i = startIndex; i < endIndex && i < size; ++i) {
			result.add(get(i));
		}
		return result;
	}
		
	public RecordKey maxValue() {
		RecordKey result = null;
		for (RecordKey key : this) {
			if (result == null || key.greaterThan(result)) result = key;
		}
		return result;		
	}
	
	public RecordKey minValue() {
		RecordKey result = null;
		for (RecordKey key : this) {
			if (result == null || key.lessThan(result)) result = key;
		}
		return result;
	}
	
	/**
	 * Return the number of unique values in this list of keys.
	 */
	@Deprecated
	int uniqueCount() {
		Hashtable<RecordKey,Boolean> hash = new Hashtable<RecordKey,Boolean>(this.size());
		for (RecordKey key : this) {
			hash.put(key, true);
		}
		return hash.size();
	}
	
}
