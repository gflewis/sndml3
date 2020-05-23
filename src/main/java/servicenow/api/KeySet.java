package servicenow.api;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Holds a list of <b>sys_id</b>s (GUIDs) 
 * as returned from a <b>getKeys</b> Web Services call. 
 * 
 */
public class KeySet extends ArrayList<Key> {

	private static final long serialVersionUID = 1L;

	public KeySet() {
		super();
	}
	
	public KeySet(int size) {
		super(size);
	}
	
	public KeySet(JSONArray array) {
		this(array.length());
		for (int i = 0; i < array.length(); ++i) {
			Object obj = array.get(i);
			if (obj instanceof String) 
				this.add(new Key((String) obj));
			else
				throw new JsonResponseError("Expected sys_id; found: " + obj.toString());
		}		
	}
	
	public KeySet(JSONObject obj, String fieldname) {
		this(obj.getJSONArray(fieldname));
	}
	
	public KeySet(Set<Key> set) {
		super(set.size());
		for (Key key : set) {
			this.add(key);
		}
	}
	
	/**
	 * Returns the complete list as a comma separated list of sys_ids.
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
	public KeySet getSlice(int startIndex, int endIndex) {
		KeySet result = new KeySet(endIndex - startIndex);
		int size = size();
		for (int i = startIndex; i < endIndex && i < size; ++i) {
			result.add(get(i));
		}
		return result;
	}
	
	/**
	 * Returns a {@link EncodedQuery}
	 * that selects all the records in a subset of this list.
	 * @param fromIndex Zero based starting index (inclusive).
	 * @param toIndex Zero based ending index (exclusive).
	 */
	public EncodedQuery encodedQuery(int fromIndex, int toIndex) {
		String queryStr = "sys_idIN" + this.getSlice(fromIndex, toIndex).toString();
		return new EncodedQuery(queryStr);
	}
	
	public EncodedQuery encodedQuery() {
		return this.encodedQuery(0,  this.size());
	}
	
	public Key maxValue() {
		Key result = null;
		for (Key key : this) {
			if (result == null || key.greaterThan(result)) result = key;
		}
		return result;		
	}
	
	public Key minValue() {
		Key result = null;
		for (Key key : this) {
			if (result == null || key.lessThan(result)) result = key;
		}
		return result;
	}
	
	/**
	 * Return the number of unique values in this list of keys.
	 */
	@Deprecated
	int uniqueCount() {
		Hashtable<Key,Boolean> hash = new Hashtable<Key,Boolean>(this.size());
		for (Key key : this) {
			hash.put(key, true);
		}
		return hash.size();
	}
	
}
