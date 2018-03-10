package servicenow.datamart;

import java.util.Hashtable;
import java.util.Set;

import servicenow.api.*;

@Deprecated
public class TableIndex {

	public class Entry {
		Key key;
		DateTime created;
		DateTime updated;
		boolean processed;
		
		Entry(Key key, DateTime created, DateTime updated) {
			this.key = key;
			this.created = created;
			this.updated = updated;
			this.processed = false;
		}

		public void setProcessed(boolean value) {
			this.processed = value;
		}
	
	}
	
	Hashtable<Key,Entry> hash;	

	public TableIndex() {
		hash = new Hashtable<Key,Entry>();
	}
	
	Entry add(Key key, DateTime created, DateTime updated) {
		Entry entry = new Entry(key, created, updated);
		hash.put(key,  entry);
		return entry;
	}
	
	int size() {
		return hash.size();
	}
	
	public Entry get(Key key) {
		return hash.get(key);
	}

	public Set<Key> getKeySet() {
		return hash.keySet();
	}
	
	KeySet getKeys() {
		KeySet keys = new KeySet(hash.size());
		for (Key key : hash.keySet()) {
			keys.add(key);			
		}
		return keys;
	}
	
}
