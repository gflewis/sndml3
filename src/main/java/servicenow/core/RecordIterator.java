package servicenow.core;

import java.util.*;

/**
 * An iterator used to loop through a {@link RecordList}.
 */
public class RecordIterator implements Iterator<Record> {

	private ListIterator<Record> iter;
	
	RecordIterator(RecordList list) {
		iter = list.listIterator();
	}
	
	public boolean hasNext() {
		return iter.hasNext();
	}

	public Record next() {
		return iter.next();
	}

	public void remove() {
		iter.remove();		
	}

}
