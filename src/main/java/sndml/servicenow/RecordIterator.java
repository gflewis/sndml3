package sndml.servicenow;

import java.util.*;

/**
 * An iterator used to loop through a {@link RecordList}.
 */
public class RecordIterator implements Iterator<TableRecord> {

	private ListIterator<TableRecord> iter;
	
	RecordIterator(RecordList list) {
		iter = list.listIterator();
	}
	
	public boolean hasNext() {
		return iter.hasNext();
	}

	public TableRecord next() {
		return iter.next();
	}

	public void remove() {
		iter.remove();		
	}

}
