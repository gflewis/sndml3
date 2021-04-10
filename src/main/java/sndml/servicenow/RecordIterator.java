package sndml.servicenow;

import java.util.*;

/**
 * An iterator used to loop through a {@link RecordList}.
 */
public class RecordIterator implements Iterator<BaseRecord> {

	private ListIterator<BaseRecord> iter;
	
	RecordIterator(RecordList list) {
		iter = list.listIterator();
	}
	
	public boolean hasNext() {
		return iter.hasNext();
	}

	public BaseRecord next() {
		return iter.next();
	}

	public void remove() {
		iter.remove();		
	}

}
