package servicenow.core;

/**
 * Simple class to collect a bunch of records in a list.
 */
public class RecordListAccumulator extends Writer {

	RecordList all;
			
	public RecordListAccumulator(Table table) {
		all = new RecordList(table);
	}
	
	public void processRecords(RecordList recs) {
		all.addAll(recs);
		metrics.addInserted(recs.size());
	}
	
	public RecordList getRecords() {
		return all;
	}

}
