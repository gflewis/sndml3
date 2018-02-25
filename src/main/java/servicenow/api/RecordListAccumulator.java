package servicenow.api;

/**
 * Simple class to collect a bunch of records in a list.
 */
public class RecordListAccumulator extends Writer {

	RecordList allRecords;
			
	public RecordListAccumulator(Table table) {
		super();
		allRecords = new RecordList(table);
	}
	
	public void processRecords(TableReader reader, RecordList recs) {
		allRecords.addAll(recs);
		writerMetrics.addInserted(recs.size());
	}
	
	public RecordList getRecords() {
		return allRecords;
	}

}
