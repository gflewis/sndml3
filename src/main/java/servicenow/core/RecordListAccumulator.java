package servicenow.core;

/**
 * Simple class to collect a bunch of records in a list.
 */
public class RecordListAccumulator extends Writer {

	RecordList allRecords;
			
	public RecordListAccumulator(Table table) {
		super("_accumulator_");
		allRecords = new RecordList(table);
	}
	
	public void processRecords(RecordList recs) {
		allRecords.addAll(recs);
		writerMetrics.addInserted(recs.size());
	}
	
	public RecordList getRecords() {
		return allRecords;
	}

}
