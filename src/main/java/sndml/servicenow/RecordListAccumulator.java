package sndml.servicenow;

/**
 * Simple class to collect a bunch of records in a list.
 */
public class RecordListAccumulator extends RecordWriter {

	RecordList allRecords;
			
	public RecordListAccumulator(Table table) {
		super(new NullProgressLogger());
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
