package sndml.servicenow;

/**
 * Simple class to collect a bunch of records in a list.
 */
public class RecordListAccumulator extends RecordWriter {

	RecordList allRecords;
			
	public RecordListAccumulator(TableReader reader) {
		this(reader.table);
	}
	
	public RecordListAccumulator(Table table) {
		super();
		allRecords = new RecordList(table);
	}
	
	public void processRecords(RecordList recs, Metrics metrics, ProgressLogger progressLogger) {
		allRecords.addAll(recs);
		metrics.addInserted(recs.size());
	}
	
	public RecordList getRecords() {
		return allRecords;
	}

}
