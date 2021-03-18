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
		super(null);
		allRecords = new RecordList(table);
	}
	
	public void processRecords(RecordList recs, Metrics writerMetrics, ProgressLogger progressLogger) {
		allRecords.addAll(recs);
		writerMetrics.addInserted(recs.size());
	}
	
	public RecordList getRecords() {
		return allRecords;
	}

}
