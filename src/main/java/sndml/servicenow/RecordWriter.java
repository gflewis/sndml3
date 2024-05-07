package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;

import sndml.agent.JobCancelledException;
import sndml.util.Metrics;

/**
 * A class which knows how to process records retrieved from ServiceNow.
 * 
 * Although this is normally instantiated as a {@link sndml.loader.DatabaseTableWriter}.
 * there are several other subclasses.
 *
 */
public abstract class RecordWriter {

	public RecordWriter() {
	}
			
	public abstract void processRecords(
			RecordList recs, Metrics metrics, ProgressLogger progressLogger) 
		throws JobCancelledException, IOException, SQLException;	

	public RecordWriter open(Metrics metrics) throws IOException, SQLException {
		metrics.start();
		return this;
	}
	
	public void close(Metrics metrics) {
		metrics.finish();
	}
			
	
}
