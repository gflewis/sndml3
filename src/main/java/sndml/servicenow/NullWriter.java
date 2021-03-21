package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;

/**
 * {@link RecordWriter} which discards all input.
 *
 */
public class NullWriter extends RecordWriter {

	public NullWriter() {
		super();
	}

	@Override
	public void processRecords(
			RecordList recs, Metrics writerMetrics, ProgressLogger progressLogger) 
			throws IOException, SQLException {
		writerMetrics.addSkipped(recs.size());
	}

}
