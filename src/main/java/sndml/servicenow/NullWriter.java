package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;

/**
 * {@link RecordWriter} which discards all input.
 *
 */
public class NullWriter extends RecordWriter {

	public NullWriter() {
		super(new NullProgressLogger());
	}

	@Override
	public void processRecords(TableReader reader, RecordList recs) throws IOException, SQLException {
		writerMetrics.addSkipped(recs.size());
	}

}
