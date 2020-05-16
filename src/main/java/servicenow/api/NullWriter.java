package servicenow.api;

import java.io.IOException;
import java.sql.SQLException;

public class NullWriter extends RecordWriter {

	public NullWriter() {
		super();
	}

	@Override
	public void processRecords(TableReader reader, RecordList recs) throws IOException, SQLException {
		writerMetrics.addSkipped(recs.size());
	}

}
