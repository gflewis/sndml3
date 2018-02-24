package servicenow.core;

import java.io.IOException;
import java.sql.SQLException;

public class NullWriter extends Writer {

	public NullWriter() {
		super();
	}

	@Override
	public void processRecords(RecordList recs) throws IOException, SQLException {
		writerMetrics.addSkipped(recs.size());
	}

}
