package servicenow.api;

import java.io.IOException;
import java.sql.SQLException;

public abstract class Writer {

	protected WriterMetrics writerMetrics = new WriterMetrics();

	public Writer() {
	}

	public abstract void processRecords(TableReader source, RecordList recs) throws IOException, SQLException;	

	public Writer open() throws IOException, SQLException {
		writerMetrics.start();
		return this;
	}
	
	public void close() throws SQLException {
		writerMetrics.finish();
	}

	public WriterMetrics getMetrics() {
		if (writerMetrics.getFinished() == null) writerMetrics.finish();
		return writerMetrics;
	}
	
}
