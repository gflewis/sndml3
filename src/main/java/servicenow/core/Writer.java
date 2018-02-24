package servicenow.core;

import java.io.IOException;
import java.sql.SQLException;

public abstract class Writer {

	protected WriterMetrics writerMetrics;

	
	public Writer() {
		this.writerMetrics = new WriterMetrics(null);
	}

	public abstract void processRecords(TableReader source, RecordList recs) throws IOException, SQLException;	

	public void open() throws IOException, SQLException {
		writerMetrics.start();
	}
	
	public void close() {
		writerMetrics.finish();
	}

	public WriterMetrics getMetrics() {
		if (this.writerMetrics.getFinished() == null) this.writerMetrics.finish();
		return writerMetrics;
	}
	
}
