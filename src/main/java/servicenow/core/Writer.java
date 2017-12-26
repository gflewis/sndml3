package servicenow.core;

import java.io.IOException;
import java.sql.SQLException;

public abstract class Writer {

	protected WriterMetrics metrics = new WriterMetrics();
	protected TableReader reader;
	
	public abstract void processRecords(RecordList recs) throws IOException, SQLException;	

	public void open() throws IOException, SQLException {
		metrics.start();
	}
	
	public void close() {
		metrics.finish();
	}
	
	public WriterMetrics getMetrics() {
		if (this.metrics.getFinished() == null) this.metrics.finish();
		return metrics;
	}
	
	public void setReader(TableReader reader) {
		this.reader = reader;
	}
	
	public TableReader getReader() {
		return this.reader;
	}

}
