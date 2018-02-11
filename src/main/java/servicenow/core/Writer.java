package servicenow.core;

import java.io.IOException;
import java.sql.SQLException;

public abstract class Writer {

	protected final String writerName;
	protected WriterMetrics writerMetrics;
	protected TableReader reader;

	
	public Writer(String name) {
		this.writerName = name;
		this.writerMetrics = new WriterMetrics(name);
	}
	
	public abstract void processRecords(RecordList recs) throws IOException, SQLException;	

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
	
	public void setReader(TableReader reader) {
		this.reader = reader;
	}
	
	public TableReader getReader() {
		return this.reader;
	}
	
	public String getWriterName() {
		return this.writerName;
	}

}
