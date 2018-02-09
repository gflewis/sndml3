package servicenow.datamart;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;

import servicenow.core.*;

public abstract class TableWriter extends Writer {

	final private Database db;
	final private Table table;
	final private String sqlTableName;
	
	private ColumnDefinitions columns;
	protected InsertStatement insertStmt;
	protected UpdateStatement updateStmt;
	protected DeleteStatement deleteStmt;
	
	final private Logger logger = Log.logger(this.getClass());
	
	public TableWriter(String name, Database db, Table table, String sqlTableName) throws IOException, SQLException {
		super(name);
		this.db = db;
		this.table = table;
		this.sqlTableName = sqlTableName == null ? table.getName() : sqlTableName;
		Log.setTableContext(this.table);
	}
	
	@Override
	public void open() throws SQLException, IOException {
		columns = new ColumnDefinitions(this.db, this.table, this.sqlTableName);
		insertStmt = new InsertStatement(this.db, this.sqlTableName, columns);
		updateStmt = new UpdateStatement(this.db, this.sqlTableName, columns);
		deleteStmt = new DeleteStatement(this.db, this.sqlTableName);
		writerMetrics.start();
	}

	/*
	void insertRecord(Record rec) throws SQLException {
		insertStmt.insert(rec);
		metrics.incrementInserted();
	}
	
	void updateRecord(Record rec) throws SQLException {
		if (updateStmt.update(rec))
			metrics.incrementUpdated();
	}
	
	void deleteRecord(Key key) throws SQLException {
		if (deleteStmt.deleteRecord(key))
			metrics.incrementDeleted();
	}
	*/
	
	@Override
	public synchronized void processRecords(RecordList recs) throws IOException, SQLException {
		Log.setWriterContext(this);
		writerMetrics.start();
		for (Record rec : recs) {
			writeRecord(rec);
			logger.debug(Log.PROCESS, String.format("processing %s", rec.getKey().toString()));
		}
		writerMetrics.finish();
		logProgress("loaded");
		db.commit();
	}
	
	private synchronized void logProgress(String status) {
		assert this.reader != null;
		reader.setLogContext();
		ReaderMetrics readerMetrics = getReader().readerMetrics();
		assert readerMetrics != null;
		if (readerMetrics.getParent() == null) 
			logger.info(Log.PROCESS, String.format("%s %s", status, readerMetrics.getProgress()));
		else
			logger.info(Log.PROCESS, String.format("%s %s (%s)", status, 
					readerMetrics.getProgress(), readerMetrics.getParent().getProgress())); 
	}

	abstract void writeRecord(Record rec) throws SQLException;
					
}
