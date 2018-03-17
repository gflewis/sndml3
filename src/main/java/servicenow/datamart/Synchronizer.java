package servicenow.datamart;

import servicenow.api.*;

import java.io.IOException;
import java.sql.SQLException;

public class Synchronizer extends TableReader {

	final Database db;
	final String sqlTableName;
	final WriterMetrics writerMetrics = new WriterMetrics();
	TimestampHash dbTimestamps;
	RecordList snTimestamps;
	KeySet insertSet;
	KeySet updateSet;
	KeySet deleteSet;
	KeySet skipSet;
	
	public Synchronizer(Table table, Database db, String sqlTableName, WriterMetrics parentMetrics) {
		super(table);
		this.db = db;
		this.sqlTableName = sqlTableName;
		writerMetrics.setParent(parentMetrics);
	}

	@Override
	public WriterMetrics getWriterMetrics() {
		return this.writerMetrics;
	}
	
	@Override
	public int getDefaultPageSize() {
		return 200;
	}

	@Override
	public void initialize() throws IOException, SQLException, InterruptedException {
		this.initialize(this.getCreatedRange());
	}
	
	public void initialize(DateTimeRange createdRange) 
			throws IOException, SQLException, InterruptedException {
		super.initialize();
		logger.debug(Log.INIT, 
				"initialize table=" + table.getName() + " created=" + createdRange);
		DatabaseTimestampReader dbtsr = new DatabaseTimestampReader(db);
		if (createdRange == null) 
			dbTimestamps = dbtsr.getTimestamps(sqlTableName);
		else
			dbTimestamps = dbtsr.getTimestamps(sqlTableName, createdRange);
		logger.debug(Log.INIT, String.format("database rows=%d", dbTimestamps.size()));
		RestTableReader sntsr = new RestTableReader(this.table);
		sntsr.setFields(new FieldNames("sys_id,sys_updated_on"));
		sntsr.setCreatedRange(createdRange);
		sntsr.setPageSize(10000);
		sntsr.enableStats(false);
		sntsr.initialize();
		snTimestamps = sntsr.getAllRecords();
		TimestampHash examined = new TimestampHash();
		insertSet = new KeySet();
		updateSet = new KeySet();
		deleteSet = new KeySet();
		skipSet = new KeySet();
		for (Record rec : snTimestamps) {
			Key key = rec.getKey();
			DateTime snts = rec.getUpdatedTimestamp();
			DateTime dbts = dbTimestamps.get(key);
			if (dbts == null)
				insertSet.add(key);
			else if (dbts.equals(snts))
				skipSet.add(key);
			else
				updateSet.add(key);
			examined.put(key, snts);
		}
		logger.debug(Log.INIT, String.format("inserts=%d updated=%d skips=%d", 
				insertSet.size(), updateSet.size(), skipSet.size()));
		assert examined.size() == (insertSet.size() + updateSet.size() + skipSet.size()); 
		for (Key key : dbTimestamps.keySet()) {
			if (examined.get(key) == null) 
				deleteSet.add(key);
		}
		logger.info(Log.INIT, String.format(
			"compare identified %d inserts, %d updates, %d deletes, %d skips", 
			insertSet.size(), updateSet.size(), deleteSet.size(), skipSet.size()));
		int expected = insertSet.size() + updateSet.size() + deleteSet.size();
		this.setExpected(expected);
	}

	@Override
	public TableReader setFilter(EncodedQuery value) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public TableReader call() throws IOException, SQLException, InterruptedException {
		assert initialized;
		assert dbTimestamps != null;
		assert snTimestamps != null;
		// Process the Inserts
		setLogContext();
		writerMetrics.start();
		logger.info(Log.PROCESS, String.format("Inserting %d rows", insertSet.size()));
		if (insertSet.size() > 0) {
			DatabaseInsertWriter insertWriter = new DatabaseInsertWriter(db, table, sqlTableName);
			insertWriter.setParentMetrics(this.writerMetrics);
			KeySetTableReader insertReader = new KeySetTableReader(table);
			insertReader.setParent(this);
			insertReader.setPageSize(this.getPageSize());
			insertReader.setWriter(insertWriter);
			insertWriter.open();
			setLogContext();
			insertReader.initialize(insertSet);
			insertReader.call();
			insertWriter.close();
			int rowsInserted = insertWriter.getMetrics().getInserted();
			if (rowsInserted != insertSet.size())
				logger.error(Log.PROCESS, String.format("inserted %d, expected to insert %d", 
					rowsInserted, insertSet.size()));
		}
		
		// Process the Updates
		setLogContext();
		logger.info(Log.PROCESS, String.format("Updating %d rows",  updateSet.size()));
		if (updateSet.size() > 0) {
			DatabaseUpdateWriter updateWriter = new DatabaseUpdateWriter(db, table, sqlTableName);
			updateWriter.setParentMetrics(this.writerMetrics);
			KeySetTableReader updateReader = new KeySetTableReader(table);
			updateReader.setParent(this);
			updateReader.setPageSize(this.getPageSize());
			updateReader.setWriter(updateWriter);
			updateWriter.open();
			setLogContext();
			updateReader.initialize(updateSet);
			updateReader.call();
			updateWriter.close();
			int rowsUpdated = updateWriter.getMetrics().getUpdated();
			if (rowsUpdated != updateSet.size())
				logger.error(Log.PROCESS, String.format("updated %d, expected to update %d", 
					rowsUpdated, updateSet.size()));
		}
					
		// Process the Deletes
		setLogContext();
		logger.info(Log.PROCESS, String.format("Deleting %d rows", deleteSet.size()));
		if (deleteSet.size() > 0) {
			DatabaseDeleteWriter deleteWriter = new DatabaseDeleteWriter(db, table, sqlTableName);
			deleteWriter.setParentMetrics(this.writerMetrics);
			deleteWriter.open();
			setLogContext();
			deleteWriter.deleteRecords(deleteSet);
			deleteWriter.close();
			int rowsDeleted = deleteWriter.getMetrics().getDeleted();
			if (rowsDeleted != deleteSet.size())
				logger.error(Log.PROCESS, String.format("deleted %d, expected to delete %d", 
					rowsDeleted, deleteSet.size()));
			writerMetrics.addSkipped(skipSet.size());
		}
		writerMetrics.finish();
		return this;
	}

}
