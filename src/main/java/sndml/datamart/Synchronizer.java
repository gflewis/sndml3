package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;

import sndml.servicenow.*;

/**
 * A class which knows how to compare the keys and timestamps from a ServiceNow table
 * with the keys and timestamps from a database table
 * and figure out which records need to be inserted, which ones need to be updated,
 * and which ones need to be deleted in the database table.
 *
 */
public class Synchronizer extends TableReader {

	final Database db;
	final String sqlTableName;
	final String writerName;
	
	TimestampHash dbTimestamps;
	KeySet insertSet;
	KeySet updateSet;
	KeySet deleteSet;
	KeySet skipSet;
	
	public Synchronizer(Table table, Database db, String sqlTableName, String writerName) {
		super(table);
		this.db = db;
		this.sqlTableName = sqlTableName;
		this.writerName = writerName;
		this.metrics = new Metrics(writerName);
	}
		
//	@Override
//	public Metrics getMetrics() {
//		return this.metrics;
//	}
	
	/**
	 * <p>This method will do the following.</p>
	 * <ol>
	 * <li>Read all the keys and sys_updated_on values from the SQL table 
	 * (within the specified sys_created_on range).</li>
	 * <li>Retrieve all keys and sys_updated_on from ServiceNow
	 * (within the specified sys_created_on range).</li>
	 * <li>Compare the two lists, and generate the following three new lists:
	 * <ul>
	 * <li>Records to be inserted</li>
	 * <li>Records to be updated</li>
	 * <li>Records to be deleted</li>
	 * </ul>
	 * </ol>
	 * @param createdRange If not null then processing will be limited to records created in this range 
	 * @throws IOException
	 * @throws SQLException
	 * @throws InterruptedException
	 */	
	@Override
	public void prepare() 
			throws IOException, SQLException, InterruptedException {
		beginInitialize();
		logger.info(Log.INIT, String.format(
				"Begin compare sn=%s db=%s", table.getName(), sqlTableName));
		DatabaseTimestampReader dbtsr = new DatabaseTimestampReader(db);
		if (createdRange == null) 
			dbTimestamps = dbtsr.getTimestamps(sqlTableName);
		else
			dbTimestamps = dbtsr.getTimestamps(sqlTableName, createdRange);		
		KeySet dbKeys = dbTimestamps.getKeys(); // for debug
		Key dbMinKey = dbKeys.minValue(); // for debug
		Key dbMaxKey = dbKeys.maxValue(); // for debug
		logger.debug(Log.INIT, String.format("database rows=%d", dbTimestamps.size()));
		if (logger.isDebugEnabled() && dbTimestamps.size() > 0) {
			logger.debug(Log.INIT, String.format("database min key=%s updated %s", 
					dbMinKey, dbTimestamps.get(dbMinKey)));
			logger.debug(Log.INIT, String.format("database max key=%s updated %s", 
					dbMaxKey, dbTimestamps.get(dbMaxKey)));
		}
		RestTableReader sntsr = new RestTableReader(this.table);
		sntsr.setFields(new FieldNames("sys_id,sys_updated_on"));
		sntsr.setCreatedRange(createdRange);
		sntsr.setPageSize(10000);
		sntsr.enableStats(true);
		RecordList snTimestamps = sntsr.getAllRecords();
		Key snMinKey = snTimestamps.minKey(); // for debug
		Key snMaxKey = snTimestamps.maxKey(); // for debug
		setLogContext();
		if (logger.isDebugEnabled() && snTimestamps.size() > 0) {
			logger.debug(Log.INIT, String.format("SN keys min=%s max=%s", snMinKey, snMaxKey));
		}
		TimestampHash examined = new TimestampHash();
		insertSet = new KeySet();
		updateSet = new KeySet();
		deleteSet = new KeySet();
		skipSet = new KeySet();
		for (Record rec : snTimestamps) {
			Key key = rec.getKey();
			assert key != null;
			assert !examined.containsKey(key) :
				String.format("duplicate key: %s", key.toString());				
			DateTime snts = rec.getUpdatedTimestamp();
			DateTime dbts = dbTimestamps.get(key);
			if (key.equals(snMinKey)) {			
				logger.debug(Log.INIT, String.format(
						"ServiceNow min key=%s snts=%s dbts=%s", key, snts, dbts));
			}
			if (key.equals(snMaxKey)) {			
				logger.debug(Log.INIT, String.format(
						"ServiceNow max key=%s snts=%s dbts=%s", key, snts, dbts));
			}
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
		assert examined.size() == (insertSet.size() + updateSet.size() + skipSet.size()) :
			String.format("examined=%d inserts=%d updated=%d skips=%d", 
					examined.size(), insertSet.size(), updateSet.size(), skipSet.size());
		for (Key key : dbTimestamps.keySet()) {
			if (examined.get(key) == null) 
				deleteSet.add(key);
		}
		logger.info(Log.INIT, String.format(
			"Compare identified %d inserts, %d updates, %d deletes, %d skips", 
			insertSet.size(), updateSet.size(), deleteSet.size(), skipSet.size()));
		int expected = insertSet.size() + updateSet.size() + deleteSet.size() + skipSet.size();
		endInitialize(expected);
	}
	
	public void prepare_new() 
			throws IOException, SQLException, InterruptedException {
		beginInitialize();
		getDatabaseTimestamps();
		RecordList snTimestamps = getServiceNowTimestamps();
		compareTimestamps(snTimestamps);
		int expected = insertSet.size() + updateSet.size() + deleteSet.size() + skipSet.size();
		endInitialize(expected);		
	}

	private TimestampHash getDatabaseTimestamps() throws SQLException {
		TimestampHash dbTimestamps;
		DatabaseTimestampReader dbtsr = new DatabaseTimestampReader(db);
		if (createdRange == null) 
			dbTimestamps = dbtsr.getTimestamps(sqlTableName);
		else
			dbTimestamps = dbtsr.getTimestamps(sqlTableName, createdRange);		
		KeySet dbKeys = dbTimestamps.getKeys(); // for debug
		Key dbMinKey = dbKeys.minValue(); // for debug
		Key dbMaxKey = dbKeys.maxValue(); // for debug
		logger.debug(Log.INIT, String.format("database rows=%d", dbTimestamps.size()));
		if (logger.isDebugEnabled() && dbTimestamps.size() > 0) {
			logger.debug(Log.INIT, String.format("database min key=%s updated %s", 
					dbMinKey, dbTimestamps.get(dbMinKey)));
			logger.debug(Log.INIT, String.format("database max key=%s updated %s", 
					dbMaxKey, dbTimestamps.get(dbMaxKey)));
		}
		return dbTimestamps;		
	}
	
	private RecordList getServiceNowTimestamps() throws IOException, InterruptedException {
		RestTableReader sntsr = new RestTableReader(this.table);
		sntsr.setFields(new FieldNames("sys_id,sys_updated_on"));
		sntsr.setCreatedRange(createdRange);
		sntsr.setPageSize(10000);
		sntsr.enableStats(true);
		RecordList snTimestamps = sntsr.getAllRecords();
		Key snMinKey = snTimestamps.minKey(); // for debug
		Key snMaxKey = snTimestamps.maxKey(); // for debug
		setLogContext();
		if (logger.isDebugEnabled() && snTimestamps.size() > 0) {
			logger.debug(Log.INIT, String.format("SN keys min=%s max=%s", snMinKey, snMaxKey));
		}
		return snTimestamps;		
	}
	
	private void compareTimestamps(RecordList snTimestamps) throws IOException, InterruptedException {
		Key snMinKey = snTimestamps.minKey(); // for debug
		Key snMaxKey = snTimestamps.maxKey(); // for debug
		TimestampHash examined = new TimestampHash();
		insertSet = new KeySet();
		updateSet = new KeySet();
		deleteSet = new KeySet();
		skipSet = new KeySet();
		for (Record rec : snTimestamps) {
			Key key = rec.getKey();
			assert key != null;
			assert !examined.containsKey(key) :
				String.format("duplicate key: %s", key.toString());				
			DateTime snts = rec.getUpdatedTimestamp();
			DateTime dbts = dbTimestamps.get(key);
			if (key.equals(snMinKey)) {			
				logger.debug(Log.INIT, String.format(
						"ServiceNow min key=%s snts=%s dbts=%s", key, snts, dbts));
			}
			if (key.equals(snMaxKey)) {			
				logger.debug(Log.INIT, String.format(
						"ServiceNow max key=%s snts=%s dbts=%s", key, snts, dbts));
			}
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
		assert examined.size() == (insertSet.size() + updateSet.size() + skipSet.size()) :
			String.format("examined=%d inserts=%d updated=%d skips=%d", 
					examined.size(), insertSet.size(), updateSet.size(), skipSet.size());
		for (Key key : dbTimestamps.keySet()) {
			if (examined.get(key) == null) 
				deleteSet.add(key);
		}
		
	}
	
	@Override
	public TableReader setFilter(EncodedQuery value) {
		if (!(value==null || value.isEmpty()))
			throw new UnsupportedOperationException();
		return this;
	}

	@Override
	public void logStart() {
		metrics.start();
		super.logStart();		
	}
	
	@Override
	public void logComplete() {
		metrics.finish();
		super.logComplete();
	}
	
	
	@Override
	public Metrics call() throws IOException, SQLException, InterruptedException {
		assert initialized;
		assert progressLogger != null;
		// Process the Inserts
		logStart();
		logger.info(Log.PROCESS, String.format("Inserting %d rows", insertSet.size()));
		assert writerName != null;
		if (insertSet.size() > 0) {
			String insertWriterName = writerName + ".INSERT";
			DatabaseInsertWriter insertWriter =	
					new DatabaseInsertWriter(db, table, sqlTableName, insertWriterName);
			Metrics insertWriterMetrics = new Metrics(insertWriterName, this.metrics);
			KeySetTableReader insertReader = new KeySetTableReader(table);
//			insertReader.setParent(this);
			insertReader.setFields(this.fieldNames);
			insertReader.setPageSize(this.getPageSize());
			insertReader.setWriter(insertWriter, insertWriterMetrics);
			insertReader.setProgressLogger(progressLogger);
			insertWriter.open(insertWriterMetrics);
			setLogContext();
			insertReader.parepare(insertSet);
			insertReader.call();
			insertWriter.close(insertWriterMetrics);
			int rowsInserted = insertWriterMetrics.getInserted();
			if (rowsInserted != insertSet.size())
				logger.error(Log.PROCESS, String.format("inserted %d, expected to insert %d", 
					rowsInserted, insertSet.size()));
		}
		
		// Process the Updates
		logger.info(Log.PROCESS, String.format("Updating %d rows",  updateSet.size()));
		if (updateSet.size() > 0) {
			String updateWriterName = writerName + ".UPDATE";
			DatabaseUpdateWriter updateWriter = 
					new DatabaseUpdateWriter(db, table, sqlTableName, updateWriterName);
			Metrics updateWriterMetrics = new Metrics(updateWriterName, this.metrics);
			KeySetTableReader updateReader = new KeySetTableReader(table);
//			updateReader.setParent(this);
			updateReader.setFields(this.fieldNames);
			updateReader.setPageSize(this.getPageSize());
			updateReader.setWriter(updateWriter, updateWriterMetrics);
			updateReader.setProgressLogger(progressLogger);
			updateWriter.open(updateWriterMetrics);
			setLogContext();
			updateReader.parepare(updateSet);
			updateReader.call();
			updateWriter.close(updateWriterMetrics);
			int rowsUpdated = updateWriterMetrics.getUpdated();
			if (rowsUpdated != updateSet.size())
				logger.error(Log.PROCESS, String.format("updated %d, expected to update %d", 
					rowsUpdated, updateSet.size()));
		}
					
		// Process the Deletes
		logger.info(Log.PROCESS, String.format("Deleting %d rows", deleteSet.size()));
		if (deleteSet.size() > 0) {
			String deleteWriterName = writerName + ".DELETE";
			DatabaseDeleteWriter deleteWriter = 
					new DatabaseDeleteWriter(db, table, sqlTableName, deleteWriterName);
			Metrics deleteWriterMetrics = new Metrics(deleteWriterName, this.metrics);
			deleteWriter.open(deleteWriterMetrics);
			setLogContext();
			deleteWriter.deleteRecords(deleteSet, deleteWriterMetrics, progressLogger);
			deleteWriter.close(deleteWriterMetrics);
			int rowsDeleted = deleteWriterMetrics.getDeleted();
			if (rowsDeleted != deleteSet.size())
				logger.error(Log.PROCESS, String.format("deleted %d, expected to delete %d", 
					rowsDeleted, deleteSet.size()));
		}
		metrics.addSkipped(skipSet.size());
		logComplete();
		// Release memory
		insertSet = null;
		updateSet = null;
		deleteSet = null;
		skipSet = null;
		return metrics;
	}

}
