package servicenow.datamart;

import servicenow.api.*;

import java.io.IOException;
import java.sql.SQLException;

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
	final WriterMetrics writerMetrics = new WriterMetrics();
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
	
	public void initialize(DateTimeRange createdRange) 
			throws IOException, SQLException, InterruptedException {
		TimestampHash dbTimestamps;
		RecordList snTimestamps;
		super.initialize();
		logger.info(Log.INIT, "begin compare");
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
		sntsr.setReaderName(this.getReaderName());
		sntsr.setFields(new FieldNames("sys_id,sys_updated_on"));
		sntsr.setCreatedRange(createdRange);
		sntsr.setPageSize(10000);
		sntsr.enableStats(true);
		sntsr.initialize();
		snTimestamps = sntsr.getAllRecords();
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
						"servicenow min key=%s snts=%s dbts=%s", key, snts, dbts));
			}
			if (key.equals(snMaxKey)) {			
				logger.debug(Log.INIT, String.format(
						"servicenow max key=%s snts=%s dbts=%s", key, snts, dbts));
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
			"compare identified %d inserts, %d updates, %d deletes, %d skips", 
			insertSet.size(), updateSet.size(), deleteSet.size(), skipSet.size()));
		int expected = insertSet.size() + updateSet.size() + deleteSet.size();
		this.setExpected(expected);
	}

	@Override
	public TableReader setQuery(EncodedQuery value) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public TableReader call() throws IOException, SQLException, InterruptedException {
		assert initialized;
		// Process the Inserts
		setLogContext();
		writerMetrics.start();
		logger.info(Log.PROCESS, String.format("Inserting %d rows", insertSet.size()));
		if (insertSet.size() > 0) {
			DatabaseInsertWriter insertWriter = new DatabaseInsertWriter(db, table, sqlTableName);
			insertWriter.setParentMetrics(this.writerMetrics);
			KeySetTableReader insertReader = new KeySetTableReader(table);
			insertReader.setParent(this);
			insertReader.setFields(this.fieldNames);
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
		logger.info(Log.PROCESS, String.format("Updating %d rows",  updateSet.size()));
		if (updateSet.size() > 0) {
			DatabaseUpdateWriter updateWriter = new DatabaseUpdateWriter(db, table, sqlTableName);
			updateWriter.setParentMetrics(this.writerMetrics);
			KeySetTableReader updateReader = new KeySetTableReader(table);
			updateReader.setParent(this);
			updateReader.setFields(this.fieldNames);
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
		}
		writerMetrics.addSkipped(skipSet.size());
		writerMetrics.finish();
		// Release memory
		insertSet = null;
		updateSet = null;
		deleteSet = null;
		skipSet = null;
		return this;
	}

}
