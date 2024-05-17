package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;

import sndml.agent.JobCancelledException;
import sndml.util.Log;
import sndml.util.Metrics;
import sndml.util.Parameters;

/**
 * <p>This class is designed to reliably read a large number of records from a ServiceNow table.
 * The records are read in chunks as defined by the page size.
 * Each chunk is read into a {@link RecordList} object 
 * and passed to a {@link RecordWriter} for processing.</p>
 * 
 * <p>This class does <b>not</b> use ServiceNow pagination (<i>i.e.</i> <code>sysparm_offset</code>).
 * This is because of the risk of a record getting inserted into the table
 * before all records have been read. 
 * If <code>sysparm_offset</code> is used and new records are inserted into the table
 * while the process is running, then there is a possibility of records getting skipped.
 * Instead, the records are read in <code>sys_id</code> order.
 * With each chunk the <code>sysparm_query</code> is modified to only include records
 * with <code>sys_id</code> greater than the highest value from the previous chunk.</p>
 * 
 * <p>Prior to starting, this class used the ServiceNow Aggregate API to count 
 * the number of expected records. If the number of records retrieved
 * does not match the expected number, then a warning is generated.
 * The {@link RestPetitTableReader} class can be used to eliminate the overhead
 * of this checking in cases where the number of records is small 
 * or known in advance to be immutable.</p>
 */
public class RestTableReader extends TableReader {

	final protected RestTableAPI restAPI;
	protected boolean statsEnabled;
	protected TableStats stats = null;
	
	private final int DEFAULT_PAGE_SIZE = 200;
		
	public RestTableReader(Table table) {
		super(table);
		this.restAPI = table.rest();
		this.statsEnabled = true;
		this.orderBy = OrderBy.KEYS;
	}
			
	public int getDefaultPageSize() {
		return DEFAULT_PAGE_SIZE;
	}
	
	public RestTableReader enableStats(boolean value) {
		this.statsEnabled = value;
		return this;
	}
	
	public void prepare(RecordWriter writer, Metrics metrics, ProgressLogger progressLogger) 
			throws IOException, InterruptedException  {
		beginPrepare(writer, metrics, progressLogger);
		EncodedQuery statsQuery = getStatsQuery();
		logger.debug(Log.INIT, String.format(
			"initialize statsEnabled=%b query=\"%s\"", statsEnabled, statsQuery));
		if (statsEnabled) {
			stats = restAPI.getStats(statsQuery, false);
			endPrepare(stats.getCount());
			logger.debug(Log.INIT, String.format("expected=%d", getExpected()));
		}
		else {
			endPrepare(null);
		}
	}
	
	public Metrics call() throws IOException, SQLException, JobCancelledException, InterruptedException {
		Log.setTableContext(table, this.getReaderName());
		progress.logStart();
		assert writer != null;
		assert metrics != null;
		int rowCount = 0;
		RecordKey maxKey = null;
		boolean finished = false;
		if (statsEnabled && stats.count == 0) {
			finished = true;
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing query");
		}
		int offset = 0;
		int pageSize = getPageSize();
		assert pageSize > 0;
		while (!finished) {
			Parameters params = new Parameters();
			if (orderBy == OrderBy.KEYS) {
				setKeyExclusion(maxKey);
			}
			else {
				// Should be dead code. We always order by sys_id.
				params.add("sysparm_offset", Integer.toString(offset));				
			}
			params.add("sysparm_limit", Integer.toString(pageSize));
			params.add("sysparm_exclude_reference_link", "true");			
			params.add("sysparm_display_value", displayValue ? "all" : "false");
			if (fieldNames != null) params.add("sysparm_fields", fieldNames.addKey().toString());
			if (viewName != null) params.add("sysparm_view", viewName);
			EncodedQuery query = getQuery();
			if (!query.isEmpty()) params.add("sysparm_query", query.toString());
			RecordList recs = restAPI.getRecords(params);
			logger.debug(Log.RESPONSE, String.format("retrieved %d rows", recs.size()));
			incrementInput(recs.size());
			maxKey = recs.maxKey();
			writer.processRecords(recs, metrics, progress);	
			rowCount += recs.size();
			offset += recs.size();
			if (isFinished(recs.size(), rowCount)) finished = true;
			logger.debug(Log.PROCESS, String.format("processed %d rows so far", rowCount));
			if (maxRows != null && rowCount > maxRows)
				throw new TooManyRowsException(table, maxRows, rowCount);
		}
		if (statsEnabled) {
			if (rowCount != getExpected()) {
				logger.warn(Log.PROCESS, 
					String.format("Expected %d rows but processed %d rows", getExpected(), rowCount));
			}
		}
		progress.logComplete();
		return metrics;
	}
	
	protected boolean isFinished(int pageRows, int totalRows) {
		if (pageRows == 0) return true;
		if (statsEnabled && totalRows >= getExpected()) return true;
		return false;
	}
		
}
