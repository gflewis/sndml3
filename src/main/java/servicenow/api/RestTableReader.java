package servicenow.api;

import java.io.IOException;
import java.sql.SQLException;

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
	
	public void initialize() throws IOException, InterruptedException  {		
		try {
			super.initialize();
		} catch (SQLException e) {
			// impossible
			throw new AssertionError(e);
		}
		EncodedQuery statsQuery = getStatsQuery();
		logger.debug(Log.INIT, String.format(
			"initialize statsEnabled=%b query=\"%s\"", statsEnabled, statsQuery));
		if (statsEnabled) {
			stats = restAPI.getStats(statsQuery, false);
			setExpected(stats.getCount());
			logger.debug(Log.INIT, String.format("expected=%d", getExpected()));	
		}
	}
	
	public RestTableReader call() throws IOException, SQLException, InterruptedException {
		assert initialized;
		RecordWriter writer = getWriter();
		assert writer != null;
		setLogContext();
		int rowCount = 0;
		Key maxKey = null;
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
			getReaderMetrics().increment(recs.size());
			maxKey = recs.maxKey();
			writer.processRecords(this, recs);			
			rowCount += recs.size();
			offset += recs.size();
			if (isFinished(recs.size(), rowCount)) finished = true;
			logger.debug(Log.PROCESS, String.format("processed %d rows so far", rowCount));
			if (maxRows != null && rowCount > maxRows)
				throw new RowCountExceededException(table, 
					String.format("processed %d rows (MaxRows=%d)", rowCount, maxRows));
		}
		if (statsEnabled) {
			if (rowCount != getExpected()) {
				logger.warn(Log.PROCESS, 
					String.format("Expected %d rows but processed %d rows", getExpected(), rowCount));
			}
		}
		return this;
	}
	
	protected boolean isFinished(int pageRows, int totalRows) {
		if (pageRows == 0) return true;
		if (statsEnabled && totalRows >= getExpected()) return true;
		return false;
	}
		
}
