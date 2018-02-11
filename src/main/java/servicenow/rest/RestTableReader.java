package servicenow.rest;

import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.*;

public class RestTableReader extends TableReader {

	final protected RestTableAPI apiREST;
	private boolean statsEnabled = false;
	protected TableStats stats = null;
	
	static final int DEFAULT_PAGE_SIZE = 200;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public RestTableReader(Table table) {
		super(table);
		this.apiREST = table.rest();
	}
			
	public int getDefaultPageSize() {
		return DEFAULT_PAGE_SIZE;
	}
	
	public RestTableReader enableStats(boolean value) {
		this.statsEnabled = value;
		return this;
	}

	public void initialize() throws IOException {
		super.initialize();
		if (statsEnabled) {
			stats = apiREST.getStats(getQuery(), false);
			setExpected(stats.getCount());
		}
	}
	
	public RestTableReader call() throws IOException, SQLException, InterruptedException {
		Writer writer = getWriter();
		assert writer != null : "Writer not set";
		setLogContext();
		int rowCount = 0;
		boolean finished = false;
		if (statsEnabled && stats.count == 0) {
			finished = true;
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing query");
		}
		int offset = 0;
		while (!finished) {
			Parameters params = new Parameters();
			params.add("sysparm_offset", Integer.toString(offset));
			params.add("sysparm_limit", Integer.toString(pageSize));
			params.add("sysparm_exclude_reference_link", "true");			
			params.add("sysparm_display_value", displayValue ? "all" : "false");
			if (!EncodedQuery.isEmpty(getQuery())) params.add("sysparm_query", getQuery().toString());
			if (fieldNames != null) params.add("sysparm_fields", fieldNames.toString());
			if (viewName != null) params.add("sysparm_view", viewName);
			RecordList recs = apiREST.getRecords(params);
			readerMetrics().increment(recs.size());
			writer.processRecords(recs);			
			rowCount += recs.size();
			offset += recs.size();
			finished = (recs.size() < pageSize);
			logger.debug(Log.PROCESS, String.format("processed %d rows", rowCount));
		}
		return this;
	}
		
}
