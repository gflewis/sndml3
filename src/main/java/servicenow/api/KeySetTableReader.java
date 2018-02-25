package servicenow.api;

import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeySetTableReader extends TableReader {

	protected final JsonTableAPI api;
	protected KeySet allKeys;

	private Logger logger = LoggerFactory.getLogger(this.getClass());
		
	public KeySetTableReader(Table table) {
		super(table);
		api = table.json();
	}

	public int getDefaultPageSize() {
		return 200;
	}

	public void initialize() throws IOException {
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, String.format("initialize query=\"%s\"", query));
		super.initialize();
		allKeys = table.json().getKeys(query);
		// JSONv2 API limits the number of keys to 10000
		// If we got more than 999 then check the result
		if (allKeys.size() > 999) {
			TableStats stats = table.rest().getStats(query, false);
			int expected = stats.getCount();
			if (allKeys.size() != expected) {
				logger.warn(Log.PROCESS, 
						String.format("Expected %d keys but JSON only returned %d; reverting to SOAP", expected, allKeys.size()));
				allKeys = table.soap().getKeys(getQuery());
				if (allKeys.size() != expected) {
					throw new ServiceNowException(
						String.format("Expected %d keys but SOAP only returned %d", expected, allKeys.size()));
				}
			}
		}
		setExpected(allKeys.size());
		logger.debug(Log.INIT, String.format("expected=%d", getExpected()));	
	}

	public void initialize(KeySet keys) throws IOException {
		logger.debug(Log.INIT, String.format("initialize numkeys=%d", keys.size()));
		super.initialize();
		allKeys = keys;
		setExpected(allKeys.size());
	}
	
	public Integer getExpected() {
		assert allKeys != null : "Not initialized";
		return allKeys.size();
	}
		
	@Override
	public TableReader call() throws IOException, SQLException, InterruptedException {
		assert initialized;
		setLogContext();
		Writer writer = this.getWriter();
		if (writer == null) throw new IllegalStateException("writer not defined");
		if (allKeys == null) throw new IllegalStateException("not initialized");
		if (pageSize <= 0) throw new IllegalStateException("invalid pageSize");
		int fromIndex = 0;
		int totalRows = allKeys.size();
		int rowCount = 0;
		while (fromIndex < totalRows) {
			int toIndex = fromIndex + pageSize;
			if (toIndex > totalRows) toIndex = totalRows;
			KeySet slice = allKeys.getSlice(fromIndex, toIndex);
			EncodedQuery sliceQuery = new EncodedQuery(slice);
			Parameters params = new Parameters();
			if (this.viewName != null) params.add("sysparm_view", this.viewName);
			if (this.displayValue) params.add("displayvalue", "all");
			params.add("sysparm_query", sliceQuery.toString());
			RecordList recs = api.getRecords(params);
			readerMetrics().increment(recs.size());			
			writer.processRecords(this, recs);
			rowCount += recs.size();
			logger.debug(String.format("processed %d / %d rows", rowCount, totalRows));
			fromIndex += pageSize;
		}
		return this;
	}

}
