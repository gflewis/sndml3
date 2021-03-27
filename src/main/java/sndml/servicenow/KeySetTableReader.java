package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;

/**
 * A {@link TableReader} which attempts to read records using a {@link KeySet}.
 */
public class KeySetTableReader extends TableReader {

	protected final JsonTableAPI jsonAPI;
	protected KeySet allKeys;

	public KeySetTableReader(Table table) {
		super(table);
		jsonAPI = table.json();
	}


	@Override
	public void prepare(RecordWriter writer, Metrics metrics, ProgressLogger progress) 
			throws IOException, InterruptedException {
		logger.error(Log.INIT, "Deprecated function. Missing keys argument.");
		beginPrepare(writer, metrics, progress);
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, String.format("initialize query=\"%s\"", query));
		TableStats stats = table.rest().getStats(query, false);
		int expected = stats.getCount();
		// Use SOAP here because JSON limits results to 10000 but SOAP does not
		allKeys = table.soap().getKeys(getQuery());
		if (allKeys.size() != expected)
			logger.warn(Log.PROCESS,
					String.format("Expected %d keys but SOAP only returned %d; Please check ACLs",
						expected, allKeys.size()));				
		endPrepare(allKeys.size());
		logger.debug(Log.INIT, String.format("expected=%d", getExpected()));
	}

	public void prepare(KeySet keys, RecordWriter writer, Metrics metrics, ProgressLogger progress) 
			throws IOException {
		beginPrepare(writer, metrics, progress);					
		logger.debug(Log.INIT, String.format("prepare numkeys=%d", keys.size()));
		allKeys = keys;
		endPrepare(allKeys.size());
	}
	
	public Integer getExpected() {
		assert allKeys != null : "Not initialized";
		return allKeys.size();
	}
		
	@Override
	public Metrics call() throws IOException, SQLException, InterruptedException {
		progress.logStart();
		int pageSize = this.getPageSize();
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
			EncodedQuery sliceQuery = new EncodedQuery(table, slice);
			Parameters params = new Parameters();
			if (this.viewName != null) params.add("sysparm_view", this.viewName);
			if (this.displayValue) params.add("displayvalue", "all");
			params.add("sysparm_query", sliceQuery.toString());
			RecordList recs = jsonAPI.getRecords(params);
			incrementInput(recs.size());			
			writer.processRecords(recs, metrics, progress);
			rowCount += recs.size();
			logger.debug(String.format("processed %d / %d rows", rowCount, totalRows));
			if (maxRows != null && rowCount > maxRows)
				throw new TooManyRowsException(table, maxRows, rowCount);
			fromIndex += pageSize;
		}
		return metrics;
	}

}
