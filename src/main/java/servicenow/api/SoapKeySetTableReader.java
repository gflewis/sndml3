package servicenow.api;

import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SoapKeySetTableReader extends TableReader {

	protected final SoapTableAPI apiSOAP;
	private KeySet allKeys;

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	static final int DEFAULT_PAGE_SIZE = 200;
		
	public SoapKeySetTableReader(Table table) {
		super(table);
		apiSOAP = table.soap();
	}
	
	public int getDefaultPageSize() {
		return DEFAULT_PAGE_SIZE;
	}

	@Override
	public void initialize() throws IOException, InterruptedException, SQLException {
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, "initialize query=" + query);
		super.initialize();
		allKeys = apiSOAP.getKeys(query);
		setExpected(allKeys.size());
		logger.debug(Log.INIT, String.format("expected=%d", getExpected()));	
	}

	public Integer getExpected() {
		assert allKeys != null : "TableReader not initialized";
		return allKeys.size();
	}
	
	public SoapKeySetTableReader call() throws IOException, InterruptedException, SQLException {
		Writer writer = this.getWriter();
		assert writer != null;
		assert allKeys != null;
		assert pageSize > 0;
		int fromIndex = 0;
		int totalRows = allKeys.size();
		int rowCount = 0;
		while (fromIndex < totalRows) {
			int toIndex = fromIndex + pageSize;
			if (toIndex > totalRows) toIndex = totalRows;
			KeySet slice = allKeys.getSlice(fromIndex, toIndex);
			EncodedQuery sliceQuery = new EncodedQuery(slice);
			Parameters params = new Parameters();
			params.add("__encoded_query", sliceQuery.toString());
			if (viewName != null) params.add("__use_view", viewName);
			RecordList recs = apiSOAP.getRecords(params, this.displayValue);
			getReaderMetrics().increment(recs.size());						
			writer.processRecords(this, recs);
			rowCount += recs.size();
			logger.info(String.format("processed %d / %d rows", rowCount, totalRows));
			fromIndex += pageSize;
		}
		return this;
	}

	@Override
	public TableReader setFields(FieldNames names) {
		throw new UnsupportedOperationException();
	}

}
