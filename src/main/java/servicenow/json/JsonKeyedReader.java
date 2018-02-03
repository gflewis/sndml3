package servicenow.json;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.EncodedQuery;
import servicenow.core.KeySet;
import servicenow.core.RecordList;
import servicenow.core.Table;
import servicenow.core.TableReader;
import servicenow.core.Writer;

public class JsonKeyedReader extends TableReader {

	protected final JsonTableAPI api;
	protected KeySet allKeys;

	static final int DEFAULT_PAGE_SIZE = 200;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
		
	public JsonKeyedReader(Table table) {
		super(table);
		api = table.json();
	}

	@Override
	public int getDefaultPageSize() {
		return DEFAULT_PAGE_SIZE;
	}

	@Override
	public void initialize() throws IOException {
		super.initialize();
		allKeys = api.getKeys(getQuery());
		setExpected(allKeys.size());
	}

	public void initialize(KeySet keys) throws IOException {
		super.initialize();
		allKeys = keys;
		setExpected(allKeys.size());
	}
	
	public Integer getExpected() {
		assert allKeys != null : "TableReader not initialized";
		return allKeys.size();
	}
		
	@Override
	public TableReader call() throws IOException, SQLException, InterruptedException {
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
//			if (viewName != null) params.add("__use_view", viewName);		
			RecordList recs = api.getRecords(sliceQuery, this.displayValue);
			writer.processRecords(recs);
			rowCount += recs.size();
			logger.info(String.format("processed %d / %d rows", rowCount, totalRows));
			fromIndex += pageSize;
		}
		return this;
	}

}
