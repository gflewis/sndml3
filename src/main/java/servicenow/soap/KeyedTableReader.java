package servicenow.soap;

import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.*;

public class KeyedTableReader extends TableReader {

	final SoapTableAPI soapImpl;
	
	private KeySet allKeys;

	static final int DEFAULT_PAGE_SIZE = 200;
		
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public KeyedTableReader(SoapTableAPI impl) {
		super(impl);
		soapImpl = impl;
	}
	
	public int getDefaultPageSize() {
		return DEFAULT_PAGE_SIZE;
	}

	@Override
	public void initialize() throws IOException {
		super.initialize();
		allKeys = impl.getKeys(getQuery());
		setExpected(allKeys.size());
	}

	public Integer getExpected() {
		assert allKeys != null : "TableReader not initialized";
		return allKeys.size();
	}
	
	public KeyedTableReader call() throws IOException, SQLException {
		Writer writer = this.getWriter();
		assert writer != null;
		assert allKeys != null: "not initialized";
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
//			params.add("__limit", Integer.toString(getPageSize()));
			if (viewName != null) params.add("__use_view", viewName);
			RecordList recs = soapImpl.getRecords(params, this.displayValue);
			writer.processRecords(recs);
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
