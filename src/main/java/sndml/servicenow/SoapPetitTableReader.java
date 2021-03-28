package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;

/**
 * <p>A {@link TableReader} which attempts to read a set of records
 * using a single Web Service call.</p>
 * <p>This class should be used only if...</p>
 * <ul>
 * <li>the number of records to be read is small, and</li>
 * <li>there is no possibility of an access control
 * which could block the ability to read some of the records.</li>
 * </ul>
 * 
 * <p>This reader does NOT precede the first read with a getKeys call.
 * It simply starts reading the rows using first_row / last_row windowing.
 * If the number of records returned is equal to the limit,  
 * then it assumes there are more records and it keeps on reading.
 * If the number of records returned is less than the limit,
 * then it assumes it has reached the end.</p> 
 * 
 * <p>For small result sets this reader will perform better than 
 * {@link SoapKeySetTableReader} because it saves a Web Service call.
 * However, the performance of the this class will degrade exponentially
 * as the number of records grows.</p>
 * 
 * <p><b>Warning:</b> If access controls are in place, the <b>getRecords</b> method
 * will sometimes return fewer records than the limit even though
 * there are more records to be read.  This will cause this reader
 * to terminate prematurely. Use a {@link SoapKeySetTableReader} 
 * if there is any possibility of access controls which could cause this behavior.</p>
 * 
 */
@Deprecated
public class SoapPetitTableReader extends TableReader {

	protected final SoapTableAPI soapAPI;
		
	public SoapPetitTableReader(Table table) {
		super(table);
		soapAPI = table.soap();
	}
	
	@Override
	public void prepare(RecordWriter writer, Metrics metrics, ProgressLogger progressLogger) {
		assert writer != null : "Writer not initialized";
		beginPrepare(writer, metrics, progressLogger);
		Integer expected = null;
		endPrepare(expected);
	}
		
	@Override
	public Integer getExpected() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableReader setFields(FieldNames names) {
		throw new UnsupportedOperationException();
	}		
	
	public Metrics call() throws IOException, SQLException {
		assert writer != null;
		assert pageSize > 1;
		int firstRow = 0;
		boolean finished = false;
		int rowCount = 0;
		while (!finished) {
			int lastRow = firstRow + pageSize;
			Parameters params = new Parameters();
			params.add("__encoded_query", this.getQuery().toString());
			params.add("__first_row", Integer.toString(firstRow));
			params.add("__last_row", Integer.toString(lastRow));
			if (this.viewName != null) params.add("__use_view", viewName);
			RecordList recs = soapAPI.getRecords(params, this.displayValue);
			incrementInput(recs.size());			
			writer.processRecords(recs, metrics, progress);
			rowCount += recs.size();
			finished = (recs.size() < pageSize);
			firstRow = lastRow;
			logger.info(String.format("processed %d rows", rowCount));			
		}
		return metrics;
	}

}
