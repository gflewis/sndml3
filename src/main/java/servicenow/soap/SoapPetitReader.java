package servicenow.soap;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.*;

/**
 * A {@link TableReader} which attempts to read a set of records
 * using a single Web Service call.
 * <p/>
 * Use a {@link SoapPetitReader} only if...
 * <ul>
 * <li>the number of records to be read is small, and</li>
 * <li>there is no possibility of an access control
 * which could block the ability to read some of the records.</li>
 * </ul>
 * <p/>
 * A {@link SoapPetitReader} does NOT precede the first read with a getKeys call.
 * It simply starts reading the rows using first_row / last_row windowing.
 * If the number of records returned is equal to the limit,  
 * then it assumes there are more records and it keeps on reading.
 * If the number of records returned is less than the limit,
 * then it assumes it has reached the end. 
 * <p/>
 * For small result sets {@link SoapPetitReader} will perform better than 
 * {@link SoapKeyReader} because it saves a Web Service call.
 * However, the performance of the {@link SoapPetitReader} will degrade exponentially
 * as the number of records grows.
 * <p/>
 * <b>Warning:</b> If access controls are in place, the <b>getRecords</b> method
 * will sometimes return fewer records than the limit even though
 * there are more records to be read.  This will cause the {@link SoapPetitReader}
 * to terminate prematurely. Use a {@link SoapKeyReader} 
 * if there is any possibility of access controls which could cause this behavior.
 * 
 */
public class SoapPetitReader extends TableReader {

	final SoapTableAPI soapImpl;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public SoapPetitReader(SoapTableAPI impl) {
		super(impl);
		soapImpl = impl;
	}
	
	@Override
	public void initialize() {
		assert writer != null : "Writer not initialized";
		setLogContext();
		writer.setReader(this);		
	}

	@Override
	public int getDefaultPageSize() {
		return 200;
	}
		
	@Override
	public Integer getExpected() {
		throw new UnsupportedOperationException();
	}

	public SoapPetitReader call() throws IOException, SQLException {
		Writer writer = this.getWriter();
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
			RecordList recs = soapImpl.getRecords(params, this.displayValue);
			writer.processRecords(recs);
			rowCount += recs.size();
			finished = (recs.size() < pageSize);
			firstRow = lastRow;
			logger.info(String.format("processed %d rows", rowCount));			
		}
		return this;
	}

}
