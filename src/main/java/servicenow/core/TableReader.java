package servicenow.core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public abstract class TableReader implements Callable<TableReader> {
 
	public final Table table;
	
	private String readerName;
	private TableReader parent;
	private EncodedQuery baseQuery;
	private DateTimeRange createdRange;
	private DateTimeRange updatedRange;
	
	protected Writer writer;
	protected int pageSize;
	protected boolean displayValue = false;
	protected String viewName = null;
	protected FieldNames fieldNames = null;	
	protected ReaderMetrics readerMetrics;

	public TableReader(Table table) {
		this.table = table;
		this.pageSize = getDefaultPageSize();
		this.readerMetrics = new ReaderMetrics();
	}
			
	public abstract int getDefaultPageSize();
	
	public void initialize() throws IOException {
		assert writer != null : "Writer not initialized";
		setLogContext();
	}
	
	public void setReaderName(String name) {
		this.readerName = name;
	}
	
	public String getReaderName() {
		return readerName == null ? table.getName() : readerName;
	}
	
	public void setParent(TableReader parent) {
		this.parent = parent;
		this.readerMetrics.setParent(parent.readerMetrics);
	}
	
	public TableReader getParent() {
		return this.parent;
	}
	
	public void setLogContext() {
		Log.setContext(table, getReaderName());
	}
	
	public ReaderMetrics readerMetrics() {
		return this.readerMetrics;
	}
	
	public void setExpected(Integer value) {
		readerMetrics.setExpected(value);
	}

	/**
	 * Return number of expected rows, if available. 
	 */
	public Integer getExpected() {
		if (readerMetrics.getExpected() == null)
			throw new IllegalStateException(this.getClass().getName() + " not initialized");
		return readerMetrics.getExpected();
	}
		
	public abstract TableReader call() throws IOException, SQLException, InterruptedException;
			
	public TableReader setPageSize(int size) {
		this.pageSize = size;
		return this;
	}
	
	public int getPageSize() {
		return pageSize > 0 ? pageSize : getDefaultPageSize();
	}
	
	public TableReader setBaseQuery(EncodedQuery value) {
		// argument may be null to clear
		this.baseQuery = value;
		return this;
	}

	public EncodedQuery getBaseQuery() {
		return baseQuery == null ? EncodedQuery.all() : baseQuery;
	}
	
	public TableReader setCreatedRange(DateTimeRange range) {
		// argument may be null to clear the range
		this.createdRange = range;
		return this;
	}
	
	public DateTimeRange getCreatedRange() {
		return this.createdRange;
	}
	
	public TableReader setUpdatedRange(DateTimeRange range) {
		// argument may be null to clear the range
		this.updatedRange = range;
		return this;
	}
	
	public DateTimeRange getUpdatedRange() {
		return this.updatedRange;
	}
	
	public EncodedQuery getQuery() {
		EncodedQuery query = new EncodedQuery(baseQuery);
		if (createdRange != null) query.addCreated(createdRange);
		if (updatedRange != null) query.addUpdated(updatedRange);
		return query;
	}
	
	public TableReader setDisplayValue(boolean value) {
		this.displayValue = value;
		return this;
	}

	public TableReader setView(String name) {
		this.viewName = name;
		return this;
	}

	public String getView() {
		return this.viewName;
	}
		
	public TableReader setFields(FieldNames names) {
		this.fieldNames = names;
		return this;
	}
	
	public TableReader setWriter(Writer value) {
		assert value != null;
		this.writer = value;
		return this;
	}
	
	public Writer getWriter() {
		assert this.writer != null;
		return this.writer;
	}			

	public RecordList getAllRecords() throws IOException, InterruptedException {
		RecordListAccumulator accumulator = new RecordListAccumulator(this.table);
		setWriter(accumulator);
		try {
			this.call();
		} catch (SQLException e) {
			// this should be impossible
			// since RecordAccumulator does not throw SQLException
			throw new ServiceNowError(e);
		}
		return accumulator.getRecords();
	}

}
