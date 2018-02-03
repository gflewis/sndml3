package servicenow.core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public abstract class TableReader implements Callable<TableReader>{
 
	public final Table table;
//	protected final TableAPI api;
	
	private EncodedQuery baseQuery;
	private DateTimeRange createdRange;
	private DateTimeRange updatedRange;
	private TableReader parent;
	
	protected Writer writer;
	private ReaderMetrics metrics;
	protected int pageSize;
	protected boolean displayValue = false;
	protected String viewName = null;
	protected FieldNames fieldNames = null;	

	public TableReader(Table table) {
		this.table = table;
		this.pageSize = getDefaultPageSize();
		this.metrics = new ReaderMetrics();
	}
	
	/*
	public TableReader(TableAPI api) {
		this.api = api;
		this.table = api.getTable();
		this.pageSize = getDefaultPageSize();
		this.metrics = new ReaderMetrics();
	}
	*/
		
	public abstract int getDefaultPageSize();
	
	public void initialize() throws IOException {
		assert writer != null : "Writer not initialized";
		setLogContext();
		writer.setReader(this);
	}
	
	public void setLogContext() {
		Log.setTableContext(this.table);
	}
	
	public ReaderMetrics getMetrics() {
		return this.metrics;
	}
	
	public void setExpected(Integer value) {
		metrics.setExpected(value);
	}

	/**
	 * Return number of expected rows, if available. 
	 */
	public Integer getExpected() {
		if (metrics.getExpected() == null)
			throw new IllegalStateException(this.getClass().getName() + " not initialized");
		return metrics.getExpected();
	}
		
	public abstract TableReader call() throws IOException, SQLException, InterruptedException;
			
	public TableReader setParent(TableReader parent) {
		assert parent != null;
		this.parent = parent;
		this.metrics = new ReaderMetrics(parent.getMetrics());
		return this;
	}

	public TableReader getParent() {
		return parent;
	}

	public TableReader setPageSize(int size) {
		this.pageSize = size;
		return this;
	}
	
	public int getPageSize() {
		return this.pageSize;
	}
	
	public TableReader setBaseQuery(EncodedQuery value) {
		// argument may be null to clear the query
		this.baseQuery = (value == null ? EncodedQuery.all() : value);
		return this;
	}

	public EncodedQuery getBaseQuery() {
		return this.baseQuery;
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
		this.setWriter(accumulator);
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
