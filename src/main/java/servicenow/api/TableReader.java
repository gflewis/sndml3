package servicenow.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableReader implements Callable<TableReader> {
 
	public final Table table;
	
	private String readerName;
	private TableReader parent;
	private EncodedQuery filter;
	private DateTimeRange createdRange;
	private DateTimeRange updatedRange;
	private EncodedQuery orderByQuery;
	
	protected Writer writer;
	protected int pageSize;
	protected boolean displayValue = false;
	protected String viewName = null;
	protected FieldNames fieldNames = null;	
	protected ReaderMetrics readerMetrics;
	protected Integer maxRows;
	protected boolean initialized = false;
	protected final Logger logger;
	
	public TableReader(Table table) {
		this.table = table;
		this.logger = LoggerFactory.getLogger(this.getClass());
		this.pageSize = getDefaultPageSize();
		this.readerMetrics = new ReaderMetrics();
	}
			
	public void initialize() throws IOException, SQLException, InterruptedException {
		if (initialized) throw new IllegalStateException("initialize() called more than once");
		setLogContext();
		initialized = true;
	}
	
	public abstract int getDefaultPageSize();
	
	public abstract TableReader call() throws IOException, SQLException, InterruptedException;
			
	public void setReaderName(String name) {
		if (initialized) throw new IllegalStateException();
		this.readerName = name;
	}
	
	public String getReaderName() {
		return readerName == null ? table.getName() : readerName;
	}
	
	public void setLogContext() {
		Log.setContext(table, getReaderName());
	}
	
	public void setParent(TableReader parent) {
		if (initialized) throw new IllegalStateException();
		this.parent = parent;
		this.readerMetrics.setParent(parent.readerMetrics);
	}
	
	public TableReader getParent() {
		return this.parent;
	}
	
	public ReaderMetrics getReaderMetrics() {
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
		
	public TableReader setPageSize(int size) {
		if (initialized) throw new IllegalStateException();
		this.pageSize = size;
		return this;
	}
	
	public int getPageSize() {
		if (this.pageSize > 0) return this.pageSize;
		int result = getDefaultPageSize();
		assert result > 0;
		return result;
	}
	
	public TableReader setFilter(EncodedQuery value) {
		if (initialized) throw new IllegalStateException();
		// argument may be null to clear
		this.filter = value;
		return this;
	}

	public EncodedQuery getFilter() {
		return filter == null ? EncodedQuery.all() : filter;
	}
	
	public TableReader setCreatedRange(DateTimeRange range) {
		if (initialized) throw new IllegalStateException();
		// argument may be null to clear the range
		this.createdRange = range;
		return this;
	}
	
	public DateTimeRange getCreatedRange() {
		return this.createdRange;
	}
	
	public TableReader setUpdatedRange(DateTimeRange range) {
		if (initialized) throw new IllegalStateException();
		// argument may be null to clear the range
		this.updatedRange = range;
		return this;
	}
	
	public DateTimeRange getUpdatedRange() {
		return this.updatedRange;
	}

	/**
	 * Specify an "OrderBy" or "OrderByDesc" clause for the reader.
	 * @param fieldnames - Comma delimited list of field names.
	 * each of which may be prefixed with "+" or "-" 
	 * to indicate ascending or descending respectively. 
	 */
	public TableReader setOrderBy(String fieldnames) {
		if (fieldnames == null) {
			orderByQuery = null;
		}
		else {
			orderByQuery = new EncodedQuery();
			for (String field : fieldnames.split(",\\s*")) {
				boolean desc = false;
				char c1 = field.charAt(0);
				if (c1 == '+' || c1 =='-') {
					if (c1 == '-') desc = true;
					field = field.substring(1);
				}
				if (desc) 
					orderByQuery.addOrderByDesc(field);
				else 
					orderByQuery.addOrderBy(field);				
			}
		}
		return this;
	}
		
	/**
	 * Return a composite query built from base query, created range and updated range.
	 */
	public EncodedQuery getQuery() {
		EncodedQuery query = new EncodedQuery(filter);
		if (createdRange != null) query.addCreated(createdRange);
		if (updatedRange != null) query.addUpdated(updatedRange);
		if (orderByQuery != null) query.addQuery(orderByQuery);
		return query;
	}
	
	public TableReader setDisplayValue(boolean value) {
		if (initialized) throw new IllegalStateException();
		this.displayValue = value;
		return this;
	}

	public TableReader setView(String name) {
		if (initialized) throw new IllegalStateException();
		this.viewName = name;
		return this;
	}
	
	public String getView() {
		return this.viewName;
	}
		
	public TableReader setFields(FieldNames names) {
		if (initialized) throw new IllegalStateException();
		this.fieldNames = names;
		return this;
	}
	
	public TableReader setMaxRows(Integer value) {
		this.maxRows = value;
		return this;
	}
	
	public Integer getMaxRows() {
		return this.maxRows;
	}

	public TableReader setWriter(Writer value) {
		if (initialized) throw new IllegalStateException();
		this.writer = value;
		return this;
	}
	
	public Writer getWriter() {
		assert this.writer != null;
		return this.writer;
	}
	
	public WriterMetrics getWriterMetrics() {
		return this.writer.getMetrics();
	}

	public RecordList getAllRecords() throws IOException, InterruptedException {
		if (!initialized) throw new IllegalStateException("Not initialized");
		RecordListAccumulator accumulator = new RecordListAccumulator(this.table);
		this.writer = accumulator;
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
