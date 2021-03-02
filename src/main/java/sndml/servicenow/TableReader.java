package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableReader implements Callable<TableReader> {
 
	public final Table table;
	private TableReader parent;
	
	private String readerName;
	private String partName;
	private EncodedQuery query;
	private DateTimeRange createdRange;
	private DateTimeRange updatedRange;
	private Key keyExclusion = null;
	
	protected static enum OrderBy {NONE, FIELDS, KEYS};
	// Note: RestTableReader sets this value to KEYS
	protected OrderBy orderBy = OrderBy.NONE;
	protected RecordWriter writer;
	protected int pageSize;
	protected boolean displayValue = false;
	protected String viewName = null;
	protected FieldNames fieldNames = null;	
	protected ReaderMetrics readerMetrics;
	protected ProgressLogger progressLogger;

	protected Integer maxRows;
	protected boolean initialized = false;
	protected final Logger logger;

	@Deprecated
	protected EncodedQuery orderByQuery;
	
	public TableReader(Table table) {
		this.table = table;
		this.logger = LoggerFactory.getLogger(this.getClass());
		this.pageSize = getDefaultPageSize();
		this.readerMetrics = new ReaderMetrics();
	}
			
	public void initialize() throws IOException, SQLException, InterruptedException {
		// Note: Only Synchronizer can throw SQLException during initialization
		if (initialized) throw new IllegalStateException("initialize() called more than once");
		setLogContext();
		initialized = true;
	}

	public void setLogContext() {
		Log.setContext(table, getReaderName());
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
	
	public void setParent(TableReader parent) {
		if (initialized) throw new IllegalStateException();
		this.parent = parent;
		this.readerMetrics.setParent(parent.readerMetrics);
	}
	
	public TableReader getParent() {
		return this.parent;
	}
	
	public void setPartName(String name) {
		assert !initialized;
		assert parent != null;
		this.partName = name;		
	}
	
	public String getPartname() {
		assert parent != null;
		assert partName != null;
		return partName;
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
		
	public TableReader setPageSize(Integer size) {
		if (initialized) throw new IllegalStateException();
		if (size != null) this.pageSize = size;
		return this;
	}
	
	public int getPageSize() {
		if (this.pageSize > 0) return this.pageSize;
		int result = getDefaultPageSize();
		assert result > 0;
		return result;
	}
	
	public TableReader setQuery(EncodedQuery value) {
		if (initialized) throw new IllegalStateException();
		// argument may be null to clear
		this.query = value;
		return this;
	}

	public EncodedQuery getFilter() {
		if (query == null) {
			return EncodedQuery.all(table);
		}
		else {
			return query;
		}
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
	
	public TableReader orderByKeys(boolean value) {
		this.orderBy = value ? OrderBy.KEYS : OrderBy.NONE;
		return this;
	}
		
	/**
	 * Exclude all keys less than or equal to the value
	 */
	public TableReader setKeyExclusion(Key value) {
		assert this.orderBy == OrderBy.KEYS;
		this.keyExclusion = value;
		return this;
	}
	
	/**
	 * Specify an "OrderBy" or "OrderByDesc" clause for the reader.
	 * @param fieldnames - Comma delimited list of field names.
	 * each of which may be prefixed with "+" or "-" 
	 * to indicate ascending or descending respectively. 
	 * 
	 * This code is no longer used. RestTableReader always orders by KEYS.
	 */
	@Deprecated
	public TableReader setOrderBy(String fieldnames) {
		if (fieldnames == null) {
			orderByQuery = null;
			this.orderBy = OrderBy.NONE;
		}
		else {
			this.orderBy = OrderBy.FIELDS;
			orderByQuery = new EncodedQuery(this.table);
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
	 * Return a composite query built from base query, 
	 * plus created range, updated range and key exclusion
	 */
	public EncodedQuery getStatsQuery() {
		EncodedQuery result = query == null ? new EncodedQuery(table) : new EncodedQuery(query);
		if (createdRange != null) result.addCreated(createdRange);
		if (updatedRange != null) result.addUpdated(updatedRange);
		if (keyExclusion != null) result.excludeKeys(keyExclusion);
		return result;
	}
	
	/**
	 * Return a composite query built from base query, 
	 * plus created range, updated range and key exclusion
	 * plus order by clause
	 */
	public EncodedQuery getQuery() {
		EncodedQuery query = getStatsQuery();
		switch (orderBy) {
		case NONE: 
			break;
		case KEYS: 
			query.addOrderByKeys();
			break;
		case FIELDS:
			if (orderByQuery != null) query.addQuery(orderByQuery);			
			break;
		}
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
		if (names == null) {
			fieldNames = null;
		}
		else {
			// sys_id, sys_created_on and sys_updated on are included
			// whether you ask for them or not
			fieldNames = new FieldNames("sys_id,sys_created_on,sys_updated_on");
			for (String name : names) {
				if (!fieldNames.contains(name)) fieldNames.add(name);
			}			
		}
		return this;
	}
	
	public TableReader setMaxRows(Integer value) {
		this.maxRows = value;
		return this;
	}
	
	public Integer getMaxRows() {
		return this.maxRows;
	}

	public TableReader setWriter(RecordWriter value) {
		if (initialized) throw new IllegalStateException();
		this.writer = value;
		return this;
	}
	
	public RecordWriter getWriter() {
		assert this.writer != null;
		return this.writer;
	}
	
	public WriterMetrics getWriterMetrics() {
		return this.writer.getMetrics();
	}
	
	public void setProgressLogger(ProgressLogger logger) {
		this.progressLogger = logger;
	}
	
	public ProgressLogger getProgressLogger() {
		assert progressLogger != null;
		return progressLogger;
	}

	public RecordList getAllRecords() throws IOException, InterruptedException {
		if (!initialized) throw new IllegalStateException("Not initialized");
		RecordListAccumulator accumulator = new RecordListAccumulator(this);
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
