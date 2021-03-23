package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableReader implements Callable<Metrics> {
 
	public final Table table;
	
	protected TableReader parentReader;
	@Deprecated protected String readerName;
	@Deprecated protected String partName;
	
	// filter is a base query to which createdRange, updatedRange and keyExclusion are appended
	protected EncodedQuery filter; 
	protected DateTimeRange createdRange;
	protected DateTimeRange updatedRange;
	// keyExclusion is use for pagination; only values greater than the current key will be returned
	private Key keyExclusion = null;
	
	protected static enum OrderBy {NONE, KEYS, FIELDS};
	// Note: RestTableReader sets orderBy to KEYS
	protected OrderBy orderBy = OrderBy.KEYS;
	protected int pageSize;
	protected boolean displayValue = false;
	protected String viewName = null;
	protected FieldNames fieldNames = null;	
	protected RecordWriter writer;
	protected Metrics metrics;
	protected ProgressLogger progressLogger;

	protected Integer maxRows;
	protected boolean initialized = false;
	protected final Logger logger;

	@Deprecated	private EncodedQuery orderByQuery;
	
	public TableReader(Table table) {
		this.table = table;
		this.logger = LoggerFactory.getLogger(this.getClass());
		this.pageSize = table.session.defaultPageSize(table);
	}
	
	protected void beginPrepare() {		
		if (initialized) throw new IllegalStateException("initialize() called more than once");
		setLogContext();
		// TODO: should not be conditional
		assert progressLogger != null;
		if (progressLogger != null) progressLogger.logPrepare();		
	}
	
	protected void endPrepare(Integer expected) {
		metrics.setExpected(expected);
		initialized = true;		
	}

	// Note: Only Synchronizer can throw SQLException during initialization	
	public abstract void prepare() 
		throws IOException, SQLException, InterruptedException;

	@Deprecated
	protected void setLogContext() {
		//TODO Would like to deprecate but still used by Synchronizer
		Log.setTableContext(table, getReaderName());
	}

	protected void logStart() {
		assert initialized;
		if (progressLogger != null) progressLogger.logStart(getExpected());
		Log.setTableContext(table, getReaderName());
	}
	
	protected void logComplete() {
		if (progressLogger != null)	progressLogger.logComplete();
	}
		
	public abstract Metrics call() 
		throws IOException, SQLException, InterruptedException;
				
	@Deprecated public void setReaderName(String name) {
		if (initialized) throw new IllegalStateException();		
		this.readerName = name;
	}
	
	@Deprecated public String getReaderName() {
		return readerName == null ? table.getName() : readerName;
	}
	
	public void setProgressLogger(ProgressLogger progressLogger) {
		this.progressLogger = progressLogger;
	}
	
	public ProgressLogger getProgressLogger() {
		assert progressLogger != null;
		return progressLogger;
	}
	
	@Deprecated
	public void setParent(TableReader parent) {
		if (initialized) throw new IllegalStateException();
		assert parent != null;
		this.parentReader = parent;		
	}
	
	@Deprecated
	public TableReader getParent() {
		return this.parentReader;
	}
	
	@Deprecated
	public boolean hasParent() {
		return this.parentReader != null;
	}
	
	@Deprecated public void setPartName(String partName) {
		this.partName = partName;		
	}
	
	@Deprecated public String getPartName() {
		return partName;
	}
		
	/**
	 * Return number of expected rows, if available. 
	 */
	public Integer getExpected() {
		if (!metrics.hasExpected())
			throw new IllegalStateException(this.getClass().getName() + " not initialized");
		return metrics.getExpected();
	}
	
	/**
	 * Increment number of records read.
	 */
	protected void incrementInput(int count) {
//		readerMetrics.increment(count);
		metrics.addInput(count);
	}
		
	public TableReader setPageSize(Integer size) {
		if (initialized) throw new IllegalStateException();
		if (size != null) this.pageSize = size;
		return this;
	}
	
	public int getPageSize() {
		int result = this.pageSize;
		assert result > 0;
		return result;
	}
	
	/**
	 * Set a base filter to which created range, updated range and key exclusion will be appended
	 */
	public TableReader setFilter(EncodedQuery value) {
		if (initialized) throw new IllegalStateException();
		// argument may be null to clear
		this.filter = value;
		return this;
	}

	public EncodedQuery getFilter() {
		if (filter == null) {
			return EncodedQuery.all(table);
		}
		else {
			return filter;
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
		EncodedQuery result = (filter == null) ? 
				new EncodedQuery(table) : new EncodedQuery(filter);
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

	public void setWriter(RecordWriter writer, Metrics metrics) {
		if (initialized) throw new IllegalStateException();
		this.writer = writer;
		this.metrics = metrics;		
	}
	
	public RecordWriter getWriter() {
		assert this.writer != null;
		return this.writer;
	}
	
	public Metrics getMetrics() {
		return this.metrics;
	}
	
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}

	public RecordList getAllRecords() throws IOException, InterruptedException {
		assert !initialized;
		Metrics accumulatorMetrics = new Metrics("accumulator");
		if (this.metrics == null) 
			this.metrics = accumulatorMetrics;
		if (this.progressLogger == null) 
			this.progressLogger = new NullProgressLogger();
		RecordListAccumulator accumulator = new RecordListAccumulator(this);
		setWriter(accumulator, accumulatorMetrics);
		try {
			this.prepare();
			this.call();
		} catch (SQLException e) {
			// this should be impossible
			// since RecordAccumulator does not throw SQLException
			throw new ServiceNowError(e);
		}
		return accumulator.getRecords();
	}

}
