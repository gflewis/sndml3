package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableReader implements Callable<Metrics> {
 
	public final Table table;
	
	protected TableReader parentReader;
	protected String readerName;
	protected String partName;
	
	// filter is a base query to which createdRange, updatedRange and keyExclusion are appended
	protected EncodedQuery filter; 
	protected DateTimeRange createdRange;
	protected DateTimeRange updatedRange;
	// keyExclusion is use for pagination; only values greater than the current key will be returned
	private RecordKey keyExclusion = null;
	
	protected static enum OrderBy {NONE, KEYS, FIELDS};
	// Note: RestTableReader sets orderBy to KEYS
	protected OrderBy orderBy = OrderBy.KEYS;
	protected int pageSize;
	protected boolean displayValue = false;
	protected String viewName = null;
	protected FieldNames fieldNames = null;	
	protected RecordWriter writer;
	protected Metrics metrics;
	protected ProgressLogger progress;

	protected Integer maxRows;
	protected boolean initialized = false;
	protected final Logger logger;

	@Deprecated	private EncodedQuery orderByQuery;
	
	public TableReader(Table table) {
		this.table = table;
		this.logger = LoggerFactory.getLogger(this.getClass());
		this.pageSize = table.session.defaultPageSize(table);
	}
	
	protected void beginPrepare(RecordWriter writer, Metrics metrics, ProgressLogger progress) {	
		if (initialized) throw new IllegalStateException("initialize() called more than once");
		this.writer = writer;
		this.metrics = metrics;
		this.progress = progress;
		assert progress != null;
		progress.logPrepare();		
	}
	
	protected void endPrepare(Integer expected) {
		metrics.setExpected(expected);
		initialized = true;		
	}

	// Note: Only Synchronizer can throw SQLException during initialization	
	public abstract void prepare(RecordWriter writer, Metrics metrics, ProgressLogger progressLogger) 
		throws IOException, SQLException, InterruptedException;
		
	public abstract Metrics call() 
		throws IOException, SQLException, InterruptedException;
	
	public void setReaderName(String name) {
		if (initialized) throw new IllegalStateException();		
		this.readerName = name;
	}
	
	public String getReaderName() {
		return readerName == null ? table.getName() : readerName;
	}

	public void setPartName(String partName) {
		this.partName = partName;		
	}
	
	public String getPartName() {
		return partName;
	}
		
	
	public void setProgressLogger(ProgressLogger progressLogger) {
		this.progress = progressLogger;
	}
	
	public ProgressLogger getProgressLogger() {
		assert progress != null;
		return progress;
	}
		
	/**
	 * Return number of expected rows, if available. 
	 * Otherwise return null.
	 */
	public Integer getExpected() {
		return metrics.getExpected();
	}
	
	/**
	 * Increment number of records read.
	 */
	protected void incrementInput(int count) {
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
	public TableReader setKeyExclusion(RecordKey value) {
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
	
	public Metrics getMetrics() {
		return this.metrics;
	}
	
	@Deprecated
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}

	public RecordList getAllRecords() throws IOException, InterruptedException {
		assert !initialized;
		if (this.metrics == null) {
			Metrics accumulatorMetrics = new Metrics("accumulator");
			this.metrics = accumulatorMetrics;			
		}
		if (this.progress == null)  {
			this.progress = new NullProgressLogger();			
		}
		RecordListAccumulator accumulator = new RecordListAccumulator(this);
		try {
			this.prepare(accumulator, metrics, progress);
			this.call();
		} catch (SQLException e) {
			// this should be impossible
			// since RecordAccumulator does not throw SQLException
			throw new ServiceNowError(e);
		}
		return accumulator.getRecords();
	}

}
