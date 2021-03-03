package sndml.servicenow;

public abstract class TableReaderFactory {

	protected final Table table;
	protected RecordWriter writer;
	protected TableReader parent = null;
	protected EncodedQuery filter = null;
	protected DateTimeRange createdRange = null;
	protected DateTimeRange updatedRange = null;
	protected FieldNames fieldNames = null;
	protected Integer pageSize;
	protected String readerName = null;
//	protected ProgressLogger progressLogger = null;
		
	public TableReaderFactory(Table table) {
		this.table = table;
		this.writer = new NullWriter();
	}
	
	public TableReaderFactory(Table table, RecordWriter writer) {
		this.table = table;
		this.writer = writer;
	}
	
	public Table getTable() { return table; } 	
	public EncodedQuery getFilter() { return filter; }
	public DateTimeRange getCreatedRange() { return createdRange; }
	public DateTimeRange getUpdatedRange() { return updatedRange; }
	public FieldNames getFieldNames() { return fieldNames; }
	public Integer getPageSize() { return pageSize; }
	public RecordWriter getWriter() { return writer; }
//	public ProgressLogger getProgressLogger() { return progressLogger; }
		
	public abstract TableReader createReader();
	
	public void configure(TableReader reader) {
		reader.setQuery(getFilter());
		reader.setCreatedRange(getCreatedRange());
		reader.setUpdatedRange(getUpdatedRange());
		reader.setFields(getFieldNames());
		reader.setPageSize(getPageSize());
		reader.setWriter(writer);
		reader.setReaderName(readerName);
//		reader.setProgressLogger(progressLogger);
	}

	public void setWriter(RecordWriter writer) {
		this.writer = writer;
	}
	
	public void setParent(TableReader parent) {
		this.parent = parent;
	}

	public void setFilter(EncodedQuery query) {
		this.filter = query;
	}

	public void setUpdated(DateTime since) {
		this.setUpdated(new DateTimeRange(since, null));
	}
	
	public void setUpdated(DateTimeRange updated) {
		this.updatedRange = updated;
	}

	public void setCreated(DateTimeRange created) {
		this.createdRange = created;
	}
	
	public void setFields(FieldNames names) {
		this.fieldNames = names;
	}

	public void setPageSize(Integer size) {
		this.pageSize = size;
	}

	public void setReaderName(String name) {
		this.readerName = name;
	}
	
	public String getReaderName() {
		return this.readerName;
	}
	
}
