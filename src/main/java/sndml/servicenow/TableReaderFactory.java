package sndml.servicenow;

public abstract class TableReaderFactory {

	protected final Table table;

	protected RecordWriter writer;
	protected TableReader parent = null;
	protected EncodedQuery filter = null;
	protected DateTimeRange createdRange = null;
	protected DateTimeRange updatedRange = null;
	protected FieldNames fieldNames = null;
	protected int pageSize = 0;
	protected String readerName = null;
	
	public TableReaderFactory(Table table, RecordWriter writer) {
		this.table = table;
		this.writer = writer;
	}
	
	public TableReaderFactory(Table table) {
		this.table = table;
	}
	
	public abstract TableReader createReader();
	
	public void configure(TableReader reader) {
		reader.orderByKeys(true);
		reader.setQuery(filter);
		reader.setUpdatedRange(updatedRange);
		reader.setCreatedRange(createdRange);
		reader.setFields(fieldNames);
		reader.setPageSize(pageSize);
		reader.setWriter(writer);
		reader.setReaderName(readerName);		
	}

	public Table getTable() { 
		return this.table; 
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

	public void setPageSize(int size) {
		this.pageSize = size;
	}

	public RecordWriter getWriter() {
		return this.writer;
	}
	
	public void setReaderName(String name) {
		this.readerName = name;
	}
	
	public String getReaderName() {
		return this.readerName;
	}
	
}
