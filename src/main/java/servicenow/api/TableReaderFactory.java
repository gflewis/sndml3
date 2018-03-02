package servicenow.api;

public abstract class TableReaderFactory {

	protected final Table table;
	protected final Writer writer;

	protected TableReader parent;
	protected EncodedQuery baseQuery;
	protected DateTimeRange createdRange;
	protected DateTimeRange updatedRange;
	protected String orderBy;
	protected int pageSize;
	protected String readerName;
	
	public TableReaderFactory(Table table, Writer writer) {
		this.table = table;
		this.writer = writer;
	}
	
	public abstract TableReader createReader();

	public Table getTable() { 
		return this.table; 
	}

	public void setParent(TableReader parent) {
		this.parent = parent;
	}

	public void setBaseQuery(EncodedQuery query) {
		this.baseQuery = query;
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
	
	public void setOrderBy(String fieldname) {
		this.orderBy = fieldname;
	}
	
	public void setPageSize(int size) {
		this.pageSize = size;
	}

	public Writer getWriter() {
		return this.writer;
	}
	
	public void setReaderName(String name) {
		this.readerName = name;
	}
	
	public String getReaderName() {
		return this.readerName;
	}
	
}
