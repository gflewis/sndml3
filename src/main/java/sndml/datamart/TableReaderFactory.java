package sndml.datamart;

import sndml.servicenow.DateTime;
import sndml.servicenow.DateTimeRange;
import sndml.servicenow.EncodedQuery;
import sndml.servicenow.FieldNames;
import sndml.servicenow.NullWriter;
import sndml.servicenow.RecordWriter;
import sndml.servicenow.Table;
import sndml.servicenow.TableReader;

public abstract class TableReaderFactory {

	protected final Table table;
	protected RecordWriter writer;
	protected TableReader parentReader;
	protected String parentName;
	protected EncodedQuery filter;
	protected DateTimeRange createdRange;
	protected DateTimeRange updatedRange;
	protected FieldNames fieldNames;
	protected Integer pageSize;
		
	public TableReaderFactory(Table table) {
		this.table = table;
		this.writer = new NullWriter();
	}
		
	public Table getTable() { return table; } 	
	public EncodedQuery getFilter() { return filter; }
	public DateTimeRange getCreatedRange() { return createdRange; }
	public DateTimeRange getUpdatedRange() { return updatedRange; }
	public FieldNames getFieldNames() { return fieldNames; }
	public Integer getPageSize() { return pageSize; }
	public RecordWriter getWriter() { return writer; }
		
	public abstract TableReader createReader();
	
	public void configure(TableReader reader) {
		if (getFilter() != null) reader.setQuery(getFilter());
		if (getCreatedRange() != null) reader.setCreatedRange(getCreatedRange());
		if (getUpdatedRange() != null) reader.setUpdatedRange(getUpdatedRange());
		if (getFieldNames() != null) reader.setFields(getFieldNames());
		if (getPageSize() != null) reader.setPageSize(getPageSize());
		if (writer != null) reader.setWriter(writer);
		if (parentName != null) reader.setReaderName(parentName);
		if (parentReader != null) reader.setParent(parentReader);
	}

	public void setWriter(RecordWriter writer) {
		assert writer != null;
		this.writer = writer;
	}
	
	public void setParentReader(TableReader parent) {
		this.parentReader = parent;
	}

	public TableReader getParentReader() {
		assert parentReader != null;
		return parentReader;
	}
	
	public void setFilter(EncodedQuery query) {
		this.filter = query;
	}

	public void setUpdated(DateTime since) {
		assert since != null;
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

	@Deprecated
	public void setParentName(String name) {
		this.parentName = name;
	}
	
	@Deprecated
	public String getParentName() {
		assert parentName != null;
		return parentName;
	}
	
}
