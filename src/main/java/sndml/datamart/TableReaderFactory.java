package sndml.datamart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class TableReaderFactory {

	protected final Table table;
	protected final Database db;
	protected final JobConfig config;
	protected final String sqlTableName;
	protected final DateTimeRange createdRange;
	protected final DateTimeRange updatedRange;
	protected final EncodedQuery filter;
	protected final FieldNames fieldNames;
	protected Integer pageSize;
	
	protected RecordWriter writer;
	protected Metrics parentWriterMetrics;
	protected TableReader parentReader;
	protected String parentName;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
		
	public TableReaderFactory(Table table, Database db, JobConfig config) {
		this.table = table;
		this.db = db;		
		this.config = config;
		sqlTableName = config.getTarget();
		createdRange = config.getCreated();
		updatedRange = new DateTimeRange(config.getSince(), null);
		filter = new EncodedQuery(table, config.getFilter());
		fieldNames = config.getColumns();
		pageSize = config.getPageSize();
	}
		
	public TableReader createReader(DatePart datePart) {
		assert writer != null;
		assert parentWriterMetrics != null;
		TableReader reader;
		String partName = (datePart == null) ? null : datePart.getName();
		String readerName = (datePart != null) ? 
				config.getName() + "." + partName : config.getName();
				
		if (config.action == Action.SYNC) {
			reader = new Synchronizer(table, db, sqlTableName, readerName);
		}
		else {
			reader = new RestTableReader(table, readerName);
		}
		
		if (parentReader != null) reader.setParent(parentReader);
		if (readerName != null) reader.setReaderName(readerName);
		if (partName != null) reader.setPartName(partName);
		
		if (config.getFilter() != null) {
			EncodedQuery query = new EncodedQuery(table, config.getFilter());
			reader.setFilter(query);
		}
		
		DateTimeRange readerCreatedRange =
				datePart != null ? datePart.intersect(createdRange) : createdRange;
		reader.setCreatedRange(readerCreatedRange);
		
		reader.setUpdatedRange(updatedRange);
		reader.setFilter(filter);
		reader.setFields(fieldNames);
		reader.setPageSize(pageSize);

		Metrics writerMetrics = new Metrics(readerName, parentWriterMetrics);
		reader.setWriter(writer, writerMetrics);
		if (partName != null) reader.setPartName(partName);
		if (parentName != null && partName != null)	{
			String newReaderName = parentName + "." + partName;
			reader.setReaderName(newReaderName);
		}
		
		logger.debug(Log.INIT, String.format(
			"createReader part=%s name=%s created=%s", 
			partName, readerName, readerCreatedRange));

		return reader;
		
	}
	
	public Table getTable() { return table; }
	public EncodedQuery getFilter() { return filter; }
	public DateTimeRange getCreatedRange() { return createdRange; }
	public DateTimeRange getUpdatedRange() { return updatedRange; }
	public FieldNames getFieldNames() { return fieldNames; }
	public Integer getPageSize() { return pageSize; }
	public RecordWriter getWriter() { return writer; }
		
//	@Deprecated
//	public void configure(TableReader reader) {
//		configure(reader, null);
//	}
//	
//	@Deprecated
//	private void configure(TableReader reader, String partName) {
//		if (getFilter() != null) reader.setFilter(getFilter());
//		if (getCreatedRange() != null) reader.setCreatedRange(getCreatedRange());
//		if (getUpdatedRange() != null) reader.setUpdatedRange(getUpdatedRange());
//		if (getFieldNames() != null) reader.setFields(getFieldNames());
//		if (getPageSize() != null) reader.setPageSize(getPageSize());
//		if (parentReader != null) reader.setParent(parentReader);
//		if (partName != null) reader.setPartName(partName);
//		if (parentName != null && partName != null)	{
//			String newReaderName = parentName + "." + partName;
//			reader.setReaderName(newReaderName);
//		}
//	}

	public void setWriter(RecordWriter writer, Metrics writerMetrics) {
		assert writer != null;
		this.writer = writer;
		this.parentWriterMetrics = writerMetrics;
	}
	
	public void setParentReader(TableReader parent) {
		this.parentReader = parent;
	}

	// TODO: Remove this. It is only used for asserts.
	public TableReader getParentReader() {
		assert parentReader != null;
		return parentReader;
	}
	
	/*
	public void setFilter(EncodedQuery query) {
		this.filter = query;
	}

	@Deprecated
	public void setUpdated(DateTime since) {
		if (since == null) 
			this.updatedRange = null;
		else
			this.updatedRange = new DateTimeRange(since, null);
	}
	
	@Deprecated
	public void setUpdated(DateTimeRange updated) {
		this.updatedRange = updated;
	}

	@Deprecated
	public void setCreated(DateTimeRange created) {
		this.createdRange = created;
	}
	
	@Deprecated
	public void setFields(FieldNames names) {
		this.fieldNames = names;
	}

	@Deprecated
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
	*/
	
}
