package servicenow.datamart;

import servicenow.api.*;

public class TableSyncReaderFactory extends TableReaderFactory {

	final Database db;
	final String sqlTableName;
	final WriterMetrics parentMetrics;
	
	public TableSyncReaderFactory(Table table, Database db, String sqlTableName, 
			WriterMetrics parentMetrics, DateTimeRange createdRange) {
		super(table);
		this.db = db;
		this.sqlTableName = sqlTableName;
		this.parentMetrics = parentMetrics;
		this.setCreated(createdRange);
	}

	@Override
	public TableSyncReader createReader() {
		TableSyncReader syncReader = new TableSyncReader(table, db, sqlTableName, parentMetrics);
		return syncReader;
	}

}
