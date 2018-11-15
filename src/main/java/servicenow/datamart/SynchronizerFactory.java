package servicenow.datamart;

import servicenow.api.*;

public class SynchronizerFactory extends TableReaderFactory {

	final Database db;
	final String sqlTableName;
	final WriterMetrics parentMetrics;
	
	public SynchronizerFactory(Table table, Database db, String sqlTableName, 
			WriterMetrics parentMetrics, DateTimeRange createdRange) {
		super(table);
		this.db = db;
		this.sqlTableName = sqlTableName;
		this.parentMetrics = parentMetrics;
		this.setCreated(createdRange);
	}

	@Override
	public Synchronizer createReader() {
		Synchronizer syncReader = new Synchronizer(table, db, sqlTableName, parentMetrics);
		syncReader.setPageSize(this.pageSize);
		return syncReader;
	}

}
