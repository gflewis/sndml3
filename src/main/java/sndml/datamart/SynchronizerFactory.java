package sndml.datamart;

import sndml.servicenow.*;

public class SynchronizerFactory extends TableReaderFactory {

	final Database db;
	final String sqlTableName;
	
	public SynchronizerFactory(Table table, Database db, String sqlTableName, DateTimeRange createdRange) {
		super(table);
		this.db = db;
		this.sqlTableName = sqlTableName;
		this.setCreated(createdRange);
	}

	@Override
	public Synchronizer createReader() {
		Synchronizer syncReader = new Synchronizer(table, db, sqlTableName);
		syncReader.setFields(this.fieldNames);
		syncReader.setPageSize(this.pageSize);
		return syncReader;
	}

}
