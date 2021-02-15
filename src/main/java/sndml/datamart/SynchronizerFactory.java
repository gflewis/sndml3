package sndml.datamart;

import sndml.servicenow.*;

public class SynchronizerFactory extends TableReaderFactory {

	final Database db;
	final String sqlTableName;
	final WriterMetrics parentMetrics;
	final AppRunLogger appRunLogger;
	
	public SynchronizerFactory(Table table, Database db, String sqlTableName, 
			WriterMetrics parentMetrics, DateTimeRange createdRange, AppRunLogger appRunLogger) {
		super(table);
		this.db = db;
		this.sqlTableName = sqlTableName;
		this.parentMetrics = parentMetrics;
		this.setCreated(createdRange);
		this.appRunLogger = appRunLogger;
	}

	@Override
	public Synchronizer createReader() {
		ProgressLogger progressLogger = new CompositeProgressLogger(Synchronizer.class, appRunLogger);
		Synchronizer syncReader = new Synchronizer(table, db, sqlTableName, parentMetrics, progressLogger);
		syncReader.setFields(this.fieldNames);
		syncReader.setPageSize(this.pageSize);
		return syncReader;
	}

}
