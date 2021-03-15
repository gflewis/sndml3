package sndml.datamart;

import sndml.servicenow.*;

public class SynchronizerFactory extends ConfigTableReaderFactory {

	final Database db;
	
	public SynchronizerFactory(Table table, Database db, JobConfig config, DateTimeRange createdRange) {
		super(table, config);
		this.db = db;
		this.setCreated(createdRange);
	}

	@Override
	public Synchronizer createReader() {
		Synchronizer syncReader = new Synchronizer(table, db, config.getTarget(), config.getName());
		syncReader.setFields(this.fieldNames);
		syncReader.setPageSize(this.pageSize);
		return syncReader;
	}

}
