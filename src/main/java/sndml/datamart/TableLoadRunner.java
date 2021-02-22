package sndml.datamart;

import sndml.servicenow.DateTime;
import sndml.servicenow.Table;
import sndml.servicenow.WriterMetrics;

public class TableLoadRunner extends JobRunner {

	public TableLoadRunner(ConnectionProfile profile, Table table, Database database) {
		ConfigFactory configFactory = new ConfigFactory(DateTime.now());
		this.config = configFactory.tableLoader(profile, table);
		this.session = table.getSession();
		this.db = database;
		this.table = table;
		this.sqlTableName = config.getTarget();
		this.tableLoaderName = config.getName();
		this.metrics = new WriterMetrics();
		this.appRunLogger = null;
	}


}
