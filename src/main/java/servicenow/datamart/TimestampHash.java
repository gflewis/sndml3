package servicenow.datamart;

import servicenow.api.*;

import java.util.Hashtable;

public class TimestampHash extends Hashtable<Key, DateTime> {

	private static final long serialVersionUID = 1L;

	KeySet getKeys() {
		return new KeySet(this.keySet());
	}

//	@Deprecated
//	void load(Database database, String tableName) throws SQLException {
//		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
//		TimestampHash data = reader.getTimestamps(tableName);
//		putAll(data);
//	}
}
