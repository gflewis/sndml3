package servicenow.datamart;

import servicenow.api.*;

import java.sql.SQLException;
import java.util.Hashtable;

public class TimestampLookup extends Hashtable<Key, DateTime> {

	private static final long serialVersionUID = 1L;

	KeySet getKeys() {
		return new KeySet(this.keySet());
	}

	// TODO: Refactor
	void load(Database database, String tableName) throws SQLException {
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		TimestampLookup data = reader.getTimestamps(tableName);
		putAll(data);
	}
}
