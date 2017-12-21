package servicenow.core;

import java.io.IOException;
import java.sql.SQLException;

public class NullWriter extends Writer {

	@Override
	public void processRecords(RecordList recs) throws IOException, SQLException {
	}

}
