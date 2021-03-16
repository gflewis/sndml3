package sndml.datamart;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.regex.Pattern;

import sndml.servicenow.*;

public class DatabaseInsertWriter extends DatabaseTableWriter {

	protected DatabaseInsertStatement insertStmt;
	
	public DatabaseInsertWriter(Database db, Table table, String sqlTableName, String writerName) 
			throws IOException, SQLException {
		super(db, table, sqlTableName, writerName);
	}

	@Override
	public DatabaseInsertWriter open() throws SQLException, IOException {
		super.open();
		insertStmt = new DatabaseInsertStatement(this.db, this.sqlTableName, columns);
		return this;
	}
	
	Pattern primaryKeyViolation = 
			Pattern.compile("\\b(primary key|unique constraint)\\b", Pattern.CASE_INSENSITIVE);
		
	@Override
	void writeRecord(Record rec) throws SQLException {
		Key key = rec.getKey();
		logger.trace(Log.PROCESS, "Insert " + key);
		try {
			insertStmt.insert(rec);
			writerMetrics.incrementInserted();
		}
		catch (SQLIntegrityConstraintViolationException e) {
			logger.debug(Log.PROCESS, e.getClass().getName() + ": " + e.getMessage());
			logger.warn(Log.PROCESS, "Failed/Skipped " + key);
			writerMetrics.incrementSkipped();
		}
		catch (SQLException e) {
			logger.debug(Log.PROCESS, e.getClass().getName() + ": " + e.getMessage());
			if (primaryKeyViolation.matcher(e.getMessage()).find()) {
				logger.warn(Log.PROCESS, "Failed/Skipped " + key);
				writerMetrics.incrementSkipped();								
			}
			else {
				throw e;
			}
		}
	}

}
