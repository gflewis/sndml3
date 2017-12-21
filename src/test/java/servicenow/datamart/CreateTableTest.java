package servicenow.datamart;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;

import servicenow.core.*;
import servicenow.datamart.Database;

public class CreateTableTest {

	Logger logger = TestingManager.getLogger(this.getClass());
	
	
	@Test
	public void testCreateTable() throws Exception {
		TestingManager.loadDefaultProfile();
		Session session = TestingManager.getSession();
		Database dbw = AllTests.getDBWriter();
		String tablename = "rm_story";
		Table table = session.table(tablename);
		dbw.createMissingTable(table, null);
		assert dbw.tableExists(tablename);
	}

	@Test
	public void testCreateTableFoo() throws Exception {
		TestingManager.loadDefaultProfile();
		Database db = AllTests.getDBWriter();
		assertFalse(db.tableExists("blahblahblah"));
		String schema = db.getSchema();
		logger.debug("schema=" + schema);
		String shortname = "foo";
		String createtable = "create table " + shortname + "(bar varchar(20))";
		String droptable = "drop table " + shortname;
		if (db.tableExists(shortname)) DBTest.sqlUpdate(droptable);
		DBTest.sqlUpdate(createtable);
		DBTest.commit();
		assertTrue(DBTest.tableExists(shortname));
		DBTest.sqlUpdate(droptable);
		DBTest.commit();
		assertFalse(DBTest.tableExists(shortname));
	}
	
}
