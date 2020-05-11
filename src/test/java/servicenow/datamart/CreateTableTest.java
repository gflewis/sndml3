package servicenow.datamart;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import servicenow.api.*;
import servicenow.datamart.Database;

@RunWith(Parameterized.class)
public class CreateTableTest {

	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
		return TestingManager.allProfiles();
	}

	Session session;
	Database database;
	Logger logger = TestingManager.getLogger(this.getClass());
	
	public CreateTableTest(String profile) throws Exception {
		TestingManager.loadProfile(profile, true);
		session = ResourceManager.getSession();
		database = ResourceManager.getDatabase();
	}
		
	@Test
	public void testCreateTable() throws Exception {				
		String tablename = "rm_story";
		Table table = session.table(tablename);
		database.createMissingTable(table, null);
		assert database.tableExists(tablename);
	}

	@Test
	public void testCreateTableFoo() throws Exception {
		assertFalse(database.tableExists(TestingManager.randomName()));
		String schema = database.getSchema();
		logger.debug("schema=" + schema);
		String shortname = TestingManager.randomName();
		String createtable = "create table " + shortname + "(bar varchar(20))";
		String droptable = "drop table " + shortname;
		if (database.tableExists(shortname)) DBTest.sqlUpdate(droptable);
		DBTest.sqlUpdate(createtable);
		DBTest.commit();
		assertTrue(DBTest.tableExists(shortname));
		DBTest.sqlUpdate(droptable);
		DBTest.commit();
		assertFalse(DBTest.tableExists(shortname));
	}
	
}
