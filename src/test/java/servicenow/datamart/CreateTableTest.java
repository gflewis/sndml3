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
		TestingManager.bannerStart(this.getClass(), "testCreateTable");
		String tablename = "problem";
		DBUtil.dropTable(tablename);
		assertFalse(database.tableExists(tablename));
		Table table = session.table(tablename);
		database.createMissingTable(table, null);
		assertTrue(database.tableExists(tablename));
	}
	
}
