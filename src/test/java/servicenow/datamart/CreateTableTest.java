package servicenow.datamart;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
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
	public static TestingProfile[] profiles() {
		return TestingManager.allProfiles();
	}

	TestingProfile profile;
	Session session;
	Database database;
	DBUtil util;
	Logger logger = TestingManager.getLogger(this.getClass());
	
	public CreateTableTest(TestingProfile profile) throws Exception {
		TestingManager.setProfile(this.getClass(), profile);		
		session = TestingManager.getProfile().getSession();
		util = new DBUtil(profile);
		database = util.getDatabase();
	}

	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
	}
		
	@Test
	public void testCreateTable() throws Exception {				
		TestingManager.bannerStart("testCreateTable");
		String tablename = "problem";
		util.dropTable(tablename);
		assertFalse(database.tableExists(tablename));
		Table table = session.table(tablename);
		database.createMissingTable(table, null);
		assertTrue(database.tableExists(tablename));
	}
	
}
