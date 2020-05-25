package servicenow.datamart;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
		// return new TestingProfile[] {TestingManager.getDefaultProfile()};
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
	public void testSchema_sys_template() throws Exception {
		String tablename = "sys_template";
		Table table = session.table(tablename);
		TableSchema schema = table.getSchema();
		FieldNames schemaFields = schema.getFieldNames();
		Log.setContext(table, "testSchema_sys_template");
		FieldNames testFields = new FieldNames(
				"sys_created_on,sys_created_by,sys_updated_on,sys_updated_by");
		for (String name: testFields) {
			logger.info(Log.TEST, "field: " + name);
			assertTrue(schemaFields.contains(name));
		}		
	}

	@Test
	public void testCreateTable_incident() throws Exception {
		String tablename = "incident";
		Table table = session.table(tablename);
		Generator generator = database.getGenerator();		
		String sql = generator.getCreateTable(table);		
		Log.setContext(table, "testCreateTable_incident");
		logger.info(Log.TEST, sql);
		assertTrue(sql.indexOf(tablename) > 5);
		FieldNames testFields = new FieldNames(
				"sys_created_on,sys_created_by,sys_updated_on,sys_updated_by," + 
				"caller_id,assignment_group,assigned_to," + 
				"opened_at,closed_at,close_code,close_notes");
		for (String name : testFields) {
			assertTrue(sql.indexOf(name) > 5);
		}
		assertNotNull(generator);
	}
	
	@Test
	public void testCreateTable_sys_template() throws Exception {
		String tablename = "sys_template";
		Table table = session.table(tablename);
		Generator generator = database.getGenerator();		
		String sql = generator.getCreateTable(table);		
		Log.setContext(table, "testCreateTable_sys_template");
		logger.info(Log.TEST, sql);
		assertTrue(sql.indexOf(tablename) > 5);
		FieldNames testFields = new FieldNames(
				"sys_created_on,sys_created_by,sys_updated_on,sys_updated_by");
		for (String name : testFields) {
			assertTrue(sql.indexOf(name) > 5);
		}
		assertNotNull(generator);
	}
	
	@Test
	public void testCreateTable_problem() throws Exception {				
		String tablename = "problem";
		Table table = session.table(tablename);
		util.dropTable(tablename);
		assertFalse(database.tableExists(tablename));
		database.createMissingTable(table, null);
		assertTrue(database.tableExists(tablename));
		Log.setContext(table, "testCreateTable_problem");
	}
	
}
