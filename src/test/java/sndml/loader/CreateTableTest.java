package sndml.loader;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import sndml.servicenow.*;
import sndml.util.FieldNames;
import sndml.util.Log;

@RunWith(Parameterized.class)
public class CreateTableTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		// return new TestingProfile[] {TestingManager.getDefaultProfile()};
		return TestManager.allProfiles();
	}

	TestingProfile profile;
	Session session;
	DatabaseWrapper dbWrapper;
	Resources resources;
	Logger logger = TestManager.getLogger(this.getClass());
	
	public CreateTableTest(TestingProfile profile) throws Exception {
		this.profile = profile;
		TestManager.setProfile(this.getClass(), profile);
	}

	@Before
	public void openDatabase() throws Exception {
		resources = new Resources(profile);
		session = resources.getReaderSession();
		dbWrapper = resources.getDatabaseWrapper();				
	}
	
	@After
	public void closeDatabase() throws Exception {
		if (session != null) session.close();
		if (dbWrapper != null) dbWrapper.close();
		session = null;
		dbWrapper = null;
	}
	
	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}

	@Test
	public void testSchema_sys_template() throws Exception {
		String tablename = "sys_template";
		Table table = session.table(tablename);
		SchemaReader schemaReader = resources.getSchemaReader();
		TableSchema schema = schemaReader.getSchema(table.getName());
		
		FieldNames schemaFields = schema.getFieldNames();
		Log.setTableContext(table, "testSchema_sys_template");
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
		Generator generator = dbWrapper.getGenerator();		
		String sql = generator.getCreateTable(table);
		logger.info(Log.TEST, sql);
		Log.setTableContext(table, "testCreateTable_incident");
		logger.info(Log.TEST, sql);
		sql = sql.toLowerCase();
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
	public void testCreateTable_problem() throws Exception {				
		String tablename = "problem";
		Table table = session.table(tablename);
		new DBUtil(dbWrapper).dropTable(tablename);
		assertFalse(dbWrapper.tableExists(tablename));
		dbWrapper.createMissingTable(table, null);
		assertTrue(dbWrapper.tableExists(tablename));
		Log.setTableContext(table, "testCreateTable_problem");
	}
	
	@Test
	public void testCreateTable_sys_template() throws Exception {
		String tablename = "sys_template";
		Table table = session.table(tablename);
		Generator generator = dbWrapper.getGenerator();		
		String sql = generator.getCreateTable(table);		
		Log.setTableContext(table, "testCreateTable_sys_template");
		logger.info(Log.TEST, sql);
		sql = sql.toLowerCase();
		assertTrue(sql.indexOf(tablename) > 5);
		FieldNames testFields = new FieldNames(
				"sys_created_on,sys_created_by,sys_updated_on,sys_updated_by");
		for (String name : testFields) {
			assertTrue(sql.indexOf(name) > 5);
		}
		assertNotNull(generator);
	}
	
}
