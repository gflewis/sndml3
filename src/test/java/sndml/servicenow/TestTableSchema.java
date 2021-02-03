package sndml.servicenow;

import static org.junit.Assert.*;

import org.junit.*;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import sndml.servicenow.FieldNames;
import sndml.servicenow.Log;
import sndml.servicenow.Session;
import sndml.servicenow.Table;
import sndml.servicenow.TableSchema;

@RunWith(Parameterized.class)
public class TestTableSchema {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		// return new TestingProfile[] {TestingManager.getDefaultProfile()};
		return TestManager.allProfiles();
	}

	TestingProfile profile;
	Session session;
	Logger logger = TestManager.getLogger(this.getClass());

	final FieldNames testFields = new FieldNames(
			"sys_created_on,sys_created_by,sys_updated_on,sys_updated_by");
	
	public TestTableSchema(TestingProfile profile) throws Exception {
		TestManager.setProfile(this.getClass(), profile);
		session = TestManager.getProfile().getSession();
	}

	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}

	@Test
	public void testDetermineParent() throws Exception {
		Table table = session.table("sys_template");
		TableSchema schema = new TableSchema(table);
		String parent = schema.getParentName();
		logger.info(Log.TEST, "parent=" + parent);
		assertEquals("sys_metadata", parent);
	}
	
	@Test
	public void testSysTemplateSchema() throws Exception {
		Table table = session.table("sys_template");
		TableSchema schema = new TableSchema(table);
		FieldNames schemaFields = schema.getFieldNames();
		for (String name: testFields) {
			logger.info(Log.TEST, "field: " + name);
			assertTrue(schemaFields.contains(name));
		}		
	}
		
}
