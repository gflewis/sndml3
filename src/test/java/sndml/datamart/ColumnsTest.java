package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import sndml.datamart.Loader;
import sndml.servicenow.*;

public class ColumnsTest {
	
	final TestingProfile profile;
	final DBUtil util;
	
	public ColumnsTest() {
		profile = TestManager.getDefaultProfile();
		TestManager.setProfile(this.getClass(), profile);
		util = new DBUtil(profile.getDatabase());
	}
	
	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}		

	@After
	public void closeProfile() {
		profile.close();
	}
			
	@Test
	public void test() throws Exception {
		TestFolder folder = new TestFolder("YAML");
		Loader loader1 = folder.getYaml("incident_include_columns").getLoader(profile);
		WriterMetrics metrics1 = loader1.loadTables();
		int processed = metrics1.getProcessed();
		assertTrue(processed > 0);
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
		Loader loader2 = folder.getYaml("load_incident_truncate").getLoader(profile);
		loader2.loadTables();
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is not null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
		Loader loader3 = folder.getYaml("incident_exclude_columns").getLoader(profile);
		loader3.loadTables();
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
	}

}
