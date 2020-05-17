package servicenow.datamart;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.Test;

import servicenow.api.*;

public class ColumnsTest {
	
	final TestingProfile profile;
	final DBUtil util;
	
	public ColumnsTest() {
		profile = TestingManager.getDefaultProfile();
		TestingManager.setProfile(this.getClass(), profile);
		util = new DBUtil(profile.getDatabase());
	}
	
	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
	}		
	
	@Test
	public void test() throws SQLException {
		TestLoader loader1 = new TestLoader(new YamlFile("incident_include_columns"));
		WriterMetrics metrics1 = loader1.load();
		int processed = metrics1.getProcessed();
		assertTrue(processed > 0);
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
		TestLoader loader2 = new TestLoader(new YamlFile("load_incident_truncate"));
		loader2.load();
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is not null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
		TestLoader loader3 = new TestLoader(new YamlFile("incident_exclude_columns"));
		loader3.load();
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
	}

}
