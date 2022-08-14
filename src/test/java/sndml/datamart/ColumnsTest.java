package sndml.datamart;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.Test;

import sndml.servicenow.*;

public class ColumnsTest {
	
	final TestingProfile profile;
	
	public ColumnsTest() throws SQLException, URISyntaxException {
		profile = TestManager.getDefaultProfile();
		TestManager.setProfile(this.getClass(), profile);
	}
	
	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}		
			
	@Test
	public void test() throws Exception {
		TestFolder folder = new TestFolder(this.getClass().getSimpleName());
		Database database = profile.getDatabase();
		DBUtil util = new DBUtil(database);
		Loader loader1 = folder.getYaml("incident-include-columns").getLoader(profile);
		Metrics metrics1 = loader1.loadTables();
		int processed = metrics1.getProcessed();
		assertTrue(processed > 0);
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
		Loader loader2 = folder.getYaml("load-incident-truncate").getLoader(profile);
		loader2.loadTables();
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is not null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
		/*
		// "exclude" is no longer supported!
		 
		Loader loader3 = folder.getYaml("incident-exclude-columns").getLoader(profile);
		loader3.loadTables();
		assertEquals(processed, util.sqlCount("select count(*) from incident"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where short_description is null"));
		assertEquals(processed, util.sqlCount("select count(*) from incident where state is not null"));
		*/
	}

}
