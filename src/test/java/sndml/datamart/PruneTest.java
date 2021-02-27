package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import sndml.servicenow.*;

@RunWith(Parameterized.class)
public class PruneTest {

	final TestingProfile profile;
	final Logger logger = TestManager.getLogger(this.getClass());
	
	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestManager.getDatamartProfiles();
	}

	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}
	
	@After
	public void closeProfile() {
		profile.close();
	}
		
	public PruneTest(TestingProfile profile) throws Exception {
		this.profile = profile;
		TestManager.setProfile(this.getClass(), profile);
	}
	
	@Test
	public void testPrune() throws Exception {
		TestManager.bannerStart("testPrune");
		String tableName = "incident";
		Session session = TestManager.getProfile().getSession();
		Table tbl = session.table(tableName);
		DBUtil db = new DBUtil(profile);
		JobFactory jf = new JobFactory();
		
		TableAPI api = tbl.api();
		TestManager.banner(logger, "Insert");		
	    FieldValues values = new FieldValues();
	    String descr1 = String.format(
    		"%s %s", this.getClass().getName(), 
    		DateTime.now().toString());	    	   
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestManager.getProperty("some_ci"));
	    Key key = api.insertRecord(values).getKey();
	    assertNotNull(api.getRecord(key));
		TestManager.banner(logger, "Load");
		assertTrue(db.tableExists(tableName));
		JobRunner load = jf.yamlJob(profile, "{source: incident, action: update}");
		WriterMetrics loadMetrics = load.call();
	    
	    TestManager.sleep(2);
	    TestManager.banner(logger,  "Delete");
	    api.deleteRecord(key);
		assertNull(api.getRecord(key));
	    TestManager.sleep(2);
	    TestManager.banner(logger,  "Prune");
	    String yaml = String.format(
	    	"{source: incident, action: prune, since: %s}", 
	    	loadMetrics.getStarted().toString());
	    JobRunner jr = jf.yamlJob(profile, yaml);
	    logger.info(Log.TEST, yaml);	    
	    WriterMetrics pruneMetrics = jr.call();
	    assertEquals(0, pruneMetrics.getInserted());
	    assertEquals(0, pruneMetrics.getUpdated());
	    assertEquals(1, pruneMetrics.getDeleted());
	    }

}
