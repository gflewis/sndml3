package sndml.datamart;

import static org.junit.Assert.*;

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
		
		TableAPI api = tbl.api();
		TestManager.banner(logger, "Insert");		
	    FieldValues values = new FieldValues();
	    String descr1 = String.format(
    		"%s %s", this.getClass().getName(), 
    		DateTime.now().toString());	    	   
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestManager.getProperty("some_ci"));
	    RecordKey key = api.insertRecord(values).getKey();
	    assertNotNull(api.getRecord(key));
	    logger.info(Log.TEST, "Inserted Incident " + key);
	    TestManager.sleep(1.5);
	    
		TestManager.banner(logger, "Load");
		DateTime testStarted = DateTime.now();
		JobFactory jf = new JobFactory(profile, session, db.getDatabase(), testStarted);
		assertTrue(db.tableExists(tableName));
		JobRunner load = jf.yamlJob("{source: incident, action: load, truncate: true, created: 2020-01-01}");
		Metrics loadMetrics = load.call();
		DateTime loadStarted = loadMetrics.getStarted();
		int countBefore = db.sqlCount("incident", null);
		logger.info(Log.TEST, String.format("database rows=%d", countBefore));
		assertEquals(loadMetrics.getInserted(), countBefore);
		assertEquals(1, db.sqlCount("incident", String.format("sys_id='%s'", key)));
			    
	    TestManager.banner(logger,  "Delete");
	    api.deleteRecord(key);
		assertNull(api.getRecord(key));
		logger.info(Log.TEST, "Deleted Incident " + key);
	    TestManager.sleep(1.5);
	    
	    TestManager.banner(logger,  "Prune");
	    DateTime pruneStart = loadStarted.subtractSeconds(1);
	    String yaml = String.format(
	    	"{source: incident, action: prune, since: %s}",	pruneStart);
	    JobRunner pruneJob = jf.yamlJob(yaml);
	    logger.info(Log.TEST, yaml);	    
	    Metrics pruneMetrics = pruneJob.call();
		int countAfter = db.sqlCount("incident", null);
		logger.info(Log.TEST, String.format("database rows=%d", countAfter));
		assertTrue(countAfter < countBefore);
	    assertEquals(0, pruneMetrics.getInserted());
	    assertEquals(0, pruneMetrics.getUpdated());
	    assertEquals(1, pruneMetrics.getDeleted());
	    }

}
