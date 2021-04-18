package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import sndml.servicenow.*;

@RunWith(Parameterized.class)
public class RefreshTest2 {

	final TestingProfile profile;
	final Logger logger = TestManager.getLogger(this.getClass());
	
	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestManager.getDatamartProfiles();
	}
	
	public RefreshTest2(TestingProfile profile) throws Exception {
		this.profile = profile;
		TestManager.setProfile(this.getClass(), profile);
	}
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRefresh() throws Exception {
		String tableName = "incident";
		Session session = profile.getSession();
		DBUtil db = new DBUtil(profile);
		JobFactory jf = new JobFactory(profile, session, db.getDatabase(), DateTime.now());		
		TableAPI api = profile.getSession().table(tableName).api();
	    TestManager.banner(logger, "Load");
		db.dropTable(tableName);
		JobRunner create = jf.yamlJob("{source: incident, action: create}");
		create.call();
		assertTrue(db.tableExists(tableName));
		JobRunner load = jf.yamlJob("{source: incident, action: load, created: 2020-01-01}");
		Metrics loadMetrics = load.call();
		int count1 = db.sqlCount(tableName, null);
		assertTrue(count1 > 0);
		assertEquals(count1, loadMetrics.getInserted());
		assertNotNull(loadMetrics.getStarted());
	    TestManager.banner(logger, "Insert");
	    FieldValues values = new FieldValues();
	    String descr1 = String.format(
    		"%s %s", this.getClass().getSimpleName(), 
    		loadMetrics.getStarted().toString());	    
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestManager.getProperty("some_ci"));
	    RecordKey key = api.insertRecord(values).getKey();
	    TestManager.sleep(2);
	    TestManager.banner(logger,  "Refresh");
	    String yaml = String.format(
	    	"{source: incident, action: refresh, since: last, last: %s}", 
	    	loadMetrics.getStarted().toString());
	    logger.info(Log.TEST, yaml);
	    JobRunner refresh = jf.yamlJob(yaml);
	    Metrics refreshMetrics = refresh.call();
	    assertEquals(1, refreshMetrics.getInserted());
	    assertEquals(0, refreshMetrics.getUpdated());
	    int count2 = db.sqlCount(tableName, null);
	    assertEquals(count1 + 1, count2);
	    api.deleteRecord(key);
	    
	}

}
