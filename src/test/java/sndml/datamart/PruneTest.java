package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

import sndml.datamart.JobConfig;
import sndml.datamart.LoaderJob;
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
		Session session = TestManager.getProfile().getSession();
		DateTime t0 = DateTime.now();
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
		TestManager.banner(logger, "Insert");
	    FieldValues values = new FieldValues();
	    String descr1 = "This is a test " + t0.toString();
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestManager.getProperty("some_ci"));
	    Key key = api.insertRecord(values).getKey();
	    assertNotNull(api.getRecord(key));
	    TestManager.sleep(2);
	    TestManager.banner(logger,  "Delete");
	    api.deleteRecord(key);
		assertNull(api.getRecord(key));
	    TestManager.sleep(2);
	    TestManager.banner(logger,  "Prune");
	    ConfigFactory factory = new ConfigFactory();
		JobConfig config = factory.jobConfigFromYaml("action: prune, source: incident");
		logger.info(Log.TEST, "PRUNE " + config.getName());
		LoaderJob loader = new LoaderJob(profile, config);
		loader.call();
	}

}
