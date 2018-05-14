package servicenow.datamart;

import servicenow.api.*;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class PruneTest {

	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
		return new String[] {"awsmysql","awsmssql"};
	}

	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final Session session;
	final Database database;
	
	public PruneTest(String profile) throws Exception {
		TestingManager.loadProfile(profile);
		session = ResourceManager.getSession();
		database = ResourceManager.getDatabase();
	}
	
	@Test
	public void testPrune() throws Exception {
		DateTime t0 = DateTime.now();
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
		TestingManager.banner(logger, "Insert");
	    FieldValues values = new FieldValues();
	    String descr1 = "This is a test " + t0.toString();
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestingManager.getProperty("some_ci"));
	    Key key = api.insertRecord(values).getKey();
	    assertNotNull(api.getRecord(key));
	    TestingManager.sleep(2);
	    TestingManager.banner(logger,  "Delete");
	    api.deleteRecord(key);
		assertNull(api.getRecord(key));
	    TestingManager.sleep(2);
	    TestingManager.banner(logger,  "Prune");
		JobConfig config = new JobConfig(tbl);
		config.setAction(LoaderAction.PRUNE);
		config.setSince(t0);
		logger.info(Log.TEST, "PRUNE " + config.getName());
		LoaderJob loader = new LoaderJob(config, null);
		loader.call();
	}

}
