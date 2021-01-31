package sndml.servicenow;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.DateTime;
import sndml.servicenow.FieldValues;
import sndml.servicenow.Key;
import sndml.servicenow.NoSuchRecordException;
import sndml.servicenow.Record;
import sndml.servicenow.Session;
import sndml.servicenow.Table;
import sndml.servicenow.TableAPI;

@RunWith(Parameterized.class)
public class CRUDTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestingManager.allProfiles();
	}

	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final TestingProfile profile;
	final Session session;
	
	public CRUDTest(TestingProfile profile) throws IOException {
		TestingManager.setProfile(this.getClass(), profile);
		this.profile = profile;
		this.session = profile.getSession();
	}

	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
	}
			
	@Test
	public void testInsertUpdateDelete() throws Exception {
		TestingManager.bannerStart("testInsertUpdateDelete");
		String now = DateTime.now().toString();
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
		TestingManager.banner(logger, "Insert");
	    FieldValues values = new FieldValues();
	    String descr1 = "This is a test " + now;
	    String descr2 = "This incident is updated " + now;
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestingManager.getProperty("some_ci"));
	    InsertResponse resp = api.insertRecord(values);
	    logger.info(Log.TEST, "InsertResponse=" + resp.toString());
	    Key key = resp.getKey();	    
	    assertNotNull(key);
	    logger.info("inserted " + key);
	    TestingManager.banner(logger,  "Update");
	    Record rec = api.getRecord(key);
	    assertEquals(descr1, rec.getValue("short_description"));
	    api.updateRecord(key, new sndml.servicenow.Parameters("short_description", descr2));
	    TestingManager.banner(logger, "Delete");
	    rec = api.getRecord(key);
	    assertEquals(descr2, rec.getValue("short_description"));
	    assertTrue("Delete record just inserted", api.deleteRecord(key));
	    assertFalse("Delete non-existent record", api.deleteRecord(key));
	    rec = api.getRecord(key);
	    assertNull(rec);
	}

	@Test(expected = NoSuchRecordException.class) 
	public void testBadUpdate() throws Exception {
		TestingManager.bannerStart("testBadUpdate");
		Key badKey = new Key("0123456789abcdef0123456789abcdef");
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
		sndml.servicenow.Parameters parms = new sndml.servicenow.Parameters();
		parms.add("short_description", "Updated incident");
		api.updateRecord(badKey,  parms);;
	}
}
