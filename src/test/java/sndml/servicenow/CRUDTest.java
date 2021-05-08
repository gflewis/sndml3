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

@RunWith(Parameterized.class)
public class CRUDTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestManager.allProfiles();
	}

	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final TestingProfile profile;
	final Session session;
	
	public CRUDTest(TestingProfile profile) throws IOException {
		TestManager.setProfile(this.getClass(), profile);
		this.profile = profile;
		this.session = profile.getSession();
	}

	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}
			
	@Test
	public void testInsertUpdateDelete() throws Exception {
		TestManager.bannerStart("testInsertUpdateDelete");
		String now = DateTime.now().toString();
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
		TestManager.banner(logger, "Insert");
	    FieldValues values = new FieldValues();
	    String descr1 = "This is a test " + now;
	    String descr2 = "This incident is updated " + now;
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestManager.getProperty("some_ci"));
	    InsertResponse resp = api.insertRecord(values);
	    logger.info(Log.TEST, "InsertResponse=" + resp.toString());
	    RecordKey key = resp.getKey();	    
	    assertNotNull(key);
	    logger.info("inserted " + key);
	    TestManager.banner(logger,  "Update");
	    TableRecord rec = api.getRecord(key);
	    assertEquals(descr1, rec.getValue("short_description"));
	    api.updateRecord(key, new sndml.servicenow.Parameters("short_description", descr2));
	    TestManager.banner(logger, "Delete");
	    rec = api.getRecord(key);
	    assertEquals(descr2, rec.getValue("short_description"));
	    assertTrue("Delete record just inserted", api.deleteRecord(key));
	    assertFalse("Delete non-existent record", api.deleteRecord(key));
	    rec = api.getRecord(key);
	    assertNull(rec);
	}

	@Test(expected = NoSuchRecordException.class) 
	public void testBadUpdate() throws Exception {
		TestManager.bannerStart("testBadUpdate");
		RecordKey badKey = new RecordKey("0123456789abcdef0123456789abcdef");
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
		sndml.servicenow.Parameters parms = new sndml.servicenow.Parameters();
		parms.add("short_description", "Updated incident");
		api.updateRecord(badKey,  parms);;
	}
}
