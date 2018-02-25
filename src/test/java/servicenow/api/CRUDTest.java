package servicenow.api;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.DateTime;
import servicenow.api.FieldValues;
import servicenow.api.Key;
import servicenow.api.Record;
import servicenow.api.Session;
import servicenow.api.Table;
import servicenow.api.TableAPI;

@RunWith(Parameterized.class)
public class CRUDTest {

	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
//		return new String[] {"mydevjson", "mydevsoap", "mydevrest"};
		return new String[] {"mydevrest"};
	}

	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final String profile;
	final Session session;
	
	public CRUDTest(String profile) throws IOException {
		this.profile = profile;
		TestingManager.loadProfile(profile);
		session = TestingManager.getSession();
	}
	
	@Test
	public void testInsertUpdateDelete() throws Exception {
		String now = DateTime.now().toString();
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
	    FieldValues values = new FieldValues();
	    String descr1 = "This is a test " + now;
	    String descr2 = "This incident is updated " + now;
	    values.put("short_description", descr1);
	    values.put("cmdb_ci",  TestingManager.getProperty("some_ci"));
	    Key key = api.insertRecord(values).getKey();	    
	    assertNotNull(key);
	    logger.info("inserted " + key);
	    Record rec = api.getRecord(key);
	    assertEquals(descr1, rec.getValue("short_description"));
	    api.updateRecord(key, new servicenow.api.Parameters("short_description", descr2));
	    rec = api.getRecord(key);
	    assertEquals(descr2, rec.getValue("short_description"));
	    assertTrue("Delete record just inserted", api.deleteRecord(key));
	    assertFalse("Delete non-existent record", api.deleteRecord(key));
	    rec = api.getRecord(key);
	    assertNull(rec);
	}

	@Test @Ignore
	public void testBadUpdate() throws Exception {
		Key badKey = new Key("0123456789abcdef0123456789abcdef");
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
		servicenow.api.Parameters parms = new servicenow.api.Parameters();
		parms.add("short_description", "Updated incident");
		api.updateRecord(badKey,  parms);;
	}
}
