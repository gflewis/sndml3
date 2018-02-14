package servicenow.core;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class CRUDTest {

	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
		return new String[] {"mydevjson", "mydevsoap", "mydevrest"};
//		return new String[] {"mydevsoap"};
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
	public void testInsertDelete() throws Exception {
		Table tbl = session.table("incident");
		TableAPI api = tbl.api();
	    FieldValues values = new FieldValues();
	    values.put("short_description", "This is a test");
	    values.put("cmdb_ci",  TestingManager.getProperty("some_ci"));
	    Key key = api.insertRecord(values).getKey();
	    logger.info("inserted " + key);
	    assertNotNull(key);
	    assertTrue("Delete record just inserted", api.deleteRecord(key));
	    assertFalse("Delete non-existent record", api.deleteRecord(key));
	}

	
}
