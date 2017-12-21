package servicenow.soap;

import static org.junit.Assert.*;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import servicenow.core.*;

public class GetRecordTest {

	Session session;
	
	@Before
	public void setUp() throws Exception {
		session = TestingManager.getSession();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetRecord() throws IOException {
		Table inc = session.table("incident");
		String number = TestingManager.getProperty("some_incident_number");
		Record rec1 = inc.getRecord("number", number);
		Key key = rec1.getKey();
		assertEquals(32, key.toString().length());
		Record rec2 = inc.getRecord(key);
		assertEquals(number, rec2.getValue("number"));
	}
	
	@Test
	public void testGetNullRecord() throws IOException {
		Key key = new Key("00000000000000000000000000000000");
		Table inc = session.table("incident");
		Record rec = inc.getRecord(key);
		assertNull(rec);
	}

}
