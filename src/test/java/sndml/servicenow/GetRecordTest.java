package sndml.servicenow;

import static org.junit.Assert.*;
import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;

public class GetRecordTest {

	Session session;
	
	GetRecordTest() {
		session = TestManager.getDefaultProfile().getSession();
	}
	
	@Ignore @Test
	public void testRecordByKey() throws IOException {
		String sys_id = TestManager.getProperty("some_incident_sys_id");
		RecordKey key = new RecordKey(sys_id);
		Table inc = session.table("incident");
		TableRecord rec = inc.getRecord(key);
		assertNotNull(rec);
	}
	
	@Test
	public void testGetRecordByNumber() throws IOException {
		Table inc = session.table("incident");
		String number = TestManager.getProperty("some_incident_number");
		TableRecord rec1 = inc.api().getRecord("number", number);
		RecordKey key = rec1.getKey();
		assertEquals(32, key.toString().length());
		TableRecord rec2 = inc.getRecord(key);
		assertEquals(number, rec2.getValue("number"));
	}
	
	@Ignore @Test
	public void testGetNullRecord() throws IOException {
		RecordKey key = new RecordKey("00000000000000000000000000000000");
		Table inc = session.table("incident");
		TableRecord rec = inc.getRecord(key);
		assertNull(rec);
	}

}
