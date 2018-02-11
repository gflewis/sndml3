package servicenow.core;

import static org.junit.Assert.*;
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.RecordList;
import servicenow.core.Session;
import servicenow.core.Table;

@RunWith(Parameterized.class)
public class GetRecordsTest {

	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
		return new String[] {"mydevjson", "mydevsoap", "mydevrest"};
	}

	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final String profile;
	final Session session;
	
	public GetRecordsTest(String profile) throws IOException {
		this.profile = profile;
		TestingManager.loadProfile(profile);
		session = new Session(TestingManager.getProperties());
	}

	@Test
	public void testGetSingleRecord() throws Exception {
		logger.info(String.format("%s %s", profile, "testGetSingleRecord"));
		Table tbl = session.table("cmn_department");
		RecordList recs = tbl.api().getRecords("id", TestingManager.getProperty("some_department_id"));
		assertTrue(recs.size() == 1);
		Key sysid = recs.get(0).getKey();
		assertTrue(Key.isGUID(sysid.toString()));
		Record rec0 = tbl.api().getRecord(sysid);
		assertTrue(rec0.getKey().equals(sysid));
	}
	
	@Test
	public void testGetEmptyRecordset() throws Exception {
		logger.info(String.format("%s %s", profile, "testGetEmptyRecordset"));
		Table tbl = session.table("sys_user");
		RecordList recs = tbl.api().getRecords("name", "Zebra Elephant");
		assertTrue(recs.size() == 0);
	}
	
	@Test
	public void getGoodKey() throws Exception {
		String goodKey = TestingManager.getProperty("some_incident_sys_id");
		Table tbl = session.table("incident");
		Record rec = tbl.getRecord(new Key(goodKey));
		assertNotNull(rec);
		assertEquals(goodKey, rec.getValue("sys_id"));
	}
	
	@Test 
	public void testGetBadKey() throws Exception {
		String badKey = "00000000000000000000000000000000";
		Table tbl = session.table("incident");
		Record rec = tbl.getRecord(new Key(badKey));
		assertNull(rec);
	}
	
}
