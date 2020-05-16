package servicenow.api;

import static org.junit.Assert.*;
import java.io.IOException;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.Key;
import servicenow.api.Record;
import servicenow.api.RecordList;
import servicenow.api.Session;
import servicenow.api.Table;

@RunWith(Parameterized.class)
public class GetRecordsTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestingManager.getProfiles("mydevjson mydevsoap mydevrest");
	}

	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final TestingProfile profile;
	final Session session;
	
	public GetRecordsTest(TestingProfile profile) throws IOException {
		TestingManager.setProfile(profile);
		this.profile = profile;
		this.session = profile.getSession();
	}

	@Test
	public void testGetSingleRecord() throws Exception {
		TestingManager.bannerStart(this.getClass(), "testGetSingleRecord");
		// logger.info(Log.TEST, String.format("%s %s", profile, "testGetSingleRecord"));
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
		TestingManager.bannerStart(this.getClass(), "testGetSingleRecord");
		// logger.info(Log.TEST, String.format("%s %s", profile, "testGetEmptyRecordset"));
		Table tbl = session.table("sys_user");
		RecordList recs = tbl.api().getRecords("name", "Zebra Elephant");
		assertTrue(recs.size() == 0);
	}
	
	@Test
	public void getGoodKey() throws Exception {
		TestingManager.bannerStart(this.getClass(), "getGoodKey");
		String goodKey = TestingManager.getProperty("some_incident_sys_id");
		Table tbl = session.table("incident");
		Record rec = tbl.getRecord(new Key(goodKey));
		assertNotNull(rec);
		assertEquals(goodKey, rec.getValue("sys_id"));
	}
	
	@Test 
	public void testGetBadKey() throws Exception {
		TestingManager.bannerStart(this.getClass(), "testGetBadKey");
		String badKey = "00000000000000000000000000000000";
		Table tbl = session.table("incident");
		Record rec = tbl.getRecord(new Key(badKey));
		assertNull(rec);
	}
	
}
