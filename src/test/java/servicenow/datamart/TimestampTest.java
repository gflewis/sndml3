package servicenow.datamart;

import servicenow.api.*;

import static org.junit.Assert.*;
import org.junit.*;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TimestampTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestingManager.getDatamartProfiles();
	}
	
	final Logger logger = LoggerFactory.getLogger(this.getClass());
	final String tablename = "incident";
	final Session session;
	final Database database;
	final DBUtil util;	
	
	public TimestampTest(TestingProfile profile) throws Exception {
		// TestingProfile profile = TestingManager.getDefaultProfile();
		TestingManager.setProfile(this.getClass(), profile);
		session = profile.getSession();
		database = profile.getDatabase();
		util = new DBUtil(database);
		util.dropTable(tablename);
		database.createMissingTable(session.table(tablename));
	}

	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
	}
		
	void loadTable() throws Exception {
		String text = String.format("tables: [{name: %s, truncate: true}]", tablename);
		TestLoader loader = new TestLoader(text);
		WriterMetrics metrics = loader.load();
		assertTrue(metrics.getProcessed() > 0);		
	}
	
	@Test
	public void testIncidentTimestamp() throws Exception {
		TestingManager.bannerStart("testIncidentTimestamps");
		Table tbl = session.table(tablename);
		database.createMissingTable(tbl, tablename);
		util.truncateTable(tablename);
		String sys_id = TestingManager.getProperty("some_incident_sys_id");
		Record rec = tbl.getRecord(new Key(sys_id));
		String created = rec.getValue("sys_created_on");
		JobConfig config = new JobConfig(tbl);
		config.setFilter(new EncodedQuery("sys_id=" + sys_id));
		DateTimeRange emptyRange = new DateTimeRange(null, null);
		config.setCreated(emptyRange);
		LoaderJob loader = new LoaderJob(config, null);
		loader.call();
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		DateTime dbcreated = reader.getTimestampCreated(tbl.getName(), new Key(sys_id));
		assertNotNull(dbcreated);
		assertEquals(created, dbcreated.toString());		
	}
	
	@Test @Ignore
	public void testGetTimestamps() throws Exception {
		TestingManager.bannerStart("testGetTimestamps");
		loadTable();
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		TimestampHash timestamps = reader.getTimestamps(tablename);
		logger.debug(Log.TEST, String.format("Hash size = %d", timestamps.size()));
		assertTrue(timestamps.size() > 0);
		KeySet keys = timestamps.getKeys();
		assertEquals(timestamps.size(), keys.size());
		Key firstKey = keys.get(0);
		DateTime firstTimestamp = timestamps.get(firstKey);
		logger.info(Log.TEST, String.format("%s=%s", firstKey, firstTimestamp));
		Record firstRec = session.table("incident").api().getRecord(firstKey);
		logger.info(Log.TEST, String.format(
				"number=%s created=%s updated=%s", firstRec.getValue("number"), 
				firstRec.getValue("sys_created_on"), firstRec.getValue("sys_updated_on")));
		DateTime firstRecUpdated = firstRec.getUpdatedTimestamp();
		assertEquals(firstRecUpdated, firstTimestamp);
	}

}
