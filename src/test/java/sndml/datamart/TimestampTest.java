package sndml.datamart;

import static org.junit.Assert.*;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.slf4j.Logger;

import sndml.servicenow.*;

@RunWith(Parameterized.class)
public class TimestampTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestManager.getDatamartProfiles();
	}
	
	final Logger logger = TestManager.getLogger(this.getClass());
	final String tablename = "incident";
	final TestingProfile profile;
	Session session;
	Database database;
	// Session session;
	// Database database;
	// DBUtil util;	
	// ConfigFactory factory = new ConfigFactory();
	
	public TimestampTest(TestingProfile profile) {
		this.profile = profile;
	}

	@Before
	public void openDatabase() throws Exception {
		session = profile.getSession();
		database = profile.getDatabase();
	}
	
	@After
	public void closeDatabase() throws Exception {
		if (session != null) session.close();
		if (database != null) database.close();
		session = null;
		database = null;
	}
	
	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}
	
	@Test
	public void testIncidentTimestamp() throws Exception {
		TestManager.bannerStart(this.getClass(), "testIncidentTimestamps");
		Table tbl = session.table(tablename);
		database.createMissingTable(tbl, tablename);
		logger.info(Log.TEST, "begin truncateTable");
		database.truncateTable(tablename);
		String sys_id = TestManager.getProperty("some_incident_sys_id");
		TableRecord rec = tbl.getRecord(new RecordKey(sys_id));
		String created = rec.getValue("sys_created_on");
		JobConfig config = new ConfigFactory().tableLoader(profile, tbl);
		config.filter = "sys_id=" + sys_id;
		DateTimeRange emptyRange = new DateTimeRange(null, null);
		config.setCreated(emptyRange);
		logger.info(Log.TEST, config.toString());
		JobRunner runner = new TestJobRunner(profile, config);
		logger.info(Log.TEST, "begin JobRunner.call");
		Metrics loadMetrics = runner.call();
		assertTrue(loadMetrics.getProcessed() > 0);
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		logger.info(Log.TEST, "begin DatabaseTimestampReader.getTimestampCreated");		
		DateTime dbcreated = reader.getTimestampCreated(tbl.getName(), new RecordKey(sys_id));
		logger.info(Log.TEST, "created=" + created + " dbcreated=" + dbcreated);
		assertNotNull(dbcreated);
		assertEquals(created, dbcreated.toString());
	}
	
	@Test
	public void testGetTimestamps() throws Exception {		
		TestManager.bannerStart(this.getClass(), "testGetTimestamps");
		TestFolder folder = new TestFolder(this.getClass().getSimpleName());				
		Loader loader = folder.getYaml("incident-load").getLoader(profile);
		logger.info(Log.TEST, "begin Loader.loadTables()");
		Metrics metrics = loader.loadTables();
		logger.info(Log.TEST, "processed=" + metrics.getProcessed());
		// assertTrue(loaderMetrics.getProcessed() > 10000);
		assertTrue(metrics.getProcessed() > 0);
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		logger.info(Log.TEST, "begin DatabaseTimestampReader.getTimestamps");
		TimestampHash timestamps = reader.getTimestamps(tablename);
		logger.debug(Log.TEST, String.format("Hash size = %d", timestamps.size()));
		assertTrue(timestamps.size() > 0);
		KeySet keys = timestamps.getKeys();
		assertEquals(timestamps.size(), keys.size());
		RecordKey firstKey = keys.get(0);
		DateTime firstTimestamp = timestamps.get(firstKey);
		logger.info(Log.TEST, String.format("%s=%s", firstKey, firstTimestamp));
		TableRecord firstRec = session.table("incident").api().getRecord(firstKey);
		logger.info(Log.TEST, String.format(
				"number=%s created=%s updated=%s", firstRec.getValue("number"), 
				firstRec.getValue("sys_created_on"), firstRec.getValue("sys_updated_on")));
		DateTime firstRecUpdated = firstRec.getUpdatedTimestamp();
		assertEquals(firstRecUpdated, firstTimestamp);
	}

}
