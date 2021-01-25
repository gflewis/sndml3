package sndml.datamart;

import sndml.datamart.Database;
import sndml.datamart.DatabaseTimestampReader;
import sndml.datamart.JobConfig;
import sndml.datamart.Loader;
import sndml.datamart.LoaderJob;
import sndml.datamart.TimestampHash;
import sndml.servicenow.*;

import static org.junit.Assert.*;
import org.junit.*;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;

@RunWith(Parameterized.class)
public class TimestampTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestingManager.getDatamartProfiles();
	}
	
	final Logger logger = TestingManager.getLogger(this.getClass());
	final String tablename = "incident";
	final TestingProfile profile;
	final Session session;
	final Database database;
	final DBUtil util;	
	
	public TimestampTest(TestingProfile profile) throws Exception {
		this.profile = profile;
		this.session = profile.getSession();
		this.database = profile.getDatabase();
		TestingManager.setProfile(this.getClass(), profile);
		util = new DBUtil(database);
		util.dropTable(tablename);
		database.createMissingTable(session.table(tablename));
	}

	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
	}

	@After
	public void closeProfile() {
		profile.close();
	}
	
	void loadTable() throws Exception {
		Table table = session.table(tablename);
		database.truncateTable(table.getName());
		Loader loader = new Loader(table, profile.getDatabase());
		loader.loadTables();
		WriterMetrics metrics = loader.lastJob().getMetrics();
		assertTrue(metrics.getProcessed() > 0);		
	}
	
	@Test
	public void testIncidentTimestamp() throws Exception {
		TestingManager.bannerStart("testIncidentTimestamps");
		Session session = profile.getSession();
		Database database = profile.getDatabase();
		Table tbl = session.table(tablename);
		database.createMissingTable(tbl, tablename);
		util.truncateTable(tablename);
		String sys_id = TestingManager.getProperty("some_incident_sys_id");
		Record rec = tbl.getRecord(new Key(sys_id));
		String created = rec.getValue("sys_created_on");
		JobConfig config = new JobConfig(tbl);
		config.setFilter("sys_id=" + sys_id);
		DateTimeRange emptyRange = new DateTimeRange(null, null);
		config.setCreated(emptyRange);
		LoaderJob loader = new LoaderJob(profile, config);
		loader.call();
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		DateTime dbcreated = reader.getTimestampCreated(tbl.getName(), new Key(sys_id));
		assertNotNull(dbcreated);
		assertEquals(created, dbcreated.toString());		
	}
	
	@Test
	public void testGetTimestamps() throws Exception {
		TestingManager.bannerStart("testGetTimestamps");
		Session session = profile.getSession();
		Database database = profile.getDatabase();
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
