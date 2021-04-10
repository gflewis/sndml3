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
	final Session session;
	final Database database;
	final DBUtil util;	
	ConfigFactory factory = new ConfigFactory();
	
	public TimestampTest(TestingProfile profile) throws Exception {
		this.profile = profile;
		this.session = profile.getSession();
		this.database = profile.getDatabase();
		TestManager.setProfile(this.getClass(), profile);
		util = new DBUtil(database);
		// util.dropTable(tablename);
		database.createMissingTable(session.table(tablename));
	}

	@AfterClass
	public static void clear() throws Exception {
		TestManager.clearAll();
	}

	@After
	public void closeProfile() {
		profile.close();
	}
	
//	private void loadTable() throws Exception {
//		Table table = session.table(tablename);
//		database.truncateTable(table.getName());		
//		SimpleTableLoader runner = new SimpleTableLoader(profile, table, database);
//		Metrics metrics = runner.call();
//		assertTrue(metrics.getProcessed() > 0);		
//	}
	
	@Test
	public void testIncidentTimestamp() throws Exception {
		TestManager.bannerStart("testIncidentTimestamps");
		Session session = profile.getSession();
		Database database = profile.getDatabase();
		Table tbl = session.table(tablename);
		database.createMissingTable(tbl, tablename);
		util.truncateTable(tablename);
		String sys_id = TestManager.getProperty("some_incident_sys_id");
		BaseRecord rec = tbl.getRecord(new RecordKey(sys_id));
		String created = rec.getValue("sys_created_on");
		JobConfig config = factory.tableLoader(profile, tbl);
		config.filter = "sys_id=" + sys_id;
		DateTimeRange emptyRange = new DateTimeRange(null, null);
		config.setCreated(emptyRange);		
		JobRunner runner = new TestJobRunner(profile, config);
		Metrics loadMetrics = runner.call();
		assertTrue(loadMetrics.getProcessed() > 0);
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		DateTime dbcreated = reader.getTimestampCreated(tbl.getName(), new RecordKey(sys_id));
		logger.info(Log.TEST, "created=" + created + " dbcreated=" + dbcreated);
		assertNotNull(dbcreated);
		assertEquals(created, dbcreated.toString());		
	}
	
	@Test
	public void testGetTimestamps() throws Exception {		
		TestManager.bannerStart("testGetTimestamps");
		TestFolder folder = new TestFolder(this.getClass().getSimpleName());				
		Session session = profile.getSession();
		Database database = profile.getDatabase();
		YamlFile yaml = folder.getYaml("incident_load");
		JobRunner loader = yaml.getJobRunner(profile);
		Metrics loaderMetrics = loader.call();
		assertTrue(loaderMetrics.getProcessed() > 10000);
		DatabaseTimestampReader reader = new DatabaseTimestampReader(database);
		TimestampHash timestamps = reader.getTimestamps(tablename);
		logger.debug(Log.TEST, String.format("Hash size = %d", timestamps.size()));
		assertTrue(timestamps.size() > 0);
		KeySet keys = timestamps.getKeys();
		assertEquals(timestamps.size(), keys.size());
		RecordKey firstKey = keys.get(0);
		DateTime firstTimestamp = timestamps.get(firstKey);
		logger.info(Log.TEST, String.format("%s=%s", firstKey, firstTimestamp));
		BaseRecord firstRec = session.table("incident").api().getRecord(firstKey);
		logger.info(Log.TEST, String.format(
				"number=%s created=%s updated=%s", firstRec.getValue("number"), 
				firstRec.getValue("sys_created_on"), firstRec.getValue("sys_updated_on")));
		DateTime firstRecUpdated = firstRec.getUpdatedTimestamp();
		assertEquals(firstRecUpdated, firstTimestamp);
	}

}
