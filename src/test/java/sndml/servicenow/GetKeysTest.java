package sndml.servicenow;

import org.slf4j.Logger;

import static org.junit.Assert.*;

import org.junit.Test;

public class GetKeysTest {

	final TestingProfile profile;
	final Logger logger = TestManager.getLogger(this.getClass());
				     
	public GetKeysTest() {
		this.profile = TestManager.getDefaultProfile();
	}

	@Test
	public void test() throws Exception {
		Session session = profile.getSession();
		Table table = session.table("sys_user");
		logger.info(Log.TEST, "Calling getStats");
		TableStats stats = table.rest().getStats(null, false);
		Integer numRecs = stats.getCount();
		assertTrue(numRecs > 200);
//		assertTrue(numRecs > 20000);		
		KeySetTableReader reader = new KeySetTableReader(table);
		logger.info(Log.TEST, "Calling initialize");
		Metrics metrics = new Metrics(null, null);
		reader.prepare(new NullWriter(), metrics, new NullProgressLogger());
		Integer numKeys = reader.getExpected();
		logger.info(Log.TEST, String.format("Count=%d Keys=%d",  numRecs, numKeys));
		assertEquals(numRecs, numKeys);
	}
	
}
