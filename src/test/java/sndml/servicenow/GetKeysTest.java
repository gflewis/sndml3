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
		Table table = session.table("incident");
		TableStats stats = table.rest().getStats(null, false);
		Integer numRecs = stats.getCount();
		assertTrue(numRecs > 20000);		
		KeySetTableReader reader = new KeySetTableReader(table);
		reader.initialize();
		Integer numKeys = reader.getExpected();
		logger.info(Log.TEST, String.format("Count=%d Keys=%d",  numRecs, numKeys));
		assertEquals(numRecs, numKeys);
	}
	
}
