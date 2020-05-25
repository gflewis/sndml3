package servicenow.api;

import org.slf4j.Logger;

import static org.junit.Assert.*;

import org.junit.Test;

public class GetKeysTest {

	final TestingProfile profile;
	final Logger logger = TestingManager.getLogger(this.getClass());
				     
	public GetKeysTest() {
		this.profile = TestingManager.getDefaultProfile();
	}

	@Test
	public void test() throws Exception {
		Session session = profile.getSession();
		Table table = session.table("cmdb_ci");
		SoapTableAPI apiSoap = new SoapTableAPI(table);
		JsonTableAPI apiJson = new JsonTableAPI(table);
		RestTableAPI apiRest = new RestTableAPI(table);
		TableStats stats = apiRest.getStats(null, false);
		int numRecs = stats.getCount();
		KeySet ksSoap = apiSoap.getKeys();
		assertEquals(numRecs, ksSoap.size());
		KeySet ksJson = apiJson.getKeys();
		assertEquals(numRecs, ksJson.size());		
	}
	
}
