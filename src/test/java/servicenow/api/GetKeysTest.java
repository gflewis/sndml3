package servicenow.api;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import servicenow.api.KeySet;
import servicenow.api.Session;
import servicenow.api.Table;

import static org.junit.Assert.*;
import java.io.IOException;

@RunWith(Parameterized.class)
public class GetKeysTest {

	@Parameters(name = "{index}:{0}")
	public static TestingProfile[] profiles() {
		return TestingManager.getProfiles("mydevsoap mydevjson");
	}
	
	TestingProfile profile;
		     
	public GetKeysTest(TestingProfile profile) {
		this.profile = profile;
	}
	
	@Test
	public void testAllKeys() throws IOException {
		Session session = profile.getSession();
		Table inc = session.table("cmn_department");
		KeySet keys = (profile.getName().contains("soap")) ? 
			inc.soap().getKeys() :
			inc.json().getKeys();
		assertNotNull(keys);
		assertTrue("keys.size() must be greater than 0", keys.size() > 0);
	}

}
