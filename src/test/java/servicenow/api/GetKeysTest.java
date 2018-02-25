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

	String profile;
    Session session;

	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
		return new String[] {"mydevsoap", "mydevjson"};
	}
     
	public GetKeysTest(String profile) {
		this.profile = profile;
		TestingManager.loadProfile(profile);
		session = new Session(TestingManager.getProperties());		
	}
	
	@Test
	public void testAllKeys() throws IOException {
		Table inc = session.table("cmn_department");
		KeySet keys = null;
		if (profile.equals("mydevsoap")) keys = inc.soap().getKeys();
		if (profile.equals("mydevjson")) keys = inc.json().getKeys();
		assertNotNull(keys);
		assertTrue("keys.size() must be greater than 0", keys.size() > 0);
	}

}
