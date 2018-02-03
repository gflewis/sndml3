package servicenow.core;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;
import java.io.IOException;

@RunWith(Parameterized.class)
public class GetKeysTest {

	String profile;
    Session session;

	@Parameters(name = "{index}:{0}")
	public static String[] profiles() {
		return new String[] {"mydevjson", "mydevsoap"};
	}
     
	public GetKeysTest(String profile) {
		this.profile = profile;
		TestingManager.loadProfile(profile);
		session = new Session(TestingManager.getProperties());		
	}
	
	@Test
	public void testAllKeys() throws IOException {
		Table inc = session.table("cmn_department");
		KeySet keys = inc.getKeys();
		assertTrue("keys.size() must be greater than 0", keys.size() > 0);
	}

}
