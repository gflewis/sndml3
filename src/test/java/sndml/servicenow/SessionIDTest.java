package sndml.servicenow;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class SessionIDTest {

	@Test
	public void testSession() throws Exception {
		Session session = TestManager.getDefaultProfile().getSession();
		TableAPI location = session.table("cmn_location").api();
		location.getRecord("name", TestManager.getProperty("location1"));;
		String session1 = session.getSessionID();
		System.out.println("JSESSIONID=" + session1);
		location.getRecord("name", TestManager.getProperty("location2"));
		String session2 = session.getSessionID();
		System.out.println("JSESSIONID=" + session2);
		location.getRecord("name", TestManager.getProperty("location3"));
		String session3 = session.getSessionID();
		System.out.println("JSESSIONID=" + session3);
		assertEquals(session1, session2);
		assertEquals(session2, session3);
	}

}
