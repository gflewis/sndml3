package sndml.servicenow;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

public class InstanceTest {

	@Test
	public void testInstance() throws URISyntaxException {
		Instance instance = new Instance("dev12345");
		URI uri1 =  instance.getURI("incident.do");
		assertEquals("https://dev12345.service-now.com/incident.do", uri1.toString());
		Parameters parms2 = new Parameters();
		parms2.add("sysparm_query", "nameINJohn Doe,Mary Smith");
		URI uri2 = instance.getURI("incident.do", parms2);
		assertEquals("https://dev12345.service-now.com/incident.do?sysparm_query=nameINJohn+Doe%2CMary+Smith", uri2.toString());
	}
	
	@Test(expected = AssertionError.class)
	public void testBadInstance1() throws Exception {
		new Instance("dev 1234");
	}

	@Test(expected = AssertionError.class)
	public void testBadInstance2() throws Exception {
		new Instance("");
	}

}
