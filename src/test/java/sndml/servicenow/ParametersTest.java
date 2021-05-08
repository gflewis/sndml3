package sndml.servicenow;

import static org.junit.Assert.*;

import org.junit.Test;

public class ParametersTest {

	
	@Test
	public void testCreate() {
		Parameters p1 = new Parameters();
		p1.add("animal", "giraffe");
		assertEquals("giraffe", p1.get("animal"));
		Parameters p2 = new Parameters(p1);
		p1.add("animal", "lion");
		assertEquals("lion", p1.get("animal"));
		assertEquals("giraffe", p2.get("animal"));
	}
}
