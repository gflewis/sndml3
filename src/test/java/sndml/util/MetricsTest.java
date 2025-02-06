package sndml.util;

import org.junit.*;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testClone() throws CloneNotSupportedException {
		Metrics m1 = new Metrics("clone-test");
		m1.addInserted(17);
		assertEquals(17, m1.getInserted());
		assertEquals(0, m1.getUpdated());
		Metrics m2 = m1.clone();
		assertEquals(17, m2.getInserted());
		assertEquals(0, m2.getUpdated());		
	}

}
