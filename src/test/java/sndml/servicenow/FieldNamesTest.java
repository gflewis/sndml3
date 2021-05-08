package sndml.servicenow;

import static org.junit.Assert.*;

import org.junit.Test;

public class FieldNamesTest {

	@Test
	public void testCommas() {
		FieldNames f1 = new FieldNames("alpha,beta,gamma");
		assertTrue(f1.contains("beta"));
		assertTrue(f1.contains("alpha"));
		assertFalse(f1.contains("delta"));
	}
	
	@Test
	public void testSpaces() {
		FieldNames f1 = new FieldNames("alpha  beta gamma");
		assertTrue(f1.contains("beta"));
		assertTrue(f1.contains("alpha"));
		assertFalse(f1.contains("delta"));		
	}

	@Test
	public void testMixture() {
		FieldNames f1 = new FieldNames("alpha, beta, gamma");
		assertTrue(f1.contains("beta"));
		assertTrue(f1.contains("alpha"));
		assertFalse(f1.contains("delta"));
	}
	
	@Test
	public void testMultiLine() {
		String s1 = "alpha\n beta\ngamma";
		FieldNames f1 = new FieldNames(s1);
		assertEquals(3, f1.size());
		assertTrue(f1.contains("beta"));
		assertTrue(f1.contains("alpha"));
		assertFalse(f1.contains("delta"));
	}
	
}
