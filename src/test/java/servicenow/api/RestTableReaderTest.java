package servicenow.api;

import org.junit.*;
import org.slf4j.Logger;

import servicenow.api.*;
import servicenow.datamart.AllTests;

import static org.junit.Assert.*;


public class RestTableReaderTest {

	Logger logger = AllTests.getLogger(this.getClass());
	static Session session;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		session = TestingManager.getSession();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSetQuery() throws Exception {
		String someDeptName = TestingManager.getProperty("some_department_name");
		boolean found = true;
		Table dept = session.table("cmn_department");
		RecordListAccumulator accumulator = new RecordListAccumulator(dept);
		TableReader reader = dept.rest().getDefaultReader();
		reader.setWriter(accumulator);
		reader.call();
		for (Record rec : accumulator.getRecords()) {
			String deptName = rec.getValue("name");
			String deptHead = rec.getValue("dept_head");
			logger.debug(deptName + "|" + deptHead);;
			if (someDeptName.equals(deptName)) found = true;			
		}
		if (!found) fail("Expected department not found");
	}

	@Test
	public void testSetDisplayValuesTrue() throws Exception {
		String someDeptName = TestingManager.getProperty("some_department_name");
		String someDeptHead = TestingManager.getProperty("some_department_head");
		Table dept = session.table("cmn_department");
		RecordListAccumulator accumulator = new RecordListAccumulator(dept);
		TableReader reader = dept.rest().getDefaultReader();
		reader.setWriter(accumulator);
		reader.setDisplayValue(true);
		reader.call();
		for (Record rec : accumulator.getRecords()) {
			String deptName = rec.getValue("name");
			String deptHead = rec.getDisplayValue("dept_head");
			logger.debug(deptName + "|" + deptHead);;
			if (someDeptName.equals(deptName)) {
				if (!someDeptHead.equals(deptHead)) 
					fail("Department head name not found");
			}			
		}
	}

	@Test
	public void testSetDisplayValuesFalse() throws Exception {
		Table dept = session.table("cmn_department");
		RecordListAccumulator accumulator = new RecordListAccumulator(dept);
		TableReader reader = dept.rest().getDefaultReader();
		reader.setWriter(accumulator);
		reader.setDisplayValue(false);
		reader.call();
		for (Record rec : accumulator.getRecords()) {
			String deptName = rec.getValue("name");
			String deptHead = rec.getDisplayValue("dept_head");
			logger.debug(deptName + "|" + deptHead);
			if (deptHead != null) fail("Department head should be null");
		}
	}
	
	
	@Test @Ignore
	public void testSetColumns() {
		fail("Not yet implemented");
	}

}
