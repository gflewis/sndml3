package servicenow.api;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetFieldsTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testSetColumns() throws Exception {
		Session session = TestingManager.getDefaultProfile().getSession();
		Table incident = session.table("incident");
		RecordListAccumulator accumulator = new RecordListAccumulator(incident);
		TableReader reader = incident.rest().getDefaultReader();
		reader.setWriter(accumulator);
		reader.setFields(new FieldNames("number,state,short_description"));
		reader.initialize();
		reader.call();
		int rows = accumulator.getRecords().size();
		assertTrue(rows > 0);
		for (Record rec : accumulator.getRecords()) {
			assertNotNull(rec.getValue("sys_id"));
			assertNotNull(rec.getValue("sys_created_on"));
			assertNotNull(rec.getValue("sys_updated_on"));
			assertNotNull(rec.getValue("number"));
			assertNotNull(rec.getValue("state"));
			assertNotNull(rec.getValue("short_description"));
			assertNull(rec.getValue("close_notes"));
			assertNull(rec.getValue("assignment_group"));
		}
		logger.info(Log.TEST, rows + " rows processed");
		
	}
	
}
