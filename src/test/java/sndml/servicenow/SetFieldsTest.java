package sndml.servicenow;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.FieldNames;
import sndml.servicenow.Log;
import sndml.servicenow.Record;
import sndml.servicenow.RecordListAccumulator;
import sndml.servicenow.Session;
import sndml.servicenow.Table;
import sndml.servicenow.TableReader;

public class SetFieldsTest {

	Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testSetColumns() throws Exception {
		Session session = TestManager.getDefaultProfile().getSession();
		Table incident = session.table("change_request");
		RecordListAccumulator accumulator = new RecordListAccumulator(incident);
		TableReader reader = incident.rest().getDefaultReader();
		Metrics writerMetrics = new Metrics(this.getClass().getSimpleName());
		reader.setWriter(accumulator, writerMetrics);		
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
			// assertNotNull(rec.getValue("short_description"));
			assertNull(rec.getValue("close_notes"));
			assertNull(rec.getValue("assignment_group"));
		}
		logger.info(Log.TEST, rows + " rows processed");
		
	}
	
}
