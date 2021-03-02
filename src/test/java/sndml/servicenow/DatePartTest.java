package sndml.servicenow;

import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.DatePartitionedTableReader;
import sndml.servicenow.Log;
import sndml.servicenow.RecordListAccumulator;
import sndml.servicenow.RestTableReaderFactory;
import sndml.servicenow.Session;
import sndml.servicenow.Table;
import sndml.servicenow.TableReaderFactory;
import sndml.servicenow.TableStats;

public class DatePartTest {

	final private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void testDatePart() throws Exception {
		Session session = TestManager.getDefaultProfile().getSession();
		Table table = session.table("incident");
		RecordListAccumulator accumulator = new RecordListAccumulator(table);
		TableStats stats = table.rest().getStats(null, true);
		TableReaderFactory factory = new RestTableReaderFactory(table, accumulator);
		String parentName = "my_parent_reader";
		factory.setReaderName(parentName);
		logger.info(Log.TEST, "range=" + stats.getCreated().toString());
		Interval interval = Interval.MONTH;
		DatePartitionedTableReader parentReader = new DatePartitionedTableReader(factory, interval, 0);
		DatePartition partition;
		parentReader.initialize();
		partition = parentReader.getPartition();
		assertNotNull(partition);
		// TODO Implement me
//		fail("Not yet implemented");
//		List<TableReader> partReaders = parentReader.getReaders();
//		logger.info(Log.TEST, String.format("readers=%d", partReaders.size()));
//		String childName = partReaders.get(0).getReaderName();
//		logger.info(Log.TEST, partReaders.get(0).getReaderName());
//		assertTrue(childName.startsWith(parentName + ".M"));		
	}

}
