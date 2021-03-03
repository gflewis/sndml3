package sndml.servicenow;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatePartitionedTableReader extends TableReader {

	final TableReaderFactory factory;
	final int threads;
	final Interval interval;
	final WriterMetrics writerMetrics;
	
	DateTimeRange range;
	DatePartition partition;
	List<Future<TableReader>> futures;

	final int REST_DEFAULT_PAGE_SIZE = 200;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public DatePartitionedTableReader(TableReaderFactory factory, Interval interval, Integer threads) {
		super(factory.getTable());
		this.factory = factory;
		setQuery(factory.filter);
		setCreatedRange(factory.createdRange);
		setUpdatedRange(factory.updatedRange);
		setFields(factory.fieldNames);
		setPageSize(factory.pageSize);
		this.interval = interval;
		this.threads = (threads == null ? 0 : threads.intValue());
		this.writerMetrics = new WriterMetrics();
		setWriter(factory.getWriter());
	}

	public int getDefaultPageSize() {
		return REST_DEFAULT_PAGE_SIZE;
	}
	
	public DatePartition getPartition() {
		assert partition != null : "Not initialized";
		return partition;
	}
		
//	private int getThreadCount() {
//		return this.threads;
//	}

	@Override
	public WriterMetrics getWriterMetrics() {
		return this.writerMetrics;
	}

	private int numPartsTotal() {
		assert futures != null;
		return futures.size();
	}
		
//	private int numPartsComplete() {
//		assert futures != null;
//		int count = 0;
//		for (Future<TableReader> part : futures) {
//			count += (part.isDone() ? 1 : 0);
//		}
//		return count;
//	}

	private int numPartsIncomplete() {
		assert futures != null;
		int count = 0;
		for (Future<TableReader> part : futures) {
			count += (part.isDone() ? 0 : 1);
		}
		return count;
	}
	
	@Override
	public void initialize() throws IOException, InterruptedException {
		try {
			super.initialize();
		} catch (SQLException e) {
			// Impossible. 
			// Only a Syncronizer can throw SQLException during initialization.
			throw new AssertionError(e);
		}
		// Use Stats API to determine min and max dates
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, String.format("initialize query=\"%s\"", query));
		TableStats stats = table.rest().getStats(query, true);
		setExpected(stats.getCount());
		logger.debug(Log.INIT, String.format("expected=%d", getExpected()));	
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; no readers created");
			return;
		}
		range = stats.getCreated();
		assert range.getStart() != null;
		assert range.getEnd() != null;
		partition = new DatePartition(range, interval);
	}
	
	private TableReader createReader(DateTimeRange partRange) {
		String partName = DatePart.getName(interval, partRange.getStart());
		TableReader partReader = factory.createReader();
		partReader.setCreatedRange(partRange.intersect(partReader.getCreatedRange()));
		partReader.setPartName(partName);
		partReader.setReaderName(partReader.getReaderName() + "." + partName);
		partReader.setParent(this);
		partReader.getReaderMetrics().setParent(this.readerMetrics);
		partReader.getWriterMetrics().setParent(this.writerMetrics);
		assert this.progressLogger != null;
		partReader.setProgressLogger(this.progressLogger);
		return partReader;		
	}

	@Override
	public DatePartitionedTableReader call() throws IOException, SQLException, InterruptedException {
		setLogContext();
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing call");
			return this;
		}
		if (threads > 1) {			
			futures = new ArrayList<Future<TableReader>>();
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads);
			for (DateTimeRange partRange : partition) {
				TableReader reader = createReader(partRange);
				logger.debug("Submit " + reader.getReaderName());
				reader.initialize();
				Future<TableReader> future = executor.submit(reader);
				futures.add(future);				
			}
			executor.shutdown();
			while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				logger.info(Log.FINISH, String.format("Waiting for %d / %d partitions to complete", 
						numPartsIncomplete(), numPartsTotal()));
			}
		}
		else {
			for (DateTimeRange partRange : partition) {
				TableReader reader = createReader(partRange);
				reader.initialize();
				reader.call();
			}
		}
		// Free resources
		futures = null;
		partition = null;
		return this;
	}

}
