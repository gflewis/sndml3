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
		this.writerMetrics.setParent(factory.getWriter().getWriterMetrics());
		setWriter(factory.getWriter());
	}

	public int getDefaultPageSize() {
		return REST_DEFAULT_PAGE_SIZE;
	}
	
	public DatePartition getPartition() {
		assert partition != null : "Not initialized";
		return partition;
	}
		
	@SuppressWarnings("unused")
	private int getThreadCount() {
		return this.threads;
	}

	@Override
	public WriterMetrics getWriterMetrics() {
		return this.writerMetrics;
	}

	private int numPartsTotal() {
		assert futures != null;
		return futures.size();
	}
		
	@SuppressWarnings("unused")
	private int numPartsComplete() {
		assert futures != null;
		int count = 0;
		for (Future<TableReader> part : futures) {
			count += (part.isDone() ? 1 : 0);
		}
		return count;
	}

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
		beginInitialize();
		// Use Stats API to determine min and max dates
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, String.format("initialize query=\"%s\"", query));
		TableStats stats = table.rest().getStats(query, true);
		Integer expected = stats.getCount();
		endInitialize(expected);
		logger.debug(Log.INIT, String.format("expected=%d", expected));	
		if (expected == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; no readers created");
			return;
		}
		range = stats.getCreated();
		assert range.getStart() != null;
		assert range.getEnd() != null;
		partition = new DatePartition(range, interval);
	}
	
	private TableReader createReader(DatePart partRange) {
		String partName = DatePart.getName(interval, partRange.getStart());
		TableReader partReader = factory.createReader();
		String parentName = partReader.getPartName();
		assert parentName != null;		
		String metricsName = parentName + "." + partName;
		partReader.setCreatedRange(partRange.intersect(partReader.getCreatedRange()));
		partReader.setPartName(partName);
		partReader.setReaderName(partReader.getReaderName() + "." + partName);
		partReader.setParent(this);
		ReaderMetrics readerMetrics = partReader.getReaderMetrics();
		WriterMetrics writerMetrics = partReader.getWriterMetrics();
		readerMetrics.setParent(this.readerMetrics);		
		writerMetrics.setParent(this.writerMetrics);
		writerMetrics.setName(metricsName);		
		ProgressLogger partLogger = progressLogger.newPartLogger(partReader, partRange);
		assert partLogger.reader == partReader;
		assert partLogger.hasPart();
		partReader.setProgressLogger(partLogger);
		return partReader;		
	}

	@Override
	public DatePartitionedTableReader call() throws IOException, SQLException, InterruptedException {
		logStart();
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing call");
			return this;
		}
		if (threads > 1) {			
			futures = new ArrayList<Future<TableReader>>();
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads);
			for (DatePart partRange : partition) {
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
			for (DatePart partRange : partition) {
				TableReader reader = createReader(partRange);
				reader.initialize();
				reader.call();
			}
		}
		logComplete();
		// Free resources
		futures = null;
		partition = null;
		return this;
	}

}
