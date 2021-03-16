package sndml.datamart;

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

import sndml.servicenow.DateTimeRange;
import sndml.servicenow.EncodedQuery;
import sndml.servicenow.Log;
import sndml.servicenow.ProgressLogger;
import sndml.servicenow.ReaderMetrics;
import sndml.servicenow.TableReader;
import sndml.servicenow.TableStats;
import sndml.servicenow.WriterMetrics;

public final class DatePartitionedTableReader extends TableReader {

	final TableReaderFactory factory;	
	final int threads;
	final Interval interval;
	// A normal TableReader does not have WriterMetrics
	// but a DatePartitionedTable reader does
	// because it needs to accumulate the metrics of its children
	final WriterMetrics writerMetrics;
	
	private DateTimeRange range;
	private DatePartition partition;
	private List<Future<WriterMetrics>> futures;

	final int REST_DEFAULT_PAGE_SIZE = 200;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public DatePartitionedTableReader(
			TableReaderFactory factory, String readerName, 
			Interval interval, Integer threads) {
		super(factory.getTable());
		this.factory = factory;
		this.setReaderName(readerName);
		setQuery(factory.getFilter());
		setCreatedRange(factory.getCreatedRange());
		setUpdatedRange(factory.getUpdatedRange());
		setFields(factory.getFieldNames());
		setPageSize(factory.getPageSize());
		this.interval = interval;
		this.threads = (threads == null ? 0 : threads.intValue());
		this.writerMetrics = new WriterMetrics();
		this.writerMetrics.setName(readerName);
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
		for (Future<WriterMetrics> part : futures) {
			count += (part.isDone() ? 1 : 0);
		}
		return count;
	}

	private int numPartsIncomplete() {
		assert futures != null;
		int count = 0;
		for (Future<WriterMetrics> part : futures) {
			count += (part.isDone() ? 0 : 1);
		}
		return count;
	}
	
	@Override
	public void initialize() throws IOException, InterruptedException {
		super.beginInitialize();
		// Use Stats API to determine min and max dates
		EncodedQuery query = getQuery();
		logger.debug(Log.INIT, String.format("initialize query=\"%s\"", query));
		TableStats stats = table.rest().getStats(query, true);
		Integer expected = stats.getCount();
		logger.debug(Log.INIT, String.format("expected=%d", expected));	
		if (expected == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; no readers created");
			return;
		}
		range = stats.getCreated();
		assert range.getStart() != null;
		assert range.getEnd() != null;
		this.partition = new DatePartition(range, interval);
		super.endInitialize(expected);
	}
	
	private TableReader createReader(DatePart partRange) {
		assert this.readerName != null;
		String newPartName = DatePart.getName(interval, partRange.getStart());
		TableReader partReader = factory.createReader();
		logger.debug(Log.INIT, String.format("partReader.readerName=%s", partReader.getReaderName()));		
		factory.configure(partReader);		
		assert partReader.hasParent();
		assert partReader.getParent() == this;
		partReader.setCreatedRange(partRange.intersect(partReader.getCreatedRange()));
		partReader.setPartName(newPartName);
		partReader.setReaderName(this.readerName + "." + newPartName);
		ReaderMetrics partReaderMetrics = partReader.getReaderMetrics();
		WriterMetrics partWriterMetrics = partReader.getWriterMetrics();
		logger.debug(Log.INIT, String.format("partWriterMetrics.name(1)=%s", partWriterMetrics.getName()));
		assert partWriterMetrics.getName() == null;
		assert partReaderMetrics != this.getReaderMetrics();
		assert partWriterMetrics != this.getWriterMetrics();
		partReaderMetrics.setParent(this.readerMetrics);		
		partWriterMetrics.setParent(this.writerMetrics);
		logger.debug(Log.INIT, String.format("partWriterMetrics.name(2)=%s", partWriterMetrics.getName()));
		partWriterMetrics.setName(newPartName);		
		assert partWriterMetrics.hasParent();
		assert partWriterMetrics.getParent() == this.writerMetrics;
		ProgressLogger partLogger = progressLogger.newPartLogger(partReader, partRange);
		assert partLogger.getReader() == partReader;
		assert partLogger.hasPart();
		partReader.setProgressLogger(partLogger);
		return partReader;		
	}

	@Override
	public WriterMetrics call() throws IOException, SQLException, InterruptedException {
		logStart();
		if (getExpected() == 0) {
			logger.debug(Log.PROCESS, "expecting 0 rows; bypassing call");
			return writerMetrics;
		}
		if (threads > 1) {			
			futures = new ArrayList<Future<WriterMetrics>>();
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads);
			for (DatePart partRange : partition) {
				TableReader reader = createReader(partRange);
				logger.debug("Submit " + reader.getReaderName());
				reader.initialize();
				Future<WriterMetrics> future = executor.submit(reader);
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
		return writerMetrics;
	}

}
