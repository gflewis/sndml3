package servicenow.rest;

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

import servicenow.core.*;
import servicenow.datamart.Globals;

public class PartSumTableReader extends TableReader {

	final TableReaderFactory factory;
	protected TableStats stats = null;
	final int threads;
	final DateTime.Interval interval;
	DateTimeRange range;
	DatePartition partition;
	WriterMetrics processStats;
	List<TableReader> partReaders;
	List<Future<TableReader>> futures;

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public PartSumTableReader(TableReaderFactory factory, DateTime.Interval interval, Integer threads) {
		super(factory.getTable());
		this.factory = factory;
		this.interval = interval;
		this.threads = (threads == null ? 0 : threads.intValue());
		setWriter(factory.getWriter());
	}

	public int getDefaultPageSize() {
		return Globals.getInteger("rest_default_page_size", 200);
	}
	
	public DatePartition getPartition() {
		if (this.partition == null) throw new IllegalStateException();
		return this.partition;
	}
	
	public List<TableReader> getReaders() {
		return this.partReaders;
	}
	
	public int getThreadCount() {
		return this.threads;
	}

	public int numPartsTotal() {
		return futures.size();
	}
	
	public int numPartsComplete() {
		int count = 0;
		for (Future<TableReader> part : futures) {
			count += (part.isDone() ? 1 : 0);
		}
		return count;
	}

	public int numPartsIncomplete() {
		int count = 0;
		for (Future<TableReader> part : futures) {
			count += (part.isDone() ? 0 : 1);
		}
		return count;
	}
	
	@Override
	public void initialize() throws IOException {
		stats = table.rest().getStats(getQuery(), true);
		setExpected(stats.getCount());
		range = stats.getCreated();
		partition = new DatePartition(range, interval);
		partReaders = new ArrayList<TableReader>();
		logger.debug(Log.INIT, "partition=" + partition.toString());		
		for (DateTimeRange partRange : partition) {
			String partName = interval.toString().substring(0, 1) + partRange.getStart().toString();
			TableReader partReader = factory.createReader();
			partReader.setCreatedRange(partRange.intersect(partReader.getCreatedRange()));
			partReader.setReaderName(partReader.getReaderName() + "." + partName);
			// add to the beginning of the list so it will be sorted by decreasing date
			partReaders.add(0, partReader);			
		}
	}

	@Override
	public PartSumTableReader call() throws IOException, SQLException, InterruptedException {
		setLogContext();
		futures = new ArrayList<Future<TableReader>>();
		if (threads > 1) {			
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads); 
			for (TableReader reader : partReaders) {
				logger.debug("Submit " + reader.getReaderName());
				reader.initialize();
				Future<TableReader> future = executor.submit(reader);
				futures.add(future);
			}
			while (!	executor.awaitTermination(60, TimeUnit.SECONDS)) {
				logger.debug(Log.TERM, String.format("Waiting for %d / %d partitions to complete", 
						numPartsIncomplete(), numPartsTotal()));
			}
		}
		else {
			for (TableReader reader : partReaders) {
				reader.initialize();
				reader.call();
			}
		}
		return this;
	}

}
