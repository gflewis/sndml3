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

public class MultiDatePartReader extends RestTableReader {

	final int threads;
	final DateTime.Interval interval;
	DateTimeRange range;
	DatePartition partition;
	WriterMetrics processStats;
	List<DatePartReader> readers = new ArrayList<DatePartReader>();
	List<Future<TableReader>> futures = new ArrayList<Future<TableReader>>();

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public MultiDatePartReader(
			Table table, 
			DateTime.Interval interval, 
			EncodedQuery baseQuery,
			DateTimeRange createdRange,
			DateTimeRange updatedRange,
			Integer threads,
			Writer writer) {
		super(table);
		this.setBaseQuery(baseQuery);
		this.setCreatedRange(createdRange);
		this.setUpdatedRange(updatedRange);
		this.setWriter(writer);
		this.interval = interval;
		this.threads = (threads == null ? 0 : threads.intValue());
	}
		
	public DatePartition getPartition() {
		if (this.partition == null) throw new IllegalStateException();
		return this.partition;
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
		getWriter().setReader(this);
		stats = this.apiREST.getStats(getQuery(), true);
		setExpected(stats.getCount());
		range = stats.getCreated();
		partition = new DatePartition(range, interval);
		logger.debug(Log.INIT, "partition=" + partition.toString());		
		for (DateTimeRange partRange : partition) {
			String partName = interval.toString().substring(0, 1) + partRange.getStart().toString();
			DatePartReader reader = new DatePartReader(this, partName, partRange);
			// add to the beginning of the list so it will be sorted by decreasing date
			readers.add(0, reader);			
		}
	}

	@Override
	public MultiDatePartReader call() throws IOException, SQLException, InterruptedException {
		if (threads > 1) {			
			logger.info(Log.INIT, String.format("starting %d threads", threads));			
			ExecutorService executor = Executors.newFixedThreadPool(this.threads); 
			for (DatePartReader reader : readers) {
				logger.debug("Submit " + reader.getName());
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
			for (TableReader reader : readers) {
				reader.initialize();
				reader.call();
			}
		}
		return this;
	}
	
}
