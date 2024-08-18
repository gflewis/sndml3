package sndml.agent;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConnectionProfile;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.Metrics;

/**
 * Wrapper class for a ThreadPoolExecutor. 
 */
public class WorkerPool extends ThreadPoolExecutor {

	private static final int CORE_POOL_SIZE = 0;
	private static final long KEEP_ALIVE_SECONDS = 60;
	
	private static final Logger logger = Log.getLogger(WorkerPool.class);

	private final int threadCount;
			
	final List<WorkerEntry> jobList = Collections.synchronizedList(new LinkedList<WorkerEntry>());
	
	public WorkerPool(ConnectionProfile profile) {
		this(profile.getThreadCount());
	}
	
	public WorkerPool(int threadCount) {
		super(
			CORE_POOL_SIZE, threadCount, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());
		this.threadCount = threadCount;
		logger.info(
			Log.INIT, String.format("instantiate threads=%d", threadCount));
	}
		
	int getThreadCount() {
		return threadCount;
	}
	
	synchronized public Future<Metrics> submit(AppJobRunner runner) {
		logger.info(Log.INIT, "submit " + runner.getNumber());
		this.cleanup();
		if (jobList.size() > 0 && logger.isDebugEnabled()) dumpJobList();
		Future<Metrics> future = super.submit((Callable<Metrics>) runner);
		WorkerEntry entry = new WorkerEntry(runner, future);
		jobList.add(entry);
		return future;
	}

	boolean isActive(WorkerEntry entry) {
		Future<Metrics> future = entry.future;
		return (future.isDone()) ? false : true;		
	}
	
	/**
	 * Returns a list of all active entries
	 */
	synchronized ArrayList<WorkerEntry> activeTasks() {
		ArrayList<WorkerEntry> result = new ArrayList<WorkerEntry>();
		Iterator<WorkerEntry> iter = jobList.iterator();
		while (iter.hasNext()) {
			WorkerEntry entry = iter.next();
			if (isActive(entry)) result.add(entry);			
		}		
		return result;
	}
	
	/**
	 * Print the content of jobList. Used for debugging.
	 */
	synchronized private void dumpJobList() {
		logger.info(Log.PROCESS, String.format("%d active jobs", jobList.size()));
		Iterator<WorkerEntry> iter = jobList.iterator();
		int count = 0;
		while (iter.hasNext()) {
			WorkerEntry entry = iter.next();
			Future<Metrics> future = entry.future;
			logger.info(Log.PROCESS, String.format(
				"%d %s cancelled=%b done=%b", 
				++count, entry.number, future.isCancelled(), future.isDone()));
		}		
	}
	
	synchronized int activeTaskCount() {
		int count = 0;
		Iterator<WorkerEntry> iter = jobList.iterator();
		while (iter.hasNext()) {
			WorkerEntry entry = iter.next();
			if (isActive(entry)) count += 1; 			
		}
		return count;				
	}
	
	/**
	 * Interrupt a single job
	 */
	// TODO finish cancelJob
	synchronized void cancelJob(RecordKey jobKey) {
		int count = 0;
		Iterator<WorkerEntry> iter = jobList.iterator();
		while (iter.hasNext()) {
			WorkerEntry entry = iter.next();
			Future<Metrics> future = entry.future;
			AppJobConfig config = entry.config;
			if (config.getRunKey().equals(jobKey)) {
				if (future.isCancelled() || future.isDone()) {
					logger.info(Log.PROCESS, String.format("%s not active", config.getName()));
				}
				else {
					logger.info(Log.FINISH, String.format("Cancel %s", entry.config.getName()));
					future.cancel(true);
				}
				count += 1;
				
			}
		} 
		if (count > 0)
			logger.info(Log.FINISH, String.format("%d job(s) cancelled", count));
		else 
			logger.info(Log.FINISH, String.format("%s not found", jobKey.toString()));	
	}
		
	/**
	 * Remove finished or cancelled tasks from list
	 */
	synchronized void cleanup() {
		Iterator<WorkerEntry> iter = jobList.iterator();
		while (iter.hasNext()) {
			WorkerEntry entry = iter.next();
			Future<Metrics> future = entry.future;
			if (future.isDone() || future.isCancelled()) {
				logger.info(Log.PROCESS, String.format("cleanup: remove %s", entry.number));
				iter.remove();
			}
		}		
	}

	@SuppressWarnings("unused")
	private void setRunStatus(AppSession appSession, RecordKey runKey, AppJobStatus status, String message) {
		logger.warn(Log.FINISH, String.format(
			"setRunStatus %s %s", runKey.toString(), status.toString()));
		URI uriPutJobRun = appSession.uriPutJobRunStatus(runKey);
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("runkey", runKey.toString());		
		body.put("status", status.toString().toLowerCase());
		if (message != null) body.put("message",  message);
		JsonRequest request = new JsonRequest(appSession, uriPutJobRun, HttpMethod.PUT, body, runKey);		
		try {
			request.execute();
		} catch (IOException e1) {
			logger.warn(Log.FINISH, "setRunStatus: " + e1.getMessage());
		}
	}

}
