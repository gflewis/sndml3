package sndml.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import sndml.loader.ConnectionProfile;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.Metrics;

/**
 * Wrapper class for a ThreadPoolExecutor. 
 */
public class WorkerPool {

//	private static final int CORE_POOL_SIZE = 0;
	private static final long KEEP_ALIVE_SECONDS = 60;
	
	private static final Logger logger = Log.getLogger(WorkerPool.class);

	private final int threadCount;
	private final int backlog;
	private final int shutdownSeconds;
	
	final BlockingQueue<Runnable> queue;
	final ThreadPoolExecutor executor;
			
	final List<WorkerEntry> jobList = Collections.synchronizedList(new LinkedList<WorkerEntry>());
	
	public WorkerPool(ConnectionProfile profile) {
		this.threadCount = profile.getInteger("agent.workers");
		this.backlog = profile.getInteger("agent.backlog");
		this.shutdownSeconds = profile.getInteger("agent.shutdown_seconds");
		
		this.queue = new LinkedBlockingQueue<Runnable>(backlog);
		this.executor = new ThreadPoolExecutor(
				threadCount, threadCount, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, queue);
	}
			
	int getThreadCount() {
		return threadCount;
	}
	
	private String getAgentName() {
		return AgentMain.getAgentName();
	}
	
	synchronized public Future<Metrics> submit(AppJobRunner runner) {
		logger.info(Log.INIT, "submit " + runner.getNumber());
		this.cleanup();
		if (jobList.size() > 0 && logger.isDebugEnabled()) dumpJobList();
		Future<Metrics> future = executor.submit((Callable<Metrics>) runner);
		WorkerEntry entry = new WorkerEntry(runner, future);
		jobList.add(entry);
		return future;
	}

	synchronized boolean isActive(WorkerEntry entry) {
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
	 * Print the content of jobList. Used for debugging.
	 */
	synchronized public void dumpJobList() {
		logger.info(Log.PROCESS, String.format("%d active jobs", jobList.size()));
		Iterator<WorkerEntry> iter = jobList.iterator();
		int count = 0;
		while (iter.hasNext()) {
			WorkerEntry entry = iter.next();
			Future<Metrics> future = entry.future;
			boolean isQueued = queue.contains((Runnable) entry.runner);
			boolean isCancelled = future.isCancelled();
			boolean isDone = future.isDone();
			logger.info(Log.PROCESS, String.format(
				"%d %s queued=%b cancelled=%b done=%b", 
				++count, entry.number, isQueued, isCancelled, isDone));
		}		
	}
	
	public void shutdown() {
		Log.setJobContext(getAgentName());
		logger.debug(Log.FINISH, "Begin stop");
		// shutdownNow will send an interrupt to all threads
		executor.shutdown();
		try { 
			executor.awaitTermination(shutdownSeconds, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) { 
			logger.warn("Shutdown interrupted");
		};
		if (!executor.isTerminated()) {
			logger.warn("Some threads failed to terminate");
		}
		logger.info(Log.FINISH, "End stop");
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


}
