package sndml.agent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.ConnectionProfile;
import sndml.util.Log;

/**
 * Wrapper class for a ThreadPoolExecutor. This is a singleton class.
 */
public class WorkerPool extends ThreadPoolExecutor {

	private static WorkerPool INSTANCE;
	private static final long KEEP_ALIVE_SECONDS = 60;
	private static final Logger logger = LoggerFactory.getLogger(WorkerPool.class);

	private final int threadCount; 
	
	public WorkerPool(ConnectionProfile profile) {
		this(profile.getThreadCount());
	}
	
	public WorkerPool(int threadCount) {
		super(
			threadCount, 
			threadCount,
			KEEP_ALIVE_SECONDS,
			TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());
		INSTANCE = this;
		this.threadCount = threadCount;
		logger.info(
			Log.INIT, String.format("instantiate threads=%d", threadCount));
	}
		
	static public WorkerPool getWorkerPool(ConnectionProfile profile) {
		if (INSTANCE == null) {
			INSTANCE = new WorkerPool(profile);
		}
		return INSTANCE;		
	}
	
	public int getThreadCount() {
		return threadCount;
	}
	
	@Deprecated
	public WorkerPool getWorkerPool() {
		assert INSTANCE != null: "Class not initialized";
		return INSTANCE;
	}
	
}
