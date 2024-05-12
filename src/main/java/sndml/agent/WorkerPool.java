package sndml.agent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import sndml.loader.ConnectionProfile;

/**
 * Wrapper class for a ThreadPoolExecutor. This is a singleton class.
 */
public class WorkerPool extends ThreadPoolExecutor {

	private static WorkerPool INSTANCE;
	private static final long KEEP_ALIVE_SECONDS = 60;	

	public WorkerPool(ConnectionProfile profile) {
		super(
			profile.getThreadCount(), 
			profile.getThreadCount(),
			KEEP_ALIVE_SECONDS,
			TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());
		INSTANCE = this;
	}
		
	@Deprecated
	public WorkerPool getWorkerPool() {
		assert INSTANCE != null: "Class not initialized";
		return INSTANCE;
	}
	
}
