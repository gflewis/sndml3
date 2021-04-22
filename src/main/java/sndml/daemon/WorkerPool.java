package sndml.daemon;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerPool extends ThreadPoolExecutor {

	private static final long KEEP_ALIVE_SECONDS = 60;
	
	@SuppressWarnings("unused")
	private final AgentDaemon daemon; 
	
	public WorkerPool(AgentDaemon daemon, int threadCount) {	
		super(threadCount, threadCount, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, newWorkQueue());
		this.daemon = daemon;
	}
	
	static LinkedBlockingQueue<Runnable> newWorkQueue() {
		return new LinkedBlockingQueue<Runnable>(); 
	}
		
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		// This is unnecessary since rescan is called from AppJobRunner.call()
		// daemon.rescan();
	}
	
}
