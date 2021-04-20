package sndml.daemon;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerPool extends ThreadPoolExecutor {

	private static final long keepAliveSeconds = 60;
	
	private final AgentDaemon daemon; 
	
	public WorkerPool(AgentDaemon daemon, int threadCount) {	
		super(threadCount, threadCount, keepAliveSeconds, TimeUnit.SECONDS, newWorkQueue());
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
