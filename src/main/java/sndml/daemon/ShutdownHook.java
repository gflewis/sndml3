package sndml.daemon;

import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConnectionProfile;
import sndml.servicenow.Log;

public class ShutdownHook extends Thread {

	final Logger logger = LoggerFactory.getLogger(ShutdownHook.class);
	final ConnectionProfile profile;
	final TimerTask dispenser;
	final ExecutorService workerPool;  // null if single threaded
	
	ShutdownHook(ConnectionProfile profile, TimerTask dispenser, ExecutorService workers) {
		this.profile = profile;
		this.dispenser = dispenser;
		this.workerPool = workers;
		this.setName("ShutdownHook");
	}
			
	@Override
	public void run() {
		Log.setGlobalContext();
		logger.info(Log.FINISH, "ShutdownHook invoked");
		if (dispenser != null) dispenser.cancel();
		if (workerPool != null) {
			logger.info(Log.FINISH, "Shutting down workerPool");
			// send interrupt to all workers
			workerPool.shutdown();
			int waitSec = profile.getPropertyInt("daemon.shutdown_seconds", 30);
			logger.info(Log.FINISH, "Awaiting worker pool termination");
			try {
				workerPool.awaitTermination(waitSec, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error(Log.FINISH, e.getMessage(), e);
			}
			if (!workerPool.isTerminated()) {
				logger.warn(Log.FINISH, "Some threads failed to terminate");
			}
		}
		logger.info(Log.FINISH, "ShutdownHook complete");
	}
	
}
