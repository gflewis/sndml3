package sndml.agent;

import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.ConnectionProfile;
import sndml.loader.Resources;
import sndml.util.Log;


public class ShutdownHook extends Thread {

	final Logger logger = LoggerFactory.getLogger(ShutdownHook.class);
	final ConnectionProfile profile;
	final TimerTask scanner;
	final AgentHttpServer server;
	final ExecutorService workerPool;  // null if single threaded
	final int waitSec;
	
	ShutdownHook(ConnectionProfile profile, TimerTask scanner, ExecutorService workers) {
		this.profile = profile;
		this.scanner = scanner;
		this.server = null;
		this.workerPool = workers;
		this.setName(this.getClass().getSimpleName());
		waitSec = Integer.parseInt(profile.getProperty("server.shutdown_seconds"));
	}
	
	ShutdownHook(AgentHttpServer server, Resources resources) {
		this.profile = resources.getProfile();
		this.scanner = null;
		this.server = server;
		this.workerPool = resources.getWorkerPool();
		this.setName(this.getClass().getSimpleName());
		waitSec = Integer.parseInt(profile.getProperty("server.shutdown_seconds"));
	}
	
			
	@Override
	public void run() {
		Log.setGlobalContext();
		if (scanner != null) {
			logger.info(Log.FINISH, "ShutdownHook invoked on scanner");
			scanner.cancel();
			if (workerPool != null) {
				logger.info(Log.FINISH, "Shutting down workerPool");
				workerPool.shutdown(); // wait for tasks to complete			
				logger.info(Log.FINISH, 
					String.format("Awaiting worker pool termination (%d sec)", waitSec));
				try {
					workerPool.awaitTermination(waitSec, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					logger.error(Log.FINISH, e.getMessage(), e);
				}
				if (!workerPool.isTerminated()) {
					logger.warn(Log.FINISH, "Some threads failed to terminate");
					workerPool.shutdownNow();
				}
			}
		}
		if (server != null) {
			logger.info(Log.FINISH, "ShutdownHook invoked on server");
			server.shutdown();
		}
		logger.info(Log.FINISH, "ShutdownHook complete");
	}
	
}
