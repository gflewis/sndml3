package sndml.service;

import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConnectionProfile;
import sndml.servicenow.Log;


public class Daemon implements org.apache.commons.daemon.Daemon {

	static Logger logger = LoggerFactory.getLogger(Daemon.class);
		
	private final ConnectionProfile profile;
	private ExecutorService workerPool = null;
	private int intervalSeconds;
	private int threadCount;

	private Timer timer;
	private Scanner scanner;
	
	public Daemon(ConnectionProfile profile) {
		this.profile = profile;
	}
	
	public void run() throws Exception {
		start();
		while (!workerPool.isTerminated()) {
			logger.info("main awaiting threadpool termination");
			workerPool.awaitTermination(120, TimeUnit.SECONDS);
		}
		stop();	
	}
			
	@Override
	public void init(DaemonContext context) throws DaemonInitException, Exception {
		logger.info(Log.INIT, "begin init");		
	}

	@Override
	public void start() throws Exception {
		logger.info(Log.INIT, "begin start");
				
		threadCount = profile.getPropertyInt("daemon.threads", 3);
		intervalSeconds = profile.getPropertyInt("daemon.interval_seconds", 20);
		assert threadCount > 0;
		assert intervalSeconds > 0;
				
		workerPool = Executors.newFixedThreadPool(threadCount);
        this.timer = new Timer("scanner", true);
        scanner = new Scanner(profile);
        
		ShutdownHook shutdownHook = new ShutdownHook(profile, scanner, workerPool);
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		
        timer.schedule(scanner, 0, 1000 * intervalSeconds);
		logger.info(Log.INIT,"end start");		
	}
	
	@Override
	public void stop() {
		logger.info(Log.FINISH, "begin stop");
		int waitSec = profile.getPropertyInt("shutdown_seconds", 30);
		boolean terminated = false;
		// shutdownNow will send an interrupt to all threads
		workerPool.shutdownNow();
		try {
			terminated = 
				workerPool.awaitTermination(waitSec, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			terminated = false;
		}
		if (terminated) {
			logger.info("Shutdown Successful");
		}
		else {
			logger.warn("Some threads failed to terminate");
		}
		logger.info("end stop");		
	}
	
	@Override
	public void destroy() {
		
	}


}
