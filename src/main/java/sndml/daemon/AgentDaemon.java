package sndml.daemon;

import java.util.Timer;
import java.util.concurrent.*;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConnectionProfile;
import sndml.servicenow.Log;

public class AgentDaemon implements Daemon {

	static Logger logger = LoggerFactory.getLogger(AgentDaemon.class);
		
	private static AgentDaemon daemon;

	private static Thread daemonThread;

	private final ConnectionProfile profile;
	private final String agentName;
	private final AgentScanner scanner;
	private final int threadCount;	
	private final int intervalSeconds;
	private final WorkerPool workerPool; // null if threadCount < 2
	
	private static volatile boolean isRunning = false;
	
	private Timer timer;
	
	public AgentDaemon(ConnectionProfile profile) {
		if (daemon != null) throw new AssertionError("Daemon already instantiated");
        daemon = this;
		daemonThread = Thread.currentThread();
		this.profile = profile;
		this.agentName = profile.getProperty("daemon.agent", "main");
		this.threadCount = profile.getPropertyInt("daemon.threads", 3);
		this.intervalSeconds = profile.getPropertyInt("daemon.interval", 60);
		assert intervalSeconds > 0;
		this.workerPool = threadCount > 1 ? new WorkerPool(this, threadCount) : null;		
        this.scanner = new AgentScanner(profile, workerPool);
		Log.setJobContext(agentName);
	}
	
	public static AgentDaemon getDaemon() {
		return daemon;		
	}
	
	public static String getAgentName() {
		return getDaemon().agentName;
	}
	
	/**
	 * Return the Daemon thread, which is the main thread.
	 */
	public static Thread getThread() {
		return daemonThread;
	}
	
	public static ConnectionProfile getConnectionProfile() {
		return getDaemon().profile;
	}
	
	/**
	 * This function can be called by any thread to abort the daemon.
	 */
	public static void abort() {
		logger.error(Log.FINISH, "Aborting the daemon");
		Runtime.getRuntime().exit(-1);
	}
	
	public static boolean isRunning() {
		return isRunning;
	}

	/**
	 * Run the {@link AgentScanner} a single time. 
	 * Wait for all jobs to complete.
	 * Shut down the worker pool.
	 */
	public void scanOnce() throws InterruptedException {
		assert Thread.currentThread() == daemonThread;
		assert !isRunning;
		isRunning = true;
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
			"runOnce agent=%s threads=%d", agentName, threadCount));
		try {
			scanner.scanUntilDone();
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(-1);
		}
		this.stop();
	}


	/**
	 * Called by {@link WorkerPool} as each job completes.
	 */
	void rescan() {
		logger.info(Log.PROCESS, "rescan");
		daemon.scanner.run();
	}
			
	public void runForever() {
		assert !isRunning;
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
				"run agent=%s interval=%ds", agentName, intervalSeconds));								
		if (logger.isDebugEnabled()) logger.debug(Log.INIT, "Debug is enabled");
		this.start();
		// Daemon now goes into an endless loop
		boolean isInterrupted = false;
		while (!isInterrupted && !workerPool.isTerminated()) {
			logger.debug(Log.PROCESS, "awaiting threadpool termination");
			try {
				workerPool.awaitTermination(300, TimeUnit.SECONDS);
			} catch (InterruptedException e) {			
				logger.info(Log.FINISH, "Interrupt detected");
				isInterrupted = true;
			}
		}
		logger.info(Log.FINISH, "Calling stop");
		this.stop();
	}

	@Override
	public void init(DaemonContext context) throws DaemonInitException, Exception {
		logger.info(Log.INIT, "begin init");		
	}

	// TODO Make this class work with JSCV and PROCRUN
	@Override
	public void start() {
		assert !isRunning;
		isRunning = true;
		Log.setJobContext(agentName);
		logger.debug(Log.INIT, String.format(
			"start agent=%s interval=%ds", agentName, intervalSeconds));								
        this.timer = new Timer("scanner", true);
		ShutdownHook shutdownHook = new ShutdownHook(profile, scanner, workerPool);
		Runtime.getRuntime().addShutdownHook(shutdownHook);		
        timer.schedule(scanner, 0, 1000 * intervalSeconds);
		logger.debug(Log.INIT,"End start");		
	}
	
	@Override
	public void stop() {
		Log.setGlobalContext();
		logger.info(Log.FINISH, "Begin stop");
		int waitSec = profile.getPropertyInt("daemon.shutdown_seconds", 30);
		// shutdownNow will send an interrupt to all threads
		workerPool.shutdown();
		try { 
			workerPool.awaitTermination(waitSec, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) { 
			logger.warn("Shutdown interrupted");
		};
		if (!workerPool.isTerminated()) {
			logger.warn("Some threads failed to terminate");
		}
		logger.info(Log.FINISH, "End stop");
	}
	
	@Override
	public void destroy() {
		
	}
	
}
