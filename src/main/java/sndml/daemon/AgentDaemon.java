package sndml.daemon;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Timer;
import java.util.concurrent.*;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConfigParseException;
import sndml.datamart.ConnectionProfile;
import sndml.servicenow.Log;

public class AgentDaemon implements Daemon {

	static Logger logger = LoggerFactory.getLogger(AgentDaemon.class);
		
	static final Thread daemonThread = Thread.currentThread();
	static AgentDaemon daemon;

	private final ConnectionProfile profile;
	private final String agentName;
	private final AgentScanner scanner;
	private final int threadCount;	
	private final int intervalSeconds;
	private final WorkerPool workerPool; // null if threadCount < 2
	
	private static volatile boolean isRunning = false;
	
	private Timer timer;
	
	public AgentDaemon(ConnectionProfile profile) throws SQLException {
		if (daemon != null) throw new AssertionError("Daemon already instantiated");
        daemon = this;
		this.profile = profile;
		this.agentName = profile.getProperty("daemon.agent", "main");
		this.threadCount = profile.getPropertyInt("daemon.threads", 0);
		this.intervalSeconds = profile.getPropertyInt("daemon.interval", 60);
		assert intervalSeconds > 0;
		if (threadCount > 1) {
			this.workerPool = new WorkerPool(this, threadCount);
			this.scanner = new MultiThreadScanner(profile, workerPool);
		}
		else {
			this.workerPool = null;
			this.scanner = new SingleThreadScanner(profile);
		}
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
			
	public void runForever() throws InterruptedException, ConfigParseException, IOException, SQLException {
		if (threadCount > 1) {
			start();
			waitForever();			
		}
		else {
			scanForever();
		}
		
	}
	
	public void runForever_old() {
		assert !isRunning;
		Log.setJobContext(agentName);
		if (logger.isDebugEnabled()) logger.debug(Log.INIT, "Debug is enabled");
		this.start();
		// Daemon now goes into an endless loop
		boolean isInterrupted = false;
		final int sleepSeconds = 300;  
		while (!isInterrupted) {
			try {
				Thread.sleep(1000 * sleepSeconds);				
			}
			catch (InterruptedException e) {
				logger.info(Log.FINISH, "Interrupt detected");
				isInterrupted = true;				
			}
		}
		logger.info(Log.FINISH, "Calling stop");
		this.stop();
	}

	public void scanForever() throws ConfigParseException, IOException, SQLException, InterruptedException {
		boolean isInterrupted = false;
		while (!isInterrupted) {
			scanner.scan();
			Thread.sleep(1000 * intervalSeconds);
		}		
	}
	
	public void waitForever() throws InterruptedException {
		final int sleepSeconds = 300;
		while (true) {
			Thread.sleep(1000 * sleepSeconds);
		}
	}
	
	@Override
	public void init(DaemonContext context) throws DaemonInitException, Exception {
		logger.info(Log.INIT, "begin init");		
	}

	// TODO Make this class work with JSCV and PROCRUN
	@Override
	public void start() {
		if (isRunning) 
			throw new AssertionError("start already called");
		if (workerPool == null)
			throw new AssertionError("WorkerPool not created");
		isRunning = true;
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
			"start agent=%s interval=%ds", agentName, intervalSeconds));								
        this.timer = new Timer(AgentScanner.THREAD_NAME, true);
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
