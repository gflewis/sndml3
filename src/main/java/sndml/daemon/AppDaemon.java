package sndml.daemon;

import java.net.URI;
import java.util.Timer;
import java.util.concurrent.*;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConnectionProfile;
import sndml.servicenow.Log;
import sndml.servicenow.Session;


public class AppDaemon implements Daemon {

	static Logger logger = LoggerFactory.getLogger(AppDaemon.class);
		
	private static ThreadPoolExecutor workerPool;
	private static int intervalSeconds;
	private static int threadCount;
	
	private static AppDaemon daemon;
	private static Thread daemonThread;
	private static String agentName;
	private static ConnectionProfile daemonProfile;
	private static Scanner scanner;
	
	private static volatile boolean isRunning = false;
	
	private Timer timer;
	
	public AppDaemon(ConnectionProfile profile) {
		if (daemon != null) throw new AssertionError("Daemon already instantiated");
        daemon = this;
		daemonProfile = profile;
		daemonThread = Thread.currentThread();
		agentName = profile.getProperty("daemon.agent", "main");
		Log.setJobContext(agentName);
		threadCount = profile.getPropertyInt("daemon.threads", 3);
		intervalSeconds = profile.getPropertyInt("daemon.interval", 60);
		assert threadCount > 0;
		assert intervalSeconds > 0;
		BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<Runnable>();
		workerPool = new ThreadPoolExecutor(threadCount, threadCount, 60, TimeUnit.SECONDS, blockingQueue);
        scanner = new Scanner(profile, workerPool);
	}
	
	public static AppDaemon getDaemon() {
		return daemon;		
	}
	
	public static String getAgentName() {
		return agentName;
	}
	
	/**
	 * Return the Daemon thread, which is the main thread.
	 */
	public static Thread getThread() {
		return daemonThread;
	}
	
	public static ConnectionProfile getConnectionProfile() {
		return daemonProfile;
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
	
	public int scanOnce() throws InterruptedException {
		assert Thread.currentThread() == daemonThread;
		assert !isRunning;
		isRunning = true;
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
			"runOnce agent=%s threads=%d", agentName, threadCount));
		int jobCount = 0;
		try {
			jobCount = scanner.scan();
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(-1);
		}
		if (jobCount > 0) {
			logger.info(Log.INIT, String.format("scanOnce: %d jobs initiated", jobCount));		
			while (workerPool.getActiveCount() > 0) {
				logger.debug(Log.PROCESS, 
					String.format("scanOnce: %d threads running", workerPool.getActiveCount()));
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					logger.error(Log.PROCESS, e.getMessage());
					throw e;
				}
			}
			logger.info(Log.FINISH, "scanOnce: all threads complete");			
		}
		return 0;
	}
		
	public void run() {
		assert !isRunning;
		isRunning = true;
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

	public static void rescan() {
		logger.info(Log.PROCESS, "rescan");
		scanner.run();
	}
	
	// TODO Make this class work with JSCV and PROCRUN
	@Override
	public void start() {
		Log.setJobContext(agentName);
		logger.debug(Log.INIT, String.format(
			"start agent=%s interval=%ds", agentName, intervalSeconds));								
        this.timer = new Timer("scanner", true);
		ShutdownHook shutdownHook = new ShutdownHook(daemonProfile, scanner, workerPool);
		Runtime.getRuntime().addShutdownHook(shutdownHook);		
        timer.schedule(scanner, 0, 1000 * intervalSeconds);
		logger.debug(Log.INIT,"end start");		
	}
	
	@Override
	public void stop() {
		Log.setGlobalContext();
		int waitSec = daemonProfile.getPropertyInt("daemon.shutdown_seconds", 30);
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

	/**
	 * Return the URI of an API
	 */
	static URI getAPI(Session session, String apiName) {
		return getAPI(session, apiName, null);
	}
	
	static URI getAPI(Session session, String apiName, String parameter) {
		ConnectionProfile profile = AppDaemon.getConnectionProfile();
		assert profile != null;
		String defaultScope = "x_108443_sndml";
		String propName = "loader.api." + apiName;
		String apiPath = profile.getProperty(propName);
		if (apiPath == null) apiPath = "api/" + defaultScope + "/" + apiName;
		if (parameter != null) apiPath += "/" + parameter;
		return session.getURI(apiPath);		
	}
	
}
