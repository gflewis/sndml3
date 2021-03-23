package sndml.daemon;

import java.net.URI;
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
import sndml.servicenow.Session;


public class Daemon implements org.apache.commons.daemon.Daemon {

	static Logger logger = LoggerFactory.getLogger(Daemon.class);
		
	private final ExecutorService workerPool;
	private final int intervalSeconds;
	private final int threadCount;
	
	private static ConnectionProfile daemonProfile;
	private static Thread daemonThread; 
	private static Scanner scanner;
	private static String agentName;
	
	private Timer timer;
	
	public Daemon(ConnectionProfile profile) {
		if (daemonProfile != null) throw new AssertionError("Daemon already instantiated");
		daemonProfile = profile;
		daemonThread = Thread.currentThread();
		agentName = profile.getProperty("daemon.agent", "main");
		Log.setJobContext(agentName);
		threadCount = profile.getPropertyInt("daemon.threads", 3);
		intervalSeconds = profile.getPropertyInt("daemon.interval", 60);
		assert threadCount > 0;
		assert intervalSeconds > 0;
		workerPool = Executors.newFixedThreadPool(threadCount);
        scanner = new Scanner(profile, workerPool);
	}
	
	/**
	 * Return the Daemon thread, which is the main thread.
	 */
	public static Thread getThread() {
		return daemonThread;
	}
	
	public static String agentName() {
		return agentName;
	}
	
	public static ConnectionProfile getConnectionProfile() {
		return daemonProfile;
	}
	
	/**
	 * Return the URI of an API
	 */
	static URI getAPI(Session session, String apiName) {
		return getAPI(session, apiName, null);
	}
	
	static URI getAPI(Session session, String apiName, String parameter) {
		ConnectionProfile profile = Daemon.getConnectionProfile();
		assert profile != null;
		String defaultScope = "x_108443_sndml";
		String propName = "loader.api." + apiName;
		String apiPath = profile.getProperty(propName);
		if (apiPath == null) apiPath = "api/" + defaultScope + "/" + apiName;
		if (!apiPath.endsWith("/")) apiPath += "/";
		if (parameter != null) apiPath += parameter;
		return session.getURI(apiPath);		
	}
	
	public void run() throws Exception {
		Log.setJobContext(agentName);
		if (logger.isDebugEnabled()) logger.debug(Log.INIT, "Debug is enabled");
		start();
		// Daemon now goes into an endless loop
		while (!workerPool.isTerminated()) {
			logger.info(Log.DAEMON, "main awaiting threadpool termination");
			workerPool.awaitTermination(300, TimeUnit.SECONDS);
		}
		stop();
	}

	@Override
	public void init(DaemonContext context) throws DaemonInitException, Exception {
		logger.info(Log.INIT, "begin init");		
	}

	public static void rescan() {
		if (scanner != null) scanner.run();
	}
	
	@Override
	public void start() throws Exception {
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format("agent=%s interval=%ds", agentName, intervalSeconds));								
        this.timer = new Timer("scanner", true);
		ShutdownHook shutdownHook = new ShutdownHook(daemonProfile, scanner, workerPool);
		Runtime.getRuntime().addShutdownHook(shutdownHook);		
        timer.schedule(scanner, 0, 1000 * intervalSeconds);
		logger.debug(Log.INIT,"end start");		
	}
	
	@Override
	public void stop() {
		Log.setJobContext(agentName);
		logger.info(Log.FINISH, "begin stop");
		int waitSec = daemonProfile.getPropertyInt("shutdown_seconds", 30);
		boolean terminated = false;
		// shutdownNow will send an interrupt to all threads
		workerPool.shutdownNow();
		try {
			terminated = workerPool.awaitTermination(waitSec, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			terminated = false;
		}
		if (terminated) {
			logger.info(Log.FINISH, "Shutdown Successful");
		}
		else {
			logger.warn("Some threads failed to terminate");
		}
		logger.info(Log.FINISH, "end stop");		
	}
	
	@Override
	public void destroy() {
		
	}


}
