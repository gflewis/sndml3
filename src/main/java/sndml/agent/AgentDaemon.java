package sndml.agent;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Timer;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;

import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.loader.Main;
import sndml.loader.Resources;
import sndml.util.Log;

/**
 * A class which runs forever in a loop, periodically scanning the app
 * for new jobs to run. Note that this is a singleton class.
 */
public class AgentDaemon implements Daemon, Runnable {
		
	static private final Thread daemonThread = AgentMain.getThread(); /* Thread.currentThread(); */
	static private AgentDaemon daemon;

	private final Resources resources;
	private final ConnectionProfile profile;
	private final String agentName;
	private final AgentScanner scanner;
	private final int intervalSeconds;
	private final WorkerPool workerPool; // null if threadCount < 2
	private final boolean multiThreaded;
	
	private static volatile boolean isStarted = false;
	DaemonContext context = null;
	
	private Timer timer;
	
	private final Logger logger = Log.getLogger(this.getClass());
	
	@SuppressWarnings("static-access")
	public AgentDaemon(Resources resources) throws SQLException {
		this.resources = resources;
		this.profile = resources.getProfile();
		// This is a singleton class, so save me as a static variable
		if (daemon != null) throw new AssertionError("Daemon already instantiated");
        this.daemon = this;
		this.agentName = profile.getAgentName();
		this.multiThreaded = (profile.getInteger("agent.workers") > 1);
		this.intervalSeconds = Integer.parseInt(profile.getProperty("daemon.interval"));		
		
		assert intervalSeconds > 0;
		if (multiThreaded) {
			this.workerPool = resources.getWorkerPool();
			this.scanner = new MultiThreadScanner(resources, workerPool);
		}
		else {
			this.workerPool = null;
			this.scanner = new SingleThreadScanner(resources);
		}
		assert agentName != null;
		assert agentName != "";
		Log.setJobContext(agentName);
	}
	
	static AgentDaemon getDaemon() {
		assert daemon != null: "Class not initialized";
		return daemon;
	}
	
	static String getAgentName() {
		return getDaemon().agentName;
	}
	
	/**
	 * @return the Daemon thread, which is the main thread.
	 */
	static Thread getThread() {
		return daemonThread;
	}
	
	static ConnectionProfile getConnectionProfile() {
		return getDaemon().resources.getProfile();
	}
	
	/**
	 * This function can be called by any thread to abort the daemon.
	 */
	public static void abort() {
		daemon.logger.error(Log.FINISH, "Aborting the daemon");
		Runtime.getRuntime().exit(-1);
	}
	
	public static boolean isRunning() {
		return isStarted;
	}

	@Override
	public void run() {
		@SuppressWarnings("unused")
		boolean isInterrupted = false;
		try {
			if (multiThreaded) {
				start();
				isInterrupted = waitForever();			
			}
			else {
				isInterrupted = scanForever();
			}
		} catch (Exception e) {
			logger.error(Log.FINISH, e.getMessage(), e);
		}
		logger.info(Log.FINISH, "run shutdown");
		resources.shutdown();
	}
	
	
	private boolean scanForever() throws ConfigParseException, IOException, SQLException {
		assert isStarted : "scanForever: not started";
		boolean isInterrupted = false;
		while (!isInterrupted) {
			scanner.scan();
			isInterrupted = Main.sleep(1000 * intervalSeconds, logger);
		}
		return isInterrupted;
	}
	
	private boolean waitForever() {
		assert isStarted : "waitForever: not started";
		boolean isInterrupted = false;
		final int sleepSeconds = 300;
		while (!isInterrupted) {
			isInterrupted = Main.sleep(1000 * sleepSeconds, logger);
		}
		return isInterrupted;
	}
	
	/**
	 * Run the {@link AgentScanner} a single time. 
	 * Rescan until all jobs to complete.
	 * Shut down the worker pool.
	 * 
	 * @throws DaemonInitException
	 * @throws InterruptedException
	 */
	public void scanOnce() throws DaemonInitException, InterruptedException {
		assert Thread.currentThread() == daemonThread;
		assert !isStarted;
		init(null);		
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
			"runOnce agent=%s", agentName));
		try {
			scanner.scanUntilDone();
		} catch (Exception e) {
			logger.error(Log.ERROR, "scanOnce caught " + e.getClass().getName());
			e.printStackTrace();
			Runtime.getRuntime().exit(-1);
		}
		this.stop();
	}

	@Override
	public void init(DaemonContext context) throws DaemonInitException {
		this.context = context;
	}

	// TODO Make this class work with JSCV and PROCRUN
	@Override
	public void start() {
		if (isStarted) 
			throw new AssertionError("start already called");
		if (workerPool == null)
			throw new AssertionError("WorkerPool not created");
		isStarted = true;
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
			"start agent=%s interval=%ds", agentName, intervalSeconds));								
        this.timer = new Timer(AgentScanner.THREAD_NAME, true);
		ShutdownHook shutdownHook = new ShutdownHook(resources);
		Runtime.getRuntime().addShutdownHook(shutdownHook);		
        timer.schedule(scanner, 0, 1000 * intervalSeconds);
		logger.debug(Log.INIT,"End start");		
	}
	
	@Override
	public void stop() {
		Log.setJobContext(agentName);	
		logger.debug(Log.FINISH, "Begin stop");
		workerPool.shutdown();
		logger.info(Log.FINISH, "End stop");
	}
	
	/*
	@Override
	public void stop() {
		Log.setJobContext(agentName);	
		logger.debug(Log.FINISH, "Begin stop");
		// shutdownNow will send an interrupt to all threads
		executor.shutdown();
		isRunning = false;
		try { 
			executor.awaitTermination(shutdownSeconds, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) { 
			logger.warn("Shutdown interrupted");
		};
		if (!executor.isTerminated()) {
			logger.warn("Some threads failed to terminate");
		}
		logger.info(Log.FINISH, "End stop");
	}
	*/
	
	@Override
	public void destroy() {		
	}

}
