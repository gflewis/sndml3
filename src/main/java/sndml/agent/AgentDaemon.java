package sndml.agent;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Timer;
import java.util.concurrent.*;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.ConfigParseException;
import sndml.loader.ConnectionProfile;
import sndml.util.Log;

/**
 * A class which runs forever in a loop, periodically scanning the app
 * for new jobs to run. Note that this is a singleton class.
 */
public class AgentDaemon implements Daemon, Runnable {
		
	static private final Thread daemonThread = Thread.currentThread();
	static private AgentDaemon daemon;

	private final ProcessHandle process;
	private final ConnectionProfile profile;
	private final String agentName;
	private final AgentScanner scanner;
	private final int threadCount;	
	private final int intervalSeconds;
	private final String pidFileName;
	private final WorkerPool executor; // null if threadCount < 2
	
	private static volatile boolean isRunning = false;
	DaemonContext context = null;
	
	private Timer timer;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@SuppressWarnings("static-access")
	public AgentDaemon(ConnectionProfile profile) throws SQLException {
		// This is a singleton class, so save me as a static variable
		if (daemon != null) throw new AssertionError("Daemon already instantiated");
        this.daemon = this;
        this.process = ProcessHandle.current();
		this.profile = profile;
		this.agentName = profile.getAgentName();
		this.threadCount = Integer.parseInt(profile.getProperty("daemon.threads"));
		this.intervalSeconds = Integer.parseInt(profile.getProperty("daemon.interval"));
		this.pidFileName = profile.getProperty("daemon.pidfile");
		
		assert intervalSeconds > 0;
		if (threadCount > 1) {
			// TODO Move WorkerPool constructor to ResourceManager
			this.executor = new WorkerPool(threadCount);
			this.scanner = new MultiThreadScanner(profile, executor);
		}
		else {
			this.executor = null;
			this.scanner = new SingleThreadScanner(profile);
		}
		assert agentName != null;
		assert agentName != "";
		Log.setJobContext(agentName);
		logger.info(String.format("instantiate agent=%s pidfile=%s", agentName, pidFileName));
	}
	
	public static AgentDaemon getDaemon() {
		assert daemon != null: "Class not initialized";
		return daemon;
	}
	
	public static String getAgentName() {
		return getDaemon().agentName;
	}
	
	/**
	 * @return the Daemon thread, which is the main thread.
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
		daemon.logger.error(Log.FINISH, "Aborting the daemon");
		Runtime.getRuntime().exit(-1);
	}
	
	public static boolean isRunning() {
		return isRunning;
	}

	@Override
	public void run() {
		try {
			runForever();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public void runForever() 
			throws DaemonInitException, InterruptedException, ConfigParseException, IOException, SQLException {
		init(null);
		if (threadCount > 1) {
			start();
			waitForever();			
		}
		else {
			scanForever();
		}		
	}
	
	private void scanForever() throws ConfigParseException, IOException, SQLException, InterruptedException {
		boolean isInterrupted = false;
		assert isRunning;
		while (!isInterrupted) {
			scanner.scan();
			Thread.sleep(1000 * intervalSeconds);
		}		
	}
	
	private void waitForever() throws InterruptedException {
		assert isRunning;
		final int sleepSeconds = 300;
		while (true) {
			Thread.sleep(1000 * sleepSeconds);
		}
	}
	
	/**
	 * Run the {@link AgentScanner} a single time. 
	 * Wait for all jobs to complete.
	 * Shut down the worker pool.
	 * 
	 * @throws DaemonInitException
	 * @throws InterruptedException
	 */
	public void scanOnce() throws DaemonInitException, InterruptedException {
		assert Thread.currentThread() == daemonThread;
		assert !isRunning;
		init(null);		
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
			"runOnce agent=%s threads=%d", agentName, threadCount));
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
		long pid = process.pid();
		this.context = context;
		if (pidFileName == null) {
			logger.info(Log.INIT, String.format("init pid=%d", pid));			
		}
		else {
			File pidFile = new File(pidFileName);
			logger.info(Log.INIT, String.format(
				"init pid=%d pidfile=%s", pid, pidFile.getAbsolutePath()));
			try {
				PrintWriter pidWriter = new PrintWriter(pidFile);
				pidWriter.println(pid);
				pidWriter.close();
			}
			catch (IOException e) {
				throw new DaemonInitException(
					"Unable to write pidfile: " + pidFileName, e);
			}			
		}
	}

	// TODO Make this class work with JSCV and PROCRUN
	@Override
	public void start() {
		if (isRunning) 
			throw new AssertionError("start already called");
		if (executor == null)
			throw new AssertionError("WorkerPool not created");
		isRunning = true;
		Log.setJobContext(agentName);
		logger.info(Log.INIT, String.format(
			"start agent=%s interval=%ds", agentName, intervalSeconds));								
        this.timer = new Timer(AgentScanner.THREAD_NAME, true);
		ShutdownHook shutdownHook = new ShutdownHook(profile, scanner, executor);
		Runtime.getRuntime().addShutdownHook(shutdownHook);		
        timer.schedule(scanner, 0, 1000 * intervalSeconds);
		logger.debug(Log.INIT,"End start");		
	}
	
	@Override
	public void stop() {
		Log.setJobContext(agentName);		
		logger.debug(Log.FINISH, "Begin stop");
		int waitSec = profile.app.getInt("shutdown_seconds", 30);
		// shutdownNow will send an interrupt to all threads
		executor.shutdown();
		isRunning = false;
		try { 
			executor.awaitTermination(waitSec, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) { 
			logger.warn("Shutdown interrupted");
		};
		if (!executor.isTerminated()) {
			logger.warn("Some threads failed to terminate");
		}
		logger.info(Log.FINISH, "End stop");
	}
	
	@Override
	public void destroy() {		
	}

}
