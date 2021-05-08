package sndml.daemon;

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

import sndml.datamart.ConfigParseException;
import sndml.datamart.ConnectionProfile;
import sndml.servicenow.Log;

public class AgentDaemon implements Daemon {
		
	static final Thread daemonThread = Thread.currentThread();
	static AgentDaemon daemon;

	private final ProcessHandle process;
	private final ConnectionProfile profile;
	private final String agentName;
	private final AgentScanner scanner;
	private final int threadCount;	
	private final int intervalSeconds;
	private final WorkerPool workerPool; // null if threadCount < 2
	private final Logger logger;
	
	private static volatile boolean isRunning = false;
	DaemonContext context = null;	
	private Timer timer;
	
	public AgentDaemon(ConnectionProfile profile) throws SQLException {
		if (daemon != null) throw new AssertionError("Daemon already instantiated");
        daemon = this;
        this.process = ProcessHandle.current();
		this.profile = profile;
		this.agentName = profile.getProperty("daemon.agent", "main");
		this.threadCount = profile.getPropertyInt("daemon.threads", 3);
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
		this.logger = LoggerFactory.getLogger(this.getClass());
		assert agentName != null;
		assert agentName != "";
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
		daemon.logger.error(Log.FINISH, "Aborting the daemon");
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
	
	@Override
	public void init(DaemonContext context) throws DaemonInitException {
		logger.debug(Log.INIT, "begin init");
		this.context = context;
		String pidFileName = profile.getProperty("daemon.pidfile");
		if (pidFileName != null) {
			File pidFile = new File(pidFileName);
			long pid = process.pid();
			logger.info(Log.INIT, String.format(
				"pid=%d pidfile=%s", pid, pidFile.getAbsolutePath()));
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
		Log.setJobContext(agentName);		
		logger.debug(Log.FINISH, "Begin stop");
		int waitSec = profile.getPropertyInt("daemon.shutdown_seconds", 30);
		// shutdownNow will send an interrupt to all threads
		workerPool.shutdown();
		isRunning = false;
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
