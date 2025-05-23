package sndml.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;

import org.slf4j.Logger;

import com.sun.net.httpserver.HttpServer;

import sndml.loader.ConnectionProfile;
import sndml.loader.Resources;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.MissingPropertyException;

/**
 * Implements the Java built-in HttpServer class.
 */
public class AgentHttpServer implements Runnable {

	static AgentHttpServer instance;
	static HttpServer server;
	final Resources resources;
	final int port;
	// final int backlog;
	final String agentName;
	final RecordKey agentKey;
	final AgentRequestHandler handler;
	final WorkerPool workerPool;
	
	private HeartbeatTask heartbeatTask;
	private Timer heartbeatTimer;
	private final int heartbeatInterval;
	
	private final Logger logger = Log.getLogger(this.getClass());
		
	public AgentHttpServer(Resources resources) throws IOException {
		// This is a singleton class, so save me as a static variable
		if (instance != null) throw new AssertionError("Server already instantiated");
        instance = this;	
		this.resources = resources;
		ConnectionProfile profile = resources.getProfile();
		this.agentName = profile.getAgentName();
		this.workerPool = resources.getWorkerPool();
		// TODO Not the best way to get agent sys_id
		GetRunListRequest getRunList = 
			new GetRunListRequest(resources.getAppSession(), agentName);
		getRunList.execute();
		agentKey = getRunList.getAgentKey();
		
		String portValue = profile.getProperty("server.port");
		if (portValue == null) 
			throw new MissingPropertyException("server.port not specified");
		this.port = Integer.parseInt(portValue);
		assert this.port != 0;
		// this.backlog = Integer.parseInt(profile.getProperty("server.backlog"));
		this.heartbeatInterval = profile.getInteger("server.heartbeat_seconds");
		
		logger.info(Log.INIT, String.format(
				"agent=%s/%s port=%d heartbeat=%d", 
				agentName, agentKey, port, heartbeatInterval, agentKey));
		server = HttpServer.create(new InetSocketAddress(port), 0);		
		handler = new AgentRequestHandler(resources);
		server.createContext("/", handler);
		
		// Server executor is used for receiving HTTP requests.
		// It is not used for running jobs.
		// WorkerPool is used for running jobs, not for HTTP executor.
		server.setExecutor(null); // creates a default executor
        this.heartbeatTimer = new Timer("heartbeat", true);
		this.heartbeatTask = new HeartbeatTask(resources);
	}
			
	public void run() {
		logger.info(Log.INIT, String.format("start port=%d", port));		
		server.start();
		ShutdownHook shutdownHook = new ShutdownHook(resources);
		Runtime.getRuntime().addShutdownHook(shutdownHook);			
        heartbeatTimer.schedule(this.heartbeatTask, 0, 1000 * heartbeatInterval);		
	}
		
}
