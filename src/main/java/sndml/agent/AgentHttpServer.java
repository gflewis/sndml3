package sndml.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import sndml.loader.ConnectionProfile;
import sndml.loader.Resources;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.MissingPropertyException;

/**
 * Implements the Java built-in HttpServer class.
 */
public class AgentHttpServer {

	static HttpServer server;
	final int port;
	final int backlog;
	final String agentName;
	final RecordKey agentKey;
	final AgentRequestHandler handler;
	
	private HeartbeatTask heartbeatTask;
	private Timer heartbeatTimer;
	private final int heartbeatInterval;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public AgentHttpServer(Resources resources) throws IOException {
		ConnectionProfile profile = resources.getProfile();
		this.agentName = profile.getAgentName();
		GetRunListRequest getRunList = 
			new GetRunListRequest(profile.newAppSession(), agentName);
		getRunList.execute();
		agentKey = getRunList.getAgentKey();				
		
		String portValue = profile.getProperty("server.port");
		if (portValue == null) 
			throw new MissingPropertyException("server.port not specified");
		this.port = Integer.parseInt(portValue);
		assert this.port != 0;
		this.backlog = Integer.parseInt(profile.getProperty("server.backlog"));
		this.heartbeatInterval = Integer.parseInt(profile.getProperty("server.heartbeat"));
		
		logger.info(Log.INIT, String.format(
				"instantiate port=%d backlog=%d heartbeat=%d", port, backlog, heartbeatInterval));
		server = HttpServer.create(new InetSocketAddress(port), backlog);		
		handler = new AgentRequestHandler(resources);
		server.createContext("/", handler);
		
		// Server executor is used for receiving HTTP requests.
		// It is not used for running jobs.
		// WorkerPool is used for running jobs, not for HTTP executor.
		server.setExecutor(null); // creates a default executor
        this.heartbeatTimer = new Timer("heartbeat", true);
		this.heartbeatTask = new HeartbeatTask(resources);
	}
			
	public void start() {
		logger.info(Log.INIT, String.format("start port=%d", port));		
		server.start();
        heartbeatTimer.schedule(this.heartbeatTask, 0, 1000 * heartbeatInterval);		
	}
	
	public void shutdown() {
		server.stop(5);
		Runtime.getRuntime().exit(0);
	}
	
}
