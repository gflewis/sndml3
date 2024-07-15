package sndml.agent;

import java.io.IOException;
import java.net.InetSocketAddress;

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
	final String agentName;
	final RecordKey agentKey;
	final AgentRequestHandler handler;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public AgentHttpServer(Resources resources) throws IOException {
		ConnectionProfile profile = resources.getProfile();
		this.agentName = profile.getAgentName();
		// This scanner is not used. 
		// We are just making sure that we can connect.
		/*
		AgentScanner scanner = new SingleThreadScanner(profile);
		scanner.getAppSession().verifyUser();
		agentKey = scanner.getAgentKey();
		*/
		GetRunListRequest getRunList = 
			new GetRunListRequest(profile.newAppSession(), agentName);
		getRunList.execute();
		agentKey = getRunList.getAgentKey();				
		
		this.port = profile.serverProperties().getInt("port", 0);
		int backlog = profile.serverProperties().getInt("backlog", 0);
		if (port == 0) throw new MissingPropertyException("server.port not specified");
		logger.info(Log.INIT, String.format(
				"instantiate port=%d backlog=%d", port, backlog));
		server = HttpServer.create(new InetSocketAddress(port), backlog);		
		handler = new AgentRequestHandler(resources);
		server.createContext("/", handler);
		
		// Server executor is used for receiving HTTP requests.
		// It is not used for running jobs.
		// WorkerPool is used for running jobs, not for HTTP executor.
		server.setExecutor(null); // creates a default executor
	}
			
	public void start() {
		logger.info(Log.INIT, String.format("start port=%d", port));
		server.start();
	}
	
	public void shutdown() {
		server.stop(5);
		Runtime.getRuntime().exit(0);
	}
	
}
