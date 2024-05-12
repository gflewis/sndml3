package sndml.agent;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import sndml.loader.ConnectionProfile;
import sndml.util.Log;

/**
 * Implements the Java built-in HttpServer class.
 */
public class AgentHttpServer {

	static HttpServer server;
	final int port;
	final AgentRequestHandler handler;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public AgentHttpServer(ConnectionProfile profile) throws IOException {
		this.port = profile.server.getInt("port", 0);
		if (port == 0) throw new AssertionError("server.port not specified");
		int backlog = profile.server.getInt("backlog",  0);
		logger.info(Log.INIT, String.format(
				"instantiate port=%d backlog=%d", port, backlog));
		server = HttpServer.create(new InetSocketAddress(port), backlog);		
		handler = new AgentRequestHandler(profile);
		server.createContext("/", handler);
		// Note: WorkerPool is used for running jobs, not for HTTP executor.
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
