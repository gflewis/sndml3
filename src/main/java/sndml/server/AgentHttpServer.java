package sndml.server;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import sndml.agent.WorkerPool;
import sndml.loader.ConnectionProfile;
import sndml.util.Log;

public class AgentHttpServer {

	static HttpServer server;
	final int port;
	final AgentRequestHandler handler;
	WorkerPool workerPool = new WorkerPool(null, 10);
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public AgentHttpServer(ConnectionProfile profile) throws IOException {
		this.port = profile.server.getInt("port", 0);
		if (port == 0) throw new AssertionError("server.port not specified");
		int backlog = profile.server.getInt("backlog",  0);
		logger.info(Log.INIT, String.format(
				"instantiate port=%d backlog=%d", port, backlog));
		server = HttpServer.create(new InetSocketAddress(port), backlog);		
		handler = new AgentRequestHandler(profile, workerPool);
		server.createContext("/", handler);
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
