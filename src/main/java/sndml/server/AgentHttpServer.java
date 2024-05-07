package sndml.server;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import sndml.loader.ConnectionProfile;
import sndml.util.Log;

// TODO Implement AgentServer
public class AgentHttpServer {

	static HttpServer server;
	final int port;
	final AgentConfigHandler handler;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public AgentHttpServer(ConnectionProfile profile) throws IOException {
		this.port = profile.server.getInt("port", 0);
		if (port == 0) throw new AssertionError("server.port not specified");
		int backlog = profile.server.getInt("backlog",  0);
		server = HttpServer.create(new InetSocketAddress(port), backlog);
		String context = profile.server.getProperty("context", "/start");
		handler = new AgentConfigHandler(profile);
		server.createContext(context, handler);
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
