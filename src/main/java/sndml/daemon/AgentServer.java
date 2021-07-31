package sndml.daemon;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import sndml.datamart.ConnectionProfile;
import sndml.servicenow.Log;

public class AgentServer {

	final int port;
	final HttpServer server;
	final AppJobHandler handler;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public AgentServer(ConnectionProfile profile) throws IOException {
		this.port = profile.getPropertyInt("server.port", 0);
		if (port == 0) throw new AssertionError("server.port not specified");
		int backlog = profile.getPropertyInt("server.backlog",  3);
		this.server = HttpServer.create(new InetSocketAddress(port), backlog);
		String context = profile.getProperty("server.context", "/start");
		handler = new AppJobHandler(profile);
		server.createContext(context, handler);
		server.setExecutor(null); // creates a default executor
	}
			
	public void start() {
		logger.info(Log.INIT, String.format("start port=%d", port));
		server.start();
	}
	
}
