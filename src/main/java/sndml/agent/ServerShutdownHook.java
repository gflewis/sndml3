package sndml.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.util.Log;

@Deprecated
public class ServerShutdownHook extends Thread {
	
	final AgentHttpServer server;
	
	final Logger logger = LoggerFactory.getLogger(ServerShutdownHook.class);	

	public ServerShutdownHook(AgentHttpServer server) {
		this.server = server;
		this.setName(this.getClass().getSimpleName());		
	}

	@Override
	public void run() {
		Log.setGlobalContext();
//		server.shutdown();
	}

}
