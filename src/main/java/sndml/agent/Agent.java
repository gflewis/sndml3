package sndml.agent;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.RecordKey;
import sndml.util.Log;

//Q: Why does this class exist? 
//A: Because many of the classes in the package are not public. 

public class Agent extends sndml.loader.Main {

	static final Logger logger = LoggerFactory.getLogger(Agent.class);
	
	public static void main(CommandLine cmd) throws Exception {
		
		if (cmd.hasOption(optScan)) {
			// Scan once
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
		else if (cmd.hasOption(optDaemon)) {
			// Scan forever
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		else if (cmd.hasOption(optJobRun)) {
			// Run a single job
			String sys_id = cmd.getOptionValue("jobrun");
			RecordKey jobkey = new RecordKey(sys_id);
			SingleJobRunner jobRunner = new SingleJobRunner(profile, jobkey);
			jobRunner.run();			
		}
		else if (cmd.hasOption(optServer)) {
			// Run as an HTTP Server
			AgentHttpServer server = new AgentHttpServer(profile);
			server.start();
		}
		else {
			throw new IllegalStateException(); // should never be here
		}
		
	}

	public String getAgentName() {
		return profile.agent.getNotEmpty("agent");
	}

}
