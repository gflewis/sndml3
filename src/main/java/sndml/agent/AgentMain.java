package sndml.agent;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.Main;
import sndml.servicenow.RecordKey;
import sndml.util.Log;

public class AgentMain extends Main {

	static final Logger logger = LoggerFactory.getLogger(AgentMain.class);
	
	public static void main (CommandLine cmd) throws Exception {
		
		if (cmd.hasOption(optDaemon)) {
			// Daemon
			requiresApp = true;
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		if (cmd.hasOption(optScan)) {
			// Scan once
			requiresApp = true;
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
		if (cmd.hasOption(optJobRun)) {
			// Run a single job
			requiresApp = true;
			String sys_id = cmd.getOptionValue("jobrun");
			RecordKey jobkey = new RecordKey(sys_id);
			SingleJobRunner jobRunner = new SingleJobRunner(profile, jobkey);
			jobRunner.run();			
		}
		if (cmd.hasOption(optServer)) {
			// Server
			AgentHttpServer server = new AgentHttpServer(profile);
			server.start();
		}
		
	}

	public String getAgentName() {
		return profile.getAgentName();
	}

}
