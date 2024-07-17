package sndml.agent;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.Resources;
import sndml.servicenow.RecordKey;
import sndml.util.Log;

//Q: Why does this class exist? 
//A: Because many of the classes in the package are not public. 

public class AgentMain extends sndml.loader.Main {

	static private final Thread mainThread = Thread.currentThread();	
	static final Logger logger = LoggerFactory.getLogger(AgentMain.class);
	
	public static void main(CommandLine cmd, Resources resources) throws Exception {
		
		assert resources != null;		
		AppSession appSession = resources.getAppSession();
		
		if (cmd.hasOption(optScan)) {
			// Scan once
			AgentDaemon daemon = new AgentDaemon(resources);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
		else if (cmd.hasOption(optDaemon)) {
			// Scan forever
			AgentDaemon daemon = new AgentDaemon(resources);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		else if (cmd.hasOption(optJobRun)) {
			// Run a single job
			String sys_id = cmd.getOptionValue("jobrun");
			RecordKey jobkey = new RecordKey(sys_id);
//			SingleJobRunner jobrunner = new SingleJobRunner(profile, jobkey);			
			AppConfigFactory factory = new AppConfigFactory(appSession);
			AppJobConfig jobconfig = factory.appJobConfig(jobkey);
			AppJobRunner jobrunner = new AppJobRunner(resources, jobconfig);			
			jobrunner.run();			
		}
		else if (cmd.hasOption(optServer)) {
			// Run as an HTTP Server
			AgentHttpServer server = new AgentHttpServer(resources);
			server.start();
		}
		else {
			throw new IllegalStateException(); // should never be here
		}
		
	}

	public String getAgentName() {
		return profile.app.getNotEmpty("agent");
	}

	/**
	 * @return the main thread.
	 */
	public static Thread getThread() {
		return mainThread;
	}
	
}
